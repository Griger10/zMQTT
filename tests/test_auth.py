import asyncio
from collections import deque

import pytest

from zmqtt import MQTTClient, ReconnectConfig
from zmqtt._internal.packets.auth import Auth
from zmqtt._internal.packets.codec import AnyPacket, encode
from zmqtt._internal.packets.connect import ConnAck, Connect
from zmqtt._internal.packets.disconnect import Disconnect
from zmqtt._internal.packets.properties import AuthProperties, ConnectProperties, DisconnectProperties
from zmqtt._internal.packets.reader import PacketBuffer
from zmqtt._internal.protocol import MQTTProtocol
from zmqtt._internal.transport.base import Transport
from zmqtt.errors import (
    MQTTAuthError,
    MQTTConnectError,
    MQTTDisconnectedError,
    MQTTProtocolError,
    MQTTTimeoutError,
)

from .test_protocol import FakeTransport, _answer_after, _run_read_loop, _stop_task, make_protocol


class FakeAuthHandler:
    def __init__(self, method: str = "TEST", responses: list[bytes | None] | None = None) -> None:
        self.method = method
        self._responses: deque[bytes | None] = deque(responses if responses is not None else [None])
        self.initial_calls = 0
        self.continue_calls: list[bytes | None] = []

    async def initial_data(self) -> bytes | None:
        self.initial_calls += 1
        return self._responses.popleft()

    async def continue_data(self, data: bytes | None) -> bytes | None:
        self.continue_calls.append(data)
        return self._responses.popleft()


def _decode(data: bytes) -> AnyPacket:
    buf = PacketBuffer(version="5.0")
    buf.feed(data)
    (packet,) = list(buf)
    return packet


async def _connected(handler: FakeAuthHandler | None = None) -> tuple[MQTTProtocol, FakeTransport]:
    protocol, transport = make_protocol(version="5.0", auth_handler=handler)
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="5.0"))
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))
    transport.sent.clear()
    return protocol, transport


async def test_connect_single_challenge_round_completes() -> None:
    handler = FakeAuthHandler(method="TEST", responses=[b"resp1"])
    protocol, transport = make_protocol(version="5.0", auth_handler=handler)
    transport.feed(
        encode(
            Auth(reason_code=0x18, properties=AuthProperties(authentication_data=b"challenge1")),
            version="5.0",
        ),
    )
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="5.0"))

    ack = await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))

    assert ack.return_code == 0
    assert handler.continue_calls == [b"challenge1"]
    response = _decode(transport.sent[1])
    assert isinstance(response, Auth)
    assert response.reason_code == 0x18
    assert response.properties is not None
    assert response.properties.authentication_method == "TEST"
    assert response.properties.authentication_data == b"resp1"


async def test_connect_multiple_challenge_rounds_completes() -> None:
    handler = FakeAuthHandler(responses=[b"resp1", b"resp2"])
    protocol, transport = make_protocol(version="5.0", auth_handler=handler)
    transport.feed(
        encode(Auth(reason_code=0x18, properties=AuthProperties(authentication_data=b"c1")), version="5.0"),
    )
    transport.feed(
        encode(Auth(reason_code=0x18, properties=AuthProperties(authentication_data=b"c2")), version="5.0"),
    )
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="5.0"))

    ack = await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))

    assert ack.return_code == 0
    assert handler.continue_calls == [b"c1", b"c2"]
    assert len(transport.sent) == 3


async def test_connect_auth_unexpected_reason_code_raises() -> None:
    handler = FakeAuthHandler()
    protocol, transport = make_protocol(version="5.0", auth_handler=handler)
    transport.feed(encode(Auth(reason_code=0x00), version="5.0"))

    with pytest.raises(MQTTProtocolError):
        await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))


async def test_connect_auth_without_handler_raises() -> None:
    protocol, transport = make_protocol(version="5.0")
    transport.feed(encode(Auth(reason_code=0x18), version="5.0"))

    with pytest.raises(MQTTProtocolError):
        await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))


async def test_connect_refused_after_auth_exchange_raises() -> None:
    handler = FakeAuthHandler(responses=[b"resp1"])
    protocol, transport = make_protocol(version="5.0", auth_handler=handler)
    transport.feed(
        encode(Auth(reason_code=0x18, properties=AuthProperties(authentication_data=b"c1")), version="5.0"),
    )
    transport.feed(encode(ConnAck(session_present=False, return_code=0x87), version="5.0"))

    with pytest.raises(MQTTConnectError) as exc_info:
        await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))

    assert exc_info.value.return_code == 0x87


async def test_successful_connect_stores_negotiated_auth_method() -> None:
    handler = FakeAuthHandler(method="SCRAM-SHA-256")
    protocol, transport = make_protocol(version="5.0", auth_handler=handler)
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="5.0"))

    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))

    assert protocol._state.auth_method == "SCRAM-SHA-256"


async def test_connect_without_handler_leaves_auth_method_unset() -> None:
    protocol, transport = make_protocol(version="5.0")
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="5.0"))

    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))

    assert protocol._state.auth_method is None


async def test_reauthenticate_direct_success() -> None:
    handler = FakeAuthHandler(method="TEST")
    protocol, transport = await _connected(handler)
    read_task = await _run_read_loop(protocol)

    reauth_task = asyncio.create_task(protocol.reauthenticate(b"start"))
    await _answer_after(transport, sent=1, packet=Auth(reason_code=0x00))
    await reauth_task

    assert protocol._state.pending_auth is None
    sent = _decode(transport.sent[0])
    assert isinstance(sent, Auth)
    assert sent.reason_code == 0x19
    assert sent.properties is not None
    assert sent.properties.authentication_method == "TEST"
    assert sent.properties.authentication_data == b"start"

    await _stop_task(read_task)


async def test_reauthenticate_with_challenge_round() -> None:
    handler = FakeAuthHandler(method="TEST", responses=[b"resp1"])
    protocol, transport = await _connected(handler)
    read_task = await _run_read_loop(protocol)

    reauth_task = asyncio.create_task(protocol.reauthenticate())
    await _answer_after(
        transport,
        sent=1,
        packet=Auth(reason_code=0x18, properties=AuthProperties(authentication_data=b"challenge")),
    )
    await _answer_after(transport, sent=2, packet=Auth(reason_code=0x00))
    await reauth_task

    assert handler.continue_calls == [b"challenge"]
    response = _decode(transport.sent[1])
    assert isinstance(response, Auth)
    assert response.reason_code == 0x18
    assert response.properties is not None
    assert response.properties.authentication_data == b"resp1"

    await _stop_task(read_task)


async def test_reauthenticate_without_negotiated_method_raises() -> None:
    protocol, _transport = await _connected()

    with pytest.raises(RuntimeError):
        await protocol.reauthenticate()


async def test_reauthenticate_concurrent_call_raises() -> None:
    handler = FakeAuthHandler()
    protocol, transport = await _connected(handler)
    read_task = await _run_read_loop(protocol)

    first = asyncio.create_task(protocol.reauthenticate())
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError):
        await protocol.reauthenticate()

    await _answer_after(transport, sent=1, packet=Auth(reason_code=0x00))
    await first
    await _stop_task(read_task)


async def test_reauthenticate_timeout_clears_pending() -> None:
    handler = FakeAuthHandler()
    protocol, _transport = await _connected(handler)
    read_task = await _run_read_loop(protocol)

    with pytest.raises(MQTTTimeoutError):
        await protocol.reauthenticate(timeout=0.05)

    assert protocol._state.pending_auth is None

    await _stop_task(read_task)


async def test_auth_without_pending_exchange_raises() -> None:
    handler = FakeAuthHandler()
    protocol, transport = await _connected(handler)
    transport.feed(encode(Auth(reason_code=0x00), version="5.0"))

    with pytest.raises(MQTTProtocolError):
        await asyncio.wait_for(protocol._read_loop(), timeout=1)


async def test_auth_in_v311_session_raises() -> None:
    protocol, transport = make_protocol(version="3.1.1")
    transport.feed(encode(Auth(reason_code=0x00), version="5.0"))

    with pytest.raises(MQTTProtocolError):
        await asyncio.wait_for(protocol._read_loop(), timeout=1)


async def test_auth_unexpected_reason_code_during_reauth_fails_pending() -> None:
    handler = FakeAuthHandler()
    protocol, transport = await _connected(handler)
    run_task = asyncio.create_task(protocol.run())
    await protocol.started_event.wait()

    reauth_task = asyncio.create_task(protocol.reauthenticate())
    await _answer_after(transport, sent=1, packet=Auth(reason_code=0x80))

    with pytest.raises(MQTTProtocolError):
        await run_task

    with pytest.raises(MQTTDisconnectedError):
        await reauth_task


async def test_disconnect_during_reauthenticate_raises_auth_error() -> None:
    handler = FakeAuthHandler()
    protocol, transport = await _connected(handler)
    run_task = asyncio.create_task(protocol.run())
    await protocol.started_event.wait()

    reauth_task = asyncio.create_task(protocol.reauthenticate())
    await _answer_after(
        transport,
        sent=1,
        packet=Disconnect(reason_code=0x87, properties=DisconnectProperties(reason_string="bad creds")),
    )

    with pytest.raises(MQTTDisconnectedError):
        await run_task

    with pytest.raises(MQTTAuthError) as exc_info:
        await reauth_task

    assert exc_info.value.reason_code == 0x87
    assert exc_info.value.reason_string == "bad creds"


async def test_cancel_pending_fails_pending_reauthenticate() -> None:
    protocol, _transport = make_protocol(version="5.0")
    future: asyncio.Future[Auth] = asyncio.get_running_loop().create_future()
    protocol._state.pending_auth = future

    protocol._cancel_pending()

    with pytest.raises(MQTTDisconnectedError):
        await future


class _ClientFakeTransport:
    def __init__(self, feed: bytes | None = None) -> None:
        self.sent: list[bytes] = []
        self.closed = False
        self._rx: deque[bytes | Exception] = deque()
        if feed is not None:
            self._rx.append(feed)

    async def read(self, n: int) -> bytes:  # noqa: ARG002
        while not self._rx:  # noqa: ASYNC110
            await asyncio.sleep(0)
        item = self._rx.popleft()
        if isinstance(item, Exception):
            raise item
        return item

    async def write(self, data: bytes) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        self.closed = True

    @property
    def is_connected(self) -> bool:
        return not self.closed


def test_client_v311_rejects_auth_handler() -> None:
    handler = FakeAuthHandler()

    with pytest.raises(RuntimeError, match=r"auth_handler require MQTT 5\.0"):
        MQTTClient("localhost", version="3.1.1", auth_handler=handler)


async def test_client_connect_embeds_negotiated_method_and_initial_data() -> None:
    handler = FakeAuthHandler(method="TEST", responses=[b"init"])
    connack = encode(ConnAck(session_present=False, return_code=0), version="5.0")
    transport = _ClientFakeTransport(feed=connack)

    async def factory(host: str, port: int, tls: object) -> Transport:  # noqa: ARG001
        return transport

    client = MQTTClient("localhost", version="5.0", auth_handler=handler, transport_factory=factory)

    await client._connect()

    sent = _decode(transport.sent[0])
    assert isinstance(sent, Connect)
    assert sent.properties is not None
    assert isinstance(sent.properties, ConnectProperties)
    assert sent.properties.authentication_method == "TEST"
    assert sent.properties.authentication_data == b"init"
    assert handler.initial_calls == 1


async def test_client_reconnect_calls_initial_data_again() -> None:
    handler = FakeAuthHandler(method="TEST", responses=[b"first", b"second"])
    connack = encode(ConnAck(session_present=False, return_code=0), version="5.0")
    transports = [_ClientFakeTransport(feed=connack), _ClientFakeTransport(feed=connack)]
    made: list[_ClientFakeTransport] = []

    async def factory(host: str, port: int, tls: object) -> Transport:  # noqa: ARG001
        transport = transports[len(made)]
        made.append(transport)
        return transport

    client = MQTTClient(
        "localhost",
        version="5.0",
        auth_handler=handler,
        reconnect=ReconnectConfig(initial_delay=0.0, max_attempts=None),
        transport_factory=factory,
    )

    await client._connect_with_retry()
    await client._connect_with_retry()

    assert handler.initial_calls == 2


async def test_client_reauthenticate_without_connection_raises() -> None:
    handler = FakeAuthHandler()
    client = MQTTClient("localhost", version="5.0", auth_handler=handler)

    with pytest.raises(MQTTDisconnectedError):
        await client.reauthenticate()


async def test_client_reauthenticate_on_v311_raises() -> None:
    client = MQTTClient("localhost", version="3.1.1")

    with pytest.raises(RuntimeError, match=r"AUTH is not allowed in MQTT 3\.1\.1"):
        await client.reauthenticate()
