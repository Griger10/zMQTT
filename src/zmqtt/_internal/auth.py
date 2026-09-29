from typing import Protocol


class AuthHandler(Protocol):
    """Pluggable MQTT 5.0 enhanced-authentication mechanism.

    ``initial_data()`` is called once per CONNECT, including every reconnect.
    It is not called for ``reauthenticate()``; the first data of a
    re-authentication is passed as ``reauthenticate(data=...)``.

    ``continue_data()`` is called for every AUTH challenge from the broker,
    both during CONNECT and during re-authentication.

    Any exception raised by a handler method aborts the connection.

    Attributes:
        method: Authentication method name. Must stay constant for the
            lifetime of the client.
    """

    method: str

    async def initial_data(self) -> bytes | None:
        """Return the Authentication Data for the CONNECT packet.

        Must complete within ``mqtt_connect_timeout``.
        """
        ...

    async def continue_data(self, data: bytes | None) -> bytes | None:
        """Return the response to a broker challenge.

        Runs inside the connection's read loop: a slow handler delays processing
        of all incoming packets, so it should not block for long.

        Args:
            data: Authentication Data of the broker's AUTH packet, if any.
        """
        ...
