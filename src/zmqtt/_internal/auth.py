from typing import Protocol, runtime_checkable


@runtime_checkable
class AuthHandler(Protocol):
    method: str

    async def initial_data(self) -> bytes | None: ...

    async def continue_data(self, data: bytes | None) -> bytes | None: ...
