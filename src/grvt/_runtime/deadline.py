"""One call's absolute deadline and native cancellation boundary."""

import asyncio


class CallDeadline:
    def __init__(self, timeout: float):
        self.expires = asyncio.get_running_loop().time() + timeout
        self._task = asyncio.current_task()
        self._initial_cancellations = self._task.cancelling() if self._task is not None else 0

    def check(self) -> None:
        # A hook can catch CancelledError while the caller's cancellation remains pending.
        if self._task is not None and self._task.cancelling() > self._initial_cancellations:
            raise asyncio.CancelledError
        if asyncio.get_running_loop().time() >= self.expires:
            raise TimeoutError("call deadline exceeded before sending")
