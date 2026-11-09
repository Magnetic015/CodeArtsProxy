from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import AccountManager, Settings
from .service import AccountRouter, ChatService, CheckinService, FileService
from .state import StateStore
from .upstream import CodeArtsClient


@dataclass
class Runtime:
    settings: Settings
    accounts: AccountManager
    state: StateStore
    upstream: CodeArtsClient
    router: AccountRouter
    chat: ChatService
    files: FileService
    checkin: CheckinService
    maintenance: Any = None

    async def close(self) -> None:
        if self.maintenance is not None:
            await self.maintenance.close()
        await self.upstream.close()
        self.state.close()


def build_runtime(settings: Settings) -> Runtime:
    accounts = AccountManager(settings)
    state = StateStore(settings.state_db)
    upstream = CodeArtsClient(settings, accounts)
    router = AccountRouter(settings, accounts, upstream, state)
    return Runtime(
        settings=settings,
        accounts=accounts,
        state=state,
        upstream=upstream,
        router=router,
        chat=ChatService(settings, router, upstream, state),
        files=FileService(settings, router, upstream, state),
        checkin=CheckinService(settings, router, upstream, state),
    )
