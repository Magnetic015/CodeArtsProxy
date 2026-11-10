from __future__ import annotations

import asyncio
import base64
import binascii
import re
import time
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass
from typing import Any

import anyio

from .checkin import CheckinService, business_success  # noqa: F401
from .config import AccountManager, Settings
from .errors import OwnershipError, UnsupportedFeature, UpstreamError
from .schemas import ChatCompletionRequest
from .state import StateStore
from .upstream import CodeArtsClient


@dataclass
class Caller:
    api_key: str
    owner_hash: str


@dataclass
class SelectedAccount:
    alias: str
    identity: dict[str, Any]
    models: list[dict[str, Any]]
    fingerprint: str = ""


def safe_filename(name: str) -> str:
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    name = re.sub(r'[\x00-\x1f\x7f";]', "_", name).strip(". ")
    if not name or len(name.encode()) > 255:
        raise UnsupportedFeature("invalid attachment filename")
    return name


class AccountRouter:
    def __init__(
        self,
        settings: Settings,
        accounts: AccountManager,
        upstream: CodeArtsClient,
        state: StateStore,
    ):
        self.settings, self.accounts, self.upstream, self.state = (
            settings,
            accounts,
            upstream,
            state,
        )
        self._semaphores = {
            a: asyncio.Semaphore(settings.max_concurrent_per_account) for a in accounts.aliases
        }
        self._waiting = dict.fromkeys(accounts.aliases, 0)
        self._identities: dict[str, dict[str, Any]] = {}
        self._models: dict[str, list[dict[str, Any]]] = {}
        self._fingerprints: dict[str, str] = {}
        self._verified: dict[str, float] = {}
        self._cursor = 0

    async def refresh_identity(self, alias: str) -> dict[str, Any]:
        if alias not in self.accounts.aliases:
            raise OwnershipError("unknown account alias")
        fp = self.accounts.fingerprint(alias)
        identity = await self.upstream.identity(alias)
        if self.accounts.fingerprint(alias) != fp:
            raise UpstreamError("credentials changed during identity verification")
        if self._fingerprints.get(alias) != fp:
            self._models.pop(alias, None)
        self._identities[alias] = identity
        self._fingerprints[alias] = fp
        self._verified[alias] = time.monotonic()
        self.state.remember_principal(alias, identity)
        return identity

    async def account(
        self, alias: str | None = None, model: str | None = None, preferred: str | None = None
    ) -> SelectedAccount:
        if preferred and alias and preferred != alias:
            raise OwnershipError("attachment/account affinity conflict")
        chosen = preferred or alias
        all_aliases = list(self.accounts.aliases)
        if chosen and chosen not in all_aliases:
            raise OwnershipError("unknown account alias")
        if chosen:
            aliases = [chosen]
        else:
            offset = self._cursor % len(all_aliases)
            aliases = all_aliases[offset:] + all_aliases[:offset]
            aliases.sort(key=lambda a: self._semaphores[a].locked())
        for candidate in aliases:
            try:
                fp = self.accounts.fingerprint(candidate)
                identity = self._identities.get(candidate)
                if (
                    not identity
                    or fp != self._fingerprints.get(candidate)
                    or time.monotonic() - self._verified.get(candidate, 0) > 60
                ):
                    identity = await self.refresh_identity(candidate)
                models = self._models.get(candidate)
                if models is None:
                    models = await self.upstream.models(candidate)
                    self._models[candidate] = models
                if model and not any(
                    m.get("model_id") == model and m.get("enable", True) for m in models
                ):
                    continue
                self._cursor += 1
                return SelectedAccount(candidate, identity, models, fp)
            except (UpstreamError, OSError, ValueError):
                self._identities.pop(candidate, None)
                self._models.pop(candidate, None)
                continue
        raise UpstreamError(
            "no available account supports this request", code="no_eligible_account"
        )

    async def models(self, alias: str | None = None) -> list[dict[str, Any]]:
        return (await self.account(alias=alias)).models

    def assert_current(self, selected: SelectedAccount) -> None:
        if self.accounts.fingerprint(selected.alias) != selected.fingerprint:
            raise UpstreamError(
                "credentials changed; start a new request", code="credentials_changed"
            )

    @asynccontextmanager
    async def slot(self, selected: SelectedAccount):
        alias = selected.alias
        if self._waiting[alias] >= self.settings.max_waiters_per_account:
            raise UpstreamError("account queue is full", status_code=429, code="account_busy")
        self._waiting[alias] += 1
        acquired = False
        try:
            try:
                await asyncio.wait_for(
                    self._semaphores[alias].acquire(), self.settings.queue_timeout
                )
                acquired = True
            except TimeoutError as exc:
                raise UpstreamError(
                    "account is busy", status_code=429, code="account_busy"
                ) from exc
            self.assert_current(selected)
            yield
        finally:
            self._waiting[alias] -= 1
            if acquired:
                self._semaphores[alias].release()


@dataclass
class PreparedContent:
    prompt: str
    attachments: list[dict[str, Any]]
    attachment_accounts: set[str]


def _content_text(content: Any) -> tuple[str, list[dict[str, Any]]]:
    if isinstance(content, str):
        return content, []
    if not isinstance(content, list):
        raise UnsupportedFeature("message content must be text or a content-part list")
    text, attachments = [], []
    for part in content:
        if not isinstance(part, dict):
            raise UnsupportedFeature("invalid content part")
        if part.get("type") == "text" and isinstance(part.get("text"), str):
            text.append(part["text"])
        elif part.get("type") in {"image_url", "image_file", "input_file", "file"}:
            attachments.append(part)
        else:
            raise UnsupportedFeature("unsupported content part")
    return "".join(text), attachments


def _file_id(part: dict) -> str | None:
    kind = part.get("type")
    ref = part.get(kind) if kind in {"image_file", "file"} else part
    if kind == "input_file" and isinstance(part.get("input_file"), dict):
        ref = part["input_file"]
    value = ref.get("file_id") if isinstance(ref, dict) else None
    return value if isinstance(value, str) and value else None


def _image(part: dict, settings: Settings) -> tuple[bytes, str, str]:
    image_url = part.get("image_url")
    url = image_url.get("url") if isinstance(image_url, dict) else image_url
    if not isinstance(url, str) or not url.startswith("data:image/"):
        raise UnsupportedFeature("remote image URLs are not supported; use a data image")
    if len(url) > settings.max_image_bytes * 4 // 3 + 128:
        raise UnsupportedFeature("image exceeds size limit")
    match = re.fullmatch(r"data:(image/(?:png|jpeg|webp|gif));base64,([A-Za-z0-9+/=]+)", url)
    if not match:
        raise UnsupportedFeature("unsupported image MIME or invalid base64 data URL")
    mime = match[1]
    try:
        content = base64.b64decode(match[2], validate=True)
    except binascii.Error as exc:
        raise UnsupportedFeature("invalid base64 image") from exc
    if not content or len(content) > settings.max_image_bytes:
        raise UnsupportedFeature("empty or oversized image")
    validate_image(content, mime)
    suffix = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp", "image/gif": "gif"}[
        mime
    ]
    return content, mime, f"image.{suffix}"


def validate_image(content: bytes, mime: str) -> None:
    matches = {
        "image/png": content.startswith(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg": content.startswith(b"\xff\xd8\xff"),
        "image/gif": content.startswith((b"GIF87a", b"GIF89a")),
        "image/webp": content.startswith(b"RIFF") and content[8:12] == b"WEBP",
    }
    if not matches.get(mime, False):
        raise UnsupportedFeature("image content does not match its declared MIME type")


class ChatService:
    def __init__(
        self, settings: Settings, router: AccountRouter, upstream: CodeArtsClient, state: StateStore
    ):
        self.settings, self.router, self.upstream, self.state = settings, router, upstream, state

    def validate_request(self, request: ChatCompletionRequest, caller: Caller) -> None:
        self._validated_content(request, caller)

    def _validated_content(self, request: ChatCompletionRequest, caller: Caller) -> PreparedContent:
        if request.tools is not None or request.tool_choice is not None:
            raise UnsupportedFeature("remote agent tools are not client-side function calls")
        for name in ("temperature", "top_p", "max_tokens", "max_completion_tokens"):
            if getattr(request, name, None) is not None:
                raise UnsupportedFeature(f"{name} is not exposed by the upstream session protocol")
        if request.reasoning_effort is not None and request.reasoning_effort not in {
            "low",
            "medium",
            "high",
            "none",
            "xhigh",
            "max",
        }:
            raise UnsupportedFeature("unsupported reasoning_effort")
        attachments, aliases, serialized = [], set(), []
        size = 0
        for msg in request.messages:
            text, parts = _content_text(msg.content)
            serialized.append((msg.role, text))
            for part in parts:
                if part.get("type") == "image_url":
                    content, mime, filename = _image(part, self.settings)
                    size += len(content)
                    attachments.append({"content": content, "filename": filename, "mime": mime})
                else:
                    fid = _file_id(part)
                    row = self.state.file(fid or "", caller.owner_hash)
                    if row is None:
                        raise OwnershipError("attachment not found for caller")
                    aliases.add(row["account"])
                    size += row["size"]
                    attachments.append(
                        {
                            "file_id": fid,
                            "filename": row["filename"],
                            "mime": row["mime"],
                            "account": row["account"],
                        }
                    )
                if size > self.settings.max_total_attachment_bytes or len(attachments) > 16:
                    raise UnsupportedFeature("attachment total exceeds request limit")
        if len(aliases) > 1 or (request.account and aliases and request.account not in aliases):
            raise OwnershipError("attachments must belong to the selected account")
        if len(serialized) == 1 and serialized[0][0] == "user":
            prompt = serialized[0][1]
        else:
            # Explicit transcript adaptation, NOT a native model role/system channel.
            prompt = "Conversation context (roles are transcript labels):\n\n" + "\n\n".join(
                f"[{role}]\n{text}" for role, text in serialized
            )
        if not prompt.strip() and not attachments:
            raise UnsupportedFeature("empty chat request")
        return PreparedContent(prompt, attachments, aliases)

    async def prepare(
        self, request: ChatCompletionRequest, caller: Caller
    ) -> tuple[SelectedAccount, PreparedContent]:
        prepared = self._validated_content(request, caller)
        selected = await self.router.account(
            alias=request.account,
            model=request.model,
            preferred=next(iter(prepared.attachment_accounts), None),
        )
        # Identity refresh may invalidate stored file bindings after cookie rotation.
        for item in prepared.attachments:
            if item.get("file_id"):
                if self.state.file(item["file_id"], caller.owner_hash) is None:
                    raise OwnershipError("attachment identity changed; upload it again")
        return selected, prepared

    async def stream(self, request: ChatCompletionRequest, caller: Caller):
        selected, prepared = await self.prepare(request, caller)
        session_id, completed = None, False
        async with self.router.slot(selected):
            try:
                attachments = []
                for item in prepared.attachments:
                    self.router.assert_current(selected)
                    if item.get("file_id"):
                        row = self.state.file(item["file_id"], caller.owner_hash)
                        if row is None:
                            raise OwnershipError("attachment no longer available")
                        aid = row["upstream_attachment_id"]
                    else:
                        result = await self.upstream.upload_attachment(
                            selected.alias, item["content"], item["filename"], item["mime"]
                        )
                        aid = result["attachment_id"]
                    attachments.append(
                        {"attachment_id": aid, "filename": item["filename"], "mime": item["mime"]}
                    )
                self.router.assert_current(selected)
                session_id = await self.upstream.create_session(selected.alias)
                self.state.bind_session(session_id, caller.owner_hash, selected.alias)
                self.router.assert_current(selected)
                body = {"content": prepared.prompt, "model_id": request.model, "repos": []}
                if attachments:
                    body["attachments"] = attachments
                if request.reasoning_effort:
                    body["reasoning_effort"] = request.reasoning_effort
                async with asyncio.timeout(self.settings.upstream_stream_timeout):
                    async with aclosing(
                        self.upstream.message_events(selected.alias, session_id, body)
                    ) as events:
                        async for event in events:
                            if event.event == "done":
                                completed = True
                                self.state.close_session(session_id, caller.owner_hash)
                            yield selected.alias, session_id, event
                            if event.event in {
                                "done",
                                "error",
                                "stopped",
                                "tool_authorization",
                                "question",
                            }:
                                return
            except TimeoutError as exc:
                raise UpstreamError("upstream turn deadline exceeded", code="turn_timeout") from exc
            finally:
                if session_id and not completed:
                    with anyio.move_on_after(8, shield=True):
                        try:
                            self.router.assert_current(selected)
                            await self.upstream.stop_session(selected.alias, session_id)
                            self.state.close_session(session_id, caller.owner_hash)
                        except (UpstreamError, OSError, ValueError):
                            pass

    async def stop_owned(self, caller: Caller, session_id: str) -> None:
        record = self.state.session(session_id, caller.owner_hash)
        if record is None:
            raise OwnershipError("session not found for caller")
        await self.router.refresh_identity(record["account"])
        record = self.state.session(session_id, caller.owner_hash)
        if record is None:
            raise OwnershipError("session identity no longer matches")
        if record["status"] != "closed":
            await self.upstream.stop_session(record["account"], session_id)
            self.state.close_session(session_id, caller.owner_hash)


class FileService:
    def __init__(
        self, settings: Settings, router: AccountRouter, upstream: CodeArtsClient, state: StateStore
    ):
        self.settings, self.router, self.upstream, self.state = settings, router, upstream, state

    async def upload(
        self, caller: Caller, filename: str, mime: str, content: bytes, account: str | None = None
    ) -> dict[str, Any]:
        filename = safe_filename(filename)
        if not content or len(content) > self.settings.max_file_bytes:
            raise UnsupportedFeature("empty or oversized attachment")
        if mime.startswith("image/"):
            if len(content) > self.settings.max_image_bytes:
                raise UnsupportedFeature("image exceeds configured size limit")
            validate_image(content, mime)
        if not re.fullmatch(r"[a-zA-Z0-9.+-]+/[a-zA-Z0-9.+-]+", mime):
            raise UnsupportedFeature("invalid MIME type")
        selected = await self.router.account(alias=account)
        async with self.router.slot(selected):
            result = await self.upstream.upload_attachment(selected.alias, content, filename, mime)
            self.router.assert_current(selected)
            fid = self.state.put_file(
                caller.owner_hash,
                selected.alias,
                str(result["attachment_id"]),
                filename,
                mime,
                len(content),
            )
        return {
            "id": fid,
            "object": "file",
            "filename": filename,
            "mime": mime,
            "bytes": len(content),
            "created_at": int(time.time()),
            "purpose": "user_data",
            "status": "processed",
        }

    def get(self, caller: Caller, file_id: str) -> dict[str, Any]:
        row = self.state.file(file_id, caller.owner_hash)
        if row is None:
            raise OwnershipError("file not found for caller")
        return row
