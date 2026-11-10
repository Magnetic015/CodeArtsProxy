from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

_DATA_IMAGE_RE = re.compile(r"data:image/[^;,]+;base64,[A-Za-z0-9+/=]+")


class OpenAIMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    role: Literal["system", "user", "assistant", "developer"]
    content: Any
    name: StrictStr | None = None

    @field_validator("content")
    @classmethod
    def validate_content(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value
        if not isinstance(value, list):
            raise ValueError("message content must be a string or content-part list")
        for part in value:
            if not isinstance(part, dict):
                raise ValueError("message content parts must be objects")
            kind = part.get("type")
            if kind == "text":
                if set(part) != {"type", "text"} or not isinstance(part["text"], str):
                    raise ValueError("text content parts require only a string text field")
                continue
            if kind == "image_url":
                if set(part) != {"type", "image_url"}:
                    raise ValueError("image_url content parts contain unsupported fields")
                image_url = part["image_url"]
                if not isinstance(image_url, dict) or set(image_url) - {"url", "detail"}:
                    raise ValueError("image_url content parts contain unsupported fields")
                url = image_url.get("url")
                if not isinstance(url, str) or not _DATA_IMAGE_RE.fullmatch(url):
                    raise ValueError("remote image URLs are not supported")
                continue
            if kind in {"file", "image_file", "input_file"}:
                if set(part) != {"type", kind}:
                    raise ValueError(f"{kind} content parts contain unsupported fields")
                ref = part[kind]
                if not isinstance(ref, dict) or set(ref) != {"file_id"}:
                    raise ValueError(f"{kind} content parts require only file_id")
                if not isinstance(ref["file_id"], str) or not ref["file_id"]:
                    raise ValueError(f"{kind} content parts require a file_id")
                continue
            raise ValueError(f"unsupported message content part type: {kind!r}")
        return value


class StreamOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    include_usage: StrictBool = False


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    model: StrictStr = Field(min_length=1, max_length=256)
    messages: list[OpenAIMessage] = Field(min_length=1, max_length=256)
    stream: StrictBool = False
    stream_options: StreamOptions | None = None
    n: StrictInt = Field(default=1, ge=1, le=1)
    max_tokens: StrictInt | None = Field(default=None, gt=0, le=65536)
    max_completion_tokens: StrictInt | None = Field(default=None, gt=0, le=65536)
    temperature: StrictFloat | None = Field(default=None, ge=0, le=2)
    top_p: StrictFloat | None = Field(default=None, ge=0, le=1)
    user: StrictStr | None = None
    account: StrictStr | None = None
    reasoning_effort: Literal["low", "medium", "high", "none", "xhigh", "max"] | None = None
    tools: list[dict[str, Any]] | None = None
    tool_choice: Any = None

    @model_validator(mode="after")
    def reject_unsupported_fields(self) -> ChatCompletionRequest:
        fields = self.model_fields_set
        unsupported = (
            "max_tokens",
            "max_completion_tokens",
            "temperature",
            "top_p",
            "tools",
            "tool_choice",
        )
        for field in unsupported:
            if field in fields:
                raise ValueError(f"{field} is not supported by the CodeArts session protocol")
        return self


class OpenAIErrorBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    message: StrictStr
    type: StrictStr = "invalid_request_error"
    param: StrictStr | None = None
    code: StrictStr | None = None


class OpenAIErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    error: OpenAIErrorBody


class FileObject(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    id: StrictStr
    object: Literal["file"] = "file"
    bytes: StrictInt
    created_at: StrictInt
    filename: StrictStr
    purpose: Literal["user_data", "vision"]
    status: Literal["processed", "error"] = "processed"


class ModelObject(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    id: StrictStr
    object: Literal["model"] = "model"
    created: StrictInt
    owned_by: StrictStr = "codearts"


class ModelList(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    object: Literal["list"] = "list"
    data: list[ModelObject]


class KeepaliveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    accounts: list[StrictStr] | None = None
    force_observe: StrictBool = False
