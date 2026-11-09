"""Operator CLI for a running proxy; never connects directly to CodeArts."""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
from pathlib import Path

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["models", "accounts", "checkin", "upload", "chat"])
    parser.add_argument("value", nargs="?")
    parser.add_argument("--account")
    parser.add_argument("--model", default="glm-5.3-flash")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--file", action="append", default=[])
    args = parser.parse_args()
    is_admin = args.command in {"accounts", "checkin"}
    key_name = "CODEARTS_ADMIN_API_KEY" if is_admin else "CODEARTS_CLIENT_API_KEY"
    key_file = os.environ.get(key_name + "_FILE")
    key = Path(key_file).read_text().strip() if key_file else os.environ.get(key_name)
    if not key:
        parser.error(f"set {key_name} or {key_name}_FILE")
    headers = {"Authorization": f"Bearer {key}"}
    if args.account:
        headers["X-CodeArts-Account"] = args.account
    with httpx.Client(
        base_url=args.base_url,
        headers=headers,
        trust_env=False,
        follow_redirects=False,
        timeout=360,
    ) as client:
        if args.command == "models":
            response = client.get("/v1/models")
        elif args.command == "accounts":
            response = client.get("/native/accounts")
        elif args.command == "checkin":
            response = client.post(
                "/admin/keepalive", json={"accounts": [args.account] if args.account else None}
            )
        elif args.command == "upload":
            if not args.value:
                parser.error("upload requires a filename")
            with Path(args.value).open("rb") as f:
                response = client.post(
                    "/v1/files",
                    data={"purpose": "user_data"},
                    files={
                        "file": (
                            Path(args.value).name,
                            f,
                            mimetypes.guess_type(args.value)[0] or "application/octet-stream",
                        )
                    },
                )
        else:
            if not args.value:
                parser.error("chat requires a prompt")
            parts = [{"type": "text", "text": args.value}]
            for name in args.file:
                with Path(name).open("rb") as f:
                    uploaded = client.post(
                        "/v1/files",
                        data={"purpose": "user_data"},
                        files={
                            "file": (
                                Path(name).name,
                                f,
                                mimetypes.guess_type(name)[0] or "application/octet-stream",
                            )
                        },
                    )
                uploaded.raise_for_status()
                parts.append({"type": "file", "file": {"file_id": uploaded.json()["id"]}})
            response = client.post(
                "/v1/chat/completions",
                json={
                    "model": args.model,
                    "messages": [{"role": "user", "content": parts}],
                    "stream": False,
                },
            )
        response.raise_for_status()
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
