from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx


def main() -> int:
    # Loopback only; never send the management key to a configurable remote URL.
    base = "http://127.0.0.1:8080"
    try:
        key_path = os.environ.get("CODEARTS_ADMIN_API_KEY_FILE")
        key = (
            Path(key_path).read_text(encoding="utf-8").strip()
            if key_path
            else os.environ.get("CODEARTS_ADMIN_API_KEY", "")
        )
        with httpx.Client(timeout=5.0, trust_env=False, follow_redirects=False) as client:
            if client.get(base + "/healthz").status_code != 200:
                return 1
            if key:
                try:
                    response = client.post(
                        base + "/admin/keepalive", headers={"X-Admin-Key": key}, json={}
                    )
                    if response.status_code != 202:
                        print("maintenance enqueue rejected; liveness healthy", file=sys.stderr)
                except httpx.HTTPError:
                    print("maintenance enqueue unavailable; liveness healthy", file=sys.stderr)
            return 0
    except (OSError, httpx.HTTPError):
        return 1


if __name__ == "__main__":
    sys.exit(main())
