#!/usr/bin/env python3
"""Exchange a Blackbaud auth code for tokens and update .secrets/config.json.

1. Open the printed AUTH_URL in a browser, log into RE NXT, approve the app.
2. After redirect to hotglue.xyz/callback?code=..., copy the code query param.
3. Run: python scripts/exchange_auth_code.py --config .secrets/config.json --code <CODE>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlencode

import requests

TOKEN_URL = "https://oauth2.sky.blackbaud.com/token"
AUTH_URL = "https://oauth2.sky.blackbaud.com/authorization"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=".secrets/config.json")
    parser.add_argument("--code", help="Authorization code from redirect URL")
    parser.add_argument("--print-url", action="store_true", help="Only print AUTH_URL")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = json.loads(config_path.read_text())
    params = {
        "client_id": config["client_id"],
        "response_type": "code",
        "redirect_uri": config["redirect_uri"],
    }
    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    print(f"AUTH_URL={auth_url}")
    if args.print_url or not args.code:
        if not args.code:
            print("Pass --code <authorization_code> after approving the app.")
        return 0

    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": args.code,
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "redirect_uri": config["redirect_uri"],
        },
        timeout=60,
    )
    print(f"status={resp.status_code}")
    body = resp.json()
    if resp.status_code != 200:
        print(body)
        return 1

    config["access_token"] = body["access_token"]
    if body.get("refresh_token"):
        config["refresh_token"] = body["refresh_token"]
    if body.get("expires_in"):
        config["expires_in"] = body["expires_in"]
    config_path.write_text(json.dumps(config, indent=4) + "\n")
    print(f"Updated {config_path} with fresh tokens.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
