"""Shared Hotglue OAuth, without logging rotating credentials."""

import json

import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from hotglue_singer_sdk.helpers._util import utc_now


class BlackbaudAuthenticator(OAuthAuthenticator):
    """Share one authenticator per tap, not a process-wide singleton."""

    def __init__(self, stream, **kwargs):
        super().__init__(stream, **kwargs)
        self.access_token = self._tap.config.get("access_token")
        self.expires_in = self._tap.config.get("expires_in")
        if self.access_token and self.expires_in:
            self.last_refreshed = utc_now()

    @property
    def oauth_request_body(self):
        config = self._tap.config
        return {
            "grant_type": "refresh_token",
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "refresh_token": config["refresh_token"],
        }

    def update_access_token_locally(self):
        # SDK 1.0.51 logs the rotated refresh token in its local-refresh method.
        # Keep managed refresh in the SDK, but perform this fallback without it.
        requested = utc_now()
        response = requests.post(
            self.auth_endpoint, data=self.oauth_request_body, timeout=60
        )
        if response.status_code != 200:
            raise InvalidCredentialsError(
                f"Blackbaud OAuth refresh failed (HTTP {response.status_code})"
            )
        data = response.json()
        self.access_token = data["access_token"]
        self.expires_in = int(requested.timestamp()) + int(data["expires_in"])
        self.last_refreshed = requested
        self._tap._config.update(
            access_token=self.access_token, expires_in=self.expires_in
        )
        if data.get("refresh_token"):
            self._tap._config["refresh_token"] = data["refresh_token"]
        if self._tap.config_file is not None:
            with open(self._tap.config_file, "w") as config_file:
                json.dump(self._tap._config, config_file, indent=2)
