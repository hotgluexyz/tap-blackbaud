"""Blackbaud read transport and continuation handling."""

import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs, urljoin, urlsplit

import requests
from hotglue_singer_sdk.exceptions import FatalAPIError
from hotglue_singer_sdk.streams import RESTStream

from tap_blackbaud.auth import BlackbaudAuthenticator


class BlackbaudStream(RESTStream):
    """Use SDK extraction and state, with bounded, explicit read failures."""

    url_base = "https://api.sky.blackbaud.com"
    records_jsonpath = "$.value[*]"
    primary_keys = ["id"]
    replication_key = None
    timeout = 60
    paged = True
    include_inactive = None

    @property
    def authenticator(self):
        if not hasattr(self._tap, "_blackbaud_auth"):
            self._tap._blackbaud_auth = BlackbaudAuthenticator(
                self, auth_endpoint="https://oauth2.sky.blackbaud.com/token"
            )
        return self._tap._blackbaud_auth

    @property
    def http_headers(self):
        return {
            **super().http_headers,
            "Accept": "application/json",
            "Bb-Api-Subscription-Key": self.config["subscription_key"],
        }

    def request_decorator(self, func):
        # _request owns the bounded Retry-After-aware retry loop; avoid stacking
        # the SDK's exponential retry loop on top of it.
        return func

    def _request(self, prepared_request, context):
        for attempt in range(5):
            try:
                response = self.requests_session.send(prepared_request, timeout=60)
            except (requests.Timeout, requests.ConnectionError):
                if attempt == 4:
                    raise
                time.sleep(2**attempt + random.random())
                continue
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt < 4:
                    delay = 2**attempt + random.random()
                    if response.headers.get("Retry-After"):
                        retry_after = response.headers["Retry-After"]
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            delay = (
                                parsedate_to_datetime(retry_after)
                                - datetime.now(timezone.utc)
                            ).total_seconds()
                    if delay <= 300:
                        time.sleep(max(0, delay))
                        continue
            if not 200 <= response.status_code < 300:
                raise FatalAPIError(
                    f"Blackbaud HTTP {response.status_code} for {self.name}"
                )
            return response

    def get_next_page_token(self, response, previous_token):
        data = response.json()
        # Blackbaud can return an empty terminal page with the same next_link.
        # Do not follow that stale link, or turn missing `value` into success.
        link = self.next_url(response.url, data.get("next_link"))
        return link if data["value"] else None

    @staticmethod
    def next_url(current_url, link):
        if not link:
            return None
        url = urljoin(current_url, link)
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "api.sky.blackbaud.com"
            or parsed.path != urlsplit(current_url).path
        ):
            raise ValueError("Unexpected Blackbaud pagination URL")
        return url

    def get_url_params(self, context, next_page_token):
        params = {}
        if self.paged:
            params.update(limit=self.config.get("page_size", 5000), offset=0)
        if self.include_inactive is not None:
            params["include_inactive"] = str(self.include_inactive).lower()
        if self.replication_key:
            start = self.get_starting_timestamp(context)
            if start:
                params["last_modified"] = start.isoformat()
        if next_page_token:
            params.update(
                {
                    key: values[0] if len(values) == 1 else values
                    for key, values in parse_qs(
                        urlsplit(next_page_token).query, keep_blank_values=True
                    ).items()
                }
            )
        return params

    def get_json(self, path, params=None):
        """Read a detail/enrichment using the same auth, timeout and retry policy."""
        request = self.build_prepared_request(
            method="GET",
            url=self.url_base + path,
            params=params or {},
            headers=self.http_headers,
        )
        return self._request(request, None).json()

    def get_collection(self, path, params=None):
        """Page a scoped collection without losing its native endpoint schema."""
        seen = set()
        while path:
            data = self.get_json(path, params)
            yield from data["value"]
            if not data["value"]:
                return
            link = self.next_url(self.url_base + path, data.get("next_link"))
            if link in seen:
                raise ValueError("Loop detected in Blackbaud pagination")
            seen.add(link)
            path = link.removeprefix(self.url_base) if link else None
            params = None
