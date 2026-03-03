"""Rillet target sink base client."""

import requests
from hotglue_singer_sdk.target_sdk.client import HotglueSink


class RilletSink(HotglueSink):
    """Base Rillet target sink class."""

    base_url = "https://api.rillet.com"
    endpoint = ""
    api_version = "2"
    _lookup_cache: dict = {}

    LOOKUPS = {
        "accounts": {"endpoint": "/accounts", "collection": "accounts", "key": "name", "value": "code"},
        "subsidiaries": {"endpoint": "/subsidiaries", "collection": "subsidiaries", "key": "trade_name", "value": "id"},
        "fields": {"endpoint": "/fields", "collection": "fields", "key": "name", "value": "FULL_OBJECT"},
    }

    @property
    def auth_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.config.get('api_key')}",
            "Content-Type": "application/json",
            "X-Rillet-API-Version": self.api_version,
        }

    def get_base_url(self) -> str:
        if self.config.get("sandbox", False):
            return "https://sandbox.api.rillet.com"
        return self.base_url

    def request_api(self, http_method, endpoint=None, params=None, request_data=None, headers=None):
        """Make an authenticated request to the Rillet API."""
        url = f"{self.get_base_url()}{endpoint or self.endpoint}"
        merged_headers = {**self.auth_headers, **(headers or {})}

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            json=request_data,
            headers=merged_headers,
        )
        return response
    
    def _refresh_lookup_cache(self, lookup_name: str) -> None:
        """Fetch a resource list from Rillet and build a name→value lookup cache."""
        cfg = self.LOOKUPS[lookup_name]
        response = self.request_api("GET", endpoint=cfg["endpoint"])
        items = response.json().get(cfg["collection"], [])
        if cfg["value"] == "FULL_OBJECT":
            self._lookup_cache[lookup_name] = {
                item[cfg["key"]]: item for item in items
            }
        else:
            self._lookup_cache[lookup_name] = {item[cfg["key"]]: item[cfg["value"]] for item in items}

    def lookup_in_cache(self, lookup_name: str, key: str) -> str | None:
        """Lazy-cached lookup: returns the mapped value for *key*, or None."""
        cache = self._lookup_cache.get(lookup_name, {})
        if key not in cache:
            self._refresh_lookup_cache(lookup_name)
            cache = self._lookup_cache.get(lookup_name, {})
        return cache.get(key)
