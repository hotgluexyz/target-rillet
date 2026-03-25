"""Rillet target sink base client."""

from __future__ import annotations

from typing import Dict, List, Optional

import requests
from hotglue_singer_sdk.plugin_base import PluginBase
from hotglue_singer_sdk.target_sdk.client import HotglueSink
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError


class RilletSink(HotglueSink):
    """Base Rillet target sink class."""

    base_url = "https://api.rillet.com"
    endpoint = ""
    api_version = "2"

    LOOKUPS = {
        "accounts": {"endpoint": "/accounts", "collection": "accounts", "key": "name", "value": "code"},
        "subsidiaries": {"endpoint": "/subsidiaries", "collection": "subsidiaries", "key": "trade_name", "value": "id"},
        "fields": {"endpoint": "/fields", "collection": "fields", "key": "name", "value": "FULL_OBJECT"},
    }

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        self._lookup_cache: dict = {}
        super().__init__(target, stream_name, schema, key_properties)

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
        self.validate_response(response)
        return response

    def get_error_message(self, response: requests.Response) -> str:
        try:
            json_data = response.json()
            if "violations" in json_data and type(json_data["violations"]) == list:
                messages = [
                    f"{item.get('field') or ''}: {item.get('message') or ''}" for item in json_data["violations"]
                ]
                error_message = "\n".join(messages)
            else:
                error_message = json_data["message"]
        except Exception:
            error_message = response.text
        return error_message
    
    def validate_response(self, response: requests.Response) -> None:
        if response.status_code in [401, 403]:
            raise InvalidCredentialsError(self.get_error_message(response))
        elif response.status_code == 400:
            self.logger.error("Invalid payload. Body sent: %s", response.request.body)
            raise InvalidPayloadError(self.get_error_message(response))
        super().validate_response(response)
    
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
        if lookup_name not in self._lookup_cache:
            self._refresh_lookup_cache(lookup_name)
        return self._lookup_cache.get(lookup_name, {}).get(key)
