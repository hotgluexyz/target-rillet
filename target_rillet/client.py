"""Rillet target sink base client."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import random
import tempfile
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional
from uuid import NAMESPACE_URL, uuid5

import requests
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.exceptions import RetriableAPIError
from hotglue_singer_sdk.plugin_base import PluginBase
from hotglue_singer_sdk.target_sdk.client import HotglueSink

from target_rillet.attachment_helpers import resolve_attachment_filename_and_type


class RilletSink(HotglueSink):
    """Base Rillet target sink class."""

    base_url = "https://api.rillet.com"
    endpoint = ""
    allows_upserts = True

    @property
    def api_version(self) -> str:
        return self.config.get("api_version", "2")

    LOOKUPS = {
        "accounts": {"endpoint": "/accounts", "collection": "accounts", "key": "name", "value": "code", "api_version": "4"},
        "subsidiaries": {"endpoint": "/subsidiaries", "collection": "subsidiaries", "key": "trade_name", "value": "id"},
        "fields": {"endpoint": "/fields", "collection": "fields", "key": "name", "value": "FULL_OBJECT"},
        "vendors": {"endpoint": "/vendors", "collection": "vendors", "value": "OBJECT_LIST"},
        # API v4 required so account_code is present on bank accounts
        "bank-accounts": {"endpoint": "/bank-accounts", "collection": "accounts", "key": "account_code", "value": "id", "api_version": "4"},
        "tax_rates": {"endpoint": "/tax-rates", "collection": "tax_rates", "key": "code", "value": "FULL_OBJECT"},
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
        if not hasattr(target, "_rillet_request_lock"):
            target._rillet_request_lock = threading.Lock()
            target._rillet_last_request = None

    def process_record(self, record: dict, context: dict) -> None:
        # SDK 1.0.51 removes externalId both before mapping and before upsert.
        # Keep it in per-record context, not mutable sink state or the HTTP body.
        context = dict(context)
        context["_rillet_external_id"] = record.get("externalId") or record.get("externalid")
        return super().process_record(record.copy(), context)

    def _pace_request(self) -> None:
        # Shared by all sinks in this target; leave headroom below 60/minute.
        with self._target._rillet_request_lock:
            last = self._target._rillet_last_request
            if last is not None:
                time.sleep(max(0, 1.05 - (time.monotonic() - last)))
            self._target._rillet_last_request = time.monotonic()

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

    def request_api(self, http_method, endpoint=None, params=None, request_data=None, headers=None, create_deadline=None):
        """Make an authenticated request to the Rillet API."""
        url = f"{self.get_base_url()}{endpoint or self.endpoint}"
        merged_headers = {**self.auth_headers, **(headers or {})}

        retry_safe = http_method.upper() in {"GET", "PUT", "DELETE"} or bool(
            merged_headers.get("Idempotency-Key")
        )
        for attempt in range(4):
            self._pace_request()
            if create_deadline is not None and time.time() >= create_deadline:
                raise ValueError("Rillet create intent reached its 24h retry deadline; reconcile remotely")
            try:
                response = requests.request(
                    method=http_method,
                    url=url,
                    params=params,
                    json=request_data,
                    headers=merged_headers,
                    timeout=(10, 60),
                )
            except (requests.Timeout, requests.ConnectionError):
                if not retry_safe or attempt == 3:
                    raise
                delay = 2 ** attempt + random.uniform(0, 1)
            else:
                if not (response.status_code == 429 or 500 <= response.status_code < 600):
                    self.validate_response(response)
                    return response
                if not retry_safe or attempt == 3:
                    self.validate_response(response)
                delay = 2 ** attempt + random.uniform(0, 1)
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        try:
                            delay = max(delay, (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds())
                        except (ValueError, TypeError):
                            pass
                # Do not retry earlier than requested when the wait is excessive.
                if delay > 120:
                    self.validate_response(response)
            time.sleep(delay)

    def get_error_message(self, response: requests.Response) -> str:
        try:
            json_data = response.json()
            if "violations" in json_data and isinstance(json_data["violations"], list):
                messages = [item.get("field") + ": " + item.get("message") for item in json_data["violations"]]
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
            if "Expense is missing an exchange rate" in response.text and self.name == "Bills":
                raise RetriableAPIError(self.get_error_message(response))
            self.logger.error("Invalid payload. Body sent: %s", response.request.body)
            raise InvalidPayloadError(self.get_error_message(response))
        super().validate_response(response)
    
    def get_paginated_response(self, endpoint: str, cfg: str = None) -> list:
        """Fetch a paginated response from Rillet."""
        all_items = []
        headers = None
        if cfg.get("api_version"):
            headers = {"X-Rillet-API-Version": cfg["api_version"]}
        next_cursor = None
        while True:
            params = {"cursor": next_cursor, "limit": 100} if next_cursor else {"limit": 100}
            response = self.request_api("GET", endpoint=endpoint, headers=headers, params=params)
            items = response.json().get(cfg["collection"], [])
            all_items.extend(items)
            next_cursor = response.json().get("pagination", {}).get("next_cursor")
            if not next_cursor:
                break
        return all_items
    
    def _refresh_lookup_cache(self, lookup_name: str) -> None:
        """Fetch a resource list from Rillet and build a name→value lookup cache."""
        cfg = self.LOOKUPS[lookup_name]
        items = self.get_paginated_response(cfg["endpoint"], cfg)
        if cfg["value"] == "FULL_OBJECT":
            self._lookup_cache[lookup_name] = {
                item[cfg["key"]]: item for item in items
            }
        elif cfg["value"] == "OBJECT_LIST":
            self._lookup_cache[lookup_name] = items
        else:
            if lookup_name == "bank-accounts":
                # Don't include bank accounts not mapped to a GL account
                items = [item for item in items if item.get(cfg["key"]) is not None]
            self._lookup_cache[lookup_name] = {item[cfg["key"]]: item[cfg["value"]] for item in items}
            if lookup_name == "accounts":
                self._lookup_cache[f"{lookup_name}_by_id"] = {item["id"]: item[cfg["value"]] for item in items}
                # also store all accounts of type expense that are tied to a prepaid account
                self._lookup_cache["accounts_with_prepaid_account"] = {item["code"] for item in items if item.get("prepaid_account_code")}

    def lookup_in_cache(self, lookup_name: str, key: str) -> str | None:
        """Lazy-cached lookup: returns the mapped value for *key*, or None."""
        if lookup_name not in self._lookup_cache:
            self._refresh_lookup_cache(lookup_name)
        return self._lookup_cache.get(lookup_name, {}).get(key)

    def update_lookup_cache(self, lookup_name: str, key: str, value: str) -> None:
        """Add or update a single entry in a lookup cache."""
        if lookup_name not in self._lookup_cache:
            self._lookup_cache[lookup_name] = {}
        self._lookup_cache[lookup_name][key] = value
    
    def update_lookup_cache_object_list(self, lookup_name: str, record: dict) -> None:
        """Replace a cached object with the same id, or append when the id is new."""
        if lookup_name not in self._lookup_cache:
            self._lookup_cache[lookup_name] = []
        cache = self._lookup_cache[lookup_name]
        record_id = record.get("id")
        if record_id:
            for index, item in enumerate(cache):
                if item.get("id") == record_id:
                    cache[index] = record
                    return
        cache.append(record)

    def lookup_in_cache_by_id(self, lookup_name: str, id: str) -> str | None:
        """Lazy-cached lookup: returns the mapped value for *id*, or None."""
        if lookup_name not in self._lookup_cache:
            self._refresh_lookup_cache(lookup_name)
        return self._lookup_cache.get(f"{lookup_name}_by_id", {}).get(id)
    
    def lookup_in_cache_object_list(self, lookup_name: str, key: str, value: str) -> List[dict] | None:
        """Lazy-cached lookup: returns the full object for *name*, or None."""
        if lookup_name not in self._lookup_cache:
            self._refresh_lookup_cache(lookup_name)
        matches = [item for item in self._lookup_cache.get(lookup_name, []) if item[key] == value]
        return matches

    def upsert_record(self, record: dict, context: dict):
        """Create or update a journal entry in Rillet."""
        record = record.copy()
        external_id = record.pop("externalId", None) or record.pop("externalid", None) or context.get("_rillet_external_id")
        state_updates = dict()
        method = "POST"
        endpoint = self.endpoint

        if record.get("id") and self.allows_upserts:
            id = record.pop("id")
            state_updates["is_updated"] = True
            method = "PUT"
            endpoint = f"{self.endpoint}/{id}"
        
        if record.get("id") and not self.allows_upserts:
            self.logger.warning(f"Record {record.get('id')} for {self.name} sink already exists in Rillet and upserts are not allowed. Skipping...")
            return record.get("id"), True, {"existing": True}

        headers = {}
        if method == "POST" and external_id not in (None, ""):
            scope = f"{self.get_base_url()}:{self.name}:{external_id}"
            headers["Idempotency-Key"] = str(uuid5(NAMESPACE_URL, scope))
            return self._create_with_intent(endpoint, record, headers)
        response = self.request_api(method, endpoint=endpoint, request_data=record, headers=headers)
        return response.json().get("id"), True, state_updates

    def _create_with_intent(self, endpoint: str, record: dict, headers: dict):
        """Persist before POST and immediately after confirmation, before SDK state.

        Uses the sibling Spendesk target's atomic JSON journal pattern, with a
        process lock and directory fsync. SNAPSHOT_DIR must survive job replay.
        """
        snapshot_dir = os.environ.get("SNAPSHOT_DIR")
        if not snapshot_dir:
            raise ValueError("SNAPSHOT_DIR is required for durable Rillet create intents")
        directory = Path(snapshot_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "rillet-creates.json"
        key = headers["Idempotency-Key"]
        payload_hash = hashlib.sha256(
            json.dumps([endpoint, record], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        with (directory / "rillet-creates.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            journal = json.loads(path.read_text()) if path.exists() else {}
            intent = journal.get(key)
            if intent:
                if intent["payload_hash"] != payload_hash:
                    raise ValueError("Rillet create payload changed for the same externalId; reconcile the create intent before retrying")
                if intent.get("id"):
                    return intent["id"], True, {"existing": True}
                if not 0 <= time.time() - intent["created_at"] < 24 * 60 * 60:
                    raise ValueError("Unresolved Rillet create intent is outside the 24h idempotency window; reconcile remotely before retrying")
            else:
                intent = {"payload_hash": payload_hash, "created_at": time.time()}
                journal[key] = intent
                self._write_create_journal(path, journal)
            response = self.request_api(
                "POST", endpoint=endpoint, request_data=record, headers=headers,
                create_deadline=intent["created_at"] + 24 * 60 * 60,
            )
            record_id = response.json().get("id")
            if not record_id:
                raise ValueError("Rillet create response has no id; intent remains unresolved")
            intent["id"] = record_id
            self._write_create_journal(path, journal)
            return record_id, True, {}

    @staticmethod
    def _write_create_journal(path: Path, journal: dict) -> None:
        # Fail closed on corrupt/unwritable journals; never discard unknown writes.
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as handle:
                temporary = Path(handle.name)
                json.dump(journal, handle)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(path)
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def upsert_with_attachments(self, record: dict, context: dict):
        """Retain the created document ID even when receipt upload fails."""
        record = record.copy()
        attachments = record.pop("attachments", [])
        record_id, success, updates = result = RilletSink.upsert_record(self, record, context)
        try:
            if record_id and success:
                for index, attachment in enumerate(attachments):
                    self.post_attachment(record_id, attachment, index)
        except Exception as error:
            updates.pop("existing", None)
            return record_id, False, {**updates, "error": f"Attachment upload failed: {error}", "document_id": record_id}
        return result

    def _resolve_subsidiary(self, record: dict) -> str:
        """Resolve subsidiary ID from direct ID or cached name lookup."""
        if record.get("subsidiaryId"):
            return record["subsidiaryId"]
        if record.get("subsidiaryName"):
            sub_id = self.lookup_in_cache("subsidiaries", record["subsidiaryName"])
            if sub_id:
                return sub_id
            raise ValueError(f"Subsidiary name {record['subsidiaryName']} not found in Rillet")
    
    def _resolve_vendor(self, record: dict, full_object: bool = False) -> str:
        """Resolve vendor ID from direct ID if it exists, otherwise resolve by name."""
        vendor_id = record.get("id") if self.name == "vendors" else record.get("vendorId") or record.get("vendor_id")
        vendor_name = record.get("name") if self.name == "vendors" else record.get("vendorName")
        if vendor_id:
            existing_vendor = self.lookup_in_cache_object_list("vendors", "id", vendor_id)
            if existing_vendor:
                return existing_vendor[0] if full_object else vendor_id
            self.logger.warning(f"Vendor id {vendor_id} not found in Rillet. Trying to resolve by name...")
        if vendor_name:
            existing_vendor = self.lookup_in_cache_object_list("vendors", "name", vendor_name)
            if len(existing_vendor) == 0:
                # only fail if not a vendor stream, for vendors we want to create a new vendor if it doesn't exist
                if self.name == "vendors":
                    return None
                raise ValueError(f"Vendor name {vendor_name} not found in Rillet")
            if len(existing_vendor) > 1:
                raise ValueError(f"Multiple vendors found for name '{vendor_name}'.")
            return existing_vendor[0] if full_object else existing_vendor[0]["id"]
        # only fail if not a vendor stream, for vendors we want to create a new vendor if it doesn't exist
        if self.name != "vendors":
            raise ValueError(f"One of vendorId or vendorName is required for record {record}")
    
    def _resolve_account(self, record: dict) -> str:
        """Resolve account code from number or cached name lookup."""
        if record.get("accountNumber"):
            return record["accountNumber"]
        if record.get("accountId"):
            account_code = self.lookup_in_cache_by_id("accounts", record["accountId"])
            if account_code:
                return account_code
            raise ValueError(f"Account id {record['accountId']} not found in Rillet")
        if record.get("accountName"):
            account_code = self.lookup_in_cache("accounts", record["accountName"])
            if account_code:
                return account_code
            raise ValueError(f"Account name {record['accountName']} not found in Rillet")
        raise ValueError(f"One of accountNumber, accountId or accountName is required for record {record}")
    

    def _resolve_tax_rate(self, record: dict) -> str:
        """Resolve account code from number or cached name lookup."""
        if record.get("taxCode"):
            tax = self.lookup_in_cache("tax_rates", record.get("taxCode"))
            if not tax:
                raise ValueError(f"Tax code {record.get('taxCode')} not found in Rillet")
            tax_rate = {
                "percentage": tax.get("percentage"),
                "country": tax.get("country"),
                "description": tax.get("description"),
                "type": "SALES_TAX" if tax.get("country") == "US" else "VAT", # there is no type field in the taxes, but Bills requires the tax rate type, for now most reliable way seems to be based in country "Nearly every country outside the US uses a multi-stage VAT/GST framework"
            }
            return tax_rate    

    def _resolve_custom_fields(self, custom_fields: list[dict]) -> list[dict]:
        """Resolve custom field names and values to their corresponding Rillet IDs via cache lookups."""
        fields = []
        for custom_field in custom_fields:
            if not custom_field.get("name") or not custom_field.get("value"):
                self.logger.warning(f"Custom field {custom_field} is missing name or value. Skipping...")
                continue

            field = self.lookup_in_cache("fields", custom_field["name"])
            if not field:
                self.logger.warning(f"Field name {custom_field['name']} not found in Rillet. Skipping...")
                continue

            field_value = next((value for value in field["values"] if value["name"] == custom_field["value"]), None)
            if not field_value:
                self.logger.warning(f"Field value {custom_field['value']} for field {custom_field['name']} not found in Rillet. Skipping...")
                continue

            fields.append({
                "field_id": field["id"],
                "field_value_id": field_value["id"],
            })
        return fields

    def get_attachment_name(self, record_id: str, attachment: dict, index: int) -> str:
        if attachment.get("name"):
            return f"{attachment['name']}"
        if attachment.get("id"):
            return f"{attachment['id']}"
        return f"{record_id}_{index}"

    def post_attachment(self, record_id: str, attachment: dict, index: int) -> None:
        """Journal receipt identity and outcome; never replay an unknown upload."""
        attachment_url = attachment.get("url")
        if not attachment_url:
            raise ValueError("Attachment URL is required")

        snapshot_dir = os.environ.get("SNAPSHOT_DIR")
        if not snapshot_dir:
            raise ValueError("SNAPSHOT_DIR is required for durable receipt intents")
        directory = Path(snapshot_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "rillet-creates.json"
        identity = ["id", str(attachment["id"])] if attachment.get("id") else ["url", attachment_url]
        key = hashlib.sha256(json.dumps(
            [self.get_base_url(), self.endpoint, str(record_id), identity]
        ).encode()).hexdigest()
        with (directory / "rillet-creates.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            journal = json.loads(path.read_text()) if path.exists() else {}
            receipts = journal.setdefault("_receipts", {})
            intent = receipts.get(key)
            if intent:
                if intent.get("status") == "confirmed":
                    return
                raise ValueError("Unresolved receipt upload intent; reconcile the receipt remotely before retrying")

            # Download failures cannot have uploaded anything and remain retryable.
            resp = requests.get(attachment_url, timeout=(10, 60))
            resp.raise_for_status()
            content = resp.content
            content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()

            att_name = self.get_attachment_name(record_id, attachment, index)
            filename, content_type = resolve_attachment_filename_and_type(
                att_name, content, content_type
            )

            files = {"file": (filename, content, content_type)}
            multipart_headers = {
                "Authorization": f"Bearer {self.config.get('api_key')}",
                "X-Rillet-API-Version": self.api_version,
            }
            receipts[key] = {"status": "pending", "created_at": time.time()}
            self._write_create_journal(path, journal)
            self._pace_request()
            response = requests.post(
                f"{self.get_base_url()}{self.endpoint}/{record_id}",
                files=files,
                headers=multipart_headers,
                timeout=(10, 60),
            )
            response.raise_for_status()
            self.validate_response(response)
            if not 200 <= response.status_code < 300:
                raise ValueError("Receipt upload did not return a successful HTTP status")
            receipts[key]["status"] = "confirmed"
            self._write_create_journal(path, journal)

    def process_prepaid_lines(self, lines: dict) -> list[dict]:
        """Process prepaid lines for a record."""
        has_service_period = False
        has_item_with_prepaid_account = False

        if "accounts_with_prepaid_account" not in self._lookup_cache:
            self._refresh_lookup_cache("accounts")

        for line in lines:
            if line.get("service_period"):
                has_service_period = True
                if line.get("account_code") in self._lookup_cache["accounts_with_prepaid_account"]:
                    has_item_with_prepaid_account = True
                else:
                    line.pop("service_period", None)

        if has_service_period and not has_item_with_prepaid_account:
            raise ValueError(
                "A service period is present on at least one line item, but none of the lines use a prepaid account."
            )
       
        return lines
