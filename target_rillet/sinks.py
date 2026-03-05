"""Rillet target sink class, which handles writing streams."""

from __future__ import annotations

from target_rillet.client import RilletSink

class JournalsSink(RilletSink):
    """Rillet target sink for posting journal entries."""

    name = "JournalEntries"
    endpoint = "/journal-entries"
    
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

    def _resolve_name(self, record: dict) -> str:
        """Extract the journal entry name."""
        name = (
            record.get("journalEntryNumber")
            or record.get("number")
            or record.get("description")
        )
        if not name:
            raise ValueError("Journal entry number, number, or description is required")

        return name

    def _classify_side_and_amount(self, item: dict) -> tuple[str, str]:
        """Determine debit/credit side and amount."""
        debit = item.get("debitAmount")
        credit = item.get("creditAmount")

        if debit and float(debit) > 0:
            return "DEBIT", debit
        if credit and float(credit) > 0:
            return "CREDIT", credit
        raise ValueError(f"One of debitAmount or creditAmount is required for line item {item}")

    def _resolve_account(self, item: dict) -> str:
        """Resolve account code from number or cached name lookup."""
        if item.get("accountNumber"):
            return item["accountNumber"]
        if item.get("accountName"):
            account_code = self.lookup_in_cache("accounts", item["accountName"])
            if account_code:
                return account_code
            raise ValueError(f"Account name {item['accountName']} not found in Rillet")
        raise ValueError(f"One of accountNumber or accountName is required for line item {item}")

    def _build_line_item(self, item: dict, currency: str) -> dict:
        """Build a single Rillet line-item payload."""
        side, raw_amount = self._classify_side_and_amount(item)

        account_code = self._resolve_account(item)

        line_item = {
            "amount": {
                "amount": str(raw_amount),
                "currency": currency,
            },
            "account_code": account_code,
            "side": side,
        }

        if item.get("description"):
            line_item["description"] = item["description"]

        if item.get("customFields"):
            line_item["fields"] = self._resolve_custom_fields(item["customFields"])

        return line_item

    def _resolve_subsidiary(self, record: dict) -> str:
        """Resolve subsidiary ID from direct ID or cached name lookup."""
        if record.get("subsidiaryId"):
            return record["subsidiaryId"]
        if record.get("subsidiaryName"):
            sub_id = self.lookup_in_cache("subsidiaries", record["subsidiaryName"])
            if sub_id:
                return sub_id
            raise ValueError(f"Subsidiary name {record['subsidiaryName']} not found in Rillet")
        raise ValueError(f"One of subsidiaryId or subsidiaryName is required for record {record}")

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Map a unified JournalEntry record to the Rillet API payload."""
        payload = {}
        if record.get("id"):
            payload["id"] = record["id"]

        payload["name"] = self._resolve_name(record)

        currency = record.get("currency", "USD")
        payload["currency"] = currency
        payload["date"] = record.get("transactionDate", "")

        line_items = []
        for item in record.get("lineItems") or []:
            line_item = self._build_line_item(item, currency)
            line_items.append(line_item)
        payload["items"] = line_items

        payload["subsidiary_id"] = self._resolve_subsidiary(record)

        return payload

    def upsert_record(self, record: dict, context: dict):
        """Create or update a journal entry in Rillet."""
        state_updates = dict()
        method = "POST"
        endpoint = self.endpoint

        if record.get("id"):
            id = record.pop("id")
            state_updates["is_updated"] = True
            method = "PUT"
            endpoint = f"{self.endpoint}/{id}"

        response = self.request_api(method, endpoint=endpoint, request_data=record)
        if response.status_code in [200, 201]:
            return response.json().get("id"), True, state_updates

        return None, False, response.json()
