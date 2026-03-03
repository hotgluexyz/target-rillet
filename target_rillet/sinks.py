"""Rillet target sink class, which handles writing streams."""

from target_rillet.client import RilletSink

class JournalsSink(RilletSink):
    """Rillet target sink for posting journal entries."""

    name = "JournalEntries"
    endpoint = "/journal-entries"
    
    def _handle_custom_fields(self, custom_fields: list[dict]) -> list[dict]:
        """Handle custom fields."""
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

    def _resolve_name(self, record: dict) -> tuple[str | None, str | None]:
        """Extract the journal entry name, returning (name, error)."""
        name = (
            record.get("journalEntryNumber")
            or record.get("number")
            or record.get("description")
        )
        if not name:
            return None, "Journal entry number, number, or description is required"
        return name, None

    def _classify_side_and_amount(self, item: dict) -> tuple[tuple[str, str] | None, str | None]:
        """Determine debit/credit side and amount, returning ((side, amount), error)."""
        debit = item.get("debitAmount")
        credit = item.get("creditAmount")

        if debit and float(debit) > 0:
            return ("DEBIT", debit), None
        if credit and float(credit) > 0:
            return ("CREDIT", credit), None
        return None, f"One of debitAmount or creditAmount is required for line item {item}"

    def _resolve_account(self, item: dict) -> tuple[str | None, str | None]:
        """Resolve account code from number or cached name lookup, returning (code, error)."""
        if item.get("accountNumber"):
            return item["accountNumber"], None
        if item.get("accountName"):
            account_code = self.lookup_in_cache("accounts", item["accountName"])
            if account_code:
                return account_code, None
            return None, f"Account name {item['accountName']} not found in Rillet"
        return None, f"One of accountNumber or accountName is required for line item {item}"

    def _build_line_item(self, item: dict, currency: str) -> tuple[dict | None, str | None]:
        """Build a single Rillet line-item payload, returning (line_item, error)."""
        classification, err = self._classify_side_and_amount(item)
        if err:
            return None, err
        side, raw_amount = classification

        account_code, err = self._resolve_account(item)
        if err:
            return None, err

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
            line_item["fields"] = self._handle_custom_fields(item["customFields"])

        return line_item, None

    def _resolve_subsidiary(self, record: dict) -> tuple[str | None, str | None]:
        """Resolve subsidiary ID from direct ID or cached name lookup, returning (id, error)."""
        if record.get("subsidiaryId"):
            return record["subsidiaryId"], None
        if record.get("subsidiaryName"):
            sub_id = self.lookup_in_cache("subsidiaries", record["subsidiaryName"])
            if sub_id:
                return sub_id, None
            return None, f"Subsidiary name {record['subsidiaryName']} not found in Rillet"
        return None, None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Map a unified JournalEntry record to the Rillet API payload."""
        payload = {}
        if record.get("id"):
            payload["id"] = record["id"]

        name, err = self._resolve_name(record)
        if err:
            payload["error"] = err
            return payload
        payload["name"] = name

        currency = record.get("currency", "USD")
        payload["currency"] = currency
        payload["date"] = record.get("transactionDate", "")

        line_items = []
        for item in record.get("lineItems") or []:
            line_item, err = self._build_line_item(item, currency)
            if err:
                payload["error"] = err
                return payload
            line_items.append(line_item)
        payload["items"] = line_items

        subsidiary_id, err = self._resolve_subsidiary(record)
        if err:
            payload["error"] = err
            return payload
        if subsidiary_id:
            payload["subsidiary_id"] = subsidiary_id

        return payload

    def upsert_record(self, record: dict, context: dict):
        """Create or update a journal entry in Rillet."""
        
        if "error" in record:
            return None, False, record["error"]

        method = "POST"
        endpoint = self.endpoint

        if record.get("id"):
            method = "PUT"
            endpoint = f"{self.endpoint}/{record['id']}"

        response = self.request_api(method, endpoint=endpoint, request_data=record)
        if response.status_code in [200, 201]:
            return response.json().get("id"), True, {}
        else:
            return None, False, response.json()
