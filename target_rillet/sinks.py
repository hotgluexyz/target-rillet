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

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Map a unified JournalEntry record to the Rillet API payload."""
        payload = {
            "id": record.get("id"),
        }
        name = (
            record.get("journalEntryNumber")
            or record.get("number")
            or record.get("description")
        )
        
        if not name:
            payload["error"] = "Journal entry number, number, or description is required"
            return payload

        payload["name"] = name

        currency = record.get("currency", "USD")
        payload["currency"] = currency

        payload["date"] = record.get("transactionDate", "")

        line_items = []

        for item in record.get("lineItems") or []:
            debit = item.get("debitAmount")
            credit = item.get("creditAmount")

            if debit and float(debit) > 0:
                side = "DEBIT"
                raw_amount = debit
            elif credit and float(credit) > 0:
                side = "CREDIT"
                raw_amount = credit
            else:
                payload["error"] = f"One of debitAmount or creditAmount is required for line item {item}"
                return payload

            currency = record.get("currency", "USD")
            if item.get("accountNumber"):
                account_code = item["accountNumber"]
            else:
                account_code = self.lookup_in_cache("accounts", item["accountName"])
                if not account_code:
                    payload["error"] = f"Account name {item['accountName']} not found in Rillet"
                    return payload
            if not account_code:
                    payload["error"] = f"One of accountNumber or accountName is required for line item {item}"
                    return payload

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


            line_items.append(line_item)

        payload["items"] = line_items

        if record.get("subsidiaryId"):
            payload["subsidiary_id"] = record["subsidiaryId"]
        elif record.get("subsidiaryName"):
            payload["subsidiary_id"] = self.lookup_in_cache("subsidiaries", record["subsidiaryName"])
        if not payload.get("subsidiary_id"):
            payload["error"] = f"Subsidiary name {record.get('subsidiaryName')} not found in Rillet"
            return payload

        if record.get("exchangeRate") and record.get("currency"):
            payload["exchange_rate"] = {
                "base": record["currency"],
                "target": record["currency"],
                "rate": str(record["exchangeRate"]),
                "date": record.get("transactionDate", ""),
            }

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
