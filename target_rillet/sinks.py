"""Rillet target sink class, which handles writing streams."""

from __future__ import annotations
from audioop import mul

from target_rillet.client import RilletSink
import requests


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

        subsidiary_id = self._resolve_subsidiary(record)
        if subsidiary_id:
            payload["subsidiary_id"] = subsidiary_id

        return payload


class BillsSink(RilletSink):
    name = "Bills"
    endpoint = "/bills"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Map a unified JournalEntry record to the Rillet API payload."""
        payload = {
            "vendor_id": record.get("vendorId"),
            "expense_number": record.get("billNumber"),
            "bill_date": record.get("issueDate"),
            "due_date": record.get("dueDate"),
            "subsidiary_id": record.get("subsidiaryId"),
            "attachments": record.get("attachments", []),
        }

        if record.get("id"):
            payload["id"] = record["id"]

        expenses = []
        for expense in record.get("expenses", []):
            expenses.append({
                "description": expense.get("description"),
                "account_code": expense.get("accountNumber"),
                "amount": {
                    "amount": expense.get("amount"),
                    "currency": record.get("currency")
                }
            })

        payload["items"] = expenses
        return payload

    def post_attachment(self, bill_id: str, attachment: dict):
        """Post an attachment to a bill."""
        attachment_url = attachment.get("url")
        if not attachment_url:
            raise ValueError("Attachment URL is required")
        
        # get the attachment from the url
        attachment = requests.get(attachment_url).content

        # send the attachment as a multipart/form-data request
        files = {"file": (f"{bill_id}.pdf", attachment, "application/pdf")}
        multipart_headers = {
            "Authorization": f"Bearer {self.config.get('api_key')}",
            "X-Rillet-API-Version": self.api_version,
        }
        response = requests.post(f"{self.get_base_url()}{self.endpoint}/{bill_id}", files=files, headers=multipart_headers)
        return

    def upsert_record(self, record: dict, context: dict):
        """Create or update a journal entry in Rillet."""
        attachments = record.pop("attachments", [])
        id, success, state_updates = super().upsert_record(record, context)

        if id and attachments:
            # add attachment to the bill
            for attachment in attachments:
                self.post_attachment(id, attachment)

        return id, success, state_updates


class FallbackSink(RilletSink):
    """Fallback sink for handling errors."""

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def endpoint(self) -> str:
        return f"/{self.stream_name}"
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Handle errors by posting to the fallback sink."""
        return record
