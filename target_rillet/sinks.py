"""Rillet target sink class, which handles writing streams."""

from __future__ import annotations

from target_rillet.client import RilletSink
import requests


class JournalsSink(RilletSink):
    """Rillet target sink for posting journal entries."""

    name = "JournalEntries"
    endpoint = "/journal-entries"

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
    relation_fields = [
        {
            "field": "vendorId",
            "objectName": "vendors",
        },
    ]

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Map a unified JournalEntry record to the Rillet API payload."""
        payload = {
            "vendor_id": record.get("vendorId"),
            "expense_number": record.get("billNumber"),
            "bill_date": record.get("issueDate"),
            "due_date": record.get("dueDate"),
            "subsidiary_id": record.get("subsidiaryId"),
            "attachments": record.get("attachments", []),
            "description": record.get("description"),
            "status": record.get("status")
        }

        if record.get("id"):
            payload["id"] = record["id"]

        if not record.get("subsidiaryId"):
            subsidiary_id = self._resolve_subsidiary(record)
            payload["subsidiary_id"] = subsidiary_id
        
        # add external references from custom fields
        if record.get("customFields", []):
            custom_fields = {field["name"]: field["value"] for field in record.get("customFields")}
            payload.update(custom_fields)

        expenses = []
        all_lines = record.get("expenses", []) + (record.get("lineItems", []))
        for expense in all_lines:
            account_number = self._resolve_account(expense)
            mapped_expense = {
                "description": expense.get("description"),
                "account_code": account_number,
                "amount": {
                    "amount": expense.get("amount"),
                    "currency": record.get("currency")
                },
            }
            if expense.get("customFields"):
                mapped_expense["fields"] = self._resolve_custom_fields(expense["customFields"])
            if expense.get("service_period"):
                mapped_expense["service_period"] = expense.get("service_period")
            expenses.append(mapped_expense)


        payload["items"] = expenses
        return payload
    
    def get_attachment_name(self, bill_id: str, attachment: dict, index: int) -> str:
        if attachment.get("name"):
            return f"{attachment['name']}"
        if attachment.get("id"):
            return f"{attachment['id']}"
        return f"{bill_id}_{index}"

    def post_attachment(self, bill_id: str, attachment: dict, index: int):
        """Post an attachment to a bill."""
        attachment_url = attachment.get("url")
        if not attachment_url:
            raise ValueError("Attachment URL is required")
        
        # get the attachment from the url
        resp = requests.get(attachment_url)
        content = resp.content
        content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()

        att_name = self.get_attachment_name(bill_id, attachment, index)

        if content_type == "application/pdf" or content[:4] == b"%PDF":
            filename, content_type = f"{att_name}.pdf", "application/pdf"
        elif "spreadsheetml" in content_type or content_type == "application/vnd.ms-excel":
            filename, content_type = f"{att_name}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            filename, content_type = f"{att_name}.bin", "application/octet-stream"

        # send the attachment as a multipart/form-data request
        files = {"file": (filename, content, content_type)}
        multipart_headers = {
            "Authorization": f"Bearer {self.config.get('api_key')}",
            "X-Rillet-API-Version": self.api_version,
        }
        response = requests.post(f"{self.get_base_url()}{self.endpoint}/{bill_id}", files=files, headers=multipart_headers)
        self.validate_response(response)
        return

    def upsert_record(self, record: dict, context: dict):
        """Create or update a journal entry in Rillet."""
        attachments = record.pop("attachments", [])
        id, success, state_updates = super().upsert_record(record, context)

        try:
            if id and attachments:
                # add attachment to the bill
                for index, attachment in enumerate(attachments):
                    self.post_attachment(id, attachment, index)
        except Exception as e:
            self.logger.info(f"Error posting attachments to bill {id}: {e}")

        return id, success, state_updates


class FallbackSink(RilletSink):
    """Fallback sink for handling errors."""
    lookup_subsidiary = True

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def endpoint(self) -> str:
        return getattr(self, "_endpoint", None) or f"/{self.stream_name}"

    @endpoint.setter
    def endpoint(self, value: str) -> None:
        self._endpoint = value

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Handle errors by posting to the fallback sink."""
        if self.lookup_subsidiary:
            record["subsidiary_id"] = record.get("subsidiary_id") or self._resolve_subsidiary(record)
        return record


class BankTransactionsSink(FallbackSink):
    name = "bank-transactions"
    relation_fields = [
        {
            "field": "bank_account_id",
            "objectName": "bank-accounts",
        },
    ]
    
    def upsert_record(self, record: dict, context: dict):
        try:
            return super().upsert_record(record, context)
        except Exception as e:
            if "bank_account_id: JSON parse error" in str(e):
                raise ValueError(f"bank_account_id {record.get('bank_account_id')} is not valid")
            raise e


class ChargesSink(FallbackSink):
    name = "charges"
    allows_upserts = False

    relation_fields = [
        {
            "field": "vendor_id",
            "objectName": "vendors",
        },
    ]

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        # resolve lines accounts
        for item in record.get("items", []):
            if not item.get("account_code"):
                item["account_code"] = self._resolve_account(item)
            # clean payload if needed
            item.pop("accountNumber", None)
            item.pop("accountName", None)
            item.pop("accountId", None)
        return record


class ReimbursementsSink(FallbackSink):
    name = "reimbursements"
    allows_upserts = False

    relation_fields = [
        {
            "field": "vendor_id",
            "objectName": "vendors",
        },
    ]

class VendorCreditsSink(FallbackSink):
    name = "vendor-credits"

    relation_fields = [
        {
            "field": "vendor_id",
            "objectName": "vendors",
        },
    ]

class VendorsSink(FallbackSink):
    name = "vendors"
    allows_upserts = True

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        # lookup vendor by name to not create duplicates
        vendor_id = self.lookup_in_cache("vendors", record["name"])
        if vendor_id:
            record["id"] = vendor_id
        return record

    def upsert_record(self, record: dict, context: dict):
        vendor_name = record.get("name")
        record_id, success, state_updates = super().upsert_record(record, context)
        if success and record_id and vendor_name:
            self.update_lookup_cache("vendors", vendor_name, record_id)
        return record_id, success, state_updates


class VendorCreditsSink(FallbackSink):
    name = "vendor-credits"
    allows_upserts = False


class BankAccountsSink(FallbackSink):
    name = "bank-accounts"
    allows_upserts = False
    

class BillPaymentsSink(FallbackSink):
    name = "bill_payments"
    lookup_subsidiary = False
    allows_upserts = False
    relation_fields = [
        {
            "field": "bill_id",
            "objectName": "Bills",
        },
    ]

    def upsert_record(self, record: dict, context: dict):
        bill_id = record.get("bill_id", None)
        if not bill_id:
            raise ValueError("bill_id is required")
        self.endpoint = f"/bills/{bill_id}/payments"
        return super().upsert_record(record, context)


class VendorCreditsPaymentsSink(FallbackSink):
    name = "vendor_credit_payments"
    lookup_subsidiary = False
    relation_fields = [
        {
            "field": "vendor_credit_id",
            "objectName": "vendor-credits",
        },
        {
            "field": "bill_id",
            "objectName": "Bills",
        },
    ]

    def preprocess_record(self, record: dict, context: dict) -> dict:
        vendor_credit_id = record.pop("vendor_credit_id", None)
        if not vendor_credit_id:
            raise ValueError("vendor_credit_id is required")
        bill_id = record.get("bill_id", None)
        if not bill_id:
            raise ValueError("bill_id is required")
        # wrap record in applications array
        record = {"applications": [record], "vendor_credit_id": vendor_credit_id}
        return record

    def upsert_record(self, record: dict, context: dict):
        vendor_credit_id = record.pop("vendor_credit_id", None)
        self.endpoint = f"/vendor-credits/{vendor_credit_id}/applications"
        return super().upsert_record(record, context)