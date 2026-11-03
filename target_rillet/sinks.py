"""Rillet target sink class, which handles writing streams."""

from __future__ import annotations

import os

from target_rillet.client import RilletSink
from hotglue_singer_sdk.exceptions import FatalAPIError


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
            "vendor_id": self._resolve_vendor(record),
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
            if expense.get("taxCode"):
                if not expense.get("taxAmount"):
                    raise ValueError(f"taxAmount is required for line item {expense} if taxCode is provided")
                tax_rate = self._resolve_tax_rate(expense)
                mapped_expense["tax_rate"] = {
                    "tax_code": expense.get("taxCode"),
                    "coverage": "INCLUSIVE" if expense.get("taxInclusive") else "EXCLUSIVE",
                    "tax_amount": {
                        "amount": expense.get("taxAmount"),
                        "currency": record.get("currency")
                    },
                    **tax_rate
                }
            expenses.append(mapped_expense)


        payload["items"] = self.process_prepaid_lines(expenses)
        return payload

    def upsert_record(self, record: dict, context: dict):
        """Create or update a bill in Rillet, then upload any attachments."""
        attachments = record.pop("attachments", [])
        id, success, state_updates = super().upsert_record(record, context)

        try:
            if id and attachments:
                for index, attachment in enumerate(attachments):
                    self.post_attachment(id, attachment, index)
        except Exception as e:
            self.logger.info(f"Error posting attachments to bill {id}: {e}")

        return id, success, state_updates


class UnsupportedSink(RilletSink):
    """
    Unsupported ETL streams are not supported by Rillet, but we fail loudly for export details.
    """
    unsupported_streams = frozenset({})
    allows_upserts = False

    @property
    def name(self) -> str:
        return self.stream_name

    def preprocess_record(self, record: dict, context: dict) -> dict:
        raise ValueError("This ledger entry type is not currently supported by the Rillet integration. Please record the corresponding ledger entries in Rillet and move this entry to Sync Complete.")


class FallbackSink(RilletSink):
    """Fallback sink for handling errors."""
    lookup_subsidiary = True
    lookup_vendor = False
    supports_line_prepayments = False

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
        if self.lookup_vendor:
            # confirm if vendor_id is valid or lookup by name, because id might have been deleted or merged in Rillet after it was synced to the other system 
            vendor_id = self._resolve_vendor(record)
            record["vendor_id"] = vendor_id
            record.pop("vendorName", None)
        if self.supports_line_prepayments:
            record[self.lines_field] = self.process_prepaid_lines(record.get(self.lines_field))
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
    lookup_vendor = True
    api_version = "4"

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
        record["items"] = self.process_prepaid_lines(record.get("items"))
        return record
    
    def upsert_record(self, record: dict, context: dict):
        """Create a charge in Rillet, then upload any attachments."""
        attachments = record.pop("attachments", [])
        id, success, state_updates = super().upsert_record(record, context)

        try:
            if id and attachments:
                for index, attachment in enumerate(attachments):
                    self.post_attachment(id, attachment, index)
        except Exception as e:
            self.logger.info(f"Error posting attachments to charge {id}: {e}")

        return id, success, state_updates


class ReconcileTransactionsSink(FallbackSink):
    """Match bank transactions to ledger records via Rillet cash matching.

    ETL emits external IDs (not Rillet UUIDs) in ``transaction_ids`` and any
    combination of matched-record id lists supported by
    ``POST /bank-transactions/reconcile/transactions`` (charges, bills,
    reimbursements, invoices, etc.). This sink resolves each id to a Rillet
    UUID, then POSTs the match (API v4). Success is HTTP 204 — there is no
    reconcile entity to upsert or snapshot.
    """

    name = "reconcile-transactions"
    endpoint = "/bank-transactions/reconcile/transactions"
    api_version = "4"
    allows_upserts = False
    lookup_subsidiary = False

    # MatchBankTransactionsRequest id arrays → target stream / snapshot name
    _ID_LIST_FIELDS = (
        ("transaction_ids", "bank-transactions"),
        ("bill_ids", "Bills"),
        ("reimbursement_ids", "reimbursements"),
        ("charge_ids", "charges"),
        ("invoice_ids", "invoices"),
        ("credit_memo_ids", "credit-memos"),
        ("bill_payment_ids", "bill_payments"),
        ("invoice_payment_ids", "invoice_payments"),
        ("journal_entry_ids", "JournalEntries"),
        ("vendor_credit_ids", "vendor-credits"),
    )

    def _remote_id_from_state(self, object_name: str, external_id: str) -> str | None:
        """Return a Rillet id from target bookmarks for *object_name*, if present."""
        bookmarks = (self._target._latest_state or {}).get("bookmarks", {}).get(
            object_name
        )
        if not bookmarks:
            return None
        match = next(
            (
                entry
                for entry in bookmarks
                if entry.get("externalId") == external_id and entry.get("id")
            ),
            None,
        )
        return match.get("id") if match else None

    def _remote_id_from_snapshot(self, object_name: str, external_id: str) -> str | None:
        """Return a Rillet RemoteId from the flow snapshot for *object_name*, if present."""
        flow_id = os.environ.get("FLOW")
        if not flow_id or not hasattr(self._target, "read_snapshot"):
            return None
        snapshot = self._target.read_snapshot(f"{object_name}_{flow_id}")
        if snapshot is None or snapshot.empty:
            return None
        snapshot = snapshot.copy()
        snapshot["InputId"] = snapshot["InputId"].astype(str)
        rows = snapshot[snapshot["InputId"] == str(external_id)]
        if len(rows) == 0:
            return None
        return rows.iloc[0]["RemoteId"]

    def _resolve_remote_id(self, object_name: str, external_id: str) -> str:
        """Map an ETL external id to a Rillet UUID via state, then snapshot."""
        if not external_id:
            raise ValueError(
                f"Missing external id while resolving {object_name} for reconcile"
            )
        external_id = str(external_id)
        remote_id = self._remote_id_from_state(
            object_name, external_id
        ) or self._remote_id_from_snapshot(object_name, external_id)
        if not remote_id:
            raise ValueError(
                f"Could not resolve {object_name} external id '{external_id}' "
                "to a Rillet id (not in target state or snapshots)"
            )
        return remote_id

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Resolve external ids on all present match-id lists to Rillet UUIDs."""
        record = super().preprocess_record(record, context)
        for field, object_name in self._ID_LIST_FIELDS:
            external_ids = record.get(field) or []
            if not external_ids:
                continue
            record[field] = [
                self._resolve_remote_id(object_name, external_id)
                for external_id in external_ids
            ]
        if not record.get("transaction_ids"):
            raise ValueError("transaction_ids is required for reconcile-transactions")
        return record

    def upsert_record(self, record: dict, context: dict):
        """POST the match request; Rillet returns 204 with no resource id."""
        self.request_api("POST", endpoint=self.endpoint, request_data=record)
        return None, True, {}


class ReimbursementsSink(FallbackSink):
    name = "reimbursements"
    allows_upserts = False
    lookup_vendor = True
    supports_line_prepayments = True
    lines_field = "items"

    relation_fields = [
        {
            "field": "vendor_id",
            "objectName": "vendors",
        },
    ]

    def upsert_record(self, record: dict, context: dict):
        """Create a reimbursement in Rillet, then upload any attachments."""
        attachments = record.pop("attachments", [])
        id, success, state_updates = super().upsert_record(record, context)

        try:
            if id and attachments:
                for index, attachment in enumerate(attachments):
                    self.post_attachment(id, attachment, index)
        except Exception as e:
            self.logger.info(f"Error posting attachments to reimbursement {id}: {e}")

        return id, success, state_updates


class VendorCreditsSink(FallbackSink):
    name = "vendor-credits"
    allows_upserts = False
    lookup_vendor = True

    relation_fields = [
        {
            "field": "vendor_id",
            "objectName": "vendors",
        },
    ]

class VendorsSink(FallbackSink):
    name = "vendors"
    allows_upserts = True
    _read_only_fields = ("created_at", "updated_at")

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        # lookup vendor by name to not create duplicates
        existing_vendor = self._resolve_vendor(record, full_object=True)
        payload = existing_vendor.copy() if existing_vendor else record
        if existing_vendor:
            payload.update({k:v for k,v in record.items() if k not in self._read_only_fields and v not in [None, ""]})
        return payload

    def upsert_record(self, record: dict, context: dict):
        vendor_name = record.get("name")
        record_id, success, state_updates = super().upsert_record(record, context)
        if success and record_id and vendor_name:
            self.update_lookup_cache_object_list("vendors", {**record, "id": record_id})
        return record_id, success, state_updates


class BankAccountsSink(FallbackSink):
    name = "bank-accounts"
    allows_upserts = False

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        # If Rillet already has a cash account mapped to this GL account_code, reuse it
        # (allows_upserts=False will skip POST instead of creating a new account).
        account_code = record.get("account_code")
        if account_code:
            bank_account_id = self.lookup_in_cache("bank-accounts", account_code)
            if bank_account_id:
                self.logger.info(
                    f"Existing bank account {bank_account_id} found for GL account code {account_code}"
                )
                record["id"] = bank_account_id
        return record

    def upsert_record(self, record: dict, context: dict):
        account_code = record.get("account_code")
        record_id, success, state_updates = super().upsert_record(record, context)
        if success and record_id and account_code:
            self.update_lookup_cache("bank-accounts", account_code, record_id)
        return record_id, success, state_updates


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
        
    def handle_over_payment_error(self, bill_id: str) -> None:
        # When Rillet's Auto Rec is enabled and in the Cash account it identifies a corresponding bank transaction for a bill, 
        # Rillet creates the bill payment journal entry and automatically matches the transaction. This marks the bill as paid on the Rillet side.
        # So when we receive the corresponding bill_payment application we get an error saying we're trying to over pay the bill.
        # To handle this, we fetch the bill payment generated by Rillet and use the id to mark the payment as existing.
        bill_payments = self.request_api("GET", endpoint=f"/bills/{bill_id}/payments")
        bill_payments = bill_payments.json().get("payments")
        if bill_payments:
            bill_payment_id = bill_payments[0].get("id")
            self.logger.info(f"Found payment for bill {bill_id}. Marking bill payment as existing.")
            return bill_payment_id, True, {"existing": True}

        raise FatalAPIError(f"Bill is marked as paid but no payment was found for bill {bill_id}")

    def upsert_record(self, record: dict, context: dict):
        bill_id = record.get("bill_id", None)
        if not bill_id:
            raise ValueError("bill_id is required")
        self.endpoint = f"/bills/{bill_id}/payments"
        try:
            return super().upsert_record(record, context)
        except Exception as e:
            if "Tried to over pay expense" in str(e):
                id, success, state_updates = self.handle_over_payment_error(bill_id)
                return id, success, state_updates
            raise e


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


class ExternalReferenceTypesSink(FallbackSink):
    """
    Creates external reference types in Rillet.
    """
    name = "external-reference-types"
    api_version = "4"