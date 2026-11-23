"""Rillet target sink class, which handles writing streams."""

from __future__ import annotations

from copy import deepcopy

import requests
from hotglue_singer_sdk.exceptions import FatalAPIError

from target_rillet.client import RilletSink


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
            "impact_date": record.get("impactDate"),
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
                tax_amount = expense.get("taxAmount")
                if tax_amount is None or tax_amount == "":
                    raise ValueError(f"taxAmount is required for line item {expense} if taxCode is provided")
                tax_rate = self._resolve_tax_rate(expense)
                mapped_expense["tax_rate"] = {
                    "tax_code": expense.get("taxCode"),
                    "coverage": "INCLUSIVE" if expense.get("taxInclusive") else "EXCLUSIVE",
                    "tax_amount": {
                        "amount": tax_amount,
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

    def _failed_matched_record_for_external_id(self, external_id: str) -> bool:
        """True when a same-job record bookmark for external_id failed or has no id.

        Ledger streams are the objects cash reconcile can match (like charges, bills,
        reimbursements, etc.)
        
        They are not bank-transactions themselves.
        """
        if not external_id:
            return False
        bookmarks_by_stream = (self._target._latest_state or {}).get("bookmarks") or {}
        ledger_streams = {
            rel["objectName"]
            for rel in ReconcileTransactionsSink.relation_fields
            if rel.get("objectName") and rel["objectName"] != "bank-transactions"
        }
        for stream in ledger_streams:
            state = next(
                (
                    b
                    for b in (bookmarks_by_stream.get(stream) or [])
                    if b.get("externalId") == str(external_id)
                ),
                None,
            )
            if state is not None and ((not state.get("success")) or (not state.get("id"))):
                return True
        return False

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Handle errors by posting to the fallback sink."""
        # ETL attaches hg_error on the header for failed records.
        hg_error = record.pop("hg_error", None)
        if hg_error is not None:
            raise ValueError(str(hg_error))

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

    def _apply_charge_exchange_rate(
        self, record: dict, external_id: str | None
    ) -> None:
        """Convert net_amount using per-job charge FX cache when ETL flags the BT.

        ETL sets process_transaction_exchange_rate when posted currency differs
        from the subsidiary/bank currency. Raises when the flag is set but no
        usable rate is available.
        """
        if not record.pop("process_transaction_exchange_rate", None):
            return

        net_amount_transaction = record.get("net_amount") or {}
        bt_currency = net_amount_transaction.get("currency")
        rates = getattr(self._target, "_charge_exchange_rates", None) or {}
        exchange_rate = rates.get(str(external_id)) if external_id else None
        if not exchange_rate:
            raise ValueError(
                f"Bank transaction {external_id} requires exchange rate conversion "
                "but no charge exchange_rate was cached in this job"
            )

        rate = float(exchange_rate["rate"])
        base_currency = exchange_rate.get("base")
        target_currency = exchange_rate.get("target")
        if not bt_currency or base_currency != bt_currency or not target_currency:
            raise ValueError(
                f"Bank transaction {external_id} exchange rate "
                f"{base_currency}->{target_currency} does not match "
                f"net_amount currency {bt_currency}"
            )

        amount = float(net_amount_transaction["amount"])
        net_amount_transaction["amount"] = round(amount * rate, 2)
        net_amount_transaction["currency"] = target_currency

    def upsert_record(self, record: dict, context: dict):
        # externalId is popped before upsert; ETL pairs ledger/BT via external_references.id
        refs = record.get("external_references") or []
        external_id = refs[0].get("id") if refs and isinstance(refs[0], dict) else None
        if self._failed_matched_record_for_external_id(external_id):
            self.logger.info(
                "Skipping bank-transaction %s because matching record failed",
                external_id,
            )
            return None, True, {"existing": True}
        self._apply_charge_exchange_rate(record, external_id)
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
        """Resolve line level accounts and custom fields to Rillet IDs."""
        record = super().preprocess_record(record, context)
        for item in record.get("items", []):
            if not item.get("account_code"):
                item["account_code"] = self._resolve_account(item)
            item.pop("accountNumber", None)
            item.pop("accountName", None)
            item.pop("accountId", None)
            if item.get("fields"):
                item["fields"] = self._resolve_custom_fields(item["fields"])
        record["items"] = self.process_prepaid_lines(record.get("items"))
        return record

    def _stash_charge_exchange_rate(self, record: dict, exchange_rate) -> None:
        """Store the exchange_rate returned by Rilleton the target
         for same-job bank txn conversion."""
        if exchange_rate is None:
            return
        refs = record.get("external_references") or []
        external_id = record.get("externalId")
        if not external_id and refs and isinstance(refs[0], dict):
            external_id = refs[0].get("id")
        if not external_id:
            return
        rates = getattr(self._target, "_charge_exchange_rates", None)
        if rates is None:
            rates = {}
            self._target._charge_exchange_rates = rates
        rates[str(external_id)] = exchange_rate

    def upsert_record(self, record: dict, context: dict):
        """Create a charge in Rillet, then upload any attachments."""
        attachments = record.pop("attachments", [])
        # externalId may still be present; keep a copy for FX cache key
        external_id = record.get("externalId")
        refs = record.get("external_references") or []
        if not external_id and refs and isinstance(refs[0], dict):
            external_id = refs[0].get("id")

        id, success, state_updates = super().upsert_record(record, context)

        if success and not state_updates.get("existing"):
            payload = getattr(self, "_last_upsert_response", None) or {}
            self._stash_charge_exchange_rate(
                {"externalId": external_id, "external_references": refs},
                payload.get("exchange_rate"),
            )

        try:
            if id and attachments:
                for index, attachment in enumerate(attachments):
                    self.post_attachment(id, attachment, index)
        except Exception as e:
            self.logger.info(f"Error posting attachments to charge {id}: {e}")

        return id, success, state_updates


class ReconcileTransactionsSink(FallbackSink):
    """Match bank transactions to ledger records via Rillet cash matching.

    ETL forwards external IDs in transaction_ids and optional matched-record
    id lists (charges, bills, reimbursements, etc.). SDK relation_fields
    resolves each id from same-job target state or flow snapshots.

    ETL sends synthetic id and externalId fields
    so successful matches are written to the target snapshot for debugging

    Example payload:
        {
        "id": "navan-txn-1",
        "externalId": "navan-txn-1",
        "transaction_ids": ["17509021-a931-4e66-9161-f77308bd501d"],
        "charge_ids": ["17509021-a931-4e66-9161-f77308bd501d"]
        }
    """

    name = "reconcile-transactions"
    endpoint = "/bank-transactions/reconcile/transactions"
    api_version = "4"
    allows_upserts = False
    lookup_subsidiary = False

    # All supported objects https://docs.api.rillet.com/reference/match-bank-transactions-to-records
    relation_fields = [
        {"field": "transaction_ids", "objectName": "bank-transactions", "valueType": "list"},
        {"field": "bill_ids", "objectName": "Bills", "valueType": "list"},
        {"field": "reimbursement_ids", "objectName": "reimbursements", "valueType": "list"},
        {"field": "charge_ids", "objectName": "charges", "valueType": "list"},
        {"field": "invoice_ids", "objectName": "invoices", "valueType": "list"},
        {"field": "credit_memo_ids", "objectName": "credit-memos", "valueType": "list"},
        {"field": "bill_payment_ids", "objectName": "bill_payments", "valueType": "list"},
        {"field": "invoice_payment_ids", "objectName": "invoice_payments", "valueType": "list"},
        {"field": "journal_entry_ids", "objectName": "JournalEntries", "valueType": "list"},
        {"field": "vendor_credit_ids", "objectName": "vendor-credits", "valueType": "list"},
    ]

    def upsert_record(self, record: dict, context: dict):
        """POST the match request.
           Successes return HTTP 204.

        Rematches of already-settled entries return 400 EntriesAlreadySettled
        and are logged then skipped.

        Returns the synthetic record id (provided by ETL) so the
        target can snapshot the successful match under that externalId.
        """
        # Synthetic snapshot key only — not part of the Rillet match body.
        record_id = record.pop("id", None)

        if self._failed_matched_record_for_external_id(record_id):
            self.logger.info(
                "Skipping reconcile %s because matching record failed",
                record_id,
            )
            return record_id, True, {"existing": True}

        response = requests.request(
            method="POST",
            url=f"{self.get_base_url()}{self.endpoint}",
            json=record,
            headers=self.auth_headers,
        )
        if response.status_code == 400 and "EntriesAlreadySettled" in response.text:
            self.logger.info(
                "Skipping reconcile - entries already settled: %s",
                response.text,
            )
            return record_id, True, {"existing": True}
        self.validate_response(response)
        return record_id, True, {}


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

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Resolve line custom fields to Rillet IDs."""
        record = super().preprocess_record(record, context)
        for item in record.get("items", []):
            if item.get("fields"):
                item["fields"] = self._resolve_custom_fields(item["fields"])
        return record

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

    _VENDOR_ADDRESS_REQUIRED = ("line1", "city", "zip_code", "country")

    def _vendor_address_missing_fields(self, address: dict | None) -> list[str]:
        if not address:
            return []
        return [field for field in self._VENDOR_ADDRESS_REQUIRED if not address.get(field)]

    def _validate_vendor_address(
        self,
        payload: dict,
        existing_vendor: dict | None,
    ) -> dict:
        """Drop, keep, or reject address on the upsert payload."""
        vendor_label = payload.get("name")
        vendor_id = payload.get("id")

        # for new vendors, drop address if it is incomplete
        if not existing_vendor:
            if self._vendor_address_missing_fields(payload.get("address")):
                self.logger.info(
                    f"Vendor {vendor_label!r}: incoming address is incomplete. "
                    f"Dropping address for new vendor."
                )
                payload.pop("address", None)
            return payload

        # for existing vendors, check if the existing address is incomplete and if the incoming payload address is incomplete
        existing_address = existing_vendor.get("address")
        existing_missing = self._vendor_address_missing_fields(existing_address)
        payload_address = payload.get("address")
        payload_missing = self._vendor_address_missing_fields(payload.get("address"))

        # If address in Rillet is incomplete, and incoming payload address won't fix it, raise an error.
        if existing_address and existing_missing and (payload_missing or not payload_address):
            missing = ", ".join(existing_missing)
            raise ValueError(
                f"Vendor {vendor_label!r} (id {vendor_id}) has an incomplete "
                f"address in Rillet (missing: {missing}). Fix the address in Rillet before syncing updates."
            )

        # If no address in Rillet, and incoming payload address is incomplete, drop it.
        if not existing_address and payload_missing:
            self.logger.info(f"Vendor {vendor_label!r}: incoming address is incomplete. Dropping address; updating other fields.")
            payload.pop("address", None)

        return payload

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        # lookup vendor by name to not create duplicates
        existing_vendor = self._resolve_vendor(record, full_object=True)
        payload = deepcopy(existing_vendor) if existing_vendor else record

        if existing_vendor:
            for field, value in record.items():
                if field not in self._read_only_fields and value not in (None, ""):
                    if field == "address":
                        merged = dict(payload.get("address") or {})
                        merged.update({k: v for k, v in value.items() if v not in (None, "")})
                        payload["address"] = merged
                    else:
                        payload[field] = value

        # validate address
        payload = self._validate_vendor_address(payload, existing_vendor)
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

    def _ensure_bank_accounts_cache(self) -> list[dict]:
        """Load all Rillet bank accounts once; cache is a list (not code-keyed)."""
        cache = self._lookup_cache.get("bank-accounts")
        if not isinstance(cache, list):
            self._refresh_lookup_cache("bank-accounts")
        return self._lookup_cache["bank-accounts"]

    def get_existing_bank_account(self, record: dict) -> dict | None:
        """Return a cached bank account when account_code, currency, and subsidiary_id all match."""
        account_code = record.get("account_code")
        currency = record.get("currency")
        subsidiary_id = record.get("subsidiary_id")

        matches = [
            item
            for item in self._ensure_bank_accounts_cache()
            if account_code and item.get("account_code") == account_code
            and currency and item.get("currency") == currency
            and subsidiary_id and item.get("subsidiary_id") == subsidiary_id
        ]
        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(
                f"Multiple bank accounts found for account_code={account_code}, "
                f"currency={currency}, subsidiary_id={subsidiary_id}."
            )
        return matches[0]

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        # If Rillet already has a cash account mapped to this GL account_code, reuse it
        # (allows_upserts=False will skip POST instead of creating a new account).

        if not record.get("id"):
            account_code = record.get("account_code")
            bank_account = self.get_existing_bank_account(record)
            if bank_account:
                self.logger.info(
                    f"Existing bank account {bank_account.get('id')} found for GL account code {account_code}"
                )
                record["id"] = bank_account.get("id")
        return record

    def upsert_record(self, record: dict, context: dict):
        record_id, success, state_updates = super().upsert_record(record, context)
        if success and record_id and not state_updates.get("existing"):
            self.update_lookup_cache_object_list(
                "bank-accounts",
                {
                    "id": record_id,
                    "account_code": record.get("account_code"),
                    "currency": record.get("currency"),
                    "subsidiary_id": record.get("subsidiary_id"),
                },
            )
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