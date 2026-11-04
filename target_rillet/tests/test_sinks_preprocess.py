"""Unit tests for sink preprocess_record methods."""

from copy import deepcopy
from typing import Optional, Type
from unittest.mock import MagicMock

import pytest

from target_rillet.client import RilletSink
from target_rillet.sinks import (
    BankTransactionsSink,
    BillsSink,
    ChargesSink,
    FallbackSink,
    JournalsSink,
    ReimbursementsSink,
    VendorCreditsSink,
    VendorsSink,
)
from target_rillet.target import TargetRillet


SAMPLE_CONFIG = {
    "api_key": "test-api-key",
}

LOOKUP_CACHE = {
    "accounts": {
        "Office Supplies": "6100",
        "Travel": "6200",
    },
    "accounts_by_id": {
        "acc-1": "6300",
    },
    # Seeded so process_prepaid_lines does not hit the live accounts API.
    "accounts_with_prepaid_account": set(),
    "subsidiaries": {
        "US Entity": "sub-123",
    },
    "vendors": [
        {
            "name": "Acme Corp",
            "id": "vendor-456",
        },
        {   "id": "vendor-1", 
            "name": "Acme Corp 2",
        },
    ],
    "fields": {
        "Department": {
            "id": "field-1",
            "values": [
                {"id": "val-1", "name": "Engineering"},
                {"id": "val-2", "name": "Sales"},
            ],
        },
    },
}


def make_sink(
    sink_class: Type[RilletSink],
    stream_name: str,
    config: Optional[dict] = None,
    lookup_cache: Optional[dict] = None,
) -> RilletSink:
    target = TargetRillet(config=config or SAMPLE_CONFIG)
    schema = {"type": "object", "properties": {}}
    sink = sink_class(target, stream_name, schema, [])
    if lookup_cache is not None:
        sink._lookup_cache = lookup_cache
    return sink


class TestJournalsSinkPreprocessRecord:
    def test_maps_journal_entry_to_rillet_payload(self):
        sink = make_sink(JournalsSink, "JournalEntries", lookup_cache=LOOKUP_CACHE)
        record = {
            "id": "je-1",
            "journalEntryNumber": "JE-001",
            "currency": "EUR",
            "transactionDate": "2024-03-15",
            "subsidiaryId": "sub-direct",
            "lineItems": [
                {
                    "debitAmount": "100.00",
                    "accountNumber": "1000",
                    "description": "Debit line",
                },
                {
                    "creditAmount": "100.00",
                    "accountName": "Office Supplies",
                },
            ],
        }

        payload = sink.preprocess_record(record, {})

        assert payload == {
            "id": "je-1",
            "name": "JE-001",
            "currency": "EUR",
            "date": "2024-03-15",
            "subsidiary_id": "sub-direct",
            "items": [
                {
                    "amount": {"amount": "100.00", "currency": "EUR"},
                    "account_code": "1000",
                    "side": "DEBIT",
                    "description": "Debit line",
                },
                {
                    "amount": {"amount": "100.00", "currency": "EUR"},
                    "account_code": "6100",
                    "side": "CREDIT",
                },
            ],
        }

    def test_resolves_name_from_number_or_description(self):
        sink = make_sink(JournalsSink, "JournalEntries", lookup_cache=LOOKUP_CACHE)

        assert (
            sink.preprocess_record(
                {
                    "number": "NUM-1",
                    "lineItems": [{"debitAmount": "1", "accountNumber": "1000"}],
                },
                {},
            )["name"]
            == "NUM-1"
        )
        assert (
            sink.preprocess_record(
                {
                    "description": "Monthly close",
                    "lineItems": [{"debitAmount": "1", "accountNumber": "1000"}],
                },
                {},
            )["name"]
            == "Monthly close"
        )

    def test_defaults_currency_to_usd(self):
        sink = make_sink(JournalsSink, "JournalEntries", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {
                "journalEntryNumber": "JE-002",
                "lineItems": [{"debitAmount": "1", "accountNumber": "1000"}],
            },
            {},
        )
        assert payload["currency"] == "USD"

    def test_resolves_subsidiary_from_name(self):
        sink = make_sink(JournalsSink, "JournalEntries", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {
                "journalEntryNumber": "JE-003",
                "subsidiaryName": "US Entity",
                "lineItems": [{"debitAmount": "1", "accountNumber": "1000"}],
            },
            {},
        )
        assert payload["subsidiary_id"] == "sub-123"

    def test_resolves_custom_fields_on_line_items(self):
        sink = make_sink(JournalsSink, "JournalEntries", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {
                "journalEntryNumber": "JE-004",
                "lineItems": [
                    {
                        "debitAmount": "50",
                        "accountNumber": "1000",
                        "customFields": [
                            {"name": "Department", "value": "Engineering"}
                        ],
                    }
                ],
            },
            {},
        )
        assert payload["items"][0]["fields"] == [
            {"field_id": "field-1", "field_value_id": "val-1"},
        ]

    def test_raises_when_name_is_missing(self):
        sink = make_sink(JournalsSink, "JournalEntries", lookup_cache=LOOKUP_CACHE)
        with pytest.raises(
            ValueError, match="Journal entry number, number, or description is required"
        ):
            sink.preprocess_record({"lineItems": []}, {})


class TestBillsSinkPreprocessRecord:
    def test_maps_bill_to_rillet_payload(self):
        sink = make_sink(BillsSink, "Bills", lookup_cache=LOOKUP_CACHE)
        record = {
            "id": "bill-1",
            "vendorId": "vendor-1",
            "billNumber": "BILL-100",
            "issueDate": "2024-01-01",
            "dueDate": "2024-01-31",
            "subsidiaryId": "sub-1",
            "description": "Office supplies",
            "status": "UNPAID",
            "currency": "USD",
            "attachments": [{"url": "https://example.com/file.pdf"}],
            "expenses": [
                {
                    "description": "Supplies",
                    "accountNumber": "6100",
                    "amount": "250.00",
                }
            ],
            "lineItems": [
                {
                    "description": "Shipping",
                    "accountName": "Travel",
                    "amount": "25.00",
                }
            ],
            "customFields": [{"name": "po_number", "value": "PO-99"}],
        }

        payload = sink.preprocess_record(record, {})

        assert payload["id"] == "bill-1"
        assert payload["vendor_id"] == "vendor-1"
        assert payload["expense_number"] == "BILL-100"
        assert payload["bill_date"] == "2024-01-01"
        assert payload["due_date"] == "2024-01-31"
        assert payload["subsidiary_id"] == "sub-1"
        assert payload["description"] == "Office supplies"
        assert payload["status"] == "UNPAID"
        assert payload["attachments"] == [{"url": "https://example.com/file.pdf"}]
        assert payload["po_number"] == "PO-99"
        assert payload["items"] == [
            {
                "description": "Supplies",
                "account_code": "6100",
                "amount": {"amount": "250.00", "currency": "USD"},
            },
            {
                "description": "Shipping",
                "account_code": "6200",
                "amount": {"amount": "25.00", "currency": "USD"},
            },
        ]

    def test_resolves_subsidiary_from_name_when_id_missing(self):
        sink = make_sink(BillsSink, "Bills", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {
                "vendorId": "vendor-1",
                "billNumber": "BILL-101",
                "subsidiaryName": "US Entity",
                "expenses": [{"accountNumber": "6100", "amount": "10"}],
            },
            {},
        )
        assert payload["subsidiary_id"] == "sub-123"

    def test_resolves_account_by_id(self):
        sink = make_sink(BillsSink, "Bills", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {
                "vendorId": "vendor-1",
                "expenses": [{"accountId": "acc-1", "amount": "10"}],
            },
            {},
        )
        assert payload["items"][0]["account_code"] == "6300"


class TestFallbackSinkPreprocessRecord:
    def test_preserves_existing_subsidiary_id(self):
        sink = make_sink(FallbackSink, "custom-stream", lookup_cache=LOOKUP_CACHE)
        record = {"subsidiary_id": "sub-existing", "amount": "100"}

        payload = sink.preprocess_record(record, {})

        assert payload["subsidiary_id"] == "sub-existing"
        assert payload["amount"] == "100"

    def test_resolves_subsidiary_from_subsidiary_id_field(self):
        sink = make_sink(FallbackSink, "custom-stream", lookup_cache=LOOKUP_CACHE)
        record = {"subsidiaryId": "sub-from-id"}

        payload = sink.preprocess_record(record, {})

        assert payload["subsidiary_id"] == "sub-from-id"

    def test_resolves_subsidiary_from_name(self):
        sink = make_sink(FallbackSink, "custom-stream", lookup_cache=LOOKUP_CACHE)
        record = {"subsidiaryName": "US Entity"}

        payload = sink.preprocess_record(record, {})

        assert payload["subsidiary_id"] == "sub-123"


class TestBankTransactionsSinkPreprocessRecord:
    def test_inherits_fallback_subsidiary_resolution(self):
        sink = make_sink(
            BankTransactionsSink, "bank-transactions", lookup_cache=LOOKUP_CACHE
        )
        payload = sink.preprocess_record(
            {"bank_account_id": "ba-1", "subsidiaryName": "US Entity"},
            {},
        )
        assert payload["subsidiary_id"] == "sub-123"


class TestChargesSinkPreprocessRecord:
    def test_resolves_line_item_accounts(self):
        sink = make_sink(ChargesSink, "charges", lookup_cache=LOOKUP_CACHE)
        record = {
            "vendor_id": "vendor-1",
            "subsidiaryId": "sub-1",
            "items": [
                {
                    "accountName": "Office Supplies",
                    "accountNumber": "9999",
                    "accountId": "acc-1",
                    "amount": "75.00",
                },
                {"accountName": "Travel", "amount": "25.00"},
                {"accountId": "acc-1", "amount": "30.00"},
                {"account_code": "5000", "amount": "10.00"},
            ],
        }

        payload = sink.preprocess_record(record, {})

        assert payload["subsidiary_id"] == "sub-1"
        assert payload["items"][0]["account_code"] == "9999"
        assert "accountNumber" not in payload["items"][0]
        assert "accountName" not in payload["items"][0]
        assert "accountId" not in payload["items"][0]
        assert payload["items"][1]["account_code"] == "6200"
        assert "accountName" not in payload["items"][1]
        assert payload["items"][2]["account_code"] == "6300"
        assert "accountId" not in payload["items"][2]
        assert payload["items"][3]["account_code"] == "5000"

    def test_resolves_name_value_line_fields(self):
        sink = make_sink(ChargesSink, "charges", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {
                "vendor_id": "vendor-1",
                "subsidiaryId": "sub-1",
                "items": [
                    {
                        "account_code": "60214",
                        "amount": {"amount": "10.00", "currency": "USD"},
                        "fields": [{"name": "Department", "value": "Engineering"}],
                    }
                ],
            },
            {},
        )
        assert payload["items"][0]["fields"] == [
            {"field_id": "field-1", "field_value_id": "val-1"},
        ]

    def test_leaves_airbase_style_line_fields_unchanged(self):
        sink = make_sink(ChargesSink, "charges", lookup_cache=LOOKUP_CACHE)
        airbase_fields = [
            {"field_id": "field-1", "field_value_id": "val-1"},
        ]
        payload = sink.preprocess_record(
            {
                "vendor_id": "vendor-1",
                "subsidiaryId": "sub-1",
                "items": [
                    {
                        "account_code": "60214",
                        "amount": {"amount": "10.00", "currency": "USD"},
                        "fields": airbase_fields,
                    }
                ],
            },
            {},
        )
        assert payload["items"][0]["fields"] == airbase_fields


class TestReimbursementsSinkPreprocessRecord:
    def test_inherits_fallback_subsidiary_resolution(self):
        sink = make_sink(
            ReimbursementsSink, "reimbursements", lookup_cache=LOOKUP_CACHE
        )
        payload = sink.preprocess_record(
            {"vendor_id": "vendor-1", "subsidiaryId": "sub-99", "items": []},
            {},
        )
        assert payload["subsidiary_id"] == "sub-99"

    def test_resolves_name_value_line_fields(self):
        sink = make_sink(
            ReimbursementsSink, "reimbursements", lookup_cache=LOOKUP_CACHE
        )
        payload = sink.preprocess_record(
            {
                "vendor_id": "vendor-1",
                "subsidiaryId": "sub-1",
                "items": [
                    {
                        "account_code": "60214",
                        "amount": {"amount": "10.00", "currency": "USD"},
                        "fields": [{"name": "Department", "value": "Sales"}],
                    }
                ],
            },
            {},
        )
        assert payload["items"][0]["fields"] == [
            {"field_id": "field-1", "field_value_id": "val-2"},
        ]


class TestVendorCreditsSinkPreprocessRecord:
    def test_inherits_fallback_subsidiary_resolution(self):
        sink = make_sink(VendorCreditsSink, "vendor-credits", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {"vendor_id": "vendor-1", "subsidiaryName": "US Entity"},
            {},
        )
        assert payload["subsidiary_id"] == "sub-123"


class TestVendorsSinkPreprocessRecord:
    def test_sets_id_from_vendor_cache_lookup(self):
        sink = make_sink(VendorsSink, "vendors", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {"name": "Acme Corp", "subsidiaryId": "sub-1"},
            {},
        )
        assert payload["id"] == "vendor-456"
        assert payload["subsidiary_id"] == "sub-1"

    def test_does_not_set_id_when_vendor_not_in_cache(self):
        sink = make_sink(VendorsSink, "vendors", lookup_cache=LOOKUP_CACHE)
        payload = sink.preprocess_record(
            {"name": "New Vendor Inc", "subsidiaryId": "sub-1"},
            {},
        )
        assert "id" not in payload
        assert payload["name"] == "New Vendor Inc"


class TestResolveCustomFields:
    def test_creates_missing_value_and_updates_cache(self):
        cache = deepcopy(LOOKUP_CACHE)
        sink = make_sink(ChargesSink, "charges", lookup_cache=cache)
        created_value = {"id": "val-new", "name": "Navan-Dept-X"}
        sink.request_api = MagicMock(
            return_value=MagicMock(
                json=MagicMock(
                    return_value={
                        "id": "field-1",
                        "name": "Department",
                        "values": [
                            {"id": "val-1", "name": "Engineering"},
                            {"id": "val-2", "name": "Sales"},
                            created_value,
                        ],
                    }
                )
            )
        )

        resolved = sink._resolve_custom_fields(
            [{"name": "Department", "value": "Navan-Dept-X"}]
        )

        assert resolved == [{"field_id": "field-1", "field_value_id": "val-new"}]
        sink.request_api.assert_called_once_with(
            "POST",
            endpoint="/fields/field-1/values",
            request_data={"name": "Navan-Dept-X"},
        )
        assert created_value in sink._lookup_cache["fields"]["Department"]["values"]
        # Second resolve hits the updated cache (no extra create).
        sink.request_api.reset_mock()
        assert sink._resolve_custom_fields(
            [{"name": "Department", "value": "Navan-Dept-X"}]
        ) == [{"field_id": "field-1", "field_value_id": "val-new"}]
        sink.request_api.assert_not_called()

    def test_skips_missing_field_definition(self, caplog):
        sink = make_sink(ChargesSink, "charges", lookup_cache=deepcopy(LOOKUP_CACHE))
        sink.request_api = MagicMock()

        with caplog.at_level("WARNING"):
            resolved = sink._resolve_custom_fields(
                [{"name": "Unknown Field", "value": "Anything"}]
            )

        assert resolved == []
        sink.request_api.assert_not_called()
        assert "Field name Unknown Field not found" in caplog.text

    def test_passthrough_already_resolved_ids(self):
        sink = make_sink(ChargesSink, "charges", lookup_cache=deepcopy(LOOKUP_CACHE))
        sink.request_api = MagicMock()
        resolved = sink._resolve_custom_fields(
            [{"field_id": "field-1", "field_value_id": "val-1"}]
        )
        assert resolved == [{"field_id": "field-1", "field_value_id": "val-1"}]
        sink.request_api.assert_not_called()


class TestUpdateLookupCacheObjectList:
    def test_updates_cached_vendor_by_id(self):
        sink = make_sink(
            VendorsSink,
            "vendors",
            lookup_cache={"vendors": [{"id": "vendor-1", "name": "Acme Corp"}]},
        )
        sink.update_lookup_cache_object_list(
            "vendors",
            {"id": "vendor-1", "name": "Acme Corp", "email": "a@acme.com"},
        )
        vendors = sink._lookup_cache["vendors"]
        assert len(vendors) == 1
        assert vendors[0]["email"] == "a@acme.com"
        assert (
            len(sink.lookup_in_cache_object_list("vendors", "name", "Acme Corp")) == 1
        )

    def test_appends_when_vendor_id_is_new(self):
        sink = make_sink(
            VendorsSink,
            "vendors",
            lookup_cache={"vendors": [{"id": "vendor-1", "name": "Acme Corp"}]},
        )
        sink.update_lookup_cache_object_list(
            "vendors",
            {"id": "vendor-2", "name": "New Vendor"},
        )
        vendors = sink._lookup_cache["vendors"]
        assert [vendor["id"] for vendor in vendors] == ["vendor-1", "vendor-2"]
