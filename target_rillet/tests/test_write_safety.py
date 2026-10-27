"""Observable request and SDK bookmark contracts for safe financial writes."""

import json
import time
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from target_rillet.sinks import (
    BillPaymentsSink,
    BillsSink,
    ChargesSink,
    FallbackSink,
    ReimbursementsSink,
    VendorsSink,
)
from target_rillet.tests.test_sinks_preprocess import LOOKUP_CACHE, make_sink


def response(status=200, body=None, headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body or {"id": "remote-1"}).encode()
    result.headers.update(headers or {})
    result.request = requests.Request("POST", "https://example.com", json={}).prepare()
    return result


@pytest.fixture(autouse=True)
def no_real_network(monkeypatch, tmp_path):
    monkeypatch.setenv("SNAPSHOT_DIR", str(tmp_path))
    monkeypatch.setattr("requests.request", Mock(side_effect=AssertionError("Unexpected HTTP")))
    monkeypatch.setattr("requests.get", Mock(side_effect=AssertionError("Unexpected download")))
    monkeypatch.setattr("requests.post", Mock(side_effect=AssertionError("Unexpected upload")))
    monkeypatch.setattr("target_rillet.client.time.sleep", Mock())


def bill_record():
    return {
        "externalId": "spendesk:bill:123",
        "vendorId": "vendor-1",
        "subsidiaryId": "sub-1",
        "currency": "EUR",
        "issueDate": "2026-01-01",
        "lineItems": [{"accountNumber": "6100", "amount": "10"}],
    }


def test_sdk_preserves_source_id_and_stable_key_across_lost_response_and_runs(monkeypatch):
    request = Mock(side_effect=[requests.ReadTimeout(), response(503), response()])
    monkeypatch.setattr("requests.request", request)
    record = bill_record()
    sink = make_sink(BillsSink, "Bills", lookup_cache=deepcopy(LOOKUP_CACHE))
    sink.process_record(record, {})
    state = sink.latest_state["bookmarks"]["Bills"][-1]
    assert state["success"] is True
    assert state["externalId"] == record["externalId"]
    assert state["id"] == "remote-1"
    keys = [call.kwargs["headers"]["Idempotency-Key"] for call in request.call_args_list]
    assert len(keys) == 3 and len(set(keys)) == 1
    assert all("externalId" not in call.kwargs["json"] for call in request.call_args_list)
    assert all(call.kwargs["timeout"] == (10, 60) for call in request.call_args_list)
    request.side_effect = None
    request.return_value = response()
    fresh = make_sink(BillsSink, "Bills", lookup_cache=deepcopy(LOOKUP_CACHE))
    fresh.process_record(record, {})
    assert request.call_count == 3  # Confirmed intent, even without SDK state.
    assert request.call_args.kwargs["headers"]["Idempotency-Key"] == keys[0]
    fresh.process_record({**record, "externalId": "different"}, {})
    assert request.call_args.kwargs["headers"]["Idempotency-Key"] != keys[0]
    assert record["externalId"] == "spendesk:bill:123"


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
def test_no_retry_for_permanent_errors(monkeypatch, status):
    request = Mock(return_value=response(status))
    monkeypatch.setattr("requests.request", request)
    sink = make_sink(FallbackSink, "custom")
    with pytest.raises(Exception):
        sink.upsert_record({"externalId": "source"}, {})
    assert request.call_count == 1


def test_retry_after_and_bounded_attempts(monkeypatch):
    request = Mock(return_value=response(429, headers={"Retry-After": "17"}))
    monkeypatch.setattr("requests.request", request)
    sleep = Mock()
    monkeypatch.setattr("target_rillet.client.time.sleep", sleep)
    sink = make_sink(FallbackSink, "custom")
    with pytest.raises(Exception):
        sink.upsert_record({"externalId": "source"}, {})
    assert request.call_count == 4
    assert sum(call.args[0] == 17 for call in sleep.call_args_list) == 3


@pytest.mark.parametrize("outcome", [requests.ConnectionError(), response(503)])
def test_create_without_source_id_not_retried(monkeypatch, outcome):
    request = Mock(side_effect=[outcome])
    monkeypatch.setattr("requests.request", request)
    with pytest.raises(Exception):
        make_sink(FallbackSink, "custom").upsert_record({}, {})
    assert request.call_count == 1


def test_overpayment_is_not_confirmation(monkeypatch):
    request = Mock(return_value=response(400, {"message": "Tried to over pay expense"}))
    monkeypatch.setattr("requests.request", request)
    sink = make_sink(BillPaymentsSink, "bill_payments")
    sink.process_record({"bill_id": "bill-1", "externalId": "payment-1"}, {})
    state = sink.latest_state["bookmarks"]["bill_payments"][-1]
    assert state["success"] is False
    assert "id" not in state
    assert request.call_count == 1
    assert request.call_args.kwargs["method"] == "POST"


def test_bill_mapping_preserves_period_impact_and_zero_tax():
    cache = deepcopy(LOOKUP_CACHE)
    cache["accounts_with_prepaid_account"] = {"6100"}
    cache["tax_rates"] = {"ZERO": {"percentage": "0", "country": "FR"}}
    sink = make_sink(BillsSink, "Bills", lookup_cache=cache)
    record = bill_record()
    period = {"start": "2026-01-01", "end": "2026-01-31"}
    record["impact_date"] = "2026-01-02"
    record["lineItems"][0].update(service_period=period, taxCode="ZERO", taxAmount=0)
    payload = sink.preprocess_record(record, {})
    assert payload["impact_date"] == "2026-01-02"
    assert payload["items"][0]["service_period"] == period
    assert payload["items"][0]["tax_rate"]["tax_amount"]["amount"] == "0"
    payload = sink.preprocess_record(bill_record(), {})
    assert "impact_date" not in payload
    assert "service_period" not in payload["items"][0]
    assert "tax_rate" not in payload["items"][0]


def test_explicit_native_bill_fields_take_precedence_over_name_lookups():
    sink = make_sink(BillsSink, "Bills", lookup_cache=deepcopy(LOOKUP_CACHE))
    record = bill_record()
    fields = [{"field_id": "field-native", "field_value_id": "value-native"}]
    record["lineItems"][0].update(
        fields=fields, customFields=[{"id": "ignored", "valueId": "ignored"}]
    )
    assert sink.preprocess_record(record, {})["items"][0]["fields"] == fields
    record["lineItems"][0]["fields"] = []
    assert sink.preprocess_record(record, {})["items"][0]["fields"] == []


@pytest.mark.parametrize("tax_amount", [None, ""])
def test_bill_tax_code_still_requires_tax_amount(tax_amount):
    sink = make_sink(BillsSink, "Bills", lookup_cache=deepcopy(LOOKUP_CACHE))
    record = bill_record()
    record["lineItems"][0].update(taxCode="ZERO", taxAmount=tax_amount)
    with pytest.raises(ValueError, match="taxAmount is required"):
        sink.preprocess_record(record, {})


@pytest.mark.parametrize(
    "sink_class,stream",
    [(BillsSink, "Bills"), (ChargesSink, "charges"), (ReimbursementsSink, "reimbursements")],
)
@pytest.mark.parametrize("stage", ["download", "upload"])
def test_failed_receipt_retains_document_and_fails_bookmark(monkeypatch, sink_class, stream, stage):
    request = Mock(return_value=response())
    download = Mock(return_value=response(404 if stage == "download" else 200))
    upload = Mock(return_value=response(500))
    monkeypatch.setattr("requests.request", request)
    monkeypatch.setattr("requests.get", download)
    monkeypatch.setattr("requests.post", upload)
    sink = make_sink(sink_class, stream, lookup_cache=deepcopy(LOOKUP_CACHE))
    record = (
        bill_record()
        if sink_class is BillsSink
        else {
            "externalId": "document-1",
            "vendor_id": "vendor-1",
            "subsidiary_id": "sub-1",
            "items": [],
        }
    )
    record["attachments"] = [{"url": "https://example.com/receipt.pdf"}]
    sink.process_record(record, {})
    state = sink.latest_state["bookmarks"][stream][-1]
    assert state["success"] is False
    assert state["id"] == state["document_id"] == "remote-1"
    assert state["externalId"] == record["externalId"]
    assert "Attachment upload failed" in state["error"]
    assert sink.latest_state["summary"][stream]["fail"] == 1
    assert upload.call_count == (stage == "upload")
    assert download.call_args.kwargs["timeout"] == (10, 60)
    if stage == "upload":
        assert upload.call_args.kwargs["timeout"] == (10, 60)
    # Even after receipt failure and lost SDK state, never create the document again.
    request.reset_mock()
    download.return_value = response()
    upload.return_value = response()
    fresh = make_sink(sink_class, stream, lookup_cache=deepcopy(LOOKUP_CACHE))
    fresh.process_record(record, {})
    replay = fresh.latest_state["bookmarks"][stream][-1]
    assert replay["success"] is (stage == "download")
    if stage == "upload":
        assert "Unresolved receipt upload intent" in replay["error"]
    assert upload.call_count == 1
    request.assert_not_called()


@pytest.mark.parametrize("job_directory", [False, True])
def test_confirmed_receipt_is_not_uploaded_again_with_rotated_url(
    monkeypatch, tmp_path, job_directory
):
    if job_directory:
        monkeypatch.delenv("SNAPSHOT_DIR")
        monkeypatch.setenv("JOB_ID", "receipt-job")
        monkeypatch.setattr("target_rillet.client.SDK_SNAPSHOT_DIR", str(tmp_path))
    request = Mock(return_value=response())
    download = Mock(return_value=response())
    upload = Mock(return_value=response())
    monkeypatch.setattr("requests.request", request)
    monkeypatch.setattr("requests.get", download)
    monkeypatch.setattr("requests.post", upload)
    record = bill_record()
    record["attachments"] = [{"id": "receipt-1", "url": "https://example.com/old"}]
    sink = make_sink(BillsSink, "Bills", lookup_cache=deepcopy(LOOKUP_CACHE))
    sink.process_record(record, {})
    assert sink.latest_state["bookmarks"]["Bills"][-1]["success"] is True
    record["attachments"][0]["url"] = "https://example.com/new"
    fresh = make_sink(BillsSink, "Bills", lookup_cache=deepcopy(LOOKUP_CACHE))
    fresh.process_record(record, {})
    assert fresh.latest_state["bookmarks"]["Bills"][-1]["success"] is True
    assert request.call_count == download.call_count == upload.call_count == 1


def test_vendor_put_uses_fresh_get_and_preserves_omitted_fields(monkeypatch):
    current = {
        "id": "vendor-1",
        "name": "Old",
        "email": "fresh@example.com",
        "payment_terms": 30,
        "tax_id": "tax",
        "ten_ninety_nine_eligible": True,
        "address": {"city": "Paris", "country": "FR"},
        "external_references": [{"type": "legacy", "id": "original"}],
        "created_at": "ignored",
    }
    request = Mock(side_effect=[response(body=current), response(body={"id": "vendor-1"})])
    monkeypatch.setattr("requests.request", request)
    sink = make_sink(VendorsSink, "vendors", lookup_cache=deepcopy(LOOKUP_CACHE))
    sink.process_record(
        {
            "externalId": "supplier-1",
            "id": "vendor-1",
            "name": "New",
            "email": "",
            "payment_terms": 0,
            "ten_ninety_nine_eligible": False,
            "address": {"city": "Lyon"},
            "external_references": [{"type": "spendesk", "id": "supplier-1"}],
        },
        {},
    )
    assert [call.kwargs["method"] for call in request.call_args_list] == ["GET", "PUT"]
    payload = request.call_args.kwargs["json"]
    assert payload == {
        "name": "New",
        "email": "fresh@example.com",
        "payment_terms": 0,
        "tax_id": "tax",
        "ten_ninety_nine_eligible": False,
        "address": {"city": "Lyon", "country": "FR"},
        "external_references": [
            {"type": "legacy", "id": "original"},
            {"type": "spendesk", "id": "supplier-1"},
        ],
    }


def test_pacing_shared_across_sinks(monkeypatch):
    sink = make_sink(FallbackSink, "one")
    other = FallbackSink(sink._target, "two", {"type": "object", "properties": {}}, [])
    monkeypatch.setattr(
        "target_rillet.client.time.monotonic", Mock(side_effect=[100, 100.2, 101.05])
    )
    sleep = Mock()
    monkeypatch.setattr("target_rillet.client.time.sleep", sleep)
    monkeypatch.setattr("requests.request", Mock(return_value=response()))
    sink.request_api("GET")
    other.request_api("GET")
    assert sleep.call_args.args[0] == pytest.approx(0.85)


@pytest.mark.parametrize("job_directory", [False, True])
def test_intent_exists_before_post_and_confirmation_survives_state_loss(
    monkeypatch, tmp_path, job_directory
):
    if job_directory:
        monkeypatch.delenv("SNAPSHOT_DIR")
        monkeypatch.setenv("JOB_ID", "payment-job")
        monkeypatch.setattr("target_rillet.client.SDK_SNAPSHOT_DIR", str(tmp_path))
    path = tmp_path / "rillet-creates.json"

    def post(**kwargs):
        intent = next(iter(json.loads(path.read_text()).values()))
        assert "id" not in intent and intent["payload_hash"]
        return response()

    request = Mock(side_effect=post)
    monkeypatch.setattr("requests.request", request)
    sink = make_sink(BillPaymentsSink, "bill_payments")
    record = {
        "externalId": "pay-1",
        "bill_id": "bill-1",
        "amount": {"amount": "10", "currency": "EUR"},
    }
    assert sink.upsert_record(record, {})[:2] == ("remote-1", True)
    journal = json.loads(path.read_text())
    assert next(iter(journal.values()))["id"] == "remote-1"
    next(iter(journal.values()))["created_at"] = time.time() - 90000
    path.write_text(json.dumps(journal))
    fresh = make_sink(BillPaymentsSink, "bill_payments")
    assert fresh.upsert_record(record, {}) == ("remote-1", True, {"existing": True})
    assert request.call_count == 1
    with pytest.raises(ValueError, match="payload changed"):
        fresh.upsert_record({**record, "amount": {"amount": "11", "currency": "EUR"}}, {})
    with pytest.raises(ValueError, match="payload changed"):
        fresh.upsert_record({**record, "bill_id": "different-parent"}, {})
    assert request.call_count == 1


def test_unresolved_intent_retries_same_key_only_within_window(monkeypatch, tmp_path):
    request = Mock(side_effect=requests.ReadTimeout())
    monkeypatch.setattr("requests.request", request)
    record = {"externalId": "pay-1", "bill_id": "bill-1"}
    sink = make_sink(BillPaymentsSink, "bill_payments")
    with pytest.raises(requests.ReadTimeout):
        sink.upsert_record(record, {})
    key = request.call_args.kwargs["headers"]["Idempotency-Key"]
    path = tmp_path / "rillet-creates.json"
    journal = json.loads(path.read_text())
    assert "id" not in journal[key]
    # An old unknown outcome blocks, regardless of fresh SDK state.
    journal[key]["created_at"] = time.time() - 90000
    path.write_text(json.dumps(journal))
    fresh = make_sink(BillPaymentsSink, "bill_payments")
    with pytest.raises(ValueError, match="24h"):
        fresh.upsert_record(record, {})
    assert request.call_count == 4
    journal[key]["created_at"] = time.time()
    path.write_text(json.dumps(journal))
    request.side_effect = None
    request.return_value = response()
    assert fresh.upsert_record(record, {})[:2] == ("remote-1", True)
    assert request.call_args.kwargs["headers"]["Idempotency-Key"] == key


def test_missing_or_corrupt_journal_fails_before_post(monkeypatch, tmp_path):
    request = Mock(return_value=response())
    monkeypatch.setattr("requests.request", request)
    sink = make_sink(FallbackSink, "custom")
    monkeypatch.delenv("SNAPSHOT_DIR")
    with pytest.raises(ValueError, match="SNAPSHOT_DIR"):
        sink.upsert_record({"externalId": "one"}, {})
    monkeypatch.setenv("SNAPSHOT_DIR", str(tmp_path))
    Path(tmp_path / "rillet-creates.json").write_text("not json")
    with pytest.raises(ValueError):
        sink.upsert_record({"externalId": "one"}, {})
    request.assert_not_called()


def test_retry_stops_if_intent_expires_between_attempts(monkeypatch):
    now = [100]

    def ambiguous_response(**kwargs):
        now[0] += 86401
        return response(503)

    request = Mock(side_effect=ambiguous_response)
    monkeypatch.setattr("requests.request", request)
    sink = make_sink(BillPaymentsSink, "bill_payments")
    # New intent, first request inside window, retry beyond expiry.
    monkeypatch.setattr("target_rillet.client.time.time", lambda: now[0])
    with pytest.raises(ValueError, match="24h"):
        sink.upsert_record({"externalId": "payment-1", "bill_id": "bill-1"}, {})
    assert request.call_count == 1
