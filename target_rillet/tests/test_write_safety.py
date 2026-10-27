"""Observable request and SDK bookmark contracts for safe financial writes."""

import io
import json
from copy import deepcopy
from unittest.mock import Mock

import pytest
import requests
from hotglue_singer_sdk.target_sdk import target as sdk_target

from target_rillet.sinks import (
    BillPaymentsSink,
    BillsSink,
    ChargesSink,
    FallbackSink,
    ReimbursementsSink,
    VendorsSink,
)
from target_rillet.target import TargetRillet
from target_rillet.tests.test_sinks_preprocess import LOOKUP_CACHE, make_sink


def response(status=200, body=None, headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body or {"id": "remote-1"}).encode()
    result.headers.update(headers or {})
    result.request = requests.Request("POST", "https://example.com", json={}).prepare()
    return result


@pytest.fixture(autouse=True)
def no_real_network(monkeypatch):
    monkeypatch.delenv("SNAPSHOT_DIR", raising=False)
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
    assert request.call_count == 4  # No restored mapping: reuse the provider key.
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
def test_failed_receipt_preserves_successful_document_mapping(
    monkeypatch, sink_class, stream, stage
):
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
    assert state["success"] is True
    assert state["id"] == "remote-1"
    assert state["externalId"] == record["externalId"]
    assert len(state["attachment_errors"]) == 1
    assert state["attachment_errors"][0]["error"]
    assert sink.latest_state["summary"][stream]["fail"] == 0
    assert upload.call_count == (stage == "upload")
    assert download.call_args.kwargs["timeout"] == (10, 60)
    if stage == "upload":
        assert upload.call_args.kwargs["timeout"] == (10, 60)


def test_receipt_failure_does_not_skip_remaining_attachments(monkeypatch):
    monkeypatch.setattr("requests.request", Mock(return_value=response()))
    download = Mock(side_effect=[response(404), response()])
    upload = Mock(return_value=response())
    monkeypatch.setattr("requests.get", download)
    monkeypatch.setattr("requests.post", upload)
    record = bill_record()
    record["attachments"] = [
        {"id": "missing", "url": "https://example.com/missing"},
        {"id": "available", "url": "https://example.com/available"},
    ]
    sink = make_sink(BillsSink, "Bills", lookup_cache=deepcopy(LOOKUP_CACHE))
    sink.process_record(record, {})
    state = sink.latest_state["bookmarks"]["Bills"][-1]
    assert state["success"] is True
    assert [error["id"] for error in state["attachment_errors"]] == ["missing"]
    assert upload.call_count == 1


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


@pytest.mark.parametrize("stream", ["charges", "bill_payments"])
def test_singer_replay_uses_only_standard_remote_id_snapshots(monkeypatch, tmp_path, stream):
    monkeypatch.setattr(sdk_target, "SNAPSHOT_DIR", str(tmp_path))
    monkeypatch.setattr(sdk_target, "flow_id", "test-flow")

    def api(**kwargs):
        if kwargs["method"] == "GET":
            return response(body={"vendors": LOOKUP_CACHE["vendors"], "accounts": []})
        return response()

    request = Mock(side_effect=api)
    monkeypatch.setattr("requests.request", request)
    download = Mock(return_value=response(404))
    monkeypatch.setattr("requests.get", download)
    record = {"externalId": "source-1"}
    if stream == "charges":
        record.update(
            vendor_id="vendor-1",
            subsidiary_id="sub-1",
            items=[],
            attachments=[{"id": "receipt-1", "url": "https://example.com/missing"}],
        )
    else:
        record.update(bill_id="bill-1", amount={"amount": "10", "currency": "EUR"})
    messages = [
        {
            "type": "SCHEMA",
            "stream": stream,
            "key_properties": ["externalId"],
            "schema": {"type": "object", "properties": {key: {} for key in record}},
        },
        {"type": "RECORD", "stream": stream, "record": record},
    ]
    states = []
    for replay in (False, True):
        target = TargetRillet(config={"api_key": "test-key"})
        try:
            target.listen(io.StringIO("\n".join(json.dumps(message) for message in messages)))
            state = target._latest_state["bookmarks"][stream][-1]
            states.append(state)
            assert state["success"] is True and state["id"] == "remote-1"
            assert state["externalId"] == "source-1"
            if not replay:
                (tmp_path / f"{stream}_test-flow.snapshot.csv").write_text(
                    "InputId,RemoteId\nsource-1,remote-1\n"
                )
        finally:
            target._shutdown_requested.set()
            target.signal_listener_thread.join(timeout=1)
    assert sum(call.kwargs["method"] == "POST" for call in request.call_args_list) == 1
    assert download.call_count == (stream == "charges")
    if stream == "charges":
        assert states[0]["attachment_errors"]
    assert [path.name for path in tmp_path.iterdir()] == [f"{stream}_test-flow.snapshot.csv"]
