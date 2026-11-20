"""Exercise catalog identity and full-replace updates through Singer input."""

import io
import json
from copy import deepcopy
from json import dumps

import pytest
import requests

from target_rillet.target import TargetRillet


@pytest.fixture
def catalog_api(monkeypatch):
    remote = {"customers": {}, "products": {}}

    def request(method, url, params=None, json=None, headers=None, **kwargs):
        assert url.startswith("https://sandbox.api.rillet.com/")
        assert headers["X-Rillet-API-Version"] == "4"
        parts = url.removeprefix("https://sandbox.api.rillet.com/").split("/")
        stream = parts[0]
        rows = remote[stream]
        if method == "GET" and len(parts) == 1:
            # Match on a later page and ignore the wrong provider's reference.
            values = list(rows.values())
            if not params.get("cursor"):
                data = {
                    stream: [
                        {
                            "id": "unrelated",
                            "external_references": [{"type": "OtherSystem", "id": "nue-123"}],
                        }
                    ],
                    "pagination": {"next_cursor": "page-two"},
                }
            else:
                data = {stream: deepcopy(values), "pagination": {}}
        elif method == "GET":
            data = deepcopy(rows[parts[1]])
        else:
            assert "externalId" not in json and "id" not in json
            assert "updated_at" not in json
            assert not ("address" in json and "billing_addresses" in json)
            if stream == "products" and "include_in_ssp" in json:
                response = requests.Response()
                response.status_code = 400
                response._content = b'{"message":"SSP is disabled for this organization"}'
                response.request = requests.Request(method, url, json=json).prepare()
                return response
            record_id = parts[1] if method == "PUT" else f"remote-{len(rows) + 1}"
            data = {**deepcopy(json), "id": record_id}
            if stream == "products":
                data["include_in_ssp"] = rows.get(record_id, {}).get("include_in_ssp", True)
            rows[record_id] = data
        response = requests.Response()
        response.status_code = 200
        response._content = dumps(data).encode()
        return response

    monkeypatch.setattr(requests, "request", request)
    return remote


def sync(stream, record):
    schema = {"type": "object", "properties": {key: {} for key in record}}
    lines = [
        {"type": "SCHEMA", "stream": stream, "schema": schema, "key_properties": []},
        {"type": "RECORD", "stream": stream, "record": record},
    ]
    target = TargetRillet(config={"api_key": "test-only", "sandbox": True})
    target.listen(io.StringIO("\n".join(json.dumps(line) for line in lines) + "\n"))
    return target._latest_state


@pytest.mark.parametrize("stream", ["customers", "products"])
def test_create_repeat_and_update_reconcile_without_local_state(catalog_api, stream):
    record = {
        "externalId": "nue-123",
        "name": "Atlas",
        "external_references": [
            {"type": "Nue", "id": "nue-123", "url": "https://example.com/records/nue-123"}
        ],
    }
    if stream == "products":
        record.update(
            description="Analytics",
            price={"type": "ONE_TIME", "amount": {"amount": "100.00", "currency": "USD"}},
            include_in_arr_mrr=False,
            revenue_pattern="DAILY",
            account_code="4000",
        )
    first = sync(stream, deepcopy(record))
    bookmark = first["bookmarks"][stream][-1]
    assert bookmark["success"] and bookmark["externalId"] == "nue-123"
    remote_id = bookmark["id"]
    # A later sync must recover by external reference even without snapshots.
    repeated = sync(stream, deepcopy(record))["bookmarks"][stream][-1]
    assert repeated["success"] and repeated["id"] == remote_id
    stored = catalog_api[stream][remote_id]
    stored["fields"] = [{"field_id": "department", "value_id": "engineering"}]
    stored["external_references"].append({"type": "CRM", "id": "crm-456"})
    if stream == "customers":
        stored.update(
            emails=[{"id": "email-1", "email": "demo@example.com"}],
            payment_terms=45,
            billing_addresses=[],
            address=None,
            send_invoices_automatically=False,
        )
    changed = sync(stream, {**deepcopy(record), "id": remote_id, "name": "Atlas updated"})
    bookmark = changed["bookmarks"][stream][-1]
    assert bookmark["id"] == remote_id and bookmark["success"]
    assert changed["summary"][stream]["updated"] == 1
    assert len(catalog_api[stream]) == 1
    updated = catalog_api[stream][remote_id]
    assert updated["name"] == "Atlas updated"
    assert updated["fields"] == [{"field_id": "department", "value_id": "engineering"}]
    assert {"type": "CRM", "id": "crm-456"} in updated["external_references"]
    if stream == "customers":
        assert updated["emails"] == [{"id": "email-1", "email": "demo@example.com"}]
        assert updated["payment_terms"] == 45
        assert updated["send_invoices_automatically"] is False
    else:
        assert updated["include_in_ssp"] is True
        # An explicit source change must reach the API, not be silently dropped
        # to bypass the disabled-feature error.
        before = deepcopy(updated)
        explicit_ssp = sync(stream, {**record, "id": remote_id, "include_in_ssp": False})
        assert explicit_ssp["bookmarks"][stream][-1]["success"] is False
        assert catalog_api[stream][remote_id] == before

    # A custom mapping owns only selected dimensions, not every remote association.
    catalog_api[stream][remote_id]["fields"].append(
        {"field_id": "unrelated-region", "field_value_id": "west"}
    )
    mapped = sync(
        stream,
        {
            **record,
            "id": remote_id,
            "fields": [
                {"field_id": "department", "field_value_id": "finance"},
                {"field_id": "category", "field_value_id": "software"},
            ],
        },
    )
    assert mapped["bookmarks"][stream][-1]["success"]
    assert catalog_api[stream][remote_id]["fields"] == [
        {"field_id": "department", "field_value_id": "finance"},
        {"field_id": "unrelated-region", "field_value_id": "west"},
        {"field_id": "category", "field_value_id": "software"},
    ]


@pytest.mark.parametrize("stream", ["customers", "products"])
def test_conflicting_or_ambiguous_identity_fails_without_writes(catalog_api, stream):
    catalog_api[stream]["unrelated"] = {
        "id": "unrelated",
        "name": "Do not change",
        "external_references": [{"type": "Nue", "id": "different-source"}],
    }
    record = {
        "id": "unrelated",
        "externalId": "nue-123",
        "name": "Atlas",
        "external_references": [{"type": "Nue", "id": "nue-123"}],
    }
    state = sync(stream, deepcopy(record))
    assert state["bookmarks"][stream][-1]["success"] is False
    assert catalog_api[stream]["unrelated"]["name"] == "Do not change"
    for remote_id in ["duplicate-a", "duplicate-b"]:
        catalog_api[stream][remote_id] = {
            "id": remote_id,
            "name": "Duplicate",
            "external_references": [{"type": "Nue", "id": "nue-123"}],
        }
    record.pop("id")
    before = deepcopy(catalog_api[stream])
    assert sync(stream, record)["bookmarks"][stream][-1]["success"] is False
    assert catalog_api[stream] == before


@pytest.mark.parametrize("limited_method", ["GET", "POST"])
def test_read_rate_limit_recovers_but_write_is_not_replayed(
    catalog_api, monkeypatch, limited_method
):
    original = requests.request
    limited = False

    def request(method, url, **kwargs):
        nonlocal limited
        if method == limited_method and not limited:
            limited = True
            response = requests.Response()
            response.status_code = 429
            response.headers["Retry-After"] = "3"
            response._content = b'{"message":"Rate limited"}'
            return response
        return original(method, url, **kwargs)

    waits = []
    monkeypatch.setattr(requests, "request", request)
    monkeypatch.setattr("target_rillet.client.sleep", waits.append)
    record = {
        "externalId": "nue-123",
        "name": "Atlas",
        "external_references": [{"type": "Nue", "id": "nue-123"}],
    }
    result = sync("customers", deepcopy(record))
    if limited_method == "GET":
        assert result["bookmarks"]["customers"][-1]["success"]
        assert len(catalog_api["customers"]) == 1
        assert waits == [3]
    else:
        assert result["bookmarks"]["customers"][-1]["success"] is False
        assert catalog_api["customers"] == {}
        assert waits == []
        assert sync("customers", record)["bookmarks"]["customers"][-1]["success"]
        assert len(catalog_api["customers"]) == 1
