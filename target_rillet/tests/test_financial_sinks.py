"""Financial document policies and dependencies through the public Singer input."""

import hashlib
import io
import json
from copy import deepcopy
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest
import requests

from target_rillet.target import TargetRillet


def identity(source_id):
    return {"type": "nue", "id": source_id}


def source(source_id, **fields):
    return {
        "externalId": source_id,
        "external_references": [identity(source_id)],
        **fields,
    }


def amount(value):
    return {"amount": value, "currency": "USD"}


def sync(*records, state=None):
    lines = []
    for stream, record in records:
        lines.extend(
            [
                {
                    "type": "SCHEMA",
                    "stream": stream,
                    "key_properties": [],
                    "schema": {
                        "type": "object",
                        "properties": {key: {} for key in record},
                    },
                },
                {"type": "RECORD", "stream": stream, "record": deepcopy(record)},
            ]
        )
    target = TargetRillet(config={"api_key": "test-only", "sandbox": True})
    if state:
        target._latest_state = deepcopy(state)
    target.listen(io.StringIO("\n".join(json.dumps(line) for line in lines) + "\n"))
    return target._latest_state


def bookmark(state, stream):
    return state["bookmarks"][stream][-1]


@pytest.fixture
def api(monkeypatch, tmp_path):
    # Exercise real SDK snapshots while isolating these tests from job state.
    import hotglue_singer_sdk.target_sdk.target as sdk

    monkeypatch.setattr(sdk, "SNAPSHOT_DIR", str(tmp_path))
    monkeypatch.setattr(sdk, "flow_id", "demo")
    monkeypatch.setattr(
        TargetRillet, "incremental_target_state_path", str(tmp_path / "state.json")
    )
    remote = {name: {} for name in ("customers", "products", "contracts", "invoices")}
    behavior = {
        "pending": None,
        "lost_response": None,
        "reject_parent": None,
        "canonical_hotglue": False,
    }
    writes = []

    def respond(status, data):
        response = requests.Response()
        response.status_code = status
        response._content = json.dumps(data).encode()
        return response

    def request(method, url, params=None, json=None, headers=None, **kwargs):
        assert url.startswith("https://sandbox.api.rillet.com/")
        assert headers["X-Rillet-API-Version"] == "4"
        parts = url.removeprefix("https://sandbox.api.rillet.com/").split("/")
        collection = parts[0]
        rows = remote[collection]
        if method == "GET":
            if len(parts) > 1:
                return (
                    respond(200, deepcopy(rows[parts[1]]))
                    if parts[1] in rows
                    else respond(404, {"message": "Not found"})
                )
            if collection == "contracts":
                assert "external_reference_id" not in params
            # A wrong-provider match on the first page cannot win reconciliation.
            if not params.get("cursor"):
                return respond(
                    200,
                    {
                        collection: [
                            {
                                "id": "unrelated",
                                "external_references": [
                                    {"type": "other", "id": "order-a"}
                                ],
                            }
                        ],
                        "pagination": {"next_cursor": "older-records"},
                    },
                )
            return respond(
                200, {collection: deepcopy(list(rows.values())), "pagination": {}}
            )
        assert method in ("POST", "PUT")
        assert "externalId" not in json and "id" not in json
        writes.append((method, parts, deepcopy(json), deepcopy(params)))
        if collection == behavior["reject_parent"]:
            return respond(422, {"message": "Parent rejected"})
        if collection in ("contracts", "invoices"):
            assert method == "POST"  # Never replace a financial document.
            assert json["scope"] == "REVENUE_RECOGNITION_ONLY"
            if len(parts) == 1:
                UUID(params["idempotency_key"])
        if collection == behavior["pending"]:
            return respond(
                202,
                {
                    "approval_request_id": "approval-123",
                    "id": "not-effective",
                    (
                        "contract_id" if collection == "contracts" else "invoice_id"
                    ): "proposed-123",
                },
            )
        if parts[-1] == "amendments":
            contract = rows[parts[1]]
            for item in json["items"]:
                if item["type"] == "NEW_ITEM":
                    contract["items"].append(
                        {
                            **deepcopy(item),
                            "external_references": [item["external_reference"]],
                        }
                    )
                else:
                    ref = item.get("amending_reference") or item["external_reference"]
                    matching = next(
                        i
                        for i in contract["items"]
                        if identity(ref) in i["external_references"]
                    )
                    if item["type"] == "AMENDING_ITEM":
                        matching["price"] = deepcopy(item["price"])
                    else:
                        matching["end_date"] = item.get(
                            "end_date", json["amendment_date"]
                        )
            contract["updated_at"] = "after-amendment"
            data = deepcopy(contract)
        else:
            remote_id = (
                parts[1]
                if method == "PUT"
                else str(uuid5(NAMESPACE_URL, f"{collection}/{len(rows)}"))
            )
            data = {**deepcopy(json), "id": remote_id}
            rows[remote_id] = data
            if behavior["canonical_hotglue"] and collection in (
                "contracts",
                "invoices",
            ):
                data["external_references"] = [
                    {**ref, "type": "Hotglue"}
                    for ref in data["external_references"]
                    if ref["type"] == "HOTGLUE"
                ]
            if collection == "contracts":
                for item in data["items"]:
                    item["external_references"] = [item.pop("external_reference")]
                for invoice in data["invoice_schedule"]["invoices"]:
                    invoice_id = str(uuid5(NAMESPACE_URL, invoice["invoice_number"]))
                    remote["invoices"][invoice_id] = {
                        **deepcopy(invoice),
                        "id": invoice_id,
                        "contract_id": remote_id,
                        "customer_id": data["customer_id"],
                        "subsidiary_id": data.get("subsidiary_id"),
                    }
                    if behavior["canonical_hotglue"]:
                        remote["invoices"][invoice_id]["external_references"] = [
                            {**ref, "type": "Hotglue"}
                            for ref in invoice["external_references"]
                            if ref["type"] == "HOTGLUE"
                        ]
        if collection == behavior["lost_response"]:
            behavior["lost_response"] = None
            raise requests.Timeout("Response lost after remote commit")
        return respond(200, deepcopy(data))

    monkeypatch.setattr(requests, "request", request)
    return remote, writes, behavior, tmp_path


@pytest.fixture
def parents(api):
    state = sync(
        ("customers", source("customer-a", name="Atlas")),
        ("products", source("product-a", name="Subscription")),
        ("products", source("product-b", name="Services")),
    )
    mapping = {}
    for stream in ("customers", "products"):
        for row in state["bookmarks"][stream]:
            assert row["success"]
            mapping[row["externalId"]] = row["id"]
        (api[3] / f"{stream}_demo.snapshot.csv").write_text(
            "InputId,RemoteId\n"
            + "".join(
                f"{row['externalId']},{row['id']}\n"
                for row in state["bookmarks"][stream]
            )
        )
    api[1].clear()
    return mapping


def contract():
    return source(
        "order-a",
        name="Atlas order",
        customer_id="customer-a",
        start_date="2026-11-19",
        items=[
            {
                "product_id": "product-a",
                "price": amount("12000.00"),
                "revenue_pattern": "DAILY",
                "external_reference": identity("order-line-a"),
            }
        ],
        invoice_schedule={"invoices": []},
    )


def invoice():
    return source(
        "invoice-a",
        customer_id="customer-a",
        invoice_date="2026-11-19",
        invoice_number="INV-DEMO-A",
        items=[
            {
                "product_id": "product-a",
                "description": "Prorated subscription",
                "quantity": 10,
                "total_amount": amount("400.00"),
                "external_references": [identity("invoice-line-a")],
                "customFields": [
                    {
                        "field_id": "00000000-0000-4000-8000-000000000001",
                        "field_value_id": "00000000-0000-4000-8000-000000000002",
                    }
                ],
                "revenue": {
                    "period": {"start": "2026-11-19", "end": "2026-11-30"},
                    "pattern": "DAILY",
                },
            }
        ],
    )


@pytest.mark.parametrize(
    "stream,factory", [("contracts", contract), ("invoices", invoice)]
)
@pytest.mark.parametrize("canonical_hotglue", [False, True])
def test_create_lost_response_reconcile_repeat_and_changed_input_skip(
    api, parents, stream, factory, canonical_hotglue
):
    remote, writes, behavior, _ = api
    behavior["canonical_hotglue"] = canonical_hotglue
    original = factory()
    if canonical_hotglue:
        original["external_references"].append(
            {"type": "HOTGLUE", "id": "nue:" + original["externalId"]}
        )
    behavior["lost_response"] = stream
    first = sync((stream, original))
    assert not bookmark(first, stream)["success"]
    assert len(remote[stream]) == 1
    expected_id = next(iter(remote[stream]))
    stored = deepcopy(remote[stream][expected_id])
    assert stored["customer_id"] == parents["customer-a"]
    assert stored["items"][0]["product_id"] == parents["product-a"]
    assert stored["items"][0][
        "price" if stream == "contracts" else "total_amount"
    ] == amount("12000.00" if stream == "contracts" else "400.00")
    if stream == "invoices":
        assert "customFields" not in stored["items"][0]
        assert stored["items"][0]["fields"] == [
            {
                "field_id": "00000000-0000-4000-8000-000000000001",
                "field_value_id": "00000000-0000-4000-8000-000000000002",
            }
        ]
    recovered = sync((stream, original))
    assert bookmark(recovered, stream)["success"]
    assert bookmark(recovered, stream)["id"] == expected_id
    assert bookmark(recovered, stream)["externalId"] == original["externalId"]
    assert recovered["summary"][stream]["existing"] == 1
    changed = deepcopy(original)
    changed["items"][0]["price" if stream == "contracts" else "total_amount"] = amount(
        "99999.00"
    )
    again = sync((stream, changed))
    assert again["summary"][stream]["existing"] == 1
    assert remote[stream][expected_id] == stored
    (api[3] / f"{stream}_demo.snapshot.csv").write_text(
        f"InputId,RemoteId\n{original['externalId']},{expected_id}\n"
    )
    repeated = sync((stream, original))
    assert bookmark(repeated, stream)["success"]
    assert bookmark(repeated, stream)["id"] == expected_id
    assert len(writes) == 1


@pytest.mark.parametrize("canonical_hotglue", [False, True])
def test_same_job_parents_resolve_before_contract_and_scheduled_invoice_is_reused(
    api, canonical_hotglue
):
    record = contract()
    bill = invoice()
    api[2]["canonical_hotglue"] = canonical_hotglue
    if canonical_hotglue:
        for document in (record, bill):
            document["external_references"].append(
                {"type": "HOTGLUE", "id": "nue:" + document["externalId"]}
            )
        bill["contract_reference"] = {"type": "Hotglue", "id": "nue:order-a"}
    scheduled = deepcopy(bill)
    scheduled.pop("customer_id")
    scheduled.pop("externalId")
    scheduled.pop("contract_reference", None)
    scheduled["items"][0].pop("product_id")
    scheduled["items"][0].pop("revenue")
    scheduled["items"][0].pop("customFields")
    scheduled["items"][0]["quantity"] = "10"
    scheduled["items"][0]["contract_item_reference"] = "order-line-a"
    record["invoice_schedule"]["invoices"] = [scheduled]
    state = sync(
        ("customers", source("customer-a", name="Atlas")),
        ("products", source("product-a", name="Subscription")),
        ("contracts", record),
        ("invoices", {**bill, "contract_id": "order-a"}),
    )
    for stream in ("customers", "products", "contracts", "invoices"):
        assert bookmark(state, stream)["success"]
    contract_id = bookmark(state, "contracts")["id"]
    invoice_id = bookmark(state, "invoices")["id"]
    assert api[0]["invoices"][invoice_id]["contract_id"] == contract_id
    assert (
        api[0]["contracts"][contract_id]["customer_id"]
        == bookmark(state, "customers")["id"]
    )
    assert state["summary"]["invoices"]["existing"] == 1
    assert not any(parts[0] == "invoices" for _, parts, _, _ in api[1])
    if canonical_hotglue:
        # An explicit parent reference must still guard a direct Rillet UUID.
        bill["contract_id"] = contract_id
        bill["contract_reference"]["id"] = "nue:another-order"
        rejected = sync(("invoices", bill), state=state)
        assert not bookmark(rejected, "invoices")["success"]
        assert "Mapped contracts parent" in str(bookmark(rejected, "invoices"))


@pytest.mark.parametrize("parent_stream", ["customers", "products"])
def test_failed_parent_cannot_fall_back_to_successful_snapshot(
    api, parents, parent_stream
):
    api[2]["reject_parent"] = parent_stream
    source_id = "customer-a" if parent_stream == "customers" else "product-a"
    state = sync(
        (parent_stream, source(source_id, name="Rejected change")),
        ("contracts", contract()),
        ("invoices", invoice()),
    )
    assert not bookmark(state, parent_stream)["success"]
    for stream in ("contracts", "invoices"):
        assert not bookmark(state, stream)["success"]
        assert not api[0][stream]


@pytest.mark.parametrize(
    "stream,factory", [("contracts", contract), ("invoices", invoice)]
)
def test_approval_is_failure_and_retries_keep_the_create_key(
    api, parents, stream, factory
):
    api[2]["pending"] = stream
    for _ in range(2):
        records = [(stream, factory())]
        if stream == "contracts":
            records.append(("invoices", {**invoice(), "contract_id": "order-a"}))
        state = sync(*records)
        row = bookmark(state, stream)
        assert not row["success"] and not row.get("id")
        assert "approval-123" in str(row)
        if stream == "contracts":
            assert not bookmark(state, "invoices")["success"]
            assert not api[0]["invoices"]
    assert not api[0][stream]
    assert api[1][0][3]["idempotency_key"] == api[1][1][3]["idempotency_key"]


def amendment(remote_contract):
    fingerprint = hashlib.sha256(
        json.dumps(remote_contract, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return source(
        "order-a:amendment-1",
        contract_id=remote_contract["id"],
        contract_reference=identity("order-a"),
        expected_contract_hash=fingerprint,
        amendment_date="2026-12-01",
        amendment_reason="Add licenses",
        invoice_schedule={"invoices": []},
        items=[
            {
                "type": "AMENDING_ITEM",
                "amending_reference": "order-line-a",
                "price": amount("15000.00"),
            },
            {
                "type": "NEW_ITEM",
                "product_id": "product-b",
                "price": amount("2500.00"),
                "revenue_pattern": "DAILY",
                "external_reference": identity("order-line-b"),
            },
        ],
    )


def test_amendment_updates_then_replays_with_state_snapshot_and_lost_state(
    api, parents
):
    original = contract()
    original["items"].append(
        {
            "product_id": "product-b",
            "price": amount("7500.00"),
            "revenue_pattern": "DAILY",
            "external_reference": identity("order-line-old"),
        }
    )
    state = sync(("contracts", original))
    contract_id = bookmark(state, "contracts")["id"]
    record = amendment(api[0]["contracts"][contract_id])
    record["items"].append(
        {
            "type": "TERMINATED_ITEM",
            "external_reference": "order-line-old",
            "end_date": "2026-12-03",
        }
    )
    first = sync(("contract-amendments", record))
    assert bookmark(first, "contract-amendments")["success"]
    assert bookmark(first, "contract-amendments")["externalId"] == record["externalId"]
    assert bookmark(first, "contract-amendments")["id"] == contract_id
    assert first["summary"]["contract-amendments"]["updated"] == 1
    changed = api[0]["contracts"][contract_id]
    assert changed["items"][0]["price"] == amount("15000.00")
    assert changed["items"][1]["end_date"] == "2026-12-03"
    assert changed["items"][2]["product_id"] == parents["product-b"]
    repeated = sync(("contract-amendments", record), state=first)
    assert bookmark(repeated, "contract-amendments")["success"]
    # Even changed content cannot resubmit an already-completed immutable event.
    revised = amendment(changed)
    revised["items"][0]["price"] = amount("18000.00")
    repeated = sync(("contract-amendments", revised), state=first)
    assert repeated["summary"]["contract-amendments"]["existing"] == 1
    assert changed["items"][0]["price"] == amount("15000.00")
    no_state = sync(("contract-amendments", record))
    assert not bookmark(no_state, "contract-amendments")["success"]
    assert "may already have been applied" in str(
        bookmark(no_state, "contract-amendments")
    )
    (api[3] / "contract-amendments_demo.snapshot.csv").write_text(
        f"InputId,RemoteId\n{record['externalId']},{contract_id}\n"
    )
    repeated = sync(("contract-amendments", record))
    assert bookmark(repeated, "contract-amendments")["success"]
    assert repeated["summary"]["contract-amendments"]["existing"] == 1
    assert len([w for w in api[1] if w[1][-1] == "amendments"]) == 1


@pytest.mark.parametrize("outcome", ["lost_response", "pending"])
def test_ambiguous_amendment_does_not_report_success_or_replay(api, parents, outcome):
    state = sync(("contracts", contract()))
    contract_id = bookmark(state, "contracts")["id"]
    record = amendment(api[0]["contracts"][contract_id])
    api[2][outcome] = "contracts"
    first = sync(("contract-amendments", record))
    assert not bookmark(first, "contract-amendments")["success"]
    if outcome == "pending":
        api[0]["contracts"][contract_id]["approval"] = {"status": "WAITING"}
    second = sync(("contract-amendments", record))
    assert not bookmark(second, "contract-amendments")["success"]
    assert len([w for w in api[1] if w[1][-1] == "amendments"]) == 1


@pytest.mark.parametrize(
    "case",
    [
        "full",
        "bad_schedule",
        "unit_price",
        "missing_contract_invoice",
        "conflicting_id",
        "duplicate",
    ],
)
def test_invalid_or_ambiguous_documents_do_not_write(api, parents, case):
    stream, record = "contracts", contract()
    if case == "full":
        record["scope"] = "FULL"
    elif case == "bad_schedule":
        bill = invoice()
        bill["items"][0]["contract_item_reference"] = "missing-line"
        record["invoice_schedule"]["invoices"] = [bill]
    elif case == "unit_price":
        record["items"][0]["quantity"] = 10
    elif case == "missing_contract_invoice":
        created = sync(("contracts", contract()))
        stream, record = "invoices", {
            **invoice(),
            "contract_id": bookmark(created, "contracts")["id"],
        }
    else:
        created = sync(("contracts", contract()))
        contract_id = bookmark(created, "contracts")["id"]
        if case == "conflicting_id":
            record["id"] = contract_id
            api[0]["contracts"][contract_id]["external_references"] = [
                identity("another-order")
            ]
        else:
            other = deepcopy(api[0]["contracts"][contract_id])
            other["id"] = str(uuid5(NAMESPACE_URL, "duplicate-contract"))
            api[0]["contracts"][other["id"]] = other
    before = deepcopy(api[0])
    api[1].clear()
    state = sync((stream, record))
    assert not bookmark(state, stream)["success"]
    assert api[0] == before
    assert not api[1]
