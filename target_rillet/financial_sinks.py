"""Externally billed AR contracts, explicit amendments, and invoices (API v4)."""

import hashlib
import json
from copy import deepcopy
from uuid import NAMESPACE_URL, UUID, uuid5

from target_rillet.ar_sinks import ReferencedSink


class FinancialSink(ReferencedSink):
    """Create/reuse financial documents without implicitly replacing them."""

    allows_upserts = False
    payload_fields = ()
    required_fields = ()

    @staticmethod
    def require(record, fields):
        missing = [key for key in fields if record.get(key) is None]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

    def resolve_parent(self, collection, source_id, reference_type, reference=None):
        if not source_id:
            raise ValueError(f"Missing {collection} relationship")
        # Resolve through SDK state/snapshots, but reject failed same-job parents
        # before the SDK can fall back to a stale snapshot or a failed remote ID.
        bookmarks = self._target._latest_state.get("bookmarks", {}).get(collection, [])
        parent = next(
            (row for row in reversed(bookmarks) if row.get("externalId") == source_id),
            None,
        )
        if parent and not parent.get("success"):
            raise ValueError(f"Cannot use failed {collection} parent {source_id}")
        remote_id = self._target._resolve_relation_external_id(collection, source_id)
        try:
            UUID(str(remote_id))
        except ValueError:
            raise ValueError(
                f"Unresolved {collection} ID {source_id}: sync parents first or supply a Rillet UUID"
            ) from None
        # Also validate direct UUID inputs; unresolved source UUIDs must not be
        # passed blindly to a financial write and left for the API to interpret.
        remote = self.request_api("GET", endpoint=f"/{collection}/{remote_id}").json()
        if str(remote.get("id")) != str(remote_id):
            raise ValueError(f"Invalid {collection} relationship {source_id}")
        expected = reference or {"type": reference_type, "id": source_id}
        if (remote_id != source_id or reference is not None) and not any(
            ref.get("id") == expected["id"] and ref.get("type") == expected["type"]
            for ref in remote.get("external_references", [])
        ):
            raise ValueError(
                f"Mapped {collection} parent does not reference {source_id}"
            )
        return remote_id

    def preprocess_record(self, record, context):
        identity = self.source_identity(record)
        if (
            record.get("scope", "REVENUE_RECOGNITION_ONLY")
            != "REVENUE_RECOGNITION_ONLY"
        ):
            raise ValueError(
                "Financial demo sinks only support REVENUE_RECOGNITION_ONLY scope"
            )
        self.require(record, self.required_fields)
        payload = {
            key: deepcopy(record[key]) for key in self.payload_fields if key in record
        }
        payload["scope"] = "REVENUE_RECOGNITION_ONLY"
        payload["externalId"] = record["externalId"]
        if record.get("id"):
            payload["id"] = record["id"]
        payload["customer_id"] = self.resolve_parent(
            "customers", record.get("customer_id"), identity["type"]
        )
        return payload

    def write_result(self, response, expected_id=None):
        data = response.json()
        if response.status_code == 202 or data.get("approval_request_id"):
            raise ValueError(
                "Rillet is awaiting approval; no successful write is recorded. "
                f"Resource: {data.get('contract_id') or data.get('invoice_id')}; "
                f"approval request: {data.get('approval_request_id')}. Reconcile before retrying."
            )
        remote_id = data.get("id")
        if not remote_id or (expected_id and remote_id != expected_id):
            raise ValueError(
                "Rillet response did not include the expected financial record ID"
            )
        return remote_id

    def upsert_record(self, record, context):
        identity = self.source_identity(record)
        # Revenue-only writes retain the built-in integration reference, not
        # custom source references. Rillet returns HOTGLUE as "Hotglue".
        integration_refs = [
            ref
            for ref in record["external_references"]
            if ref.get("type") in ("HOTGLUE", "Hotglue")
        ]
        if len(integration_refs) > 1:
            raise ValueError("Supply only one Hotglue integration reference")
        remote_identity = (
            {"type": "Hotglue", "id": integration_refs[0]["id"]}
            if integration_refs
            else identity
        )
        existing = self.find_existing(self.name, remote_identity, record.get("id"))
        if existing:
            if existing.get("customer_id") != record["customer_id"]:
                raise ValueError(
                    "Existing financial record belongs to a different customer"
                )
            if (
                record.get("subsidiary_id")
                and existing.get("subsidiary_id") != record["subsidiary_id"]
            ):
                raise ValueError(
                    "Existing financial record belongs to a different subsidiary"
                )
            if (
                record.get("contract_id")
                and existing.get("contract_id") != record["contract_id"]
            ):
                raise ValueError("Invoice belongs to a different contract")
            return existing["id"], True, {"existing": True}
        if record.get("contract_id"):
            raise ValueError(
                "Contract-linked invoice not found; include it in the contract/amendment "
                "invoice_schedule instead of creating a standalone invoice"
            )
        payload = {
            key: deepcopy(record[key]) for key in self.payload_fields if key in record
        }
        key = str(
            uuid5(
                NAMESPACE_URL,
                json.dumps(
                    [self.get_base_url(), self.name, identity["type"], identity["id"]],
                    separators=(",", ":"),
                ),
            )
        )
        response = self.request_api(
            "POST", request_data=payload, params={"idempotency_key": key}
        )
        return self.write_result(response), True, {}


class ContractsSink(FinancialSink):
    name = "contracts"
    endpoint = "/contracts"
    required_fields = ("name", "start_date", "customer_id", "items", "invoice_schedule")
    payload_fields = (
        "name",
        "close_date",
        "start_date",
        "end_date",
        "customer_id",
        "subsidiary_id",
        "items",
        "invoice_schedule",
        "external_references",
        "scope",
    )

    def prepare_items(self, items):
        if not items:
            raise ValueError("A contract requires at least one item")
        references = set()
        for item in items:
            self.require(
                item, ("product_id", "price", "revenue_pattern", "external_reference")
            )
            if "quantity" in item or "type" in item.get("price", {}):
                raise ValueError(
                    "Contract item price must be the total MonetaryAmount, not unit pricing"
                )
            reference = item["external_reference"]
            self.require(reference, ("type", "id"))
            if reference["id"] in references:
                raise ValueError("Contract item external reference IDs must be unique")
            references.add(reference["id"])
            item["product_id"] = self.resolve_parent(
                "products", item["product_id"], reference["type"]
            )
        return references

    def check_schedule(self, schedule, references):
        self.require(schedule, ("invoices",))
        invoice_references = set()
        for invoice in schedule["invoices"]:
            self.require(
                invoice,
                ("invoice_date", "invoice_number", "items", "external_references"),
            )
            if not invoice["items"] or not invoice["external_references"]:
                raise ValueError(
                    "Scheduled invoices require items and external references"
                )
            for reference in invoice["external_references"]:
                identity = (reference["type"], reference["id"])
                if identity in invoice_references:
                    raise ValueError(
                        "Scheduled invoice external references must be unique"
                    )
                invoice_references.add(identity)
            for item in invoice["items"]:
                self.require(
                    item,
                    (
                        "contract_item_reference",
                        "description",
                        "quantity",
                        "total_amount",
                        "external_references",
                    ),
                )
                if item["contract_item_reference"] not in references:
                    raise ValueError(
                        "Invoice schedule refers to an unknown contract item"
                    )

    def preprocess_record(self, record, context):
        payload = super().preprocess_record(record, context)
        references = self.prepare_items(payload["items"])
        self.check_schedule(payload["invoice_schedule"], references)
        return payload


class ContractAmendmentsSink(ContractsSink):
    """Apply immutable amendment events, guarded by the observed pre-change contract."""

    name = "contract-amendments"
    required_fields = (
        "contract_id",
        "contract_reference",
        "expected_contract_hash",
        "amendment_date",
        "amendment_reason",
        "items",
        "invoice_schedule",
    )
    payload_fields = (
        "amendment_date",
        "amendment_reason",
        "items",
        "invoice_schedule",
        "scope",
    )

    def preprocess_record(self, record, context):
        self.source_identity(record)
        self.require(record, self.required_fields)
        if (
            record.get("scope", "REVENUE_RECOGNITION_ONLY")
            != "REVENUE_RECOGNITION_ONLY"
        ):
            raise ValueError("Amendments only support REVENUE_RECOGNITION_ONLY scope")
        payload = deepcopy(record)
        payload["scope"] = "REVENUE_RECOGNITION_ONLY"
        contract_id = self.resolve_parent(
            "contracts",
            record["contract_id"],
            record["contract_reference"]["type"],
            record["contract_reference"],
        )
        contract = self.find_existing(
            "contracts", record["contract_reference"], contract_id
        )
        if record.get("id") and record["id"] != contract_id:
            raise ValueError("Amendment snapshot ID does not match its parent contract")
        completed = next(
            (
                row
                for row in self._target._latest_state.get("bookmarks", {}).get(
                    self.name, []
                )
                if row.get("externalId") == record["externalId"] and row.get("success")
            ),
            None,
        )
        if completed:
            if completed.get("id") != contract_id:
                raise ValueError(
                    "Amendment event was already applied to a different contract"
                )
            payload["id"] = completed["id"]
        payload["contract_id"] = contract_id
        context["contract"] = contract
        for item in payload["items"]:
            if item.get("type") == "NEW_ITEM":
                self.prepare_items([item])
        return payload

    def check_amendment(self, record, contract):
        references = {
            ref["id"]
            for item in contract["items"]
            for ref in item.get("external_references", [])
            if ref.get("type") == record["contract_reference"]["type"]
        }
        affected = set()
        for item in record["items"]:
            kind = item.get("type")
            if kind == "NEW_ITEM":
                reference = item["external_reference"]["id"]
                if reference in references:
                    raise ValueError(
                        "New amendment item already exists on the contract"
                    )
                references.add(reference)
            elif kind in ("AMENDING_ITEM", "TERMINATED_ITEM"):
                field = (
                    "amending_reference"
                    if kind == "AMENDING_ITEM"
                    else "external_reference"
                )
                self.require(
                    item, (field, "price") if kind == "AMENDING_ITEM" else (field,)
                )
                reference = item[field]
                if reference not in references:
                    raise ValueError("Amendment refers to an unknown contract item")
            else:
                raise ValueError(
                    "Amendment item type must be NEW_ITEM, AMENDING_ITEM, or TERMINATED_ITEM"
                )
            if reference in affected:
                raise ValueError(
                    "An amendment cannot change the same contract item twice"
                )
            affected.add(reference)
        self.check_schedule(record["invoice_schedule"], references)

    def upsert_record(self, record, context):
        contract = context["contract"]
        if record.get("id"):
            return contract["id"], True, {"existing": True}
        if (contract.get("approval") or {}).get("status") in ("WAITING", "PENDING"):
            raise ValueError(
                "Contract is awaiting approval; reconcile before submitting another amendment"
            )
        fingerprint = hashlib.sha256(
            json.dumps(
                contract,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if fingerprint != record["expected_contract_hash"]:
            raise ValueError(
                "Contract changed since amendment preparation; it may already have been applied. "
                "Reconcile before preparing a new amendment; do not blindly retry."
            )
        self.check_amendment(record, contract)
        response = self.request_api(
            "POST",
            endpoint=f"/contracts/{contract['id']}/amendments",
            request_data={key: deepcopy(record[key]) for key in self.payload_fields},
        )
        return self.write_result(response, contract["id"]), True, {"is_updated": True}


class InvoicesSink(FinancialSink):
    name = "invoices"
    endpoint = "/invoices"
    required_fields = ("customer_id", "invoice_date", "invoice_number", "items")
    payload_fields = (
        "customer_id",
        "invoice_date",
        "invoice_number",
        "due_date",
        "items",
        "tax_amount",
        "external_references",
        "subsidiary_id",
        "exchange_rate",
        "scope",
    )

    def preprocess_record(self, record, context):
        payload = super().preprocess_record(record, context)
        if not payload["items"]:
            raise ValueError("An invoice requires at least one item")
        for item in payload["items"]:
            self.require(
                item,
                (
                    "product_id",
                    "description",
                    "quantity",
                    "total_amount",
                    "external_references",
                ),
            )
            item["product_id"] = self.resolve_parent(
                "products", item["product_id"], self.source_identity(record)["type"]
            )
            if "customFields" in item:
                item["fields"] = self._resolve_custom_fields(item.pop("customFields"))
        if record.get("contract_id"):
            payload["contract_id"] = self.resolve_parent(
                "contracts",
                record["contract_id"],
                self.source_identity(record)["type"],
                record.get("contract_reference"),
            )
        return payload
