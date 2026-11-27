"""Accounts-receivable catalog sinks with external-reference reconciliation."""

from copy import deepcopy

from target_rillet.client import RilletSink


class ReferencedSink(RilletSink):
    """Match AR resources by typed source identity, never by display name."""

    allows_externalid = ("customers", "products", "contracts", "contract-amendments", "invoices")

    @property
    def api_version(self):
        return "4"

    def source_identity(self, record):
        if record.get("hg_error") is not None:
            raise ValueError(str(record["hg_error"]))
        source_id = record.get("externalId")
        references = record.get("external_references") or []
        identities = [ref for ref in references if ref.get("id") == source_id and ref.get("type")]
        if not source_id or len(identities) != 1:
            raise ValueError(
                "Supply externalId and exactly one matching typed external_references entry"
            )
        return identities[0]

    def find_existing(self, collection, identity, record_id=None):
        endpoint = f"/{collection}"
        source_id = identity["id"]
        existing = None
        if record_id:
            existing = self.request_api("GET", endpoint=f"{endpoint}/{record_id}").json()
            if not any(
                ref.get("type") == identity["type"] and ref.get("id") == source_id
                for ref in existing.get("external_references", [])
            ):
                raise ValueError(
                    "Target ID does not belong to the supplied source external reference"
                )
        else:
            params = {"limit": 100}
            # Contracts do not expose external-reference list filters in v4.
            if collection != "contracts":
                params["external_reference_id"] = source_id
                if collection != "products":
                    params["external_reference_type"] = identity["type"]
            matches = []
            while True:
                page = self.request_api("GET", endpoint=endpoint, params=params).json()
                for item in page[collection]:
                    if any(
                        ref.get("id") == source_id and ref.get("type") == identity["type"]
                        for ref in item.get("external_references", [])
                    ):
                        matches.append(item["id"])
                cursor = page.get("pagination", {}).get("next_cursor")
                if not cursor:
                    break
                params["cursor"] = cursor
            if len(set(matches)) > 1:
                raise ValueError(
                    "Multiple target records share the supplied source external reference"
                )
            if matches:
                existing = self.request_api("GET", endpoint=f"{endpoint}/{matches[0]}").json()
        return existing


class CatalogSink(ReferencedSink):
    """Reconcile stable source identities and preserve full-replace API fields."""

    payload_fields = ()

    def preprocess_record(self, record, context):
        identity = self.source_identity(record)
        source_id = identity["id"]
        references = record["external_references"]
        existing = self.find_existing(self.name, identity, record.get("id"))

        payload = {
            key: deepcopy(value)
            for key, value in (existing or {}).items()
            if key in self.payload_fields
        }
        payload.update(
            {key: deepcopy(value) for key, value in record.items() if key in self.payload_fields}
        )
        if existing:
            # Keep other integrations' identities during Rillet's full PUT.
            payload["external_references"] = deepcopy(existing.get("external_references") or [])
            for ref in references:
                if not any(
                    old.get("type") == ref.get("type") and old.get("id") == ref.get("id")
                    for old in payload["external_references"]
                ):
                    payload["external_references"].append(deepcopy(ref))
            payload["id"] = existing["id"]
        if "customFields" in record:
            payload["fields"] = self._resolve_custom_fields(record["customFields"])
        if payload.get("fields") and ("fields" in record or "customFields" in record):
            # Field mappings select only the associations owned by this integration.
            # Preserve unrelated dimensions during the API's full-replace PUT.
            associations = {
                field["field_id"]: deepcopy(field)
                for field in (existing or {}).get("fields", [])
            }
            associations.update({field["field_id"]: field for field in payload["fields"]})
            payload["fields"] = list(associations.values())
        # Modern address collections supersede the deprecated aliases returned
        # by GET; Rillet rejects a PUT containing both representations.
        for modern, legacy in [
            ("billing_addresses", "address"),
            ("shipping_addresses", "shipping_address"),
        ]:
            if legacy in record and modern not in record:
                payload.pop(modern, None)
            if modern in payload:
                payload.pop(legacy, None)
        payload["externalId"] = source_id
        return payload

    def upsert_record(self, record, context):
        # Keep SDK identity in the state-bearing record, never in the HTTP body.
        payload = {
            key: deepcopy(value) for key, value in record.items() if key in self.payload_fields
        }
        record_id = record.get("id")
        method = "PUT" if record_id else "POST"
        endpoint = f"{self.endpoint}/{record_id}" if record_id else self.endpoint
        response = self.request_api(method, endpoint=endpoint, request_data=payload)
        remote_id = response.json().get("id")
        if not remote_id:
            raise ValueError("Rillet response did not include a catalog record ID")
        return remote_id, True, {"is_updated": True} if record_id else {}


class CustomersSink(CatalogSink):
    name = "customers"
    endpoint = "/customers"
    payload_fields = (
        "name",
        "name_on_invoice",
        "address",
        "shipping_address",
        "billing_addresses",
        "shipping_addresses",
        "emails",
        "external_references",
        "payment_terms",
        "send_invoices_automatically",
        "send_payment_reminders",
        "allow_online_payments",
        "fields",
        "tax_ids",
    )


class ProductsSink(CatalogSink):
    name = "products"
    endpoint = "/products"
    payload_fields = (
        "type",
        "name",
        "description",
        "price",
        "include_in_arr_mrr",
        "include_in_ssp",
        "revenue_pattern",
        "progress_plan_template",
        "account_code",
        "status",
        "external_references",
        "fields",
        "bundle",
    )

    def preprocess_record(self, record, context):
        payload = super().preprocess_record(record, context)
        # GET returns this default even when SSP is disabled, in which case
        # PUT rejects it. Omitting it preserves the remote flag by API contract.
        if "include_in_ssp" not in record:
            payload.pop("include_in_ssp", None)
        return payload
