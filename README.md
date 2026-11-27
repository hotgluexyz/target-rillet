# target-rillet

`target-rillet` is a Singer target for [Rillet](https://rillet.com/), built with the Hotglue Singer SDK.

## Overview

This target allows you to sync data to Rillet via their REST API. It currently supports the following streams:

- **Journals** - Create and update journal entries
- **Bank accounts** - Create cash/bank accounts; reuse existing matches by GL mapping
- **Customers / products** - Create/update via API v4 with typed external-reference matching
- **Contracts / contract amendments / invoices** - Externally billed AR documents via API v4

## Installation

```bash
pip install target-rillet
```

Or install from source:

```bash
git clone https://github.com/hotglue/target-rillet.git
cd target-rillet
pip install .
```

## Configuration

### Required Settings

| Setting   | Description                              |
|-----------|------------------------------------------|
| `api_key` | Your Rillet API key                      |
| `sandbox` | Use the sandbox environment (default: false) |
| `create_missing_field_values` | When `true`, create missing custom field values in Rillet during resolve (default: `false`) |

Create a `config.json` file:

```json
{
  "api_key": "your-rillet-api-key",
  "sandbox": false
}
```

### Environments

- **Production**: `https://api.rillet.com`
- **Sandbox**: `https://sandbox.api.rillet.com`

Set `"sandbox": true` in your config to use the sandbox environment.

## Supported Streams

### Customers and products (`customers`, `products`)

These dedicated sinks take Rillet API v4 request fields plus `externalId` for
Hotglue state. Supply `external_references` with exactly one entry whose `id`
matches `externalId`, for example `[{"type":"Nue","id":"source-record-id"}]`.
The reference type must already be registered in Rillet; the sinks do not create it.
Product SKU may be used as the stable source identity if the ETL consistently uses
it for both `externalId` and the reference ID. Names are never used for matching.

Before create, the sink follows all filtered list pages and matches both reference
type and ID. An explicit/snapshot target ID is checked against that same identity.
Ambiguous matches or conflicting IDs fail the record instead of guessing. Repeating
an import without local state resolves the existing remote object rather than
creating another one. Concurrent writers are not serialized; run one import per
source identity at a time. Ambiguous failed POSTs are not automatically replayed.

Updates retrieve the complete remote object and preserve omitted writable fields
before PUT because Rillet updates are full replacements. Explicit source values,
including empty collections, replace the corresponding field. Other integrations'
external references are preserved. `externalId`, read-only response fields, and
the target ID are excluded from HTTP payloads. `customFields` uses the existing
Rillet custom-field resolver. These sinks use API version 4 regardless of the
default version used by older sinks.

Customer create requires `name`. Product create requires `name`, `description`,
`price`, `include_in_arr_mrr`, and, for standalone products, `account_code` and
`revenue_pattern`. Revenue account and recognition behavior belong in the mapping,
not hardcoded in the target. The sinks do not enable invoice emails or payments by
default; omit those settings or explicitly set them false for a demo.

Local Singer-path tests cover create, repeat import without state, updates that
preserve remote fields, paginated identity lookup, and rejection of conflicts.
Live sandbox validation created two synthetic customers and two products, repeated
the import without snapshots, updated/restored names through SDK snapshots, and
reused successful target state to skip unchanged records. The same four remote
IDs were retained; conflicting IDs were rejected without writes. All pre-existing
catalog records were unchanged, and customer emails/payments remained disabled.

Rillet returns `include_in_ssp` on products even when the organization's SSP feature
is disabled, but rejects that field on PUT in that case. The product sink only
sends it when explicitly supplied by the source; omission preserves its existing
value. An explicitly supplied unsupported value still fails normally.

### Financial documents (`contracts`, `contract-amendments`, `invoices`)

These dedicated API v4 sinks support **externally billed** documents only. They
default `scope` to `REVENUE_RECOGNITION_ONLY` and reject other scopes; they do not
use Rillet's `FULL` billing workflow. They take native API fields plus `externalId`
and the same matching typed `external_references` required by the catalog sinks.
Use the registered reference slug (`nue` in the demo), not its display label.

Live API validation also requires a recognized integration reference on revenue-only
contracts, invoices, and invoice lines. The custom `nue` reference alone is rejected.
For the Hotglue mapping, add a second reference such as
`{ "type": "HOTGLUE", "id": "nue:source-record-id", "url": "https://app.sandbox.nue.io/" }`.
Rillet requires `url` on this reference. Keep its ID namespaced so it does not
compete with the one reference whose ID equals `externalId`; the source identity
and customer/product mappings remain `nue`. The URL above is the source app's
landing page, not a verified record deep link.

The live API drops the custom `nue` reference on these financial documents and
returns the built-in reference as **`Hotglue`**, even when sent as `HOTGLUE`.
When a Hotglue integration reference is supplied, financial reconciliation uses
that exact ID with the canonical response type. It still retains the original
source `externalId` in SDK bookmarks/snapshots and in create idempotency keys.
Only this verified built-in spelling is normalized; custom types remain exact.

Send streams in dependency order: customers, products, contracts, amendments,
invoices. Customer IDs and nested product IDs can be source IDs resolved through
the SDK's same-job bookmarks or prior snapshots, or explicit Rillet UUIDs. Send
the parents or provide snapshots even when a remote record already exists. Failed
same-job parents block children rather than falling back to stale mappings. Mapped
parents are checked against the source reference type/ID; explicit remote UUIDs
are retrieved and validated. The target does not match relationships by names.

**Existing financial documents are reused, not overwritten.** Contract and invoice
identity lookup follows every list page and verifies explicit/snapshot IDs. An
existing document is reported as `existing`, even if incoming amounts changed;
it is never silently PUT. Customer/subsidiary conflicts fail. Use the explicit
amendment stream for contract changes. Invoice replacement/update, payments,
credits, and sending are outside these sinks' current contract.

#### Contracts

Required fields: `name`, `start_date`, `customer_id`, nonempty `items`,
`invoice_schedule`, and source identity. Optional native fields are `close_date`,
`end_date`, and `subsidiary_id`.

Each item requires `product_id`, `price`, `revenue_pattern`, and a unique typed
`external_reference`. In revenue-only scope, `price` is the **total line value**
as `{ "amount": "12000.00", "currency": "USD" }`, not a recurring Price object
or a unit price multiplied by a `quantity` property. Calculate totals in the ETL.
Optional item dates/discount/declared SSP follow the native API contract.

`invoice_schedule` is `{ "invoices": [...] }`. Each
scheduled invoice requires `invoice_date`, `invoice_number`, `external_references`,
and nonempty `items`. Schedule lines require `description`, `quantity` (a numeric
string), `total_amount`, `external_references`, and `contract_item_reference` equal
to a contract item's **source external reference ID**, not its remote UUID.
Unknown line references and duplicate invoice identities fail before POST.

The live API requires each contract item's value to equal the sum of its scheduled
invoice line amounts. An empty or partial schedule is not sufficient for a nonzero
line. The Atlas fixture's USD 12,000 subscription has only USD 400 on its current
invoice; Rillet rejects that combination with
`InvoicedAmountDoesNotMatchContractItemAmount`. A full-order demo therefore needs
the remaining USD 11,600 in the schedule, or an explicitly separate invoice-backed
contract slice. Do not inflate the original USD 2,900 Nue invoice to match the
USD 14,500 order. The same accounting constraint must be considered when preparing
amendments; a price increase cannot be assumed to work with no matching invoices.

The contract schedule creates the linked invoices. Do not also create these as
standalone invoices. The invoice sink can recover their remote IDs by reference.
Contract lookup scans all contract pages because v4 has no external-reference
filter for that resource.

#### Explicit amendments

Use one immutable source event ID per `contract-amendments` record, for example
`order-id:revision-2`, as `externalId` and in `external_references`. Required fields:

- `contract_id`: source order ID (with an SDK mapping) or Rillet contract UUID.
- `contract_reference`: the parent's persisted typed reference, for example
  `{ "type": "Hotglue", "id": "nue:order-id" }` for the Hotglue mapping above.
  Use the canonical response spelling, not the uppercase request spelling.
- `expected_contract_hash`: SHA-256 of the full `GET /contracts/{id}` JSON,
  serialized using Python `json.dumps(contract, sort_keys=True, separators=(",", ":"))`.
  Capture it once when preparing the event; **do not recalculate it on retry**.
- `amendment_date`, `amendment_reason`, `items`, and `invoice_schedule`: the native
  revenue-only amendment request. Omitted existing items stay unchanged.

Items use the API's `NEW_ITEM`, `AMENDING_ITEM`, or `TERMINATED_ITEM` discriminator.
New items have the contract-create item fields; amending items require `price`
and `amending_reference` (the original source line reference ID); terminated items
require a string `external_reference`. Dates and discount follow the API schema.
Repeated changes to one line in a single event are rejected. The target posts to
`/contracts/{id}/amendments`, never a generic contract PUT.

Successful bookmarks/snapshots map the amendment event ID to the parent contract
UUID and prevent resubmission. An amendment snapshot asserts that this immutable
event was successfully applied; do not manufacture one or reuse an event ID for
a different change. With lost state, a changed remote fingerprint stops the
event for reconciliation instead of risking a second amendment. A pending remote
approval also blocks submission. The fingerprint is a precondition, not an atomic
server-side lock or provider idempotency guarantee: serialize writers, and inspect
any ambiguous timeout/approval before rerunning. Rillet's amendment endpoint does
not support an idempotency key.

#### Invoices

Standalone invoice creates require `customer_id`, `invoice_date`, `invoice_number`,
nonempty `items`, and source identity. Each item requires `product_id`, `description`,
numeric `quantity`, `total_amount`, and `external_references`. Optional native
fields include `due_date`, `tax_amount`, `subsidiary_id`, and `exchange_rate`.
Line `revenue` can specify `period: { "start": "YYYY-MM-DD", "end": "YYYY-MM-DD" }`,
`pattern`, and `account_code`. Line `fields` accepts native assignments; line
`customFields` uses the existing field resolver. Contract create/schedule schemas
do not support those custom-field assignments; map only API-supported fields.

An optional `contract_id` is a **link-validation/reuse guard**, not a create field.
It resolves through SDK state/snapshots and requires an existing invoice with the
same typed source identity, customer, and contract. If it is absent remotely, the
sink fails with instructions to use the contract/amendment schedule, rather than
creating a disconnected invoice. Never match invoices by invoice number alone.
For source-ID parent mappings whose persisted identity differs from the raw source
ID, also supply `contract_reference`, for example
`{ "type": "Hotglue", "id": "nue:order-id" }`. This validates the mapped parent
without inventing a source-ID prefix or bypassing reference ownership checks.
An explicitly supplied reference is also checked for a direct contract UUID.

#### Retry and approval behavior

Creates send deterministic UUID idempotency keys derived from host, stream, source
reference type, and ID. For contracts this protects create retries; Rillet only
honors invoice keys for approval proposals, not legacy creation or contract-linked
invoices. Remote reconciliation is therefore still necessary and concurrent
imports for the same identity are unsupported. Writes are not automatically retried.
HTTP 202/approval proposals produce a failed bookmark with the approval/resource
IDs, not a false success or a usable downstream parent mapping.

Local Singer-path tests cover create/reuse, changed-input skip, pagination,
snapshots, same-job dependencies, failed-parent blocking, all three amendment
item types, lost-response/state cases, and pending approvals. Representative
outgoing payloads were also checked against the downloaded official OpenAPI.
Live sandbox attempts confirmed the integration-reference, URL, and full-schedule
requirements above. Three initial attempts were rejected before creation. The
subsequent approved full schedule created one USD 14,500 Atlas contract and two
linked invoices: the original USD 2,900 draft fixture and a clearly labeled
synthetic USD 11,600 remaining-value fixture (not a Nue-issued invoice).

The first invoice-ID reconciliation failed because of Rillet's reference
normalization. After correcting financial identity matching, the actual target CLI
recovered all three existing IDs. Fresh-state imports, SDK snapshots, and prior
successful target state all reused those same IDs. These repeat runs enforced an
HTTP write barrier, and conflicting financial/parent references failed without
writes. Final full inventories contained exactly one additional contract and two
additional invoices; all 1,272 pre-existing inventory rows were unchanged. Both
invoices remained UNBILLED, unsent, and unchanged through all repeat checks; the
customer had no email recipients, automatic sending, reminders, or online payments.

**The revenue-only date-extension amendment is now live-validated.** The amendment
preview requires an effective date at least one day after contract/item start.
Using its documented full-contract request shape with a next-day USD 12,000
one-time continuing item and the extended end date returned USD 26,500 total
contract value, rather than the intended unchanged USD 14,500. That preview rejects
the revenue-only amendment request shape, so it does not establish the outcome of
the dedicated revenue-only write.

The first explicitly approved revenue-only probe submitted one amendment POST
through the target CLI: effective 2026-11-20, continuing-item price USD 12,000,
service end 2027-12-18, and the existing two-invoice schedule. Rillet rejected it
with HTTP 400 `InvoicedAmountDoesNotMatchContractItemAmount`, reporting USD 12,000
item value against USD 11,600 invoiced value. The submitted schedule included the
earlier USD 400 subscription invoice line dated 2026-11-19 and the remaining
USD 11,600 line dated 2026-12-01.

The target correctly emitted a failed bookmark. The probe made exactly one POST,
without retry. Expanded contract and invoice GETs and all four full inventories
were unchanged after rejection: USD 14,500 contract, the same two UNBILLED/unsent
invoices, and no changes to customers/products or unrelated documents.

The separately approved second probe submitted exactly one POST with the same
effective date, end-date extension, and schedule, but USD 11,600 continuing-item
price. Rillet returned HTTP 200 and the target emitted a successful updated
bookmark. The subscription became a USD 400 historical AMENDED item ending
2026-11-19 plus a USD 11,600 ACTIVE item covering 2026-11-20 through 2027-12-18.
The USD 2,500 implementation item and USD 14,500 total contract value remained
unchanged. In this fixture the continuing price must match the remaining invoice
value after the amendment cutover, not the original USD 12,000 subscription value.
The contract header end date stayed 2027-11-18; the extension is on the continuing
subscription item, not a header update.

Both invoice IDs, amounts, and stable external references were retained. Rillet
regenerated their internal invoice-line IDs and updated timestamps, but all other
expanded invoice fields were unchanged; both invoices remained UNBILLED/unsent.
Do not use internal invoice-line UUIDs as stable source identities.

Read-only state and snapshot replays reused the successful amendment; losing
state rejected the old event because its contract fingerprint changed. A combined
contract/invoice/amendment repeat reused all existing IDs. All four checks made
only GET requests, with zero attempted writes. Final inventories retained one
demo contract and two demo invoices, and all 1,272 pre-existing inventory rows
were unchanged. No Nue writes, sends, or payment actions were performed. The
end-to-end Nue ETL remains unvalidated; the native remaining-value/date-extension
mapping is now proven for this sandbox fixture, not all amendment scenarios.

### Journals

Posts journal entries to Rillet via `POST /journal-entries`. Supports both create and update operations.

**Expected record format:**

```json
{
  "name": "Monthly Accrual",
  "currency": "USD",
  "date": "2025-01-31",
  "items": [
    {
      "amount": "1000.00",
      "currency": "USD",
      "account_code": "40100",
      "side": "DEBIT",
      "description": "Revenue accrual"
    },
    {
      "amount": "1000.00",
      "currency": "USD",
      "account_code": "20100",
      "side": "CREDIT",
      "description": "Deferred revenue"
    }
  ]
}
```

**Optional fields:** `subsidiary_id`, `reversal_date`, `attachmentUrl`, `exchange_rate`

To **update** an existing journal entry, include `journal_entry_id` in the record.

### Bank accounts (`bank-accounts`)

Creates cash/bank accounts in Rillet (`POST /bank-accounts`, API v4). Updates are not supported (`allows_upserts` is off); records that already have a target `id` are treated as existing and are not posted again.

**Reuse existing accounts** — Before create, the target looks up Rillet cash accounts when the incoming record has no `id` (not in target state from a prior successful sync). After preprocess, the record must have `account_code`, `currency`, and `subsidiary_id` (subsidiary is resolved from `subsidiary_id`, `subsidiaryId`, or `subsidiaryName` like other fallback streams).

The target loads Rillet bank accounts into a cache and looks for a single match where those three fields are equal. If one match is found, its Rillet `id` is set on the record and the write is skipped (counted as **existing**). If none match, a new account is created. If more than one match, the job fails with an error.

Records that already have an `id` skip lookup and are not posted again.

## Usage

### Running the Target

```bash
# Display version
target-rillet --version

# Display help
target-rillet --help

# Run with a tap
tap-some-source | target-rillet --config config.json
```

## Development

### Setup

```bash
# Install poetry
pipx install poetry

# Install dependencies
poetry install
```

### Running Tests

```bash
poetry run pytest
```

### CLI Testing

```bash
poetry run target-rillet --help
```

## License

Apache 2.0
