# target-rillet

`target-rillet` is a Singer target for [Rillet](https://rillet.com/), built with the Hotglue Singer SDK.

## Overview

This target allows you to sync data to Rillet via their REST API. It currently supports the following streams:

- **Journals** - Create and update journal entries

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

### Spendesk write-safety contract

Validated with Python 3.10 and `hotglue-singer-sdk==1.0.51`:

```bash
uv venv --python 3.10 --seed .venv
uv pip install --python .venv/bin/python -e . hotglue-singer-sdk==1.0.51 pytest ruff==0.15.12
.venv/bin/pytest -q
.venv/bin/ruff check .
SNAPSHOT_DIR=/durable/connection-scoped/snapshots .venv/bin/target-rillet --config /secure/config.json < /secure/data.singer
```

- Supply a stable, nonempty, source-namespaced `externalId`, unique within each
  stream and connection. It remains in SDK bookmarks but is never sent in JSON.
  Creates use a deterministic UUID `Idempotency-Key` scoped to API environment,
  stream, and source ID. Existing explicit remote IDs retain update/skip semantics.
- `SNAPSHOT_DIR` is **required for creates with externalId** and must be persisted
  and restored across jobs for the same connection when running standalone.
  Hotglue jobs may instead supply `JOB_ID`; the target then uses the SDK's snapshot
  directory, `/home/hotglue/${JOB_ID}/snapshots`. `rillet-creates.json` stores
  request hashes and timestamps before POST and confirmed remote IDs immediately
  afterward, before receipt upload or SDK state output. Atomic replacement,
  file/directory fsync, and a local process lock protect the journal. Do not share
  it across different Rillet connections or delete it as routine cleanup.
- A confirmed create is reused without another document POST, even after state
  loss or more than 24 hours. A changed create payload or parent endpoint for the
  same source ID fails closed. An unresolved intent can use the same key only
  within 24 hours of its original timestamp; afterward it requires manual remote
  reconciliation, not deletion and blind replay. Corrupt/unwritable journals fail
  closed. The journal is not a substitute for successfully persisting snapshots.
- JSON requests have connect/read timeouts of 10/60 seconds and at most four
  attempts, with exponential backoff/jitter and `Retry-After` (seconds or HTTP
  date). A requested delay above 120 seconds fails rather than retrying early.
  Only 429, 5xx, timeouts and connection errors retry; POST requires the same
  idempotency key. Creates without externalId retain legacy one-attempt behavior
  and have **no durable duplicate protection**. All sinks in one target share
  1.05-second request-start pacing; separate jobs do not share a rate limiter.
- Bill overpayment errors are failures, never proof that a particular source
  payment exists. Payments also use the create journal; no external-reference
  matching is assumed.
- Bills preserve optional `impact_date`. Each `expenses`/`lineItems` entry may
  supply native `fields: [{"field_id": "...", "field_value_id": "..."}]` (including
  an explicit empty array), taking precedence over name/value `customFields`.
  Native `service_period` survives the existing prepaid-account validation.
  Numeric zero `taxAmount` is accepted and serialized as a monetary string;
  absent/empty tax amounts still fail when `taxCode` is present.
- Bill, charge and reimbursement receipt failures produce `success: false` plus
  `id`, `document_id` and `error` in the bookmark. A known document ID alone is
  **not** export confirmation. Only successful bookmarks should enter successful
  ID mappings. Replaying the original create reuses its journal ID. Confirmed
  receipts are skipped using their stable attachment ID (or URL if no ID exists);
  callers should supply IDs because signed download URLs can rotate. Download
  failures remain retryable. Upload intent is saved before sending multipart data:
  an unconfirmed upload blocks replay pending manual remote reconciliation, even
  after a process restart. Downloads/uploads have bounded timeouts and HTTP status
  validation. Persist the receipt journal alongside the document journal.
- Vendor PUTs fetch the current vendor first, preserve omitted writable fields,
  merge partial addresses, and retain existing external references. Explicit
  false/zero values are retained. Empty or
  null top-level vendor values do not clear existing fields.

Live Rillet acceptance, idempotency-key retention, and platform persistence of
the journal must be verified in an authorized sandbox. The provided OpenAPI
describes the payloads but does not declare the idempotency header/window.

## License

Apache 2.0
