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
.venv/bin/target-rillet --config /secure/config.json < /secure/data.singer
```

- Supply a stable, nonempty, source-namespaced `externalId`, unique within each
  stream and connection. It remains in SDK bookmarks but is never sent in JSON.
  Creates use a deterministic UUID `Idempotency-Key` scoped to API environment,
  stream, and source ID. Existing explicit remote IDs retain update/skip semantics.
- Enable Hotglue's **Save records IDs in snapshots** setting. The SDK restores
  remote IDs from standard `${stream}_${FLOW}.snapshot.csv` or Parquet files with
  `InputId` / `RemoteId` columns. The target emits confirmed IDs in Singer state;
  it does not write custom journals or snapshot files. Create-only streams skip
  records with restored IDs. The Spendesk ETL also skips mapped financial records,
  including bills, rather than updating them.
- Standard ID mappings protect replay beyond the provider's 24-hour idempotency
  window. If a create has an ambiguous outcome and no saved mapping, reconcile
  remotely before replaying after that window; the target does not keep a second
  record of pending creates or enforce cross-job retry deadlines.
- JSON requests have connect/read timeouts of 10/60 seconds and at most four
  attempts, with exponential backoff/jitter and `Retry-After` (seconds or HTTP
  date). A requested delay above 120 seconds fails rather than retrying early.
  Only 429, 5xx, timeouts and connection errors retry; POST requires the same
  idempotency key. Creates without externalId retain legacy one-attempt behavior
  and have **no durable duplicate protection**. All sinks in one target share
  1.05-second request-start pacing; separate jobs do not share a rate limiter.
- Bill overpayment errors are failures, never proof that a particular source
  payment exists. Payments use standard ID mappings; no external-reference
  matching is assumed.
- Bills preserve optional `impact_date`. Each `expenses`/`lineItems` entry may
  supply native `fields: [{"field_id": "...", "field_value_id": "..."}]` (including
  an explicit empty array), taking precedence over name/value `customFields`.
  Native `service_period` survives the existing prepaid-account validation.
  Numeric zero `taxAmount` is accepted and serialized as a monetary string;
  absent/empty tax amounts still fail when `taxCode` is present.
- Bill, charge and reimbursement receipt failures are logged and returned as
  `attachment_errors` in the document bookmark. The confirmed document remains
  `success: true` so its ID mapping is saved; a receipt failure must not recreate
  a financial document. Remaining attachments are still attempted. Restored IDs
  in create-only streams skip both creation and receipt uploads. Receipt repair
  is separate: there is no automatic cross-job receipt retry or receipt cache.
  Downloads/uploads have bounded timeouts and HTTP status validation; ambiguous
  multipart uploads are not automatically retried.
- Vendor PUTs fetch the current vendor first, preserve omitted writable fields,
  merge partial addresses, and retain existing external references. Explicit
  false/zero values are retained. Empty or
  null top-level vendor values do not clear existing fields.

The 24-hour idempotency window comes from the integration scope. The provided
OpenAPI describes the payloads but does not declare the idempotency header/window.

## License

Apache 2.0
