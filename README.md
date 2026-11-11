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
| `lookup_existing_bank_accounts` | When `true`, match incoming **bank-accounts** records to existing Rillet cash accounts before create (default: `false`) |
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

**Optional: reuse existing accounts** — Set `"lookup_existing_bank_accounts": true` in config when you want to avoid duplicate cash accounts for the same GL mapping.

Lookup runs only when **all** of the following are true:

- The incoming record has no `id` (not in target state from a prior successful sync).
- `lookup_existing_bank_accounts` is `true`.
- After preprocess, the record has `account_code`, `currency`, and `subsidiary_id` (subsidiary is resolved from `subsidiary_id`, `subsidiaryId`, or `subsidiaryName` like other fallback streams).

The target loads Rillet bank accounts into a cache and looks for a single match where those three fields are equal. If one match is found, its Rillet `id` is set on the record and the write is skipped (counted as **existing**). If none match, a new account is created. If more than one match, the job fails with an error.

If the flag is `false` or the record already has an `id`, no remote lookup is performed; behavior is unchanged from a normal create-or-skip-existing flow.

Example config:

```json
{
  "api_key": "your-rillet-api-key",
  "sandbox": false,
  "lookup_existing_bank_accounts": true
}
```

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
