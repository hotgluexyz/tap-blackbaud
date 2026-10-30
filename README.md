# tap-blackbaud

Blackbaud SKY API v1 extraction using Hotglue's Singer SDK. Requires Python 3.10.
Discovery is local: it does not refresh tokens or make API requests.

## Installation and use

```bash
uv venv --python 3.10 --seed .venv
uv pip install --python .venv/bin/python -e .
.venv/bin/tap-blackbaud --config .secrets/config.json --discover > .secrets/catalog.json
# Select the intended streams/fields in the catalog before syncing.
.venv/bin/tap-blackbaud --config .secrets/config.json --catalog .secrets/catalog-selected.json --state .secrets/state.json
```

Keep config, catalogs, state and Singer output outside source control. The state
argument is optional for the first run. Persist emitted Singer STATE messages for
subsequent incremental runs.

## Authentication and configuration

Standalone OAuth configuration (placeholders only):

```json
{
  "subscription_key": "<Blackbaud API subscription key>",
  "client_id": "<OAuth application ID>",
  "client_secret": "<OAuth application secret>",
  "refresh_token": "<OAuth refresh token>",
  "_refresh_token_via_hg_api": false,
  "start_date": "2026-01-01T00:00:00Z"
}
```

In a linked Hotglue connection, managed token refresh is enabled by default.
Hotglue supplies the connection context (`ENV_ID`, `TENANT`, `FLOW`, `TAP`,
`API_KEY`). Standalone runs should explicitly disable managed refresh as above.
Direct OAuth refresh persists rotating credentials to the supplied config file;
make that file writable and protect its permissions. Tokens are never logged by
the tap's direct refresh implementation.

An existing `access_token` can also be supplied with `expires_in`, an **absolute
Unix expiry timestamp**, matching Hotglue's config convention. It is not the
relative `expires_in` duration returned by Blackbaud's token endpoint. Tokens
within two minutes of expiry are refreshed. Credentials are shared across streams
within one tap, not across unrelated tap instances. `redirect_uri` remains
accepted for existing configurations but is unnecessary for refresh-token grants.

| Option | Behavior |
| --- | --- |
| `subscription_key` | Required Blackbaud subscription key. |
| `start_date` | Optional ISO-8601 lower bound for incremental streams. A later saved bookmark wins. |
| `page_size` | 1–5000; defaults to 5000 for paginated list endpoints. |
| `list_ids` | Optional string array selecting constituent lists. Omitted means enumerate all available lists; `[]` means none. |
| `constituent_ids` | Optional string array. Reads constituents by ID, scopes addresses/phones and constituent custom fields; filters gift lists when `gift_ids` is absent. |
| `gift_ids` | Optional string array. Reads gifts and their custom fields by ID. Overrides the gift-list constituent filter. |

Empty ID arrays never turn into an unfiltered export for their associated streams.
ID filters do not scope unrelated reference streams or educations. Explicit
constituent/gift detail reads and scoped custom-field collections are full-table:
they do not require list-index visibility or modification timestamps.

## Streams and replication

Unless noted otherwise, records use Blackbaud's immutable string `id` as key.
Incremental streams use `date_modified`, send inclusive `last_modified` boundaries,
and retain server continuation tokens, including stable-sort tokens.

| Stream | Replication | Notes |
| --- | --- | --- |
| `constituent_lists` | Full-table | All pages of constituent list metadata. |
| `constituents` | Incremental; full-table with `constituent_ids` | Includes inactive/deceased constituents; native nested contact/fuzzy-date fields. Explicit IDs fetch the fuller detail model. |
| `constituents_by_list` | Full-table | Native constituent rows plus `list_id`; composite key `list_id, id` preserves membership in multiple lists. |
| `educations` | Incremental | Education records, including nested dates and majors/minors. |
| `addresses` | Incremental | Includes inactive addresses; optional constituent filter. |
| `phones` | Incremental | Includes inactive phones; optional constituent filter. |
| `constituent_custom_fields` | Incremental; full-table with `constituent_ids` | Native `parent_id`, category, type and polymorphic value. |
| `gifts` | Incremental; full-table with `gift_ids` | Native amounts, splits, payments, recurring schedule/status, linked gifts. |
| `gift_custom_fields` | Incremental; full-table with `gift_ids` | Native gift custom-field records. |
| `address_types` | Full-table | Active names normalized to `{"name":"Home"}`; key `name`. |
| `phone_types` | Full-table | Active names; key `name`. |
| `email_address_types` | Full-table | Active names; key `name`. |
| `constituent_custom_field_categories` | Full-table | Category details endpoint; key `name`, not an invented ID. |
| `gift_custom_field_categories` | Full-table | Category metadata; key `name`. |
| `funds` | Full-table | Active funds, matching BetterWorld's requested filter. |
| `appeals` | Full-table | Includes inactive appeals. |
| `packages` | Full-table | Includes inactive packages, fetched globally without appeal fan-out. |

All selected list pages follow `next_link`. Reference endpoints without documented
pagination parameters receive no invented `limit`/`offset` arguments. Continuation
URLs must stay on the same HTTPS Blackbaud host and endpoint. Failed reads are not
silently treated as empty collections: timeouts, connection failures, 429 and 5xx
responses have a bounded five-attempt retry policy; other non-2xx responses fail
immediately. `Retry-After` is honored up to 300 seconds; a longer requested delay
fails rather than retrying too early.

`lifetime_giving` and `fundraiser_assignment_list` retain the legacy flattened
currency representation. Explicitly selecting either field enables its additional
per-constituent API calls. Leave them deselected when unnecessary.

## Values and downstream Parquet

Currencies outside the legacy enrichments remain objects such as
`{"value":25.0}`. Gift splits/payments/linked gifts and fuzzy dates retain their
native JSON structure in Singer output.

Custom-field `value` preserves the API's native string, number, boolean, object or
null type. Its schema uses `anyOf` rather than a flat boolean-containing type union
because SDK 1.0.51 otherwise coerces every nonzero value to `true`.

Both custom-field streams additionally emit `value_json`, a canonical JSON string.
The current official `target-parquet` stores polymorphic columns as strings and
does not preserve their original types in `value`. ETLs should use
`json.loads(row["value_json"])` to recover the exact value, including `false`, zero,
decimals, null, a literal string `"false"`, and partial-date objects. Other nested
objects/arrays are serialized as JSON by that target and can be decoded with
`json.loads` as needed. No write-target payload should include `value_json`.

## Migration and limits

- The four existing GitHub stream names are retained. Refresh discovery and review
  selected fields after upgrading. In particular, list membership now has a native
  record schema and a composite key; do not reuse dynamic list-name column schemas
  from the GitLab v2 tap.
- Useful pagination and list-selection behavior from GitLab v2 is retained without
  its dynamic schema/output mismatch, silent read failures, or credential logging.
- The `feature/hgi-11341` branch also implements gifts, custom fields and paging.
  Those overlap with this tap, but its Apteco-specific demo mode, default record
  caps, seeding scripts, constituencies and constituent-code streams are not part
  of this BetterWorld change. There is no implicit demo cohort or total-record cap.
- List endpoints can lag writes (Blackbaud documents roughly 30 minutes for gift
  lists). Explicit ID reads avoid that index delay; a missing list result is not
  proof that a previous create failed. Do not replay ambiguous financial writes.
- This is a replication tap, not a synchronous donor-search API. Constituent search
  and targeted fund lookup are not separate streams. Funds' list/detail models
  are identical, so list replication supplies fund lookup data.
- Full-table streams are snapshots, not deletion events. Reconcile removals in the
  consumer. Contact-type catalogs expose active values only; custom-field category
  renames cannot be reconciled by an immutable category ID because none is exposed.
- Unapproved batch gifts are not direct gifts; this tap does not approve batches
  or infer approved gift IDs.

## Development and API contracts

```bash
uv pip install --python .venv/bin/python -e . pytest responses
.venv/bin/pytest -q
```

Schemas follow Blackbaud's published OpenAPI read models, with nullable properties
and the tap-specific fields described above. Sources:

- [Constituent API](https://developer.sky.blackbaud.com/api?api=56b76470069a0509c8f1c5b3)
- [Gift API](https://developer.sky.blackbaud.com/api?api=58bdd5edd7dcde06046081d6)
- [Fundraising API](https://developer.sky.blackbaud.com/api?api=58bdd6c8d7dcde06046081d7)
- [Blackbaud OAuth authorization](https://developer.blackbaud.com/skyapi/docs/authorization/auth-code-flow/tutorial)

Tests exercise discovery and actual Singer sync output, selection, membership,
pagination, incremental boundaries, nested/mixed values, bounded read failures,
and managed/direct OAuth refresh without exposing credentials.
