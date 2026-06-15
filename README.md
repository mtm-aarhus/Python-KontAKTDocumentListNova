# Python-KontAKTDocumentListNova

Fetches the document list for a single **KMD Nova** case and pushes it into **KontAKT** (the aktindsigt / FOI request system). The Nova counterpart to `Python-KontAKTDocumentListGO`.

KontAKT triggers this when a caseworker adds a Nova case to an aktindsigt, or refreshes it.

## What it does

For one Nova case:

1. Reads the case metadata (title).
2. Fetches the case's documents, grouped into main documents and their sub-documents (bilag).
3. Builds a KontAKT document list — act numbers assigned top-down, bilag relationships preserved.
4. Posts it to KontAKT's import endpoint, together with any warnings (e.g. documents missing a date).

Documents that look already-redacted (memo / tunnel-marking / fletteliste) are pre-marked "ingen aktindsigt" with a justification, so the caseworker doesn't have to.

## Input (one case)

| Field | Meaning |
|-------|---------|
| `kontakt_case_id` | KontAKT case id |
| `kontakt_reference_id` | KontAKT reference — the Nova case within the aktindsigt |
| `source_case_id` | Nova case number |
| `source_case_title` | Optional title hint |

## Output

A POST to KontAKT's `documents/import` containing the document list (act/document numbers, titles, dates, categories, bilag links) and any warnings. The reference is set to `fetching` while the job runs and `error` if it fails.

## Required configuration

- Constant `KMDNovaURL` — Nova API base URL
- Constant `KMDTokenTimestamp` — cached token issue time (updated automatically)
- Credential `KMDClientSecret` — KMD OAuth2 client secret
- Credential `KMDAccessToken` — username = token URL, password = cached bearer token (updated automatically)
- Credential `KontAKTAPI` — username = base URL, password = API key

## Dependencies

The shared [`oomtm`](https://github.com/mtm-aarhus/oomtm) library (`nova`).
