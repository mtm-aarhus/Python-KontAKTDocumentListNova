"""KontAKT document-list robot — Nova variant.

Queue-driven robot that fetches the document list for a single KMD Nova case
and pushes it into KontAKT via the HTTP API. Sister process to
``Python-KontAKTDocumentListGO`` — split because the two source systems use
different auth (NTLM vs. KMD OAuth) and live on different worker pools.

The Nova-specific lifting (OAuth token caching, TLS-chain-aware HTTP,
case + document API wrappers) lives in ``oomtm.nova``. What stays here is the
KontAKT-specific orchestration: building the per-doc dict for KontAKT's
import schema, and posting it.

The Nova token and cached KontAKT credentials live on the ``Client`` opened in
``reset.open_all`` and are reused across every queue element (the framework
reconnects via ``reset.reset`` on a retry).

Queue payload (set by KontAKT when the caseworker clicks "Hent dokumenter"):
    {
        "kontakt_case_id": 42,
        "kontakt_reference_id": 17,
        "source_case_id": "S2024-12345",
        "source_case_title": "Optional title hint"
    }

OO constants / credentials this robot relies on:
    Constant   KMDNovaURL             — e.g. "https://novaapi.kmdnova.dk/api"
    Constant   KMDTokenTimestamp      — cached token issue time
    Credential KMDClientSecret        — KMD OAuth2 client secret
    Credential KMDAccessToken         — cached bearer token (username = token URL)
    Credential KontAKTAPI             — username = base URL, password = X-API-Key
"""
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import json
import re
from datetime import datetime

import requests

from robot_framework import reset
from oomtm import nova as oomtm_nova


# ----- Document title sanitization (preserved from legacy robot) -------------
_TITLE_BAD_CHARS = re.compile(r'[~#%&*{}\:\\<>?/+|\"\'\t\[\]`^@=!$();\€£¥₹]')


def process(
    orchestrator_connection: OrchestratorConnection,
    queue_element: QueueElement | None = None,
    client: "reset.Client | None" = None,
) -> None:
    orchestrator_connection.log_trace("Running process.")
    if queue_element is None:
        raise RuntimeError("KontAKTDocumentListNova is queue-driven; no queue_element given.")
    if client is None:  # e.g. a manual run outside the queue framework
        client = reset.open_all(orchestrator_connection)

    payload = json.loads(queue_element.data or "{}")
    kontakt_case_id = int(payload["kontakt_case_id"])
    kontakt_ref_id = payload.get("kontakt_reference_id")
    source_case_id = str(payload["source_case_id"]).strip()

    orchestrator_connection.log_info(
        f"KontAKT case={kontakt_case_id} ref={kontakt_ref_id} Nova case={source_case_id}"
    )

    _set_ref_status(orchestrator_connection, client, kontakt_case_id, kontakt_ref_id, "fetching")

    try:
        sags_title, documents, warnings = _fetch_nova(orchestrator_connection, client, source_case_id)
    except Exception as exc:
        orchestrator_connection.log_info(f"Nova document fetch failed: {exc!r}")
        _set_ref_status(orchestrator_connection, client, kontakt_case_id, kontakt_ref_id, "error", str(exc))
        raise

    orchestrator_connection.log_info(
        f"Fetched {len(documents)} documents from Nova ({len(warnings)} warnings) — posting to KontAKT."
    )

    import_payload = {
        "source_system": "nova",
        "source_case_id": source_case_id,
        "source_case_title": sags_title,
        "documents": documents,
        "warnings": warnings,
    }
    r = _kontakt_post(
        client,
        f"/api/v1/cases/{kontakt_case_id}/documents/import",
        import_payload,
        timeout=120,
    )
    if r.status_code not in (200, 201):
        msg = f"KontAKT import failed: HTTP {r.status_code} body={r.text[:400]!r}"
        _set_ref_status(orchestrator_connection, client, kontakt_case_id, kontakt_ref_id, "error", msg)
        raise RuntimeError(msg)

    orchestrator_connection.log_info(f"Done. Response: {r.json()}")


# ----- Helpers ---------------------------------------------------------------


def _shorten_title(title: str) -> str:
    """Trim long titles. Matches legacy ``shorten_document_title``."""
    if title and len(title) > 99:
        return title[:95]
    return title


def _looks_like_redacted(title: str) -> bool:
    """memo / tunnel / fletteliste detection — these auto-mark Nej."""
    t = (title or "").lower()
    return ("tunnel_marking" in t) or ("memometadata" in t) or ("fletteliste" in t)


def _coerce_doc_date(raw) -> str | None:
    """Try several date formats; return ISO YYYY-MM-DD or None."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"none", "null"}:
        return None
    if "T" in s:
        s = s.split("T", 1)[0]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(
                s if fmt != "%Y-%m-%dT%H:%M:%S" else str(raw), fmt
            ).date().isoformat()
        except ValueError:
            continue
    return None


# ----- Nova document fetch ---------------------------------------------------


def _fetch_nova(
    orchestrator_connection: OrchestratorConnection, client, sags_id: str
) -> tuple[str, list[dict], list[str]]:
    """Return (case_title, documents, warnings) for a Nova case."""
    nova_url = client.nova_url
    token = client.token

    # --- Case metadata ---
    case = oomtm_nova.get_case_metadata(
        token=token,
        base_url=nova_url,
        case_number=sags_id,
    )
    sags_title = case.get("caseAttributes", {}).get("title") or sags_id
    sags_title = _TITLE_BAD_CHARS.sub("", str(sags_title))
    sags_title = " ".join(sags_title.split())

    # --- Document list (main + sub-docs grouped) ---
    groups = oomtm_nova.get_document_list(
        token=token,
        base_url=nova_url,
        case_number=sags_id,
    )

    total = len(groups) + sum(len(g["subs"]) for g in groups)
    akt_counter = total
    documents: list[dict] = []
    warnings: list[str] = []
    has_missing_date = False

    for group in groups:
        main = group["main"]
        subs = group["subs"]
        m_title = main.get("title") or ""
        m_dok = main.get("documentNumber")
        m_kat = main.get("documentType")
        m_date = _coerce_doc_date(main.get("documentDate"))
        if not m_date:
            has_missing_date = True
        bilag_children = ", ".join(
            sub.get("documentNumber") for sub in subs if sub.get("documentNumber")
        )
        redacted = _looks_like_redacted(m_title)

        documents.append({
            "dok_id": str(m_dok or ""),
            "akt_id": akt_counter,
            "title": _shorten_title(m_title),
            "doc_category": m_kat,
            "doc_date": m_date,
            "bilag_til_dok_id": bilag_children or None,
            "bilag_index": None,
            "link_to_doc": None,
            "included_in_request": "Ja",
            "grant_access": "Nej" if redacted else None,
            "justification": "Tavshedsbelagte oplysninger - om private forhold" if redacted else None,
        })
        akt_counter -= 1

        for sub in subs:
            s_title = sub.get("title") or ""
            s_date = _coerce_doc_date(sub.get("documentDate"))
            if not s_date:
                has_missing_date = True
            s_redacted = _looks_like_redacted(s_title)
            documents.append({
                "dok_id": str(sub.get("documentNumber") or ""),
                "akt_id": akt_counter,
                "title": _shorten_title(s_title),
                "doc_category": sub.get("documentType"),
                "doc_date": s_date,
                "bilag_til_dok_id": str(m_dok or "") or None,
                "bilag_index": None,
                "link_to_doc": None,
                "included_in_request": "Ja",
                "grant_access": "Nej" if s_redacted else None,
                "justification": "Tavshedsbelagte oplysninger - om private forhold" if s_redacted else None,
            })
            akt_counter -= 1

    if has_missing_date:
        warnings.append("Et eller flere dokumenter mangler dato i Nova.")

    return sags_title, documents, warnings


# ----- KontAKT API client ----------------------------------------------------


def _kontakt_post(client, path: str, payload: dict, *, timeout: int = 60) -> requests.Response:
    return requests.post(
        f"{client.kontakt_base}{path}",
        headers={"X-API-Key": client.kontakt_key, "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
    )


def _set_ref_status(orchestrator_connection, client, case_id: int, ref_id: int | None, status: str, message: str = "") -> None:
    if not ref_id:
        return
    try:
        _kontakt_post(
            client,
            f"/api/v1/cases/{case_id}/refs/{ref_id}/status",
            {"status": status, "message": message},
            timeout=10,
        )
    except Exception as exc:  # pylint: disable=broad-except
        orchestrator_connection.log_info(f"Could not update ref status to {status!r}: {exc!r}")
