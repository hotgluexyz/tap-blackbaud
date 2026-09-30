"""Branch-scoped Apteco demo helpers for deterministic Blackbaud pulls.

When write/seed is unavailable, demo_mode selects constituents that have a
giving history by scanning gifts from epoch in stable id order.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# Optional seeded cohort prefix (scripts/seed_apteco_demo.py)
LOOKUP_PREFIX = "APTECO-DEMO"

# Caps for the deterministic demo extract.
DEMO_CONSTITUENT_LIMIT = 50
DEMO_GIFT_LIMIT = 150
# Max gift pages to scan while discovering donors (500 per page).
DEMO_GIFT_SCAN_PAGES = 20

_CACHE_KEY = "_demo_giving_cohort"

_ALLOWLIST_CANDIDATES = (
    Path(__file__).resolve().parent.parent / "scripts" / "demo_allowlist.json",
    Path("scripts/demo_allowlist.json"),
    Path("demo_allowlist.json"),
)


def demo_mode_enabled(config: Optional[dict]) -> bool:
    """Return True unless config explicitly disables demo_mode."""
    if not config:
        return True
    if "demo_mode" not in config:
        return True
    return bool(config.get("demo_mode"))


def load_allowlist(config: Optional[dict] = None) -> Dict[str, Any]:
    """Load optional frozen id allowlist for deterministic sync."""
    paths: List[Path] = []
    if config and config.get("demo_allowlist_path"):
        paths.append(Path(config["demo_allowlist_path"]))
    paths.extend(_ALLOWLIST_CANDIDATES)

    for path in paths:
        try:
            if path.is_file():
                data = json.loads(path.read_text())
                if isinstance(data, dict):
                    return data
        except (OSError, json.JSONDecodeError):
            continue
    return {}


def allowlist_constituent_ids(allowlist: Dict[str, Any]) -> Set[str]:
    ids = allowlist.get("constituent_ids") or []
    return {str(i) for i in ids if i is not None and str(i)}


def allowlist_gift_ids(allowlist: Dict[str, Any]) -> Set[str]:
    ids = allowlist.get("gift_ids") or []
    return {str(i) for i in ids if i is not None and str(i)}


def sort_by_id(rows: List[dict]) -> List[dict]:
    return sorted(rows, key=lambda r: _id_sort_key(r.get("id")))


def _id_sort_key(value: Any):
    text = str(value) if value is not None else ""
    if text.isdigit():
        return (0, int(text))
    return (1, text)


def _page_gifts(
    session: requests.Session,
    headers: dict,
    *,
    max_pages: int = DEMO_GIFT_SCAN_PAGES,
) -> List[dict]:
    """Fetch gift pages from epoch for a stable discovery scan."""
    gifts: List[dict] = []
    url = "https://api.sky.blackbaud.com/gift/v1/gifts"
    params: Dict[str, Any] = {"limit": 500, "last_modified": "0001-01-01"}
    pages = 0
    while url and pages < max_pages:
        resp = session.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json() or {}
        page = data.get("value") or []
        if not page:
            break
        gifts.extend(page)
        pages += 1
        next_link = data.get("next_link")
        url = next_link
        params = None  # next_link is a full URL
    return gifts


def select_donors_from_gifts(
    gifts: List[dict],
    *,
    donor_limit: int = DEMO_CONSTITUENT_LIMIT,
    gift_limit: int = DEMO_GIFT_LIMIT,
) -> Tuple[List[str], List[dict]]:
    """Pick a stable donor set from gifts sorted by gift id.

    Walks gifts in ascending id order and keeps the first ``donor_limit``
    distinct ``constituent_id`` values (i.e. people with giving history).
    Returns those donor ids and their gifts (also sorted, capped).
    """
    ordered_gifts = sort_by_id(gifts)
    donor_ids: List[str] = []
    seen: Set[str] = set()
    for gift in ordered_gifts:
        cid = str(gift.get("constituent_id") or "")
        if not cid or cid in seen:
            continue
        seen.add(cid)
        donor_ids.append(cid)
        if len(donor_ids) >= donor_limit:
            break

    donor_set = set(donor_ids)
    donor_gifts = [g for g in ordered_gifts if str(g.get("constituent_id") or "") in donor_set]
    donor_gifts = sort_by_id(donor_gifts)[:gift_limit]
    # Re-order donor_ids by numeric/string id for stable constituent emit order
    donor_ids = sorted(donor_ids, key=_id_sort_key)
    return donor_ids, donor_gifts


def get_giving_cohort(
    config: dict,
    headers: dict,
    logger=None,
) -> Dict[str, Any]:
    """Return cached ``{constituent_ids, gifts}`` for demo_mode syncs.

    Preference order:
    1. Non-empty committed allowlist (seeded demo)
    2. Discover donors from gift history (read-only environments)
    """
    cached = config.get(_CACHE_KEY)
    if cached:
        return cached

    allowlist = load_allowlist(config)
    allowed_constituents = allowlist_constituent_ids(allowlist)
    allowed_gifts = allowlist_gift_ids(allowlist)

    session = requests.Session()
    scanned = _page_gifts(session, headers)

    if allowed_constituents:
        # Prefer allowlist when seeded; still attach matching gifts from scan.
        donor_ids = sorted(allowed_constituents, key=_id_sort_key)[:DEMO_CONSTITUENT_LIMIT]
        donor_set = set(donor_ids)
        gifts = [
            g
            for g in sort_by_id(scanned)
            if str(g.get("id") or "") in allowed_gifts
            or str(g.get("constituent_id") or "") in donor_set
            or str(g.get("lookup_id") or "").startswith(LOOKUP_PREFIX)
        ]
        gifts = sort_by_id(gifts)[:DEMO_GIFT_LIMIT]
        if logger:
            logger.info(
                "demo_mode cohort from allowlist: donors=%s gifts=%s scanned=%s",
                len(donor_ids),
                len(gifts),
                len(scanned),
            )
    else:
        donor_ids, gifts = select_donors_from_gifts(scanned)
        if logger:
            logger.info(
                "demo_mode cohort from giving history: donors=%s gifts=%s scanned=%s",
                len(donor_ids),
                len(gifts),
                len(scanned),
            )

    cohort = {"constituent_ids": donor_ids, "gifts": gifts}
    config[_CACHE_KEY] = cohort
    return cohort
