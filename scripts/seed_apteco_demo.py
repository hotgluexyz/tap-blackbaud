#!/usr/bin/env python3
"""Seed Blackbaud RE NXT with Apteco-demo constituents and gifts.

Idempotent by lookup_id prefix APTECO-DEMO-*. Re-runs skip existing constituents
and gifts with matching lookup_ids.

Usage:
  python scripts/seed_apteco_demo.py --config .secrets/config.json
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

API_BASE = "https://api.sky.blackbaud.com"
TOKEN_URL = "https://oauth2.sky.blackbaud.com/token"
LOOKUP_PREFIX = "APTECO-DEMO"
CONSTITUENT_COUNT = 35
GIFT_TARGET = 120
ALLOWLIST_PATH = Path(__file__).resolve().parent / "demo_allowlist.json"

# Segmentation attributes seeded onto demo constituents (category -> value cycle).
SEG_DONOR_TIERS = ["Major", "Mid", "Small", "Leadership"]
SEG_INTERESTS = ["Education", "Health", "Environment", "Arts", "Community"]
SEG_VOLUNTEER = ["Yes", "No"]
CONSTITUENCY_TAGS = ["Volunteer", "Board Prospect", "Alumni", "Event Attendee"]


FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright",
]
CITIES = [
    ("Seattle", "WA", "98101"),
    ("Portland", "OR", "97201"),
    ("San Francisco", "CA", "94102"),
    ("Denver", "CO", "80202"),
    ("Austin", "TX", "78701"),
    ("Chicago", "IL", "60601"),
    ("Boston", "MA", "02108"),
    ("Atlanta", "GA", "30303"),
    ("Minneapolis", "MN", "55401"),
    ("Philadelphia", "PA", "19103"),
]
STREETS = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine Rd",
    "Elm St", "Washington Blvd", "Lake View Dr", "Park Ave", "Highland Ct",
]
PAYMENT_METHODS = ["Cash", "PersonalCheck", "CreditCard", "Other"]
# Gift amount tiers: (weight, min, max)
AMOUNT_TIERS = [
    (0.45, 15, 75),      # small
    (0.35, 100, 500),    # mid
    (0.15, 1000, 5000),  # major
    (0.05, 10000, 25000),  # leadership
]


class BlackbaudClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session = requests.Session()
        self.access_token = config.get("access_token")
        self.refresh_token = config["refresh_token"]
        self._ensure_token()

    def _ensure_token(self) -> None:
        data = {
            "grant_type": "refresh_token",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "refresh_token": self.refresh_token,
            "redirect_uri": self.config["redirect_uri"],
        }
        resp = self.session.post(TOKEN_URL, data=data, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"OAuth refresh failed ({resp.status_code}): {resp.text}")
        body = resp.json()
        self.access_token = body["access_token"]
        if body.get("refresh_token"):
            self.refresh_token = body["refresh_token"]
            self.config["refresh_token"] = self.refresh_token
        self.config["access_token"] = self.access_token

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Bb-Api-Subscription-Key": self.config["subscription_key"],
            "Content-Type": "application/json",
        }

    def request(
        self, method: str, path: str, *, params: Optional[dict] = None, json_body: Any = None
    ) -> requests.Response:
        url = path if path.startswith("http") else f"{API_BASE}{path}"
        resp = self.session.request(
            method, url, headers=self.headers, params=params, json=json_body, timeout=60
        )
        if resp.status_code == 401:
            self._ensure_token()
            resp = self.session.request(
                method, url, headers=self.headers, params=params, json=json_body, timeout=60
            )
        return resp

    def paged_get(self, path: str, params: Optional[dict] = None) -> List[dict]:
        items: List[dict] = []
        params = dict(params or {})
        resp = self.request("GET", path, params=params)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("value") or [])
        next_link = data.get("next_link")
        while next_link and data.get("value"):
            resp = self.request("GET", next_link)
            if resp.status_code != 200:
                break
            data = resp.json()
            page = data.get("value") or []
            if not page:
                break
            items.extend(page)
            next_link = data.get("next_link")
        return items


def save_config(path: Path, config: Dict[str, Any]) -> None:
    path.write_text(json.dumps(config, indent=4) + "\n")


def resolve_fund_id(client: BlackbaudClient) -> str:
    funds = client.paged_get("/fundraising/v1/funds", params={"limit": 100})
    if not funds:
        raise RuntimeError("No funds found; cannot create gifts without fund_id")
    # Prefer an active-looking fund with a name
    for fund in funds:
        if fund.get("id") and not fund.get("inactive"):
            print(f"Using fund id={fund['id']} name={fund.get('description') or fund.get('lookup_id')}")
            return str(fund["id"])
    fund = funds[0]
    print(f"Using fund id={fund['id']} name={fund.get('description') or fund.get('lookup_id')}")
    return str(fund["id"])


def existing_demo_constituents(client: BlackbaudClient) -> Dict[str, str]:
    """Map lookup_id -> constituent id for APTECO-DEMO constituents via search."""
    found: Dict[str, str] = {}
    # Prefer search to avoid paging the entire SKY demo DB
    resp = client.request(
        "GET",
        "/constituent/v1/constituents/search",
        params={"search_text": LOOKUP_PREFIX, "limit": 500},
    )
    if resp.status_code == 200:
        for row in resp.json().get("value") or []:
            lid = row.get("lookup_id") or ""
            if lid.startswith(LOOKUP_PREFIX) and row.get("id"):
                found[lid] = str(row["id"])
        if found:
            return found

    # Fallback: check expected lookup_ids via search one-by-one is too slow;
    # rely on local state + create-or-skip instead.
    return found


def existing_demo_gift_lookup_ids(client: BlackbaudClient) -> set:
    """Return known demo gift lookup_ids from local state only (full gift scan is too large)."""
    return set()


def load_seed_state(path: Path) -> dict:
    if path.is_file():
        return json.loads(path.read_text())
    return {"constituents": {}, "gifts": []}


def save_seed_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state, indent=2) + "\n")


def write_allowlist(constituent_map: Dict[str, str], gift_ids: List[str]) -> None:
    """Freeze demo URNs for deterministic tap syncs on this branch."""
    payload = {
        "lookup_prefix": LOOKUP_PREFIX,
        "constituent_ids": sorted(constituent_map.values(), key=lambda x: (0, int(x)) if str(x).isdigit() else (1, str(x))),
        "constituent_lookup_ids": sorted(constituent_map.keys()),
        "gift_ids": sorted(gift_ids, key=lambda x: (0, int(x)) if str(x).isdigit() else (1, str(x))),
    }
    ALLOWLIST_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"Wrote allowlist {ALLOWLIST_PATH} ({len(payload['constituent_ids'])} constituents, {len(payload['gift_ids'])} gifts)")


def seed_segmentation(
    client: BlackbaudClient, constituent_map: Dict[str, str]
) -> None:
    """Attach custom fields + constituencies used as Apteco seg_* columns."""
    indexed = sorted(constituent_map.items(), key=lambda kv: kv[0])
    for index, (lookup_id, constituent_id) in enumerate(indexed, start=1):
        tier = SEG_DONOR_TIERS[(index - 1) % len(SEG_DONOR_TIERS)]
        interest = SEG_INTERESTS[(index - 1) % len(SEG_INTERESTS)]
        volunteer = SEG_VOLUNTEER[0 if index % 3 == 0 else 1]
        fields = [
            ("Donor Tier", tier),
            ("Region Interest", interest),
            ("Volunteer", volunteer),
        ]
        for category, value in fields:
            resp = client.request(
                "POST",
                f"/constituent/v1/constituents/{constituent_id}/customfields",
                json_body={
                    "category": category,
                    "value": value,
                    "comment": "Apteco demo segmentation attribute",
                    "date": datetime.utcnow().strftime("%Y-%m-%dT00:00:00"),
                },
            )
            if resp.status_code not in (200, 201):
                # Idempotent-ish: already exists / category missing — log and continue.
                print(
                    f"CUSTOM FIELD {lookup_id} {category}={value}: "
                    f"{resp.status_code} {resp.text[:200]}"
                )
            time.sleep(0.05)

        tag = CONSTITUENCY_TAGS[(index - 1) % len(CONSTITUENCY_TAGS)]
        resp = client.request(
            "POST",
            f"/constituent/v1/constituents/{constituent_id}/constituencies",
            json_body={"constituency": tag},
        )
        if resp.status_code not in (200, 201):
            print(
                f"CONSTITUENCY {lookup_id} {tag}: {resp.status_code} {resp.text[:200]}"
            )
        time.sleep(0.05)
    print(f"Seeded segmentation attributes for {len(indexed)} constituents")


def build_constituent_payload(index: int) -> Tuple[str, dict]:
    rng = random.Random(1000 + index)
    first = FIRST_NAMES[index % len(FIRST_NAMES)]
    last = LAST_NAMES[(index * 3) % len(LAST_NAMES)]
    city, state, postal = CITIES[index % len(CITIES)]
    street_num = 100 + index * 7
    street = STREETS[index % len(STREETS)]
    lookup_id = f"{LOOKUP_PREFIX}-C{index:03d}"
    email = f"{first.lower()}.{last.lower()}.{index}@apteco-demo.example"
    payload = {
        "type": "Individual",
        "first": first,
        "last": last,
        "lookup_id": lookup_id,
        "email": {
            "address": email,
            "type": "Email",
            "primary": True,
        },
        "phone": {
            "number": f"({200 + (index % 800):03d}) 555-{1000 + index:04d}",
            "type": "Home",
            "primary": True,
        },
        "address": {
            "address_lines": f"{street_num} {street}",
            "city": city,
            "state": state,
            "postal_code": postal,
            "country": "United States",
            "type": "Home",
        },
        "gender": rng.choice(["Male", "Female", "Unknown"]),
    }
    return lookup_id, payload


def pick_amount(rng: random.Random) -> float:
    roll = rng.random()
    cumulative = 0.0
    for weight, lo, hi in AMOUNT_TIERS:
        cumulative += weight
        if roll <= cumulative:
            return round(rng.uniform(lo, hi), 2)
    return round(rng.uniform(15, 75), 2)


def gift_dates(rng: random.Random, count: int) -> List[datetime]:
    """Spread gift dates over ~24 months ending today."""
    end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=730)
    span = (end - start).days
    dates = [start + timedelta(days=rng.randint(0, span)) for _ in range(count)]
    dates.sort()
    return dates


def build_gift_payload(
    lookup_id: str,
    constituent_id: str,
    amount: float,
    gift_date: datetime,
    fund_id: str,
    gift_type: str,
    payment_method: str,
) -> dict:
    iso_date = gift_date.strftime("%Y-%m-%dT00:00:00")
    return {
        "constituent_id": constituent_id,
        "amount": {"value": amount},
        "type": gift_type,
        "date": iso_date,
        "post_date": iso_date,
        "lookup_id": lookup_id,
        "reference": "Apteco demo seed gift",
        "gift_splits": [
            {
                "amount": {"value": amount},
                "fund_id": fund_id,
            }
        ],
        "payments": [
            {
                "payment_method": payment_method,
            }
        ],
    }


def assign_gift_plan(constituent_ids: List[str]) -> List[Tuple[str, int]]:
    """Return list of (constituent_id, gift_count) for loyal/lapsed/one-time cohorts."""
    rng = random.Random(42)
    ids = list(constituent_ids)
    rng.shuffle(ids)
    plan = []
    remaining = GIFT_TARGET
    # ~30% loyal (4-8 gifts), ~30% mid (2-3), ~40% one-time
    n = len(ids)
    loyal = ids[: max(1, n // 3)]
    mid = ids[len(loyal): len(loyal) + max(1, n // 3)]
    one_time = ids[len(loyal) + len(mid):]

    for cid in loyal:
        count = rng.randint(4, 8)
        plan.append((cid, count))
        remaining -= count
    for cid in mid:
        count = rng.randint(2, 3)
        plan.append((cid, count))
        remaining -= count
    for cid in one_time:
        plan.append((cid, 1))
        remaining -= 1

    # Top up remaining gifts on loyal donors
    i = 0
    while remaining > 0 and plan:
        cid, count = plan[i % len(plan)]
        plan[i % len(plan)] = (cid, count + 1)
        remaining -= 1
        i += 1
    return plan


def create_constituents(
    client: BlackbaudClient, existing: Dict[str, str]
) -> Dict[str, str]:
    """Create missing demo constituents; return full lookup_id -> id map."""
    result = dict(existing)
    created = 0
    for i in range(1, CONSTITUENT_COUNT + 1):
        lookup_id, payload = build_constituent_payload(i)
        if lookup_id in result:
            continue
        resp = client.request("POST", "/constituent/v1/constituents", json_body=payload)
        if resp.status_code not in (200, 201):
            print(f"FAILED constituent {lookup_id}: {resp.status_code} {resp.text[:400]}")
            continue
        body = resp.json()
        cid = str(body.get("id") or body.get("constituent_id") or "")
        if not cid:
            # Some responses return bare id string
            if isinstance(body, str):
                cid = body
            else:
                print(f"Unexpected create response for {lookup_id}: {body}")
                continue
        result[lookup_id] = cid
        created += 1
        print(f"Created constituent {lookup_id} -> {cid}")
        time.sleep(0.15)
    print(f"Constituents: {created} created, {len(result)} total demo")
    return result


def create_gifts(
    client: BlackbaudClient,
    constituent_map: Dict[str, str],
    existing_gift_lookup_ids: set,
    existing_gift_ids: List[str],
    fund_id: str,
) -> Tuple[int, List[str]]:
    rng = random.Random(99)
    plan = assign_gift_plan(list(constituent_map.values()))
    created = 0
    gift_index = 0
    gift_ids = list(existing_gift_ids)
    for constituent_id, count in plan:
        dates = gift_dates(rng, count)
        for gift_date in dates:
            gift_index += 1
            lookup_id = f"{LOOKUP_PREFIX}-G{gift_index:04d}"
            if lookup_id in existing_gift_lookup_ids:
                continue
            amount = pick_amount(rng)
            gift_type = "Donation"
            payment_method = rng.choice(PAYMENT_METHODS)
            payload = build_gift_payload(
                lookup_id=lookup_id,
                constituent_id=constituent_id,
                amount=amount,
                gift_date=gift_date,
                fund_id=fund_id,
                gift_type=gift_type,
                payment_method=payment_method,
            )
            resp = client.request("POST", "/gift/v1/gifts", json_body=payload)
            if resp.status_code not in (200, 201):
                print(f"FAILED gift {lookup_id}: {resp.status_code} {resp.text[:400]}")
                continue
            body = resp.json() if resp.text else {}
            gift_id = str(body.get("id") or "")
            if gift_id:
                gift_ids.append(gift_id)
            created += 1
            existing_gift_lookup_ids.add(lookup_id)
            if created % 10 == 0:
                print(f"Created {created} gifts...")
            time.sleep(0.15)
    print(f"Gifts: {created} created (demo lookup_ids now {len(existing_gift_lookup_ids)})")
    return created, gift_ids


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=".secrets/config.json",
        help="Path to tap config.json with OAuth + subscription_key",
    )
    parser.add_argument(
        "--state",
        default=".secrets/seed_state.json",
        help="Local idempotency state for demo lookup_ids",
    )
    parser.add_argument(
        "--skip-segmentation",
        action="store_true",
        help="Skip seeding custom fields / constituencies",
    )
    args = parser.parse_args()
    config_path = Path(args.config)
    state_path = Path(args.state)
    if not config_path.is_file():
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 1

    config = json.loads(config_path.read_text())
    required = ["client_id", "client_secret", "refresh_token", "redirect_uri", "subscription_key"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        print(f"Config missing required keys: {missing}", file=sys.stderr)
        return 1

    client = BlackbaudClient(config)
    save_config(config_path, config)  # persist rotated refresh_token
    state = load_seed_state(state_path)

    print("Resolving fund_id...")
    fund_id = resolve_fund_id(client)

    print("Loading existing demo constituents (search + local state)...")
    existing_c = existing_demo_constituents(client)
    existing_c.update(state.get("constituents") or {})
    print(f"Found {len(existing_c)} existing {LOOKUP_PREFIX} constituents")

    constituent_map = create_constituents(client, existing_c)
    state["constituents"] = constituent_map
    save_seed_state(state_path, state)
    save_config(config_path, client.config)

    existing_g = set(state.get("gifts") or [])
    existing_gift_ids = list(state.get("gift_ids") or [])
    print(f"Found {len(existing_g)} existing {LOOKUP_PREFIX} gifts in local state")

    _, gift_ids = create_gifts(
        client, constituent_map, existing_g, existing_gift_ids, fund_id
    )
    state["gifts"] = sorted(existing_g)
    state["gift_ids"] = sorted(set(gift_ids))
    save_seed_state(state_path, state)
    save_config(config_path, client.config)

    if not args.skip_segmentation:
        print("Seeding custom fields and constituencies...")
        seed_segmentation(client, constituent_map)
        save_config(config_path, client.config)

    write_allowlist(constituent_map, state["gift_ids"])
    print("Seed complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
