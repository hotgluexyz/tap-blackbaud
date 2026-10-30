"""Contract tests through discovery and the public Singer sync interface."""

import json
from urllib.parse import parse_qs, urlsplit

import pytest
import responses
from jsonschema import validate

from tap_blackbaud.tap import TapBlackbaud

BASE = "https://api.sky.blackbaud.com"
TOKEN_URL = "https://oauth2.sky.blackbaud.com/token"
CONFIG = {
    "subscription_key": "test-subscription",
    "access_token": "test-access",
    "expires_in": 2100000000,
    "_refresh_token_via_hg_api": False,
}


@pytest.fixture
def api():
    with responses.RequestsMock() as mock:
        yield mock


@pytest.fixture
def run_tap(capsys):
    def run(selected, config=None, state=None, enrich=False, config_path=None):
        config = config_path or {**CONFIG, **(config or {})}
        catalog = TapBlackbaud(config=config).catalog_dict
        for stream in catalog["streams"]:
            for entry in stream["metadata"]:
                entry["metadata"]["selected"] = stream["stream"] in selected
                if not enrich and any(
                    p in ("lifetime_giving", "fundraiser_assignment_list")
                    for p in entry["breadcrumb"]
                ):
                    entry["metadata"]["selected"] = False
        tap = TapBlackbaud(config=config, catalog=catalog, state=state)
        tap.sync_all()
        messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        schemas = {m["stream"]: m["schema"] for m in messages if m["type"] == "SCHEMA"}
        records = [m for m in messages if m["type"] == "RECORD"]
        for record in records:
            validate(record["record"], schemas[record["stream"]])
        states = [m["value"] for m in messages if m["type"] == "STATE"]
        return records, states[-1] if states else {}, schemas

    return run


def test_discovery_is_local_and_exposes_native_schema(api):
    catalog = TapBlackbaud(config=CONFIG).catalog_dict
    streams = {s["stream"]: s for s in catalog["streams"]}
    assert set(streams) == {
        "constituent_lists",
        "constituents",
        "constituents_by_list",
        "educations",
        "addresses",
        "phones",
        "constituent_custom_fields",
        "gifts",
        "gift_custom_fields",
        "address_types",
        "phone_types",
        "email_address_types",
        "constituent_custom_field_categories",
        "gift_custom_field_categories",
        "funds",
        "appeals",
        "packages",
    }
    gift = streams["gifts"]["schema"]["properties"]
    assert "gift_splits" in gift and "payments" in gift and "linked_gifts" in gift
    assert streams["constituents_by_list"]["key_properties"] == ["list_id", "id"]
    assert "first" in streams["constituents_by_list"]["schema"]["properties"]
    assert not api.calls


@pytest.mark.parametrize(
    "stream,inactive", [("funds", "false"), ("appeals", "true"), ("packages", "true")]
)
def test_reference_pagination_and_activity_policy(api, run_tap, stream, inactive):
    url = BASE + "/fundraising/v1/" + stream
    api.get(
        url,
        json={
            "count": 3,
            "value": [{"id": "9", "inactive": False}],
            "next_link": url + "?offset=1&limit=1",
        },
        match=[
            responses.matchers.query_param_matcher(
                {"include_inactive": inactive, "limit": "1", "offset": "0"}
            )
        ],
    )
    api.get(
        url,
        json={
            "count": 3,
            "value": [{"id": "42", "goal": {"value": 12.75}}],
            "next_link": "?offset=2&limit=1",
        },
        match=[
            responses.matchers.query_param_matcher(
                {"include_inactive": inactive, "limit": "1", "offset": "1"}
            )
        ],
    )
    api.get(
        url,
        json={"count": 3, "value": [{"id": "17"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"include_inactive": inactive, "limit": "1", "offset": "2"}
            )
        ],
    )
    records, _, _ = run_tap([stream], {"page_size": 1})
    assert [r["record"]["id"] for r in records] == ["9", "42", "17"]
    assert records[1]["record"]["goal"] == {"value": 12.75}


def test_string_types_and_category_metadata_are_distinct(api, run_tap):
    api.get(
        BASE + "/constituent/v1/addresstypes",
        json={"value": ["Home", "Other / Office"]},
    )
    api.get(
        BASE + "/gift/v1/gifts/customfields/categories",
        json={
            "value": [{"name": "Consent", "type": "Boolean", "one_per_record": True}]
        },
    )
    records, _, _ = run_tap(["address_types", "gift_custom_field_categories"])
    assert [r["record"] for r in records if r["stream"] == "address_types"] == [
        {"name": "Home"},
        {"name": "Other / Office"},
    ]
    assert (
        next(
            r["record"]
            for r in records
            if r["stream"] == "gift_custom_field_categories"
        )["type"]
        == "Boolean"
    )
    assert all(not urlsplit(call.request.url).query for call in api.calls)


def test_selected_lists_keep_memberships_and_page_independently(api, run_tap):
    def constituents(request):
        q = parse_qs(urlsplit(request.url).query)
        list_id = q["list_id"][0]
        offset = q["offset"][0]
        assert list_id in ("B", "C")
        body = {
            "value": [{"id": "shared" if offset == "0" else "later", "first": "Test"}]
        }
        if list_id == "B" and offset == "0":
            body["next_link"] = (
                BASE + "/constituent/v1/constituents?list_id=B&limit=1&offset=1"
            )
        return 200, {}, json.dumps(body)

    api.add_callback(
        responses.GET, BASE + "/constituent/v1/constituents", callback=constituents
    )
    records, _, schemas = run_tap(
        ["constituents_by_list"], {"list_ids": ["B", "C"], "page_size": 1}
    )
    assert [(r["record"]["list_id"], r["record"]["id"]) for r in records] == [
        ("B", "shared"),
        ("B", "later"),
        ("C", "shared"),
    ]
    assert "first" in schemas["constituents_by_list"]["properties"]


def test_list_discovery_pages_before_partitioning(api, run_tap):
    api.get(
        BASE + "/list/v1/lists",
        json={"value": [{"id": "older"}], "next_link": "?offset=1&limit=1"},
        match=[
            responses.matchers.query_param_matcher(
                {"list_type": "Constituent", "offset": "0", "limit": "1"}
            )
        ],
    )
    api.get(
        BASE + "/list/v1/lists",
        json={"value": [{"id": "newer"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"list_type": "Constituent", "offset": "1", "limit": "1"}
            )
        ],
    )
    api.get(BASE + "/constituent/v1/constituents", json={"value": []})
    records, _, _ = run_tap(["constituents_by_list"], {"page_size": 1})
    assert records == []
    queries = [
        parse_qs(urlsplit(c.request.url).query)
        for c in api.calls
        if "/constituent/v1/" in c.request.url
    ]
    assert [q["list_id"] for q in queries] == [["older"], ["newer"]]


@pytest.mark.parametrize(
    "stream,config",
    [
        ("constituents_by_list", {"list_ids": []}),
        ("constituents", {"constituent_ids": []}),
        ("addresses", {"constituent_ids": []}),
        ("phones", {"constituent_ids": []}),
        ("constituent_custom_fields", {"constituent_ids": []}),
        ("gifts", {"gift_ids": []}),
        ("gift_custom_fields", {"gift_ids": []}),
    ],
)
def test_empty_id_selection_never_becomes_unfiltered(api, run_tap, stream, config):
    records, _, _ = run_tap([stream], config)
    assert records == []
    assert not api.calls


def test_incremental_state_reuses_inclusive_boundary_and_sort_token(api, run_tap):
    url = BASE + "/gift/v1/gifts"
    t1, t2 = "2025-01-03T10:00:00+00:00", "2025-01-03T11:00:00+00:00"
    api.get(
        url,
        json={
            "value": [
                {"id": "10", "date_modified": t2},
                {"id": "11", "date_modified": t1},
            ]
        },
    )
    records, state, _ = run_tap(["gifts"], {"start_date": "2025-01-01T00:00:00Z"})
    assert len(records) == 2
    assert state["bookmarks"]["gifts"]["replication_key_value"] == t2
    api.reset()

    def page(request):
        q = parse_qs(urlsplit(request.url).query)
        assert q["last_modified"][0] == t2
        if "sort_token" not in q:
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [{"id": "12", "date_modified": t2}],
                        "next_link": url + "?sort_token=a%2Bb%3D&limit=5000",
                    }
                ),
            )
        if q["sort_token"] == ["done"]:
            # Real Blackbaud terminal pages can retain their continuation link.
            return (
                200,
                {},
                json.dumps({"value": [], "next_link": url + "?sort_token=done"}),
            )
        assert q["sort_token"] == ["a+b="]
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {"id": "13", "date_modified": "2025-01-03T12:00:00+00:00"}
                    ],
                    "next_link": url + "?sort_token=done",
                }
            ),
        )

    api.add_callback(responses.GET, url, callback=page)
    records, next_state, _ = run_tap(["gifts"], state=state)
    assert [r["record"]["id"] for r in records] == ["12", "13"]
    assert (
        next_state["bookmarks"]["gifts"]["replication_key_value"]
        == "2025-01-03T12:00:00+00:00"
    )


def test_native_details_and_typed_custom_values_survive_singer(api, run_tap):
    constituent = {
        "id": "80",
        "first": "Test",
        "requests_no_email": False,
        "birthdate": {"y": 1981, "m": 7},
        "matching_gift_per_gift_max": {"value": 47.25},
    }
    api.get(BASE + "/constituent/v1/constituents/80", json=constituent)
    gift = {
        "id": "70",
        "constituent_id": "80",
        "amount": {"value": 19.27},
        "linked_gifts": ["90"],
        "payments": [{"payment_method": "Other"}],
        "gift_splits": [{"fund_id": "5", "amount": {"value": 19.27}}],
        "recurring_gift_schedule": {"frequency": "MONTHLY"},
        "recurring_gift_status_date": {"y": 2025, "m": 3},
    }
    api.get(BASE + "/gift/v1/gifts/70", json=gift)
    api.get(
        BASE + "/constituent/v1/constituents/80/customfields",
        json={
            "value": [
                {"id": "a", "parent_id": "80", "value": False},
                {"id": "b", "value": 0.31},
            ],
            "next_link": "?offset=2",
        },
        match=[responses.matchers.query_param_matcher({})],
    )
    api.get(
        BASE + "/constituent/v1/constituents/80/customfields",
        json={"value": [{"id": "c", "value": {"y": 2025, "m": 6, "d": 2}}]},
        match=[responses.matchers.query_param_matcher({"offset": "2"})],
    )
    api.get(
        BASE + "/gift/v1/gifts/70/customfields",
        json={
            "value": [
                {"id": "d", "value": "false"},
                {"id": "e", "value": None},
                {"id": "f", "value": 0},
                {"id": "g", "value": True},
            ]
        },
    )
    records, _, _ = run_tap(
        ["constituents", "gifts", "constituent_custom_fields", "gift_custom_fields"],
        {"gift_ids": ["70"], "constituent_ids": ["80"]},
    )
    assert (
        next(r["record"] for r in records if r["stream"] == "constituents")
        == constituent
    )
    assert next(r["record"] for r in records if r["stream"] == "gifts") == gift
    values = {
        r["record"]["id"]: r["record"]["value"]
        for r in records
        if r["stream"].endswith("custom_fields")
    }
    expected = {
        "a": False,
        "b": 0.31,
        "c": {"y": 2025, "m": 6, "d": 2},
        "d": "false",
        "e": None,
        "f": 0,
        "g": True,
    }
    assert values == expected
    for record in records:
        if record["stream"].endswith("custom_fields"):
            row = record["record"]
            decoded = json.loads(row["value_json"])
            assert decoded == expected[row["id"]]
            assert type(decoded) is type(expected[row["id"]])
            assert row["parent_id"] == (
                "70" if record["stream"] == "gift_custom_fields" else "80"
            )


def test_legacy_enrichments_are_authenticated_and_paginated(api, run_tap):
    api.get(
        BASE + "/constituent/v1/constituents",
        json={
            "value": [
                {"id": "8", "first": "Test", "date_modified": "2025-01-03T10:00:00Z"}
            ]
        },
    )
    api.get(
        BASE + "/constituent/v1/constituents/8/givingsummary/lifetimegiving",
        json={"total_giving": {"value": 18.75}, "total_years_given": 2},
    )
    url = BASE + "/constituent/v1/constituents/8/fundraiserassignments"
    api.get(
        url,
        json={
            "value": [{"id": "9", "amount": {"value": 3.25}}],
            "next_link": url + "?offset=1",
        },
        match=[responses.matchers.query_param_matcher({"include_inactive": "true"})],
    )
    api.get(
        url,
        json={"value": [{"id": "10", "amount": {"value": 4.75}}]},
        match=[responses.matchers.query_param_matcher({"offset": "1"})],
    )
    records, _, _ = run_tap(["constituents"], enrich=True)
    assert records[0]["record"]["lifetime_giving"]["total_giving"] == 18.75
    assert [
        a["amount"] for a in records[0]["record"]["fundraiser_assignment_list"]
    ] == [3.25, 4.75]
    assert all(
        c.request.headers["Authorization"] == "Bearer test-access" for c in api.calls
    )


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500])
def test_errors_fail_instead_of_emitting_empty_success(
    api, run_tap, monkeypatch, status
):
    monkeypatch.setattr("tap_blackbaud.client.time.sleep", lambda _: None)
    api.get(BASE + "/gift/v1/gifts", status=status, json={"error": "test-error"})
    with pytest.raises(Exception, match=f"Blackbaud HTTP {status}"):
        run_tap(["gifts"])
    assert len(api.calls) == (5 if status == 500 else 1)


def test_throttling_honors_retry_after(api, run_tap, monkeypatch):
    waits = []
    monkeypatch.setattr("tap_blackbaud.client.time.sleep", waits.append)
    api.get(BASE + "/gift/v1/gifts", status=429, headers={"Retry-After": "3"})
    api.get(
        BASE + "/gift/v1/gifts",
        json={"value": [{"id": "9", "date_modified": "2025-01-03T10:00:00Z"}]},
    )
    records, _, _ = run_tap(["gifts"])
    assert records[0]["record"]["id"] == "9"
    assert waits == [3]


@pytest.mark.parametrize(
    "link",
    [
        "https://other.example/gift/v1/gifts?offset=1",
        "https://api.sky.blackbaud.com/constituent/v1/constituents?offset=1",
    ],
)
def test_unexpected_pagination_route_is_not_followed(api, run_tap, link):
    api.get(BASE + "/gift/v1/gifts", json={"value": [], "next_link": link})
    with pytest.raises(ValueError, match="Unexpected Blackbaud pagination URL"):
        run_tap(["gifts"])
    assert len(api.calls) == 1


def test_rotated_oauth_is_shared_persisted_and_not_logged(
    api, run_tap, caplog, tmp_path
):
    api.post(
        TOKEN_URL,
        json={
            "access_token": "fresh-secret-access",
            "refresh_token": "fresh-secret-refresh",
            "expires_in": 3600,
        },
    )
    api.get(BASE + "/constituent/v1/addresstypes", json={"value": ["Home"]})
    api.get(BASE + "/gift/v1/gifts", json={"value": []})
    run_tap(
        ["address_types", "gifts"],
        {
            "access_token": "expired",
            "expires_in": 1,
            "client_id": "test-client",
            "client_secret": "test-secret",
            "refresh_token": "initial-secret-refresh",
        },
    )
    assert len([c for c in api.calls if c.request.url == TOKEN_URL]) == 1
    assert all(
        c.request.headers["Authorization"] == "Bearer fresh-secret-access"
        for c in api.calls
        if c.request.url.startswith(BASE)
    )
    assert (
        "fresh-secret-access" not in caplog.text
        and "fresh-secret-refresh" not in caplog.text
    )
    api.reset()
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                **CONFIG,
                "expires_in": 1,
                "client_id": "client",
                "client_secret": "secret",
                "refresh_token": "initial",
            }
        )
    )
    for access, refresh in (
        ("first-secret", "second-secret"),
        ("third-secret", "fourth-secret"),
    ):
        api.post(
            TOKEN_URL,
            json={"access_token": access, "refresh_token": refresh, "expires_in": 3600},
        )
    api.get(BASE + "/constituent/v1/addresstypes", json={"value": ["Home"]})
    run_tap(["address_types"], config_path=path)
    persisted = json.loads(path.read_text())
    assert persisted["access_token"] == "first-secret"
    assert persisted["refresh_token"] == "second-secret"
    persisted["expires_in"] = 1
    path.write_text(json.dumps(persisted))
    run_tap(["address_types"], config_path=path)
    oauth_calls = [c for c in api.calls if c.request.url == TOKEN_URL]
    assert parse_qs(oauth_calls[-1].request.body)["refresh_token"] == ["second-secret"]
    assert json.loads(path.read_text())["refresh_token"] == "fourth-secret"
    assert all(
        secret not in caplog.text
        for secret in ("first-secret", "second-secret", "third-secret", "fourth-secret")
    )


def test_managed_oauth_refresh_is_shared_across_streams(
    api, run_tap, monkeypatch, caplog
):
    monkeypatch.setattr(
        "hotglue_singer_sdk.authenticators.fetch_access_token_from_hotglue_api",
        lambda _: {"access_token": "managed-secret", "expires_in": 2100000000},
    )
    api.get(BASE + "/constituent/v1/addresstypes", json={"value": ["Home"]})
    api.get(BASE + "/constituent/v1/phonetypes", json={"value": ["Mobile"]})
    records, _, _ = run_tap(
        ["address_types", "phone_types"],
        {"expires_in": 1, "_refresh_token_via_hg_api": True},
    )
    assert len(records) == 2
    assert all(
        c.request.headers["Authorization"] == "Bearer managed-secret" for c in api.calls
    )
    assert "managed-secret" not in caplog.text
