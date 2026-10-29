"""Native records and reference streams for Blackbaud SKY API v1."""

import copy
import json
from functools import cached_property
from pathlib import Path
from urllib.parse import quote

from tap_blackbaud.client import BlackbaudStream

SCHEMAS = json.loads(Path(__file__).with_name("schemas.json").read_text())


class ConstituentListsStream(BlackbaudStream):
    name = "constituent_lists"
    path = "/list/v1/lists"
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "name": {"type": ["string", "null"]},
            "description": {"type": ["string", "null"]},
            "record_count": {"type": ["integer", "null"]},
            "date_modified": {"type": ["string", "null"], "format": "date-time"},
            "last_modified_by_user_name": {"type": ["string", "null"]},
            "last_modified_by_user_id": {"type": ["string", "null"]},
            "is_public": {"type": ["boolean", "null"]},
        },
    }

    def get_url_params(self, context, next_page_token):
        return {
            "list_type": "Constituent",
            **super().get_url_params(context, next_page_token),
        }


class ConstituentsStream(BlackbaudStream):
    name = "constituents"
    path = "/constituent/v1/constituents"
    replication_key = "date_modified"
    schema = SCHEMAS[name]
    include_inactive = True

    def __init__(self, tap, **kwargs):
        super().__init__(tap=tap, **kwargs)
        if (
            self.name == "constituents"
            and self.config.get("constituent_ids") is not None
        ):
            self.replication_key = None

    def get_url_params(self, context, next_page_token):
        params = {
            "include_deceased": "true",
            **super().get_url_params(context, next_page_token),
        }
        if self.config.get("constituent_ids") is not None:
            params["constituent_id"] = self.config["constituent_ids"]
        return params

    def get_records(self, context):
        ids = self.config.get("constituent_ids")
        if self.name == "constituents" and ids is not None:
            for constituent_id in dict.fromkeys(ids):
                yield self.post_process(
                    self.get_json(f"{self.path}/{quote(constituent_id, safe='')}"),
                    context,
                )
        elif ids != []:
            yield from super().get_records(context)

    def post_process(self, row, context=None):
        # Retain the original opt-in enrichment fields without bypassing auth,
        # retries, or pagination. Explicit field selection enables the extra reads.
        lifetime = self.metadata.get(("properties", "lifetime_giving"))
        if lifetime and lifetime.selected:
            data = self.get_json(
                f"/constituent/v1/constituents/{quote(row['id'], safe='')}/givingsummary/lifetimegiving"
            )
            row["lifetime_giving"] = {
                k: v.get("value") if isinstance(v, dict) and "value" in v else v
                for k, v in data.items()
            }
        assignments = self.metadata.get(("properties", "fundraiser_assignment_list"))
        if assignments and assignments.selected:
            entries = []
            path = f"/constituent/v1/constituents/{quote(row['id'], safe='')}/fundraiserassignments"
            for entry in self.get_collection(path, {"include_inactive": "true"}):
                entry = dict(entry)
                if isinstance(entry.get("amount"), dict):
                    entry["amount"] = entry["amount"].get("value")
                entries.append(entry)
            row["fundraiser_assignment_list"] = entries
        return row


class ConstituentsByListStream(ConstituentsStream):
    name = "constituents_by_list"
    replication_key = None
    primary_keys = ["list_id", "id"]
    schema = copy.deepcopy(SCHEMAS["constituents"])
    schema["properties"]["list_id"] = {"type": "string"}

    @cached_property
    def partitions(self):
        ids = self.config.get("list_ids")
        if ids is None:
            ids = [
                r["id"]
                for r in self._tap.streams["constituent_lists"].get_records(None)
            ]
        return [{"list_id": value} for value in dict.fromkeys(ids)]

    def get_records(self, context):
        if not context or not context.get("list_id"):
            return
        yield from super().get_records(context)

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if context:
            params["list_id"] = context["list_id"]
        return params

    def post_process(self, row, context=None):
        row = super().post_process(row, context)
        row["list_id"] = context["list_id"]
        return row


class EducationsStream(BlackbaudStream):
    name = "educations"
    path = "/constituent/v1/educations"
    replication_key = "date_modified"
    schema = SCHEMAS[name]


class AddressesStream(BlackbaudStream):
    name = "addresses"
    path = "/constituent/v1/addresses"
    replication_key = "date_modified"
    schema = SCHEMAS[name]
    include_inactive = True

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if self.config.get("constituent_ids") is not None:
            params["constituent_id"] = self.config["constituent_ids"]
        return params

    def get_records(self, context):
        if self.config.get("constituent_ids") == []:
            return
        yield from super().get_records(context)


class PhonesStream(AddressesStream):
    name = "phones"
    path = "/constituent/v1/phones"
    schema = SCHEMAS[name]


class CustomFieldsStream(BlackbaudStream):
    replication_key = "date_modified"

    def __init__(self, tap, **kwargs):
        super().__init__(tap=tap, **kwargs)
        if self.config.get(self.id_config) is not None:
            # Detail collections need not contain modification timestamps.
            self.replication_key = None

    @property
    def partitions(self):
        ids = self.config.get(self.id_config)
        return (
            [{"parent_id": value} for value in dict.fromkeys(ids)]
            if ids is not None
            else None
        )

    def get_records(self, context):
        if self.config.get(self.id_config) == []:
            return
        if context and context.get("parent_id"):
            parent_id = quote(context["parent_id"], safe="")
            for row in self.get_collection(
                f"{self.path.removesuffix('/customfields')}/{parent_id}/customfields"
            ):
                # Scoped responses may omit the parent identifier.
                yield self.post_process(
                    {"parent_id": context["parent_id"], **row}, context
                )
        else:
            yield from super().get_records(context)

    def post_process(self, row, context=None):
        # Keep native Singer types and a lossless encoding for string-only
        # mixed-type columns in target-parquet.
        if "value" in row:
            row["value_json"] = json.dumps(row["value"], separators=(",", ":"))
        return row


class ConstituentCustomFieldsStream(CustomFieldsStream):
    name = "constituent_custom_fields"
    path = "/constituent/v1/constituents/customfields"
    id_config = "constituent_ids"
    schema = SCHEMAS[name]


class GiftsStream(BlackbaudStream):
    name = "gifts"
    path = "/gift/v1/gifts"
    replication_key = "date_modified"
    schema = SCHEMAS[name]

    def __init__(self, tap, **kwargs):
        super().__init__(tap=tap, **kwargs)
        if self.config.get("gift_ids") is not None:
            self.replication_key = None

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if self.config.get("constituent_ids") is not None:
            params["constituent_id"] = self.config["constituent_ids"]
        return params

    def get_records(self, context):
        ids = self.config.get("gift_ids")
        if ids is not None:
            for gift_id in dict.fromkeys(ids):
                yield self.get_json(f"{self.path}/{quote(gift_id, safe='')}")
        elif self.config.get("constituent_ids") != []:
            yield from super().get_records(context)


class GiftCustomFieldsStream(CustomFieldsStream):
    name = "gift_custom_fields"
    path = "/gift/v1/gifts/customfields"
    id_config = "gift_ids"
    schema = SCHEMAS[name]


class AddressTypesStream(BlackbaudStream):
    name = "address_types"
    path = "/constituent/v1/addresstypes"
    paged = False
    primary_keys = ["name"]
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}

    def parse_response(self, response):
        for value in response.json()["value"]:
            yield {"name": value}


class PhoneTypesStream(AddressTypesStream):
    name = "phone_types"
    path = "/constituent/v1/phonetypes"


class EmailAddressTypesStream(AddressTypesStream):
    name = "email_address_types"
    path = "/constituent/v1/emailaddresstypes"


class ConstituentCustomFieldCategoriesStream(BlackbaudStream):
    name = "constituent_custom_field_categories"
    path = "/constituent/v1/constituents/customfields/categories/details"
    paged = False
    primary_keys = ["name"]
    schema = SCHEMAS[name]


class GiftCustomFieldCategoriesStream(ConstituentCustomFieldCategoriesStream):
    name = "gift_custom_field_categories"
    path = "/gift/v1/gifts/customfields/categories"
    schema = SCHEMAS[name]


class FundsStream(BlackbaudStream):
    name = "funds"
    path = "/fundraising/v1/funds"
    include_inactive = False
    schema = SCHEMAS[name]


class AppealsStream(BlackbaudStream):
    name = "appeals"
    path = "/fundraising/v1/appeals"
    include_inactive = True
    schema = SCHEMAS[name]


class PackagesStream(BlackbaudStream):
    name = "packages"
    path = "/fundraising/v1/packages"
    include_inactive = True
    schema = SCHEMAS[name]


STREAM_TYPES = [
    ConstituentListsStream,
    ConstituentsStream,
    ConstituentsByListStream,
    EducationsStream,
    AddressesStream,
    PhonesStream,
    ConstituentCustomFieldsStream,
    GiftsStream,
    GiftCustomFieldsStream,
    AddressTypesStream,
    PhoneTypesStream,
    EmailAddressTypesStream,
    ConstituentCustomFieldCategoriesStream,
    GiftCustomFieldCategoriesStream,
    FundsStream,
    AppealsStream,
    PackagesStream,
]
