"""Blackbaud tap using Hotglue's Singer SDK."""

from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th

from tap_blackbaud.auth import BlackbaudAuthenticator
from tap_blackbaud.streams import STREAM_TYPES


class TapBlackbaud(Tap):
    """Read Blackbaud native records and reference catalogs."""

    name = "tap-blackbaud"
    static_stream_names = [stream.name for stream in STREAM_TYPES]
    config_jsonschema = th.PropertiesList(
        th.Property("subscription_key", th.StringType, required=True),
        th.Property("client_id", th.StringType),
        th.Property("client_secret", th.StringType),
        th.Property("refresh_token", th.StringType),
        th.Property("redirect_uri", th.StringType),
        th.Property("access_token", th.StringType),
        th.Property("expires_in", th.IntegerType),
        th.Property("_refresh_token_via_hg_api", th.BooleanType, default=True),
        th.Property("start_date", th.DateTimeType),
        th.Property("list_ids", th.ArrayType(th.StringType)),
        th.Property("constituent_ids", th.ArrayType(th.StringType)),
        th.Property("gift_ids", th.ArrayType(th.StringType)),
        th.Property(
            "page_size",
            th.CustomType({"type": "integer", "minimum": 1, "maximum": 5000}),
            default=5000,
        ),
    ).to_dict()

    @classmethod
    def access_token_support(cls, connector=None):
        return BlackbaudAuthenticator, "https://oauth2.sky.blackbaud.com/token"

    def discover_streams(self):
        return [stream(tap=self) for stream in STREAM_TYPES]


cli = TapBlackbaud.cli
