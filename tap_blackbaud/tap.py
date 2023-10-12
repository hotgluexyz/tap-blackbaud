"""Blackbaud tap class."""
import json
import traceback

from pathlib import Path, PurePath
from typing import List, Optional, Union

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_blackbaud.streams import (
    BlackbaudStream,

    ConstituentListsStream,
    ConstituentsStream,
    ConstituentsByListStream,
    EducationsStream
)


# List of available streams
STREAM_TYPES = [
    ConstituentListsStream,
    ConstituentsStream,
    ConstituentsByListStream,
    EducationsStream
]


class TapBlackbaud(Tap):
    """Blackbaud tap class."""

    name = "tap-blackbaud"

    config_jsonschema = PropertiesList(
        Property("client_id", StringType, required=True),
        Property("client_secret", StringType, required=True),
        Property("refresh_token", StringType, required=True),
        Property("redirect_uri", StringType, required=True),
        Property("subscription_key", StringType, required=True),
        Property("start_date", DateTimeType)
    ).to_dict()


    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        *args,
        **kwargs
    ) -> None:
        """Initialize the tap."""
        self.config_path = config
        super().__init__(config=config, *args, **kwargs)

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    def sync_all(self):
        """Sync all streams."""
        # Do the sync
        exc = None
        try:
            super().sync_all()
        except Exception as e:
            exc = e
        finally:
            # Update config if needed
            self.update_config()
            
            if exc is not None:
                self.logger.error(exc)
                traceback.print_exc()
                raise exc
            

    def update_config(self):
        """Update config.json with new access + refresh token."""
        self.logger.info("Updating config.")
        path = self.config_path
        auth = self._config.pop("authenticator", None)

        if auth is not None:
            if auth.refresh_token is not None:
                self._config["refresh_token"] = auth.refresh_token
            
            if auth.access_token is not None:
                self._config["access_token"] = auth.access_token

        if isinstance(path, list):
            path = path[0]

        with open(path, 'w') as f:
            json.dump(self._config, f, indent=4)

# CLI Execution:

cli = TapBlackbaud.cli
