"""Stream class for tap-blackbaud."""

import requests
import backoff
from requests.exceptions import RequestException, ConnectionError, ReadTimeout
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer.schema import Schema

from singer_sdk.streams import RESTStream
from singer_sdk.helpers._util import utc_now
from singer_sdk.helpers._singer import (
    Catalog,
)
from singer_sdk.plugin_base import PluginBase as TapBaseClass

from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    SimpleAuthenticator,
    OAuthAuthenticator,
    OAuthJWTAuthenticator
)

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

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class BlackbaudAuthenticator(OAuthAuthenticator):

    @property
    def oauth_request_body(self) -> dict:
        req = {
            'grant_type': 'refresh_token',
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'refresh_token': self.config["refresh_token"] if not self.refresh_token else self.refresh_token,
            'redirect_uri': self.config["redirect_uri"]
        }

        return req

    def update_access_token(self):
        """Update `access_token` along with: `last_refreshed` and `expires_in`."""
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload)
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.logger.info(f"New access token received: {self.access_token}")
        self.expires_in = token_json["expires_in"]
        self.logger.info(f"New expires_in received: {self.expires_in}")
        self.last_refreshed = request_time
        self.logger.info(f"New last_refreshed time: {self.last_refreshed}")

        if token_json.get("refresh_token") is not None:
            self.refresh_token = token_json["refresh_token"]
            self.logger.info(f"New refresh_token: {self.refresh_token}")
        
        self.logger.info(f"New token response: {token_json}")


class MockedResponse:
    def __init__(self, response):
        self.response = response
    
    def json(self):
        return {}

    @property
    def headers(self):
        if self.response == None:
            return {}
        return self.response.headers


class RetriableException(Exception):
    pass


def validate_status_code(e):
    if e.response == None:
        return True
    
    if e.response.status_code in [400, 401, 402, 403]:
        return False
    
    return True


class BlackbaudStream(RESTStream):
    """Blackbaud stream class."""

    records_jsonpath: str = "$.value[*]"

    def __init__(
        self,
        tap: TapBaseClass,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None,
        path: Optional[str] = None,
    ):
        """Initialize the Blackbaud stream."""
        super().__init__(name=name, schema=schema, tap=tap, path=path)
        self._config = tap._config

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.sky.blackbaud.com"

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            RetriableException,
            ConnectionError,
            ReadTimeout
        ),
        max_tries=5,
        factor=2
    )
    def post(self, prepared_request, timeout=60):
        return self.requests_session.send(prepared_request, timeout=timeout)
    
    def _request_with_backoff(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        """TODO.

        Args:
            prepared_request: TODO
            context: Stream partition or context dictionary.

        Returns:
            TODO

        Raises:
            RuntimeError: TODO
        """
        response = None
        try:
            response = self.post(
                prepared_request,
                timeout=60
            )
        except Exception as e:
            exc = e

        if response == None:
            return MockedResponse(response)

        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = cast(str, prepared_request.path_url)
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )        

        if response.status_code == 200:
            return response

        elif response.status_code > 500 or response.status_code == 404:
            self.logger.info("{} response received, skipping request for: {}".format(
                response.status_code,
                prepared_request.url
            ))
            return MockedResponse(response)

        elif response.status_code in [400, 401, 402, 403]:
            raise RuntimeError(
                f"Failed request, response was '{response.json()}'."
            )

        self.logger.debug("Response received successfully.")
        return response

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        if not self._config.get("authenticator"):
            auth_endpoint = "https://oauth2.sky.blackbaud.com/token"

            self._config["authenticator"] = BlackbaudAuthenticator(
                stream=self,
                auth_endpoint=auth_endpoint
            )

        return self._config["authenticator"]

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests.

        If an authenticator is also specified, the authenticator's headers will be
        combined with `http_headers` when making HTTP requests.
        """
        result = super().http_headers
        result['Bb-Api-Subscription-Key'] = self._config["subscription_key"]
        return result


class ConstituentListsStream(BlackbaudStream):
    name = "constituent_lists"

    path = "/list/v1/lists?list_type=Constituent"

    primary_keys = ["id"]
    replication_key = None

    schema = PropertiesList(
        Property("id", StringType),
        Property("name", StringType),
        Property("description", StringType),
        Property("record_count", IntegerType),
        Property("date_modified", DateTimeType),
        Property("last_modified_by_user_name", StringType),
        Property("last_modified_by_user_id", StringType),
        Property("is_public", BooleanType)
    ).to_dict()


class ConstituentsStream(BlackbaudStream):

    name = "constituents"
    path = "/constituent/v1/constituents"
    primary_keys = ["id"]
    replication_key = None

    flatten_list = set(["total_committed_matching_gifts", "total_giving", "total_pledge_balance", "total_received_giving", "total_received_matching_gifts", "total_soft_credits"])

    schema = PropertiesList(
        Property("id", StringType),
        Property("address", ObjectType(
            Property("id", StringType),
            Property("address_lines", StringType),
            Property("city", StringType),
            Property("country", StringType),
            Property("county", StringType),
            Property("formatted_address", StringType),
            Property("inactive", BooleanType),
            Property("postal_code", StringType),
            Property("preferred", BooleanType),
            Property("state", StringType)
        )),
        Property("age", IntegerType),
        Property("birthdate", ObjectType(
            Property("d", IntegerType),
            Property("m", IntegerType),
            Property("y", IntegerType)
        )),
        Property("date_added", DateTimeType),
        Property("date_modified", DateTimeType),
        Property("deceased", BooleanType),
        Property("email", ObjectType(
            Property("id", StringType),
            Property("address", StringType),
            Property("do_not_email", BooleanType),
            Property("inactive", BooleanType),
            Property("primary", BooleanType),
            Property("type", StringType)
        )),
        Property("first", StringType),
        Property("fundraiser_status", StringType),
        Property("gender", StringType),
        Property("gives_anonymously", BooleanType),
        Property("inactive", BooleanType),
        Property("last", StringType),
        Property("lookup_id", StringType),
        Property("middle", StringType),
        Property("name", StringType),
        Property("online_presence", ObjectType (
            Property("id", StringType),
            Property("address", StringType),
            Property("inactive", BooleanType),
            Property("primary", BooleanType),
            Property("type", StringType)
        )),
        Property("phone", ObjectType(
            Property("id", StringType),
            Property("do_not_call", BooleanType),
            Property("inactive", BooleanType),
            Property("number", StringType),
            Property("primary", BooleanType),
            Property("type", StringType)
        )),
        Property("preferred_name", StringType),
        Property("spouse", ObjectType (
            Property("id", StringType),
            Property("first", StringType),
            Property("last", StringType),
            Property("is_head_of_household", BooleanType)
        )),
        Property("title", StringType),
        Property("type", StringType),
        Property("lifetime_giving", ObjectType(
            Property("consecutive_years_given", IntegerType),
            Property("total_committed_matching_gifts", NumberType),
            Property("total_giving", NumberType),
            Property("total_pledge_balance", NumberType),
            Property("total_received_giving", NumberType),
            Property("total_received_matching_gifts", NumberType),
            Property("total_soft_credits", NumberType),
            Property("total_years_given", IntegerType),
        )),
        Property("fundraiser_assignment_list", ArrayType(
            ObjectType(
                Property("id", StringType),             # required
                Property("campaign_id", StringType),    # required
                Property("fundraiser_id", StringType),  # required
                Property("appeal_id", StringType),      # optional
                Property("fund_id", StringType),        # optional
                Property("amount", NumberType),         # required
                Property("start", DateTimeType),        # optional
                Property("end", DateTimeType),          # optional
                Property("type", StringType),           # required
            )
        ))
    ).to_dict()


    def apply_catalog(self, catalog: Catalog) -> None:
        """Apply a catalog dict, updating any settings overridden within the catalog.

        Developers may override this method in order to introduce advanced catalog
        parsing, or to explicitly fail on advanced catalog customizations which
        are not supported by the tap.

        Args:
            catalog: Catalog object passed to the tap. Defines schema, primary and
                replication keys, as well as selection metadata.
        """
        self._tap_input_catalog = catalog

        catalog_entry = catalog.get_stream(self.name)
        if catalog_entry:
            self.primary_keys = catalog_entry.key_properties
            self.replication_key = catalog_entry.replication_key
            if catalog_entry.replication_method:
                self.forced_replication_method = catalog_entry.replication_method


        lifetime_giving_meta = self.metadata.get(('properties', 'lifetime_giving'), None)
        self.include_lifetime_giving = True if lifetime_giving_meta and lifetime_giving_meta.selected else False

        fundraiser_assignment_meta = self.metadata.get(('properties', 'fundraiser_assignment_list'), None)
        self.include_fundraiser_assignment = True if fundraiser_assignment_meta and fundraiser_assignment_meta.selected else False


    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            A new, processed record.
        """

        constituent_id = row["id"]

        # LIFETIME GIVING
        if self.include_lifetime_giving:
            lifetime_giving_endpoint = f"{self.url_base}/constituent/v1/constituents/{constituent_id}/givingsummary/lifetimegiving"
            resp = requests.get(lifetime_giving_endpoint, headers=self.http_headers)
            # todo: test response code
            lifetime_giving_json = resp.json()
            giving_object = {**lifetime_giving_json}
            for key in lifetime_giving_json:
                if (key in self.flatten_list):
                    giving_object[key] = lifetime_giving_json[key]["value"]
            row["lifetime_giving"] = giving_object

        # FUNDRAISER ASSIGNMENT
        if self.include_fundraiser_assignment:
            include_inactive = 'true'
            fundraiser_assignment_endpoint = f"{self.url_base}/constituent/v1/constituents/{constituent_id}/fundraiserassignments?include_inactive={include_inactive}"
            resp = requests.get(fundraiser_assignment_endpoint, headers=self.http_headers)
            # todo: test response code
            fundraiser_assignment_json = resp.json()
            fundraiser_list = fundraiser_assignment_json["value"]
            # flatten amount -- here or during transform?
            for item in fundraiser_list:
                if ("amount" in item):
                    item["amount"] = item["amount"]["value"]
            row["fundraiser_assignment_list"] = fundraiser_list

        # self.logger.info(row)

        return row


class ConstituentsByListStream(BlackbaudStream):
    name = "constituents_by_list"

    path = "/constituent/v1/constituents"

    primary_keys = ["id"]
    replication_key = None

    def get_lists(self, headers):
        endpoint = f"{self.url_base}/list/v1/lists?list_type=Constituent"
        r = requests.get(endpoint, headers=headers)
        if r.status_code in [404, 500]:
            return []
        
        lists = r.json()
        return lists.get("value", [])

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Return a list of partition key dicts (if applicable), otherwise None.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.
        Developers may override this property to provide a default partitions list.
        """
        result: List[dict] = []
        # Get headers
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})

        # Get lists
        lists = self.get_lists(headers)

        for l in lists:
            result.append({
                'list_id': l['id']
            })

        return result or None

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Add list_id to response
        row['list_id'] = context['list_id']
        return row

    def get_url_params(
        self,
        partition: Optional[dict],
        next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override this method with specific paging
        logic.
        """
        params = {}
        if partition == None:
            return params
        
        if partition.get("list_id"):
            params["list_id"] = partition["list_id"]
        
        return params

    schema = PropertiesList(
        Property("id", StringType),
        Property("list_id", StringType),
        Property("address", ObjectType(
            Property("id", StringType),
            Property("address_lines", StringType),
            Property("city", StringType),
            Property("constituent_id", StringType),
            Property("country", StringType),
            Property("county", StringType),
            Property("formatted_address", StringType),
            Property("inactive", BooleanType),
            Property("postal_code", StringType),
            Property("preferred", BooleanType),
            Property("state", StringType)
        )),
        Property("age", IntegerType),
        Property("birthdate", ObjectType(
            Property("d", IntegerType),
            Property("m", IntegerType),
            Property("y", IntegerType)
        )),
        Property("date_added", DateTimeType),
        Property("date_modified", DateTimeType),
        Property("deceased", BooleanType),
        Property("email", ObjectType(
            Property("id", StringType),
            Property("address", StringType),
            Property("constituent_id", StringType),
            Property("do_not_email", BooleanType),
            Property("inactive", BooleanType),
            Property("primary", BooleanType),
            Property("type", StringType)
        )),
        Property("first", StringType),
        Property("fundraiser_status", StringType),
        Property("gender", StringType),
        Property("gives_anonymously", BooleanType),
        Property("inactive", BooleanType),
        Property("last", StringType),
        Property("lookup_id", StringType),
        Property("middle", StringType),
        Property("name", StringType),
        Property("phone", ObjectType(
            Property("id", StringType),
            Property("constituent_id", StringType),
            Property("do_not_call", BooleanType),
            Property("inactive", BooleanType),
            Property("number", StringType),
            Property("primary", BooleanType),
            Property("type", StringType)
        )),
        Property("suffix", StringType),
        Property("title", StringType),
        Property("type", StringType)
    ).to_dict()


class EducationsStream(BlackbaudStream):

    name = "educations"

    path = "/constituent/v1/educations"

    primary_keys = ["id"]
    replication_key = None

    schema = PropertiesList(
        Property("id", StringType),
        Property("constituent_id", StringType),
        Property("campus", StringType),
        Property("class_of", StringType),
        Property("date_added", StringType),
        Property("date_entered", ObjectType(
            Property("d", IntegerType),
            Property("m", IntegerType),
            Property("y", IntegerType)
        )),
        Property("date_graduated", ObjectType(
            Property("d", IntegerType),
            Property("m", IntegerType),
            Property("y", IntegerType)
        )),
        Property("date_modified", StringType),
        Property("degree", StringType),
        Property("majors", ArrayType(StringType)),
        Property("minors", ArrayType(StringType)),
        Property("primary", BooleanType),
        Property("school", StringType),
        Property("type", StringType)
    ).to_dict()
