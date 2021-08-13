# tap-blackbaud

[Singer](https://www.singer.io/) tap that extracts data from the [Blackbaud](https://www.blackbaud.com/) API and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

```bash
$ mkvirtualenv -p python3 tap-blackbaud
$ pip install git+https://github.com/hotgluexyz/tap-blackbaud.git
$ tap-blackbaud --config config.json --discover
$ tap-blackbaud --config config.json --catalog catalog.json --state state.json
```

# Quickstart

## Install the tap

```
> pip install git+https://github.com/hotgluexyz/tap-blackbaud.git
```

## Create a Config file

```
{
  "client_id": "qwerty",
  "client_secret": "**********",
  "refresh_token": "**********",
  "redirect_uri": "https://example.com/callback",
  "subscription_key": "my-api-key",
  "start_date": "2018-01-08T00:00:00Z"
}
```

The `client_id`, `client_secret`, `refresh_token` and `redirect_ui` should be generated from the Blackbaud OAuth Authorization Code flow. [Learn more on the Blackbaud docs](https://developer.blackbaud.com/skyapi/docs/authorization/auth-code-flow/tutorial)

The `subscription_key` can be found in your [Blackbaud developer portal](https://developer.sky.blackbaud.com/developer)

The `start_date` is used by the tap as a bound on queries when searching for records.  This should be an [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) formatted date-time, like "2018-01-08T00:00:00Z". For more details, see the [Singer best practices for dates](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#dates).

## Run Discovery

To run discovery mode, execute the tap with the config file.

```
> tap-blackbaud --config config.json --discover > catalog.json
```

## Sync Data

To sync data, select fields in the `catalog.json` output and run the tap.

```
> tap-blackbaud --config config.json --catalog catalog.json [--state state.json]
```
