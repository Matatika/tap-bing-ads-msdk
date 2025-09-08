"""BingAds tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_bing_ads import streams


class TapBingAds(Tap):
    """BingAds tap class."""

    name = "tap-bing-ads"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            title="Client ID",
            description="App OAuth 2.0 client secret",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            title="Client secret",
            description="App OAuth 2.0 client secret",
            secret=True,
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            title="Refresh secret",
            description="App OAuth 2.0 refresh token",
            secret=True,
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.BingAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapBingAds.cli()
