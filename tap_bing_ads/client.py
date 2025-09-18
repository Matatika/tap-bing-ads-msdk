"""REST client handling, including BingAdsStream base class."""

from __future__ import annotations

import decimal
import typing as t
from functools import cached_property

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream
from typing_extensions import override

from tap_bing_ads.auth import BingAdsAuthenticator

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Auth, Context


class BingAdsStream(RESTStream):
    """BingAds stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return "https://api.mysample.com"

    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BingAdsAuthenticator.create_for_stream(self)

    @override
    @property
    def http_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "DeveloperToken": self.config["developer_token"],
        }

    def get_new_paginator(self) -> BaseAPIPaginator | None:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance, or ``None`` to indicate pagination
            is not supported.
        """
        return super().get_new_paginator()

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    @override
    def post_process(self, row, context):
        for k, v in row.items():
            row[k] = self._transform_property_value(v, breadcrumb=(k,))

        return row

    @cached_property
    def integer_property_breadcrumbs(self):
        """Breadcrumbs of integer properties."""
        return self._property_breadcrumbs_of_type("integer")

    @cached_property
    def number_property_breadcrumbs(self):
        """Breadcrumbs of number properties."""
        return self._property_breadcrumbs_of_type("number")

    @cached_property
    def boolean_property_breadcrumbs(self):
        """Breadcrumbs of boolean properties."""
        return self._property_breadcrumbs_of_type("boolean")

    @cached_property
    def date_property_breadcrumbs(self):
        """Breadcrumbs of date properties."""
        return self._property_breadcrumbs(lambda _, v: v.get("format") == "date")

    @cached_property
    def datetime_property_breadcrumbs(self):
        """Breadcrumbs of date-time properties."""
        return self._property_breadcrumbs(lambda _, v: v.get("format") == "date-time")

    def _property_breadcrumbs(
        self,
        predicate: t.Callable[[tuple[str | tuple[()], ...], dict], bool],
        *,
        parent: tuple[str, ...] = (),
        properties: dict[str, dict] | None = None,
    ):
        properties = properties or self.schema["properties"]
        breadcrumbs: set[tuple[str | tuple[()], ...]] = set()

        for k, v in properties.items():
            breadcrumb = (*parent, k)

            if "object" in v["type"]:
                breadcrumbs |= self._property_breadcrumbs(
                    predicate,
                    parent=breadcrumb,
                    properties=v["properties"],
                )

            elif "array" in v["type"]:
                breadcrumbs |= self._property_breadcrumbs(
                    predicate,
                    parent=breadcrumb,
                    properties={(): v["items"]},
                )

            elif predicate(breadcrumb, v):
                breadcrumbs.add(breadcrumb)

        return breadcrumbs

    def _property_breadcrumbs_of_type(self, type_: str):
        return self._property_breadcrumbs(lambda _, v: type_ in v["type"])

    def to_float(self, value: str) -> float | None:
        """Convert a string value into a float."""
        return float(value)

    def to_date_isoformat(self, value: str):
        """Convert a string value into a date ISO8601 format string."""
        return value

    def to_datetime_isoformat(self, value: str):
        """Convert a string value into a date-time ISO8601 format string."""
        return value

    def _transform_property_value(self, value, breadcrumb: tuple[str, ...]):  # noqa: PLR0911
        if isinstance(value, dict):
            return {
                k: self._transform_property_value(v, breadcrumb=(*breadcrumb, k))
                for k, v in value.items()
            }

        if isinstance(value, list):
            return [
                self._transform_property_value(v, breadcrumb=(*breadcrumb, ()))
                for v in value
            ]

        if not isinstance(value, str):
            return value

        if value == "":
            return value if "__".join(breadcrumb) in self.primary_keys else None

        if breadcrumb in self.integer_property_breadcrumbs:
            return int(value)

        if breadcrumb in self.number_property_breadcrumbs:
            value = self.to_float(value)
            return (
                0.0
                if value is None and "__".join(breadcrumb) in self.primary_keys
                else value
            )

        if breadcrumb in self.boolean_property_breadcrumbs:
            return value.lower() == "true"

        if breadcrumb in self.date_property_breadcrumbs:
            return self.to_date_isoformat(value)

        if breadcrumb in self.datetime_property_breadcrumbs:
            return self.to_datetime_isoformat(value)

        return value
