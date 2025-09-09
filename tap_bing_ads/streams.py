"""Stream type classes for tap-bing-ads."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_bing_ads.client import BingAdsStream


class _AccountInfoStream(BingAdsStream):
    name = "_account_info"
    selected = False

    schema = th.PropertiesList(
        th.Property("AccountLifeCycleStatus", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("Number", th.StringType),
        th.Property("PauseReason", th.IntegerType),
    ).to_dict()

    http_method = "POST"
    url_base = "https://clientcenter.api.bingads.microsoft.com"
    path = "/CustomerManagement/v13/AccountsInfo/Query"
    records_jsonpath = "$.AccountsInfo[*]"

    @override
    def prepare_request_payload(self, context, next_page_token):
        return {"CustomerId": self.config["customer_id"]}

    @override
    def get_child_context(self, record, context):
        return {"account_id": record["Id"]}


class AccountStream(BingAdsStream):
    """Define accounts stream."""

    parent_stream_type = _AccountInfoStream
    name = "accounts"

    schema = th.PropertiesList(
        th.Property("AccountFinancialStatus", th.StringType),
        th.Property("AccountLifeCycleStatus", th.StringType),
        th.Property("AccountMode", th.StringType),
        th.Property("AutoTagType", th.StringType),
        th.Property("BackUpPaymentInstrumentId", th.StringType),
        th.Property("BillingThresholdAmount", th.StringType),
        th.Property("BillToCustomerId", th.StringType),
        th.Property(
            "BusinessAddress",
            th.ObjectType(
                th.Property("City", th.StringType),
                th.Property("CountryCode", th.StringType),
                th.Property("Id", th.StringType),
                th.Property("Line1", th.StringType),
                th.Property("Line2", th.StringType),
                th.Property("Line3", th.StringType),
                th.Property("Line4", th.StringType),
                th.Property("PostalCode", th.StringType),
                th.Property("StateOrProvince", th.StringType),
                th.Property("TimeStamp", th.StringType),
                th.Property("BusinessName", th.DateTimeType),
            ),
        ),
        th.Property("CurrencyCode", th.StringType),
        th.Property("ForwardCompatibilityMap", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Language", th.StringType),
        th.Property("LastModifiedByUserId", th.StringType),
        th.Property("LastModifiedTime", th.DateTimeType),
        th.Property(
            "LinkedAgencies",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Id", th.StringType),
                    th.Property("Name", th.StringType),
                )
            ),
        ),
        th.Property("Name", th.StringType),
        th.Property("Number", th.StringType),
        th.Property("ParentCustomerId", th.StringType),
        th.Property("PauseReason", th.IntegerType),
        th.Property("PaymentMethodId", th.StringType),
        th.Property("PaymentMethodType", th.StringType),
        th.Property("PrimaryUserId", th.StringType),
        th.Property("SalesHouseCustomerId", th.StringType),
        th.Property("SoldToPaymentInstrumentId", th.StringType),
        th.Property("TaxCertificate", th.StringType),
        th.Property(
            "TaxInformation",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Key", th.StringType),
                    th.Property("Value", th.StringType),
                ),
            ),
        ),
        th.Property("TimeStamp", th.StringType),
        th.Property("TimeZone", th.StringType),
    ).to_dict()

    http_method = "POST"
    url_base = "https://clientcenter.api.bingads.microsoft.com"
    path = "/CustomerManagement/v13/Account/Query"
    records_jsonpath = "$.Account"

    @override
    def prepare_request_payload(self, context, next_page_token):
        return {"AccountId": context["account_id"]}
