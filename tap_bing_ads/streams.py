"""Stream type classes for tap-bing-ads."""

from __future__ import annotations

import csv
import fnmatch
import gzip
import io
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from functools import cached_property
from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream
from typing_extensions import override

from tap_bing_ads import BufferDeque
from tap_bing_ads.client import BingAdsStream

BULK_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS = 60
REPORT_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS = 300

REPORT_ATTRIBUTE_COLUMNS = frozenset(
    {
        "AccountId",
        "AccountName",
        "AccountNumber",
        "AccountStatus",
        "AdDescription",
        "AdDescription2",
        "AdDistribution",
        "AdExtensionId",
        "AdExtensionType",
        "AdExtensionTypeId",
        "AdExtensionVersion",
        "AdGroupCriterionId",
        "AdGroupId",
        "AdGroupLabels",
        "AdGroupName",
        "AdGroupStatus",
        "AdGroupType",
        "AdId",
        "AdLabels",
        "AdRelevance",
        "AdStatus",
        "AdTitle",
        "AdType",
        "AgeGroup",
        "AreaCode",
        "AssetGroupId",
        "AssetGroupName",
        "AssetGroupStatus",
        "AssociationId",
        "AssociationLevel",
        "AssociationStatus",
        "AttributeChanged",
        "AudienceId",
        "AudienceName",
        "BaseCampaignId",
        "BidAdjustment",
        "BidMatchType",
        "BidStrategyType",
        "BudgetAssociationStatus",
        "BudgetName",
        "BudgetStatus",
        "Business",
        "CampaignId",
        "CampaignLabels",
        "CampaignName",
        "CampaignStatus",
        "CampaignType",
        "Category0",
        "Category1",
        "Category2",
        "CategoryList",
        "ChangedBy",
        "City",
        "ClickType",
        "ClickTypeId",
        "CombinationLongHeadline",
        "CompanyName",
        "ConflictLevel",
        "ConflictType",
        "Country",
        "County",
        "CountryOfSale",
        "CurrentMaxCpc",
        "CurrencyCode",
        "CustomerId",
        "CustomerName",
        "CustomLabel0",
        "CustomLabel1",
        "CustomLabel2",
        "CustomLabel3",
        "CustomLabel4",
        "CustomParameters",
        "Date",
        "DateTime",
        "DeliveredMatchType",
        "Description1",
        "Description2",
        "DestinationUrl",
        "DeviceOS",
        "DeviceType",
        "DisplayUrl",
        "DynamicAdTarget",
        "DynamicAdTargetId",
        "DynamicAdTargetStatus",
        "EndTime",
        "EntityId",
        "EntityName",
        "ExpectedCtr",
        "FeedUrl",
        "FinalAppUrl",
        "FinalMobileUrl",
        "FinalUrl",
        "FinalUrlSuffix",
        "Gender",
        "Goal",
        "GoalId",
        "GoalType",
        "GTIN",
        "Headline",
        "Headline1",
        "Headline2",
        "Headline3",
        "HistoricalAdRelevance",
        "HistoricalExpectedCtr",
        "HistoricalLandingPageExperience",
        "HistoricalQualityScore",
        "HowChanged",
        "Image",
        "IndustryName",
        "ItemChanged",
        "JobFunctionName",
        "Keyword",
        "KeywordId",
        "KeywordLabels",
        "KeywordStatus",
        "LandingPageTitle",
        "LandingPageExperience",
        "Language",
        "LocationId",
        "LocationType",
        "LocalStoreCode",
        "Logo",
        "LongHeadline",
        "MetroArea",
        "MerchantProductId",
        "MostSpecificLocation",
        "MPN",
        "NegativeKeyword",
        "NegativeKeywordId",
        "NegativeKeywordList",
        "NegativeKeywordListId",
        "NegativeKeywordMatchType",
        "Network",
        "NewValue",
        "OfferLanguage",
        "OldValue",
        "Param1",
        "Param2",
        "Param3",
        "PartitionType",
        "Path1",
        "Path2",
        "PostalCode",
        "PricingModel",
        "ProductBought",
        "ProductBoughtTitle",
        "ProductCategory1",
        "ProductCategory2",
        "ProductCategory3",
        "ProductCategory4",
        "ProductCategory5",
        "ProductGroup",
        "ProductType1",
        "ProductType2",
        "ProductType3",
        "ProductType4",
        "ProductType5",
        "ProximityTargetLocation",
        "PublisherUrl",
        "QualityImpact",
        "QualityScore",
        "QueryIntentCity",
        "QueryIntentCountry",
        "QueryIntentCounty",
        "QueryIntentDMA",
        "QueryIntentLocationId",
        "QueryIntentPostalCode",
        "QueryIntentState",
        "Radius",
        "RevenuePerAppInstall",
        "RevenuePerDownload",
        "Status",
        "SearchQuery",
        "SellerName",
        "StartTime",
        "State",
        "TargetingSetting",
        "TimePeriod",
        "TitlePart1",
        "TitlePart2",
        "TitlePart3",
        "TopVsOther",
        "TotalClicksOnAdElements",
        "TrackingTemplate",
        "ViewThroughCostPerConversion",
        "ViewThroughConversionRate",
        "WebsiteCoverage",
    }
)


class _AccountInfoStream(BingAdsStream):
    name = "_account_info"
    selected = False
    primary_keys = ("Id",)

    schema = th.PropertiesList(
        th.Property("AccountLifeCycleStatus", th.StringType),
        th.Property("Id", th.IntegerType),
        th.Property("Name", th.StringType),
        th.Property("Number", th.StringType),
        th.Property("PauseReason", th.IntegerType),
    ).to_dict()

    http_method = "POST"
    url_base = "https://clientcenter.api.bingads.microsoft.com"
    path = "/CustomerManagement/v13/AccountsInfo/Query"
    records_jsonpath = "$.AccountsInfo[*]"

    @override
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # request reports in batches of up to 1000 accounts
        self._account_ids_buffer = BufferDeque(maxlen=1000)

    @override
    def prepare_request_payload(self, context, next_page_token):
        return {"CustomerId": self.config["customer_id"]}

    @override
    def parse_response(self, response):
        for record in super().parse_response(response):
            yield record

        # make sure we process the remaining buffer entries
        self._account_ids_buffer.finalize()
        yield record  # yield last record again to force child context generation

    @override
    def generate_child_contexts(self, record, context):
        self._account_ids_buffer.append(record["Id"])

        with self._account_ids_buffer as buf:
            if buf.flush:
                yield {"account_ids": buf}

    @override
    def get_child_context(self, record, context):
        return {"account_id": record["Id"]}


class _AccountContextStream(Stream):
    parent_stream_type = _AccountInfoStream
    name = "_account_context"
    selected = False
    schema = th.PropertiesList().to_dict()

    @override
    def get_records(self, context):
        for account_id in context["account_ids"]:
            yield {"account_id": account_id}

    @override
    def get_child_context(self, record, context):
        return {"account_id": record["account_id"]}


class AccountStream(BingAdsStream):
    """Define accounts stream."""

    parent_stream_type = _AccountContextStream
    name = "accounts"
    primary_keys = ("Id",)

    schema = th.PropertiesList(
        th.Property("AccountFinancialStatus", th.StringType),
        th.Property("AccountLifeCycleStatus", th.StringType),
        th.Property("AccountMode", th.StringType),
        th.Property("AutoTagType", th.StringType),
        th.Property("BackUpPaymentInstrumentId", th.IntegerType),
        th.Property("BillingThresholdAmount", th.NumberType),
        th.Property("BillToCustomerId", th.IntegerType),
        th.Property(
            "BusinessAddress",
            th.ObjectType(
                th.Property("City", th.StringType),
                th.Property("CountryCode", th.StringType),
                th.Property("Id", th.IntegerType),
                th.Property("Line1", th.StringType),
                th.Property("Line2", th.StringType),
                th.Property("Line3", th.StringType),
                th.Property("Line4", th.StringType),
                th.Property("PostalCode", th.StringType),
                th.Property("StateOrProvince", th.StringType),
                th.Property("TimeStamp", th.StringType),
                th.Property("BusinessName", th.StringType),
            ),
        ),
        th.Property("CurrencyCode", th.StringType),
        th.Property("ForwardCompatibilityMap", th.StringType),
        th.Property("Id", th.IntegerType),
        th.Property("Language", th.StringType),
        th.Property("LastModifiedByUserId", th.IntegerType),
        th.Property("LastModifiedTime", th.DateTimeType),
        th.Property(
            "LinkedAgencies",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Id", th.IntegerType),
                    th.Property("Name", th.StringType),
                )
            ),
        ),
        th.Property("Name", th.StringType),
        th.Property("Number", th.StringType),
        th.Property("ParentCustomerId", th.IntegerType),
        th.Property("PauseReason", th.IntegerType),
        th.Property("PaymentMethodId", th.IntegerType),
        th.Property("PaymentMethodType", th.StringType),
        th.Property("PrimaryUserId", th.IntegerType),
        th.Property("SalesHouseCustomerId", th.IntegerType),
        th.Property("SoldToPaymentInstrumentId", th.IntegerType),
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


class _BulkStream(BingAdsStream):
    parent_stream_type = _AccountContextStream
    primary_keys = ("Id",)
    replication_key = "Sync Time"
    is_timestamp_replication_key = True

    download_entity_name: str = ...
    entity_type_pattern: str = ...

    @property
    def http_headers(self):
        http_headers = super().http_headers
        http_headers["CustomerAccountId"] = str(self.context["account_id"])

        return http_headers

    @override
    def get_records(self, context):
        account_id = context["account_id"]
        bulk_file = (
            Path(tempfile.gettempdir())
            / f"{self.tap_name}_{self._tap.initialized_at}"
            / f"{account_id}.csv.gz"
        )

        if not bulk_file.exists():
            self._download_bulk_file(account_id, bulk_file)

        # to avoid iterating over the bulk CSV file more times than absolutely necessary
        # , in the interest of performance and memory constraints this logic assumes
        # rows are ordered by entity type, with FormatVersion and Account appearing as
        # the first two rows after the header
        with gzip.open(bulk_file, "rt", encoding="utf-8-sig") as f:
            self.logger.info("Processing file '%s' for account: %s", f.name, account_id)

            reader = csv.DictReader(f)

            next(reader)  # skip FormatVersion

            # extract sync time from Account
            account = next(reader)
            self.sync_time = datetime.strptime(
                account[self.replication_key],
                r"%m/%d/%Y %H:%M:%S",
            ).astimezone(tz=timezone.utc)

            is_matching_entity_type = False

            for row in reader:
                if fnmatch.fnmatch(row["Type"], self.entity_type_pattern):
                    is_matching_entity_type = True
                    yield row
                    continue

                if is_matching_entity_type:
                    return

            if not is_matching_entity_type:
                self.logger.info(
                    "No %s data available for account: %s",
                    self.download_entity_name,
                    account_id,
                )

    @override
    def post_process(self, row: dict, context=None):
        for k in row.copy():
            if k not in self.schema["properties"]:
                row.pop(k)

        return super().post_process(row, context)

    @override
    def to_datetime_isoformat(self, value):
        return (
            datetime.strptime(value, r"%m/%d/%Y %H:%M:%S.%f")
            .astimezone(timezone.utc)
            .isoformat()
        )

    # necessary to implement custom state incrementation logic as sync time is not
    # present in any entity data other than Account (which we do not sync from the bulk
    # CSV file)
    @override
    def _increment_stream_state(self, latest_record, *, context=None):
        replication_info = {self.replication_key: self.sync_time.isoformat()}

        return super()._increment_stream_state(
            latest_record | replication_info,
            context=context,
        )

    def _download_bulk_file(self, account_id: str, bulk_file: Path):
        now = datetime.now(tz=timezone.utc)
        start = self.get_starting_timestamp(self.context)

        last_sync_time = (
            start.isoformat()
            if (now - start).total_seconds() < timedelta(days=30).total_seconds()
            else None
        )

        response = self.requests_session.post(
            "https://bulk.api.bingads.microsoft.com/Bulk/v13/Campaigns/DownloadByAccountIds",
            json={
                "AccountIds": [account_id],
                "CompressionType": "GZip",
                "DownloadEntities": [
                    s.download_entity_name
                    for s in self._tap.streams.values()
                    if isinstance(s, _BulkStream)
                ],
                "FormatVersion": "6.0",
                "LastSyncTimeInUTC": last_sync_time,
            },
            headers=self.http_headers,
            auth=self.authenticator,
        )
        self.validate_response(response)

        request_id = response.json()["DownloadRequestId"]
        attempts = 0

        while True:
            response = self.requests_session.post(
                "https://bulk.api.bingads.microsoft.com/Bulk/v13/BulkDownloadStatus/Query",
                json={"RequestId": request_id},
                headers=self.http_headers,
                auth=self.authenticator,
            )
            self.validate_response(response)

            download_request_status = response.json()
            status = download_request_status["RequestStatus"]
            attempts += 1

            self.logger.info(
                "[%*d/%d] Bulk download request status: %s (%d%% complete)",
                len(str(BULK_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS)),
                attempts,
                BULK_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS,
                status,
                download_request_status["PercentComplete"],
            )

            if status == "InProgress":
                if attempts >= BULK_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS:
                    msg = (
                        f"Bulk download request incomplete ({status}) after checking "
                        f"{attempts} times"
                    )
                    raise RuntimeError(msg)

                time.sleep(1)
                continue

            if status == "Completed":
                break

            msg = "Bulk download request failed ({}): {}".format(
                status,
                download_request_status["Errors"],
            )
            raise RuntimeError(msg)

        download_url = download_request_status["ResultFileUrl"]
        bulk_file.parent.mkdir(exist_ok=True)

        with (
            self.requests_session.get(download_url, stream=True) as r,
            bulk_file.open("wb") as f,
        ):
            self.validate_response(r)

            for chunk in r.iter_content(chunk_size=8192):  # 8 KB chunks
                if chunk:  # skip keep-alive chunks
                    f.write(chunk)


class CampaignStream(_BulkStream):
    """Define campaigns stream."""

    name = "campaigns"

    schema = th.PropertiesList(
        th.Property("App Id", th.IntegerType),
        th.Property("App Store", th.StringType),
        th.Property("Auto Generated Image Assets Opt Out", th.BooleanType),
        th.Property("Auto Generated Text Assets Opt Out", th.BooleanType),
        th.Property("Automated Call To Action Opt Out", th.BooleanType),
        th.Property("Ad Schedule Use Searcher Time Zone", th.BooleanType),
        th.Property("Bid Adjustment", th.StringType),
        th.Property("Bid Strategy Commission", th.NumberType),
        th.Property("Bid Strategy Id", th.IntegerType),
        th.Property("Bid Strategy ManualCpc", th.NumberType),
        th.Property("Bid Strategy MaxCpc", th.NumberType),
        th.Property("Bid Strategy Name", th.StringType),
        th.Property("Bid Strategy PercentCpc", th.NumberType),
        th.Property("Bid Strategy TargetAdPosition", th.StringType),
        th.Property("Bid Strategy TargetCostPerSale", th.NumberType),
        th.Property("Bid Strategy TargetCpa", th.NumberType),
        th.Property("Bid Strategy TargetImpressionShare", th.StringType),
        th.Property("Bid Strategy TargetRoas", th.StringType),
        th.Property("Bid Strategy Type", th.StringType),
        th.Property("Budget", th.NumberType),
        th.Property("Budget Id", th.IntegerType),
        th.Property("Budget Name", th.StringType),
        th.Property("Budget Type", th.StringType),
        th.Property("Campaign", th.StringType),
        th.Property("Campaign Type", th.StringType),
        th.Property("Client Id", th.StringType),
        th.Property("Country Code", th.StringType),
        th.Property("Custom Parameter", th.StringType),
        th.Property("Destination Channel", th.StringType),
        th.Property("Domain Language", th.StringType),
        th.Property("Disclaimer Ads Enabled", th.BooleanType),
        th.Property("Dynamic Description Enabled", th.BooleanType),
        th.Property("Experiment Id", th.IntegerType),
        th.Property("Feed Label", th.StringType),
        th.Property("Final Url Suffix", th.StringType),
        th.Property("Id", th.IntegerType),
        th.Property("Is Multi Channel Campaign", th.StringType),
        th.Property("Keyword Relevance", th.NumberType),
        th.Property("Landing Page Relevance", th.NumberType),
        th.Property("Landing Page User Experience", th.NumberType),
        th.Property("Language", th.StringType),
        th.Property("Local Inventory Ads Enabled", th.BooleanType),
        th.Property("Modified Time", th.DateTimeType),
        th.Property("Page Feed Ids", th.StringType),
        th.Property("Parent Id", th.IntegerType),
        th.Property("Priority", th.IntegerType),
        th.Property("Quality Score", th.NumberType),
        th.Property("RSA Auto Generated Assets Enabled", th.BooleanType),
        th.Property("Sales Country Code", th.StringType),
        th.Property("Shoppable Ads Enabled", th.BooleanType),
        th.Property("Source", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("Store Id", th.IntegerType),
        th.Property("Sub Type", th.StringType),
        th.Property("Target Setting", th.StringType),
        th.Property("Tracking Template", th.StringType),
        th.Property("Vanity Pharma Display Url Mode", th.StringType),
        th.Property("Vanity Pharma Website Description", th.StringType),
        th.Property("URL Expansion Opt Out", th.BooleanType),
        th.Property("Website", th.StringType),
    ).to_dict()

    download_entity_name = "Campaigns"
    entity_type_pattern = "Campaign"


class AdGroupStream(_BulkStream):
    """Define ad groups stream."""

    name = "ad_groups"

    schema = th.PropertiesList(
        th.Property("Ad Group", th.StringType),
        th.Property("Ad Group Type", th.StringType),
        th.Property("Ad Rotation", th.StringType),
        th.Property("Ad Schedule Use Searcher Time Zone", th.BooleanType),
        th.Property("Bid Adjustment", th.NumberType),
        th.Property("Bid Boost Value", th.NumberType),
        th.Property("Bid Option", th.StringType),
        th.Property("Bid Strategy Type", th.StringType),
        th.Property("Campaign", th.StringType),
        th.Property("Client Id", th.StringType),
        th.Property("Cpc Bid", th.NumberType),
        th.Property("Custom Parameter", th.StringType),
        th.Property("End Date", th.DateType),
        th.Property("Final Url Suffix", th.StringType),
        th.Property("Frequency Cap Settings", th.StringType),
        th.Property("Id", th.IntegerType),
        th.Property("Inherited Bid Strategy Type", th.StringType),
        th.Property("Keyword Relevance", th.NumberType),
        th.Property("Landing Page Relevance", th.NumberType),
        th.Property("Landing Page User Experience", th.NumberType),
        th.Property("Language", th.StringType),
        th.Property("Maximum Bid", th.NumberType),
        th.Property("Modified Time", th.DateTimeType),
        th.Property("Network Distribution", th.StringType),
        th.Property("Parent Id", th.IntegerType),
        th.Property("Privacy Status", th.StringType),
        th.Property("Quality Score", th.NumberType),
        th.Property("Start Date", th.DateType),
        th.Property("Status", th.StringType),
        th.Property("Target Setting", th.StringType),
        th.Property("Tracking Template", th.StringType),
        th.Property("Use Optimized Targeting", th.BooleanType),
        th.Property("Use Predictive Targeting", th.BooleanType),
    ).to_dict()

    download_entity_name = "AdGroups"
    entity_type_pattern = "Ad Group"


class AdStream(_BulkStream):
    """Define ads stream."""

    name = "ads"
    schema = th.PropertiesList(
        th.Property("Ad Format Preference", th.StringType),
        th.Property("Ad Group", th.StringType),
        th.Property("App Id", th.IntegerType),
        th.Property("App Platform", th.StringType),
        th.Property("Business Name", th.StringType),
        th.Property("Call To Action", th.StringType),
        th.Property("Call To Action Langauge", th.StringType),
        th.Property("Campaign", th.StringType),
        th.Property("Client Id", th.StringType),
        th.Property("Custom Parameter", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Descriptions", th.StringType),
        th.Property("Device Preference", th.StringType),
        th.Property("Display Url", th.URIType),
        th.Property("Domain", th.StringType),
        th.Property("Editorial Appeal Status", th.StringType),
        th.Property("Editorial Location", th.StringType),
        th.Property("Editorial Reason Code", th.IntegerType),
        th.Property("Editorial Status", th.StringType),
        th.Property("Editorial Term", th.StringType),
        th.Property("Final Url", th.URIType),
        th.Property("Final Url Suffix", th.StringType),
        th.Property("Headline", th.StringType),
        th.Property("Headlines", th.StringType),
        th.Property("Id", th.IntegerType),
        th.Property("Images", th.StringType),
        th.Property("Impression Tracking Urls", th.StringType),
        th.Property("Landscape Image Media Id", th.StringType),
        th.Property("Landscape Logo Media Id", th.StringType),
        th.Property("Long Headline", th.StringType),
        th.Property("Long Headline String", th.StringType),
        th.Property("Long Headlines", th.StringType),
        th.Property("Mobile Final Url", th.URIType),
        th.Property("Modified Time", th.DateTimeType),
        th.Property("Parent Id", th.IntegerType),
        th.Property("Path 1", th.StringType),
        th.Property("Path 2", th.StringType),
        th.Property("Publisher Countries", th.StringType),
        th.Property("Square Image Media Id", th.StringType),
        th.Property("Square Logo Media Id", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("Text", th.StringType),
        th.Property("Text Part 2", th.StringType),
        th.Property("Title", th.StringType),
        th.Property("Title Part 1", th.StringType),
        th.Property("Title Part 2", th.StringType),
        th.Property("Title Part 3", th.StringType),
        th.Property("Tracking Template", th.StringType),
    ).to_dict()

    download_entity_name = "Ads"
    entity_type_pattern = "* Ad"


class KeywordStream(_BulkStream):
    """Define keyword stream."""

    name = "keywords"
    schema = th.PropertiesList(
        th.Property("Ad Group", th.StringType),
        th.Property("Bid", th.NumberType),
        th.Property("Bid Strategy Type", th.StringType),
        th.Property("Campaign", th.StringType),
        th.Property("Client Id", th.StringType),
        th.Property("Custom Parameter", th.StringType),
        th.Property("Destination Url", th.URIType),
        th.Property("Editorial Appeal Status", th.StringType),
        th.Property("Editorial Location", th.StringType),
        th.Property("Editorial Reason Code", th.IntegerType),
        th.Property("Editorial Status", th.StringType),
        th.Property("Editorial Term", th.StringType),
        th.Property("Final Url", th.URIType),
        th.Property("Final Url Suffix", th.StringType),
        th.Property("Id", th.IntegerType),
        th.Property("Inherited Bid Strategy Type", th.StringType),
        th.Property("Keyword", th.StringType),
        th.Property("Keyword Relevance", th.NumberType),
        th.Property("Landing Page Relevance", th.NumberType),
        th.Property("Landing Page User Experience", th.NumberType),
        th.Property("Match Type", th.StringType),
        th.Property("Mobile Final Url", th.URIType),
        th.Property("Modified Time", th.DateTimeType),
        th.Property("Param1", th.StringType),
        th.Property("Param2", th.StringType),
        th.Property("Param3", th.StringType),
        th.Property("Parent Id", th.IntegerType),
        th.Property("Publisher Countries", th.StringType),
        th.Property("Quality Score", th.NumberType),
        th.Property("Status", th.StringType),
        th.Property("Tracking Template", th.StringType),
    ).to_dict()

    download_entity_name = "Keywords"
    entity_type_pattern = "Keyword"


class _DailyPerformanceReportStream(BingAdsStream):
    parent_stream_type = _AccountInfoStream
    replication_key = "TimePeriod"
    is_timestamp_replication_key = True
    state_partitioning_keys = ()

    report_request_name: str = ...
    column_restrictions: tuple[tuple[set[str], set[str]], ...] = ()

    @override
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.primary_keys = ("TimePeriod",)  # for automatic metadata inclusion

        # https://learn.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#columnsdata
        self.primary_keys = tuple(
            c for c in self.columns if c in REPORT_ATTRIBUTE_COLUMNS
        )

    @override
    def get_records(self, context):
        account_ids = list(context["account_ids"])
        report_file = (
            Path(tempfile.gettempdir())
            / f"{self.tap_name}_{self._tap.initialized_at}"
            / f"{self.name}.zip"
        )

        start = self.get_starting_timestamp(context)
        now = datetime.now(tz=timezone.utc)

        response = self.requests_session.post(
            "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit",
            json={
                "ReportRequest": {
                    "Aggregation": "Daily",
                    "Columns": list(self.columns),
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "FormatVersion": "2.0",
                    "Scope": {"AccountIds": account_ids},
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": now.day,
                            "Month": now.month,
                            "Year": now.year,
                        },
                        "CustomDateRangeStart": {
                            "Day": start.day,
                            "Month": start.month,
                            "Year": start.year,
                        },
                        "ReportTimeZone": "GreenwichMeanTimeDublinEdinburghLisbonLondon",  # noqa: E501
                    },
                    "Type": self.report_request_name,
                }
            },
            headers=self.http_headers,
            auth=self.authenticator,
        )
        self.validate_response(response)

        request_id = response.json()["ReportRequestId"]
        attempts = 0

        while True:
            response = self.requests_session.post(
                "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll",
                json={"ReportRequestId": request_id},
                headers=self.http_headers,
                auth=self.authenticator,
            )
            self.validate_response(response)

            report_request_status = response.json()["ReportRequestStatus"]
            status = report_request_status["Status"]
            attempts += 1

            self.logger.info(
                "[%*d/%d] %s status: %s",
                len(str(REPORT_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS)),
                attempts,
                REPORT_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS,
                self.report_request_name,
                status,
            )

            if status == "Pending":
                if attempts >= REPORT_DOWNLOAD_REQUEST_POLL_MAX_ATTEMPTS:
                    msg = (
                        f"{self.report_request_name} incomplete ({status}) after "
                        f"checking {attempts} times"
                    )
                    raise RuntimeError(msg)

                time.sleep(1)
                continue

            if status == "Success":
                break

            msg = f"{self.report_request_name} failed ({status})"
            raise RuntimeError(msg)

        download_url = report_request_status["ReportDownloadUrl"]

        if not download_url:
            self.logger.info(
                "No %s data available for account: %s",
                self.report_request_name,
                account_ids,
            )
            return

        report_file.parent.mkdir(exist_ok=True)

        with (
            self.requests_session.get(download_url, stream=True) as r,
            report_file.open("wb") as f,
        ):
            self.validate_response(r)
            self.logger.info(
                "Downloading: %s (%.1f MB)",
                f.name,
                int(r.headers["Content-Length"]) / (1000**2),
            )

            for chunk in r.iter_content(chunk_size=1000**2):  # 1 MB chunks
                if chunk:  # skip keep-alive chunks
                    f.write(chunk)

        with zipfile.ZipFile(report_file) as z:
            self.logger.info(
                "Processing file '%s' for accounts: %s",
                f.name,
                account_ids,
            )

            for info in z.infolist():
                with (
                    z.open(info.filename) as f,
                    io.TextIOWrapper(f, encoding="utf-8-sig") as f,
                ):
                    self.logger.info(
                        "Processing zipped file: %s (%.1fMB)",
                        f.name,
                        info.file_size / (1000**2),
                    )
                    yield from csv.DictReader(f)

    @override
    def to_float(self, value):
        if value == "--":
            return None

        value = value.removesuffix("%")
        value = value.replace(",", "")

        return super().to_float(value)

    @cached_property
    def columns(self):
        # only selected properties
        column_names = {
            p
            for p in self.schema["properties"]
            if self.metadata[("properties", p)].selected is not False
        }

        # resolve restricted column combinations
        # https://learn.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#columnrestrictions
        for restrictions in self.column_restrictions:
            attributes, impression_share_performance_statistics = restrictions

            # preserve attributes over impression_share_performance_statistics
            if any(a in column_names for a in attributes):
                column_names -= impression_share_performance_statistics
            elif any(
                isps in column_names for isps in impression_share_performance_statistics
            ):
                column_names -= attributes

        return column_names


class AdGroupDailyPerformanceStream(_DailyPerformanceReportStream):
    """Define ad group daily performance stream."""

    name = "ad_group_daily_performance"

    schema = th.PropertiesList(
        th.Property("AbsoluteTopImpressionRatePercent", th.NumberType),
        th.Property("AbsoluteTopImpressionShareLostToBudgetPercent", th.NumberType),
        th.Property("AbsoluteTopImpressionShareLostToRankPercent", th.NumberType),
        th.Property("AbsoluteTopImpressionSharePercent", th.NumberType),
        th.Property("AccountId", th.IntegerType),
        th.Property("AccountName", th.StringType),
        th.Property("AccountNumber", th.StringType),
        th.Property("AccountStatus", th.StringType),
        th.Property("AdDistribution", th.StringType),
        th.Property("AdGroupId", th.IntegerType),
        th.Property("AdGroupLabels", th.StringType),
        th.Property("AdGroupName", th.StringType),
        th.Property("AdGroupType", th.StringType),
        th.Property("AdRelevance", th.NumberType),
        th.Property("AllConversionRate", th.NumberType),
        th.Property("AllConversions", th.NumberType),
        th.Property("AllConversionsQualified", th.NumberType),
        th.Property("AllCostPerConversion", th.NumberType),
        th.Property("AllReturnOnAdSpend", th.NumberType),
        th.Property("AllRevenue", th.NumberType),
        th.Property("AllRevenuePerConversion", th.NumberType),
        th.Property("Assists", th.IntegerType),
        th.Property("AudienceImpressionLostToBudgetPercent", th.NumberType),
        th.Property("AudienceImpressionLostToRankPercent", th.NumberType),
        th.Property("AudienceImpressionSharePercent", th.NumberType),
        th.Property("AverageCpc", th.NumberType),
        th.Property("AverageCpm", th.NumberType),
        th.Property("AverageCPV", th.NumberType),
        th.Property("AveragePosition", th.NumberType),
        th.Property("AverageWatchTimePerImpression", th.NumberType),
        th.Property("AverageWatchTimePerVideoView", th.NumberType),
        th.Property("BaseCampaignId", th.IntegerType),
        th.Property("BidMatchType", th.StringType),
        th.Property("CampaignId", th.IntegerType),
        th.Property("CampaignName", th.StringType),
        th.Property("CampaignStatus", th.StringType),
        th.Property("CampaignType", th.StringType),
        th.Property("Clicks", th.IntegerType),
        th.Property("ClickSharePercent", th.NumberType),
        th.Property("CompletedVideoViews", th.IntegerType),
        th.Property("ConversionRate", th.NumberType),
        th.Property("Conversions", th.NumberType),
        th.Property("ConversionsQualified", th.NumberType),
        th.Property("CostPerAssist", th.NumberType),
        th.Property("CostPerConversion", th.NumberType),
        th.Property("CostPerInstall", th.NumberType),
        th.Property("CostPerSale", th.NumberType),
        th.Property("Ctr", th.NumberType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("CustomerId", th.IntegerType),
        th.Property("CustomerName", th.StringType),
        th.Property("CustomParameters", th.StringType),
        th.Property("DeliveredMatchType", th.StringType),
        th.Property("DeviceOS", th.StringType),
        th.Property("DeviceType", th.StringType),
        th.Property("ExactMatchImpressionSharePercent", th.NumberType),
        th.Property("ExpectedCtr", th.NumberType),
        th.Property("FinalUrlSuffix", th.StringType),
        th.Property("Goal", th.StringType),
        th.Property("GoalId", th.IntegerType),
        th.Property("GoalType", th.StringType),
        th.Property("HistoricalAdRelevance", th.StringType),
        th.Property("HistoricalExpectedCtr", th.NumberType),
        th.Property("HistoricalLandingPageExperience", th.NumberType),
        th.Property("HistoricalQualityScore", th.NumberType),
        th.Property("ImpressionLostToBudgetPercent", th.NumberType),
        th.Property("ImpressionLostToRankAggPercent", th.NumberType),
        th.Property("Impressions", th.IntegerType),
        th.Property("ImpressionSharePercent", th.NumberType),
        th.Property("Installs", th.IntegerType),
        th.Property("LandingPageExperience", th.NumberType),
        th.Property("Language", th.StringType),
        th.Property("Network", th.StringType),
        th.Property("PhoneCalls", th.IntegerType),
        th.Property("PhoneImpressions", th.IntegerType),
        th.Property("Ptr", th.NumberType),
        th.Property("QualityScore", th.NumberType),
        th.Property("RelativeCtr", th.NumberType),
        th.Property("ReturnOnAdSpend", th.NumberType),
        th.Property("Revenue", th.NumberType),
        th.Property("RevenuePerAssist", th.NumberType),
        th.Property("RevenuePerConversion", th.NumberType),
        th.Property("RevenuePerInstall", th.NumberType),
        th.Property("RevenuePerSale", th.NumberType),
        th.Property("Sales", th.IntegerType),
        th.Property("Spend", th.NumberType),
        th.Property("Status", th.StringType),
        th.Property("TimePeriod", th.DateType),
        th.Property("TopImpressionRatePercent", th.NumberType),
        th.Property("TopImpressionShareLostToBudgetPercent", th.NumberType),
        th.Property("TopImpressionShareLostToRankPercent", th.NumberType),
        th.Property("TopImpressionSharePercent", th.NumberType),
        th.Property("TopVsOther", th.StringType),
        th.Property("TotalWatchTimeInMS", th.IntegerType),
        th.Property("TrackingTemplate", th.StringType),
        th.Property("VideoCompletionRate", th.NumberType),
        th.Property("VideoViews", th.IntegerType),
        th.Property("VideoViewsAt25Percent", th.IntegerType),
        th.Property("VideoViewsAt50Percent", th.IntegerType),
        th.Property("VideoViewsAt75Percent", th.IntegerType),
        th.Property("ViewThroughConversions", th.IntegerType),
        th.Property("ViewThroughConversionsQualified", th.IntegerType),
        th.Property("ViewThroughRate", th.NumberType),
        th.Property("ViewThroughRevenue", th.NumberType),
    ).to_dict()

    # https://learn.microsoft.com/en-us/advertising/reporting-service/adgroupperformancereportrequest?view=bingads-13&tabs=json
    report_request_name = "AdGroupPerformanceReportRequest"

    column_restrictions = (
        (
            {
                "BidMatchType",
                "DeviceOS",
                "Goal",
                "GoalType",
                "TopVsOther",
            },
            {
                "AbsoluteTopImpressionRatePercent",
                "AbsoluteTopImpressionShareLostToBudgetPercent",
                "AbsoluteTopImpressionShareLostToRankPercent",
                "AbsoluteTopImpressionSharePercent",
                "AudienceImpressionLostToBudgetPercent",
                "AudienceImpressionLostToRankPercent",
                "AudienceImpressionSharePercent",
                "ClickSharePercent",
                "ExactMatchImpressionSharePercent",
                "ImpressionLostToAdRelevancePercent",
                "ImpressionLostToBidPercent",
                "ImpressionLostToBudgetPercent",
                "ImpressionLostToExpectedCtrPercent",
                "ImpressionLostToRankAggPercent",
                "ImpressionLostToRankPercent",
                "ImpressionSharePercent",
                "RelativeCtr",
                "TopImpressionRatePercent",
                "TopImpressionShareLostToBudgetPercent",
                "TopImpressionShareLostToRankPercent",
                "TopImpressionSharePercent",
            },
        ),
        (
            {
                "CustomerId",
                "CustomerName",
                "DeliveredMatchType",
            },
            {
                "AudienceImpressionLostToBudgetPercent",
                "AudienceImpressionLostToRankPercent",
                "AudienceImpressionSharePercent",
                "RelativeCtr",
            },
        ),
    )


class AdDailyPerformanceStream(_DailyPerformanceReportStream):
    """Define ad daily performance stream."""

    name = "ad_daily_performance"

    schema = th.PropertiesList(
        th.Property("AbsoluteTopImpressionRatePercent", th.NumberType),
        th.Property("AccountId", th.IntegerType),
        th.Property("AccountName", th.StringType),
        th.Property("AccountNumber", th.StringType),
        th.Property("AccountStatus", th.StringType),
        th.Property("AdDescription", th.StringType),
        th.Property("AdDescription2", th.StringType),
        th.Property("AdDistribution", th.StringType),
        th.Property("AdGroupId", th.IntegerType),
        th.Property("AdGroupName", th.StringType),
        th.Property("AdGroupStatus", th.StringType),
        th.Property("AdId", th.IntegerType),
        th.Property("AdLabels", th.StringType),
        th.Property("AdStatus", th.StringType),
        th.Property("AdStrength", th.StringType),
        th.Property("AdStrengthActionItems", th.StringType),
        th.Property("AdTitle", th.StringType),
        th.Property("AdType", th.StringType),
        th.Property("AllConversionRate", th.NumberType),
        th.Property("AllConversions", th.NumberType),
        th.Property("AllConversionsQualified", th.NumberType),
        th.Property("AllCostPerConversion", th.NumberType),
        th.Property("AllReturnOnAdSpend", th.NumberType),
        th.Property("AllRevenue", th.NumberType),
        th.Property("AllRevenuePerConversion", th.NumberType),
        th.Property("Assists", th.IntegerType),
        th.Property("AverageCpc", th.NumberType),
        th.Property("AverageCpm", th.NumberType),
        th.Property("AverageCPV", th.NumberType),
        th.Property("AveragePosition", th.NumberType),
        th.Property("AverageWatchTimePerImpression", th.NumberType),
        th.Property("AverageWatchTimePerVideoView", th.NumberType),
        th.Property("BaseCampaignId", th.IntegerType),
        th.Property("BidMatchType", th.StringType),
        th.Property("BusinessName", th.StringType),
        th.Property("CampaignId", th.IntegerType),
        th.Property("CampaignName", th.StringType),
        th.Property("CampaignStatus", th.StringType),
        th.Property("CampaignType", th.StringType),
        th.Property("Clicks", th.IntegerType),
        th.Property("CompletedVideoViews", th.IntegerType),
        th.Property("ConversionRate", th.NumberType),
        th.Property("Conversions", th.NumberType),
        th.Property("ConversionsQualified", th.NumberType),
        th.Property("CostPerAssist", th.NumberType),
        th.Property("CostPerConversion", th.NumberType),
        th.Property("Ctr", th.NumberType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("CustomerId", th.IntegerType),
        th.Property("CustomerName", th.StringType),
        th.Property("CustomParameters", th.StringType),
        th.Property("DeliveredMatchType", th.StringType),
        th.Property("DestinationUrl", th.URIType),
        th.Property("DeviceOS", th.StringType),
        th.Property("DeviceType", th.StringType),
        th.Property("DisplayUrl", th.URIType),
        th.Property("FinalAppUrl", th.URIType),
        th.Property("FinalMobileUrl", th.URIType),
        th.Property("FinalUrl", th.URIType),
        th.Property("FinalUrlSuffix", th.StringType),
        th.Property("Goal", th.StringType),
        th.Property("GoalId", th.IntegerType),
        th.Property("GoalType", th.StringType),
        th.Property("Headline", th.StringType),
        th.Property("Impressions", th.IntegerType),
        th.Property("Language", th.StringType),
        th.Property("LongHeadline", th.StringType),
        th.Property("Network", th.StringType),
        th.Property("Path1", th.StringType),
        th.Property("Path2", th.StringType),
        th.Property("ReturnOnAdSpend", th.NumberType),
        th.Property("Revenue", th.NumberType),
        th.Property("RevenuePerAssist", th.NumberType),
        th.Property("RevenuePerConversion", th.NumberType),
        th.Property("Spend", th.NumberType),
        th.Property("TimePeriod", th.DateType),
        th.Property("TitlePart1", th.StringType),
        th.Property("TitlePart2", th.StringType),
        th.Property("TitlePart3", th.StringType),
        th.Property("TopImpressionRatePercent", th.NumberType),
        th.Property("TopVsOther", th.StringType),
        th.Property("TotalWatchTimeInMS", th.IntegerType),
        th.Property("TrackingTemplate", th.StringType),
        th.Property("VideoCompletionRate", th.NumberType),
        th.Property("VideoViews", th.IntegerType),
        th.Property("VideoViewsAt25Percent", th.IntegerType),
        th.Property("VideoViewsAt50Percent", th.IntegerType),
        th.Property("VideoViewsAt75Percent", th.IntegerType),
        th.Property("ViewThroughConversions", th.IntegerType),
        th.Property("ViewThroughConversionsQualified", th.IntegerType),
        th.Property("ViewThroughRate", th.NumberType),
        th.Property("ViewThroughRevenue", th.NumberType),
    ).to_dict()

    # https://learn.microsoft.com/en-us/advertising/reporting-service/adperformancereportrequest?view=bingads-13&tabs=json
    report_request_name = "AdPerformanceReportRequest"


class KeywordDailyPerformanceStream(_DailyPerformanceReportStream):
    """Define kewword daily performance stream."""

    name = "keyword_daily_performance"

    schema = th.PropertiesList(
        th.Property("AbsoluteTopImpressionRatePercent", th.NumberType),
        th.Property("AccountId", th.IntegerType),
        th.Property("AccountName", th.StringType),
        th.Property("AccountNumber", th.StringType),
        th.Property("AccountStatus", th.StringType),
        th.Property("AdDistribution", th.StringType),
        th.Property("AdGroupId", th.IntegerType),
        th.Property("AdGroupName", th.StringType),
        th.Property("AdGroupStatus", th.StringType),
        th.Property("AdId", th.IntegerType),
        th.Property("AdRelevance", th.NumberType),
        th.Property("AdType", th.StringType),
        th.Property("AllConversionRate", th.NumberType),
        th.Property("AllConversions", th.NumberType),
        th.Property("AllConversionsQualified", th.NumberType),
        th.Property("AllCostPerConversion", th.NumberType),
        th.Property("AllReturnOnAdSpend", th.NumberType),
        th.Property("AllRevenue", th.NumberType),
        th.Property("AllRevenuePerConversion", th.NumberType),
        th.Property("Assists", th.IntegerType),
        th.Property("AverageCpc", th.NumberType),
        th.Property("AverageCpm", th.NumberType),
        th.Property("AveragePosition", th.NumberType),
        th.Property("BaseCampaignId", th.IntegerType),
        th.Property("BidMatchType", th.StringType),
        th.Property("BidStrategyType", th.StringType),
        th.Property("CampaignId", th.IntegerType),
        th.Property("CampaignName", th.StringType),
        th.Property("CampaignStatus", th.StringType),
        th.Property("Clicks", th.IntegerType),
        th.Property("ConversionRate", th.NumberType),
        th.Property("Conversions", th.NumberType),
        th.Property("ConversionsQualified", th.NumberType),
        th.Property("CostPerAssist", th.NumberType),
        th.Property("CostPerConversion", th.NumberType),
        th.Property("Ctr", th.NumberType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("CurrentMaxCpc", th.NumberType),
        th.Property("CustomParameters", th.StringType),
        th.Property("DeliveredMatchType", th.StringType),
        th.Property("DestinationUrl", th.URIType),
        th.Property("DeviceOS", th.StringType),
        th.Property("DeviceType", th.StringType),
        th.Property("ExpectedCtr", th.NumberType),
        th.Property("FinalAppUrl", th.URIType),
        th.Property("FinalMobileUrl", th.URIType),
        th.Property("FinalUrl", th.URIType),
        th.Property("FinalUrlSuffix", th.StringType),
        th.Property("FirstPageBid", th.NumberType),
        th.Property("Goal", th.StringType),
        th.Property("GoalId", th.IntegerType),
        th.Property("GoalType", th.StringType),
        th.Property("HistoricalAdRelevance", th.NumberType),
        th.Property("HistoricalExpectedCtr", th.NumberType),
        th.Property("HistoricalLandingPageExperience", th.NumberType),
        th.Property("HistoricalQualityScore", th.NumberType),
        th.Property("Impressions", th.IntegerType),
        th.Property("Keyword", th.StringType),
        th.Property("KeywordId", th.IntegerType),
        th.Property("KeywordLabels", th.StringType),
        th.Property("KeywordStatus", th.StringType),
        th.Property("LandingPageExperience", th.NumberType),
        th.Property("Language", th.StringType),
        th.Property("Mainline1Bid", th.NumberType),
        th.Property("MainlineBid", th.NumberType),
        th.Property("Network", th.StringType),
        th.Property("QualityImpact", th.NumberType),
        th.Property("QualityScore", th.NumberType),
        th.Property("ReturnOnAdSpend", th.NumberType),
        th.Property("Revenue", th.NumberType),
        th.Property("RevenuePerAssist", th.NumberType),
        th.Property("RevenuePerConversion", th.NumberType),
        th.Property("Spend", th.NumberType),
        th.Property("TimePeriod", th.DateType),
        th.Property("TopImpressionRatePercent", th.NumberType),
        th.Property("TopVsOther", th.StringType),
        th.Property("TrackingTemplate", th.StringType),
        th.Property("ViewThroughConversions", th.IntegerType),
        th.Property("ViewThroughConversionsQualified", th.IntegerType),
        th.Property("ViewThroughRevenue", th.NumberType),
    ).to_dict()

    # https://learn.microsoft.com/en-us/advertising/reporting-service/keywordperformancereportrequest?view=bingads-13&tabs=json
    report_request_name = "KeywordPerformanceReportRequest"
