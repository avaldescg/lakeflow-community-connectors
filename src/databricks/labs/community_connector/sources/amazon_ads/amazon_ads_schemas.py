"""Schemas and metadata for Amazon Ads connector tables."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Constants for retry logic
RETRIABLE_STATUS_CODES = {429, 500, 503}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry
REQUEST_TIMEOUT = 20  # seconds


# Schema definitions for each table
TABLE_SCHEMAS = {
    "profiles": StructType(
        [
            StructField("profileId", LongType(), nullable=False),
            StructField("countryCode", StringType(), nullable=True),
            StructField("currencyCode", StringType(), nullable=True),
            StructField("dailyBudget", DoubleType(), nullable=True),
            StructField("timezone", StringType(), nullable=True),
            StructField("accountInfo", StructType([
                StructField("marketplaceStringId", StringType(), nullable=True),
                StructField("id", StringType(), nullable=True),
                StructField("type", StringType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("validPaymentMethod", BooleanType(), nullable=True),
            ]), nullable=True),
            StructField("profileTypeFilter", StringType(), nullable=True),
        ]
    ),
    "portfolios": StructType(
        [
            StructField("portfolioId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("budget", DoubleType(), nullable=True),
            StructField("budgetCurrencyCode", StringType(), nullable=True),
            StructField("inBudget", BooleanType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("creationDate", LongType(), nullable=True),
            StructField("lastUpdatedDate", LongType(), nullable=True),
        ]
    ),
    "sponsored_product_campaigns": StructType(
        [
            StructField("campaignId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("budget", DoubleType(), nullable=True),
            StructField("budgetType", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("portfolioId", LongType(), nullable=True),
            StructField("startDate", DateType(), nullable=True),
            StructField("endDate", DateType(), nullable=True),
            StructField("targetingType", StringType(), nullable=True),
            StructField("premiumBidAdjustment", BooleanType(), nullable=True),
            StructField("dynamicBidStrategy", BooleanType(), nullable=True),
            StructField("servingStatus", StringType(), nullable=True),
            StructField("createTime", LongType(), nullable=True),
            StructField("lastUpdateTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_product_ad_groups": StructType(
        [
            StructField("adGroupId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("defaultBid", DoubleType(), nullable=True),
            StructField("createTime", LongType(), nullable=True),
            StructField("lastUpdateTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_product_keywords": StructType(
        [
            StructField("keywordId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("adGroupId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("keywordText", StringType(), nullable=True),
            StructField("matchType", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("bid", DoubleType(), nullable=True),
            StructField("nativeLanguageKeyword", StringType(), nullable=True),
            StructField("nativeLanguageMatchType", StringType(), nullable=True),
            StructField("createTime", LongType(), nullable=True),
            StructField("lastUpdateTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_product_ads": StructType(
        [
            StructField("adId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("adGroupId", LongType(), nullable=False),
            StructField("sku", StringType(), nullable=True),
            StructField("asin", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("servingStatus", StringType(), nullable=True),
            StructField("creativeId", StringType(), nullable=True),
            StructField("createTime", LongType(), nullable=True),
            StructField("lastUpdateTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_product_targetings": StructType(
        [
            StructField("targetId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("adGroupId", LongType(), nullable=False),
            StructField("state", StringType(), nullable=True),
            StructField("bid", DoubleType(), nullable=True),
            StructField("targetingExpression", ArrayType(StructType([
                StructField("predicateType", StringType(), nullable=True),
                StructField("value", StringType(), nullable=True),
            ])), nullable=True),
            StructField("creativeId", StringType(), nullable=True),
            StructField("createTime", LongType(), nullable=True),
            StructField("lastUpdateTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_brands_campaigns": StructType(
        [
            StructField("campaignId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("budget", DoubleType(), nullable=True),
            StructField("budgetType", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("portfolioId", LongType(), nullable=True),
            StructField("startDate", StringType(), nullable=True),
            StructField("endDate", StringType(), nullable=True),
            StructField("servingStatus", StringType(), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_display_campaigns": StructType(
        [
            StructField("campaignId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("budget", DoubleType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("startDate", StringType(), nullable=True),
            StructField("endDate", StringType(), nullable=True),
            StructField("servingStatus", StringType(), nullable=True),
        ]
    ),
    "sponsored_brands_ad_groups": StructType(
        [
            StructField("adGroupId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("defaultBid", DoubleType(), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_brands_keywords": StructType(
        [
            StructField("keywordId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("adGroupId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("keywordText", StringType(), nullable=True),
            StructField("matchType", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("bid", DoubleType(), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_brands_ads": StructType(
        [
            StructField("adId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("adGroupId", LongType(), nullable=False),
            StructField("headline", StringType(), nullable=True),
            StructField("description", StringType(), nullable=True),
            StructField("asins", ArrayType(StringType()), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("servingStatus", StringType(), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_display_ad_groups": StructType(
        [
            StructField("adGroupId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("defaultBid", DoubleType(), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_display_product_ads": StructType(
        [
            StructField("adId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("adGroupId", LongType(), nullable=False),
            StructField("sku", StringType(), nullable=True),
            StructField("asin", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("servingStatus", StringType(), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
    "sponsored_display_targetings": StructType(
        [
            StructField("targetId", LongType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("campaignId", LongType(), nullable=False),
            StructField("adGroupId", LongType(), nullable=False),
            StructField("state", StringType(), nullable=True),
            StructField("bid", DoubleType(), nullable=True),
            StructField("targetingExpression", ArrayType(StructType([
                StructField("predicateType", StringType(), nullable=True),
                StructField("value", StringType(), nullable=True),
            ])), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
    "stores": StructType(
        [
            StructField("storeId", StringType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("brandName", StringType(), nullable=True),
            StructField("websiteUrl", StringType(), nullable=True),
            StructField("storeType", StringType(), nullable=True),
            StructField("countryCode", StringType(), nullable=True),
        ]
    ),
    "store_assets": StructType(
        [
            StructField("assetId", StringType(), nullable=False),
            StructField("profileId", LongType(), nullable=False),
            StructField("storeId", StringType(), nullable=False),
            StructField("assetType", StringType(), nullable=True),
            StructField("assetUrl", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
    "brand_safety_profiles": StructType(
        [
            StructField("profileId", LongType(), nullable=False),
            StructField("safetyProfileId", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("description", StringType(), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("createdTime", LongType(), nullable=True),
            StructField("modifiedTime", LongType(), nullable=True),
        ]
    ),
}

# Metadata for each table (primary keys, cursor field, ingestion type)
TABLE_METADATA = {
    "profiles": {
        "primary_keys": ["profileId"],
        "cursor_field": None,  # snapshot table
    },
    "portfolios": {
        "primary_keys": ["portfolioId"],
        "cursor_field": "lastUpdatedDate",
    },
    "sponsored_product_campaigns": {
        "primary_keys": ["campaignId"],
        "cursor_field": "lastUpdateTime",
    },
    "sponsored_product_ad_groups": {
        "primary_keys": ["adGroupId"],
        "cursor_field": "lastUpdateTime",
    },
    "sponsored_product_keywords": {
        "primary_keys": ["keywordId"],
        "cursor_field": "lastUpdateTime",
    },
    "sponsored_product_ads": {
        "primary_keys": ["adId"],
        "cursor_field": "lastUpdateTime",
    },
    "sponsored_product_targetings": {
        "primary_keys": ["targetId"],
        "cursor_field": "lastUpdateTime",
    },
    "sponsored_brands_campaigns": {
        "primary_keys": ["campaignId"],
        "cursor_field": "modifiedTime",
    },
    "sponsored_brands_ad_groups": {
        "primary_keys": ["adGroupId"],
        "cursor_field": "modifiedTime",
    },
    "sponsored_brands_keywords": {
        "primary_keys": ["keywordId"],
        "cursor_field": "modifiedTime",
    },
    "sponsored_brands_ads": {
        "primary_keys": ["adId"],
        "cursor_field": "modifiedTime",
    },
    "sponsored_display_campaigns": {
        "primary_keys": ["campaignId"],
        "cursor_field": None,  # snapshot for now
    },
    "sponsored_display_ad_groups": {
        "primary_keys": ["adGroupId"],
        "cursor_field": "modifiedTime",
    },
    "sponsored_display_product_ads": {
        "primary_keys": ["adId"],
        "cursor_field": "modifiedTime",
    },
    "sponsored_display_targetings": {
        "primary_keys": ["targetId"],
        "cursor_field": "modifiedTime",
    },
    "stores": {
        "primary_keys": ["storeId"],
        "cursor_field": None,  # snapshot table
    },
    "store_assets": {
        "primary_keys": ["assetId"],
        "cursor_field": "modifiedTime",
    },
    "brand_safety_profiles": {
        "primary_keys": ["safetyProfileId"],
        "cursor_field": "modifiedTime",
    },
}

# Tables whose ingestion type cannot be inferred from metadata alone
# These overrides are applied before the fallback logic
INGESTION_TYPE_OVERRIDES = {
    "profiles": "snapshot",
    "portfolios": "cdc",
    "sponsored_product_campaigns": "cdc",
    "sponsored_product_ad_groups": "cdc",
    "sponsored_product_keywords": "cdc",
    "sponsored_product_ads": "cdc",
    "sponsored_product_targetings": "cdc",
    "sponsored_brands_campaigns": "cdc",
    "sponsored_brands_ad_groups": "cdc",
    "sponsored_brands_keywords": "cdc",
    "sponsored_brands_ads": "cdc",
    "sponsored_display_campaigns": "snapshot",
    "sponsored_display_ad_groups": "cdc",
    "sponsored_display_product_ads": "cdc",
    "sponsored_display_targetings": "cdc",
    "stores": "snapshot",
    "store_assets": "cdc",
    "brand_safety_profiles": "cdc",
}
