"""Static schemas and metadata for LinkedIn Ads connector tables."""

from __future__ import annotations

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---- Core entity schemas (minimal, forward-compatible) ----


AD_ACCOUNTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("reference", StringType(), nullable=True),
        StructField("last_modified_at", LongType(), nullable=True),
        StructField("raw_json", StringType(), nullable=True),
    ]
)

CAMPAIGN_GROUPS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("account", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("run_schedule", StringType(), nullable=True),
        StructField("last_modified_at", LongType(), nullable=True),
        StructField("raw_json", StringType(), nullable=True),
    ]
)

CAMPAIGNS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("account", StringType(), nullable=True),
        StructField("campaign_group", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("objective_type", StringType(), nullable=True),
        StructField("last_modified_at", LongType(), nullable=True),
        StructField("raw_json", StringType(), nullable=True),
    ]
)

CREATIVES_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("account", StringType(), nullable=True),
        StructField("campaign", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("last_modified_at", LongType(), nullable=True),
        StructField("raw_json", StringType(), nullable=True),
    ]
)


# ---- Analytics schema (daily) ----

AD_ANALYTICS_DAILY_SCHEMA = StructType(
    [
        StructField("date", StringType(), nullable=False),  # YYYY-MM-DD
        StructField("pivot", StringType(), nullable=False),  # ACCOUNT|CAMPAIGN_GROUP|CAMPAIGN|CREATIVE
        StructField("pivot_value", StringType(), nullable=False),  # pivot URN
        StructField("account", StringType(), nullable=True),  # account URN when available
        StructField("campaign_group", StringType(), nullable=True),
        StructField("campaign", StringType(), nullable=True),
        StructField("creative", StringType(), nullable=True),
        # Common metrics (subset; raw_json contains the full set requested)
        StructField("impressions", LongType(), nullable=True),
        StructField("clicks", LongType(), nullable=True),
        StructField("spend_local", StringType(), nullable=True),
        StructField("raw_json", StringType(), nullable=True),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "ad_accounts": AD_ACCOUNTS_SCHEMA,
    "campaign_groups": CAMPAIGN_GROUPS_SCHEMA,
    "campaigns": CAMPAIGNS_SCHEMA,
    "creatives": CREATIVES_SCHEMA,
    "ad_analytics_account_daily": AD_ANALYTICS_DAILY_SCHEMA,
    "ad_analytics_campaign_group_daily": AD_ANALYTICS_DAILY_SCHEMA,
    "ad_analytics_campaign_daily": AD_ANALYTICS_DAILY_SCHEMA,
    "ad_analytics_creative_daily": AD_ANALYTICS_DAILY_SCHEMA,
}


TABLE_METADATA: dict[str, dict] = {
    "ad_accounts": {
        "primary_keys": ["id"],
        "cursor_field": "last_modified_at",
        "ingestion_type": "snapshot",
    },
    "campaign_groups": {
        "primary_keys": ["id"],
        "cursor_field": "last_modified_at",
        "ingestion_type": "snapshot",
    },
    "campaigns": {
        "primary_keys": ["id"],
        "cursor_field": "last_modified_at",
        "ingestion_type": "snapshot",
    },
    "creatives": {
        "primary_keys": ["id"],
        "cursor_field": "last_modified_at",
        "ingestion_type": "snapshot",
    },
    # Analytics are append-only by date with reprocessing allowed.
    "ad_analytics_account_daily": {
        "primary_keys": ["date", "pivot", "pivot_value"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
    "ad_analytics_campaign_group_daily": {
        "primary_keys": ["date", "pivot", "pivot_value"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
    "ad_analytics_campaign_daily": {
        "primary_keys": ["date", "pivot", "pivot_value"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
    "ad_analytics_creative_daily": {
        "primary_keys": ["date", "pivot", "pivot_value"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
}


DEFAULT_AD_ANALYTICS_FIELDS: list[str] = [
    # Keep defaults small; user can override with options["ad_analytics_fields"].
    "impressions",
    "clicks",
    # spend often comes back as nested money; we keep raw_json and a best-effort flattened field.
    "costInLocalCurrency",
]


def list_supported_tables() -> list[str]:
    return sorted(TABLE_SCHEMAS.keys())


def get_analytics_pivot_for_table(table_name: str) -> str:
    mapping = {
        "ad_analytics_account_daily": "ACCOUNT",
        "ad_analytics_campaign_group_daily": "CAMPAIGN_GROUP",
        "ad_analytics_campaign_daily": "CAMPAIGN",
        "ad_analytics_creative_daily": "CREATIVE",
    }
    if table_name not in mapping:
        raise ValueError(f"Not an analytics table: {table_name}")
    return mapping[table_name]

