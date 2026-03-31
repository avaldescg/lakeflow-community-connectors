import json
from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.linkedin_ads.linkedin_ads import (
    LinkedinAdsLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


EXPECTED_TABLES = {
    "ad_accounts",
    "campaign_groups",
    "campaigns",
    "creatives",
    "ad_analytics_account_daily",
    "ad_analytics_campaign_group_daily",
    "ad_analytics_campaign_daily",
    "ad_analytics_creative_daily",
}


class TestLinkedinAdsConnector(LakeflowConnectTests):
    connector_class = LinkedinAdsLakeflowConnect

    @classmethod
    def setup_class(cls):
        config_path = cls._config_dir() / "dev_config.json"
        if not config_path.exists():
            pytest.skip(f"Missing config file: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        # Real-source tests require an OAuth access token.
        token = (
            config.get("access_token")
            or config.get("token")
            or ""
        )
        if not str(token).strip():
            pytest.skip(
                "LinkedIn Ads credentials missing. "
                "Set configs/dev_config.json with access_token."
            )

        cls.config = config
        cls.table_configs = cls._load_table_configs()
        super().setup_class()

    def test_expected_tables_present(self):
        tables = set(self.connector.list_tables())
        missing = EXPECTED_TABLES - tables
        assert not missing, f"Missing expected LinkedIn Ads tables: {sorted(missing)}"
