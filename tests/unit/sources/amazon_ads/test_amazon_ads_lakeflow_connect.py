"""Test suite for Amazon Ads connector."""

from databricks.labs.community_connector.sources.amazon_ads import AmazonAdsLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAmazonAdsConnector(LakeflowConnectTests):
    """Test the Amazon Ads connector implementation."""

    connector_class = AmazonAdsLakeflowConnect
    sample_records = 10
