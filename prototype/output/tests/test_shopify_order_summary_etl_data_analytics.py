import pytest
import pandas as pd
from pipelines.shopify_order_summary_etl_data_analytics import ShopifyOrderSummaryEtlDataAnalytics

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'id': [1, 2],
        'process_at': ['2023-01-01', '2023-01-02'],
        'email': ['test@example.com', 'user@domain.org'],
        'skuid': ['s1', 's2'],
        'eula_id': ['e1', 'e2'],
        'created_at': [None, None],
        'order_id': [None, None],
        'order_name': [None, None],
        'variant_title': [None, None],
        'font_name': [None, None],
        'md5': [None, None],
        'source_name': [None, None],
        'status': ['load', 'load'],
        'attempt_count': [0, 0],
        'last_attempt_on': [None, None],
        'is_reprocessed': [0, 0],
        'product_type': [None, None],
        'font_identifier': [None, None],
        'process_at_date': [None, None]
    })

def test_transform(sample_df):
    # Mocking URLs to avoid connection issues during test
    pipeline = ShopifyOrderSummaryEtlDataAnalytics("sqlite:///:memory:", "sqlite:///:memory:")
    transformed = pipeline.transform(sample_df)
    
    assert 'domain' in transformed.columns
    assert transformed['domain'].tolist() == ['example.com', 'domain.org']
    assert 'skuid' not in transformed.columns
    assert 'eula_id' not in transformed.columns
    assert len(transformed.columns) == 18

def test_validate_passes(sample_df):
    pipeline = ShopifyOrderSummaryEtlDataAnalytics("sqlite:///:memory:", "sqlite:///:memory:")
    pipeline.validate(sample_df)

def test_validate_fails_on_nulls():
    df = pd.DataFrame({'id': [1, None], 'process_at': ['2023-01-01', '2023-01-02']})
    pipeline = ShopifyOrderSummaryEtlDataAnalytics("sqlite:///:memory:", "sqlite:///:memory:")
    with pytest.raises(ValueError, match="required_fields_not_null"):
        pipeline.validate(df)
