import pytest
import pandas as pd
from pipelines.shopify_order_summary_etl2 import ShopifyOrderSummaryEtl2

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'id': [1, 2],
        'created_at': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-02-01')],
        'order_id': ['O1', 'O2'],
        'order_name': ['N1', 'N2'],
        'email': [' TEST@Example.com ', 'other@domain.com'],
        'font_name': ['F1', 'F2'],
        'product_type': ['P1', 'P2'],
        'status': ['success', 'pending'],
        'attempt_count': [1, 1],
        'is_reprocessed': [False, False],
        'skuid': ['S1', 'S2'],
        'eula_id': ['E1', 'E2'],
        'md5': ['M1', 'M2'],
        'font_identifier': ['FI1', 'FI2']
    })

def test_transform(sample_df):
    # Mocking URLs for testing purposes
    pipeline = ShopifyOrderSummaryEtl2("sqlite:///:memory:", "sqlite:///:memory:")
    transformed = pipeline.transform(sample_df)
    
    assert 'email_domain' in transformed.columns
    assert transformed.loc[0, 'email'] == 'test@example.com'
    assert transformed.loc[0, 'email_domain'] == 'example.com'
    assert transformed.loc[0, 'order_year'] == 2023
    assert transformed.loc[0, 'status_group'] == 'completed'
    assert transformed.loc[1, 'status_group'] == 'in_progress'
    assert 'skuid' not in transformed.columns

def test_validate_passes(sample_df):
    pipeline = ShopifyOrderSummaryEtl2("sqlite:///:memory:", "sqlite:///:memory:")
    transformed = pipeline.transform(sample_df)
    pipeline.validate(transformed)

def test_validate_fails():
    pipeline = ShopifyOrderSummaryEtl2("sqlite:///:memory:", "sqlite:///:memory:")
    df = pd.DataFrame({'id': [1], 'email': [None], 'status': ['success']})
    with pytest.raises(ValueError, match="required_fields_not_null"):
        pipeline.validate(df)
