import pytest
import pandas as pd
from pipelines.ecommerce_order_analytics_etl import EcommerceOrderAnalyticsETL

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'id': [1, 2],
        'order_id': ['ORD1', 'ORD2'],
        'order_name': ['Name1', 'Name2'],
        'email': ['test@gmail.com', 'user@yahoo.com'],
        'created_at': ['2023-01-01 10:00:00', '2023-01-02 10:00:00'],
        'process_at': ['2023-01-02 10:00:00', '2023-01-03 10:00:00'],
        'last_attempt_on': ['2023-01-01 11:00:00', '2023-01-02 11:00:00'],
        'attempt_count': [0, 2],
        'is_reprocessed': [0, 1],
        'status': ['success', 'pending'],
        'font_name': ['FontA', 'FontB'],
        'variant_title': ['VarA', 'VarB'],
        'source_name': ['SrcA', 'SrcB'],
        'product_type': ['TypeA', 'TypeB'],
        'skuid': ['S1', 'S2'],
        'eula_id': ['E1', 'E2'],
        'md5': ['M1', 'M2'],
        'font_identifier': ['F1', 'F2']
    })

def test_transform(sample_data):
    etl = EcommerceOrderAnalyticsETL()
    # Mocking the engine to avoid connection issues during test
    etl.source_engine = None
    etl.target_engine = None
    
    transformed_df = etl.transform(sample_data)
    
    assert 'order_record_id' in transformed_df.columns
    assert 'email_domain' in transformed_df.columns
    assert 'email_provider' in transformed_df.columns
    assert 'total_attempts' in transformed_df.columns
    assert transformed_df.iloc[0]['email_provider'] == 'Yahoo'
    assert transformed_df.iloc[1]['email_provider'] == 'Gmail'

def test_validate(sample_data):
    etl = EcommerceOrderAnalyticsETL()
    etl.source_engine = None
    etl.target_engine = None
    
    df = etl.transform(sample_data)
    # This should pass
    etl.validate(df)
    
    # Test failure
    bad_df = df.copy()
    bad_df.loc[0, 'order_record_id'] = None
    with pytest.raises(ValueError, match="Validation failed: Nulls in order_record_id"):
        etl.validate(bad_df)
