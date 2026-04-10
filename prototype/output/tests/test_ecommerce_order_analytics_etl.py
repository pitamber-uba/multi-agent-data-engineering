import pytest
import pandas as pd
from pipelines.ecommerce_order_analytics_etl import EcommerceOrderAnalyticsEtl

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'id': [1, 2],
        'order_id': ['A1', 'A2'],
        'order_name': ['N1', 'N2'],
        'email': ['test@gmail.com', 'test@yahoo.com'],
        'attempt_count': [0, 2],
        'created_at': ['2023-01-01 10:00:00', '2023-01-02 10:00:00'],
        'process_at': ['2023-01-02 10:00:00', '2023-01-03 10:00:00'],
        'last_attempt_on': ['2023-01-01 10:00:00', '2023-01-02 10:00:00'],
        'status': ['success', 'success'],
        'is_reprocessed': [0, 0],
        'font_name': ['f1', 'f2'],
        'variant_title': ['v1', 'v2'],
        'source_name': ['s1', 's2'],
        'product_type': ['p1', 'p2'],
        'skuid': [None, None],
        'eula_id': [None, None],
        'md5': [None, None]
    })

def test_transform(sample_df):
    # Mocking engine is not needed if we just test the transform method logic
    etl = EcommerceOrderAnalyticsEtl("mysql+pymysql://u:p@h:3306/d", "mysql+pymysql://u:p@h:3306/d")
    transformed = etl.transform(sample_df)
    
    assert 'order_record_id' in transformed.columns
    assert 'email_domain' in transformed.columns
    assert transformed.iloc[0]['email_provider'] == 'Yahoo'
    assert transformed.iloc[1]['email_provider'] == 'Gmail'
    assert transformed.iloc[0]['total_attempts'] == 2
    assert transformed.iloc[1]['total_attempts'] == 0

def test_validate_passes(sample_df):
    etl = EcommerceOrderAnalyticsEtl("mysql+pymysql://u:p@h:3306/d", "mysql+pymysql://u:p@h:3306/d")
    transformed = etl.transform(sample_df)
    etl.validate(transformed) # Should not raise

def test_validate_fails_on_nulls():
    df = pd.DataFrame({'order_record_id': [1], 'order_processed_at': [None], 'email': ['a@b.com']})
    etl = EcommerceOrderAnalyticsEtl("mysql+pymysql://u:p@h:3306/d", "mysql+pymysql://u:p@h:3306/d")
    with pytest.raises(ValueError, match="Required field order_processed_at has nulls"):
        etl.validate(df)
