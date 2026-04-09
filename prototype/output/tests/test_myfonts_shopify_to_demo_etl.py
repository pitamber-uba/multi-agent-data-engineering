import pytest
import pandas as pd
from sqlalchemy import create_engine
from pipelines.myfonts_shopify_to_demo_etl import MyFontsShopifyToDemoETL

@pytest.fixture
def pipeline():
    # Use sqlite for testing as per instructions
    source_url = "sqlite:///:memory:"
    target_url = "sqlite:///:memory:"
    
    # Setup dummy source table
    engine = create_engine(source_url)
    df = pd.DataFrame({
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
        'status': ['pending', 'pending'],
        'attempt_count': [0, 0],
        'last_attempt_on': [None, None],
        'is_reprocessed': [0, 0],
        'product_type': [None, None],
        'font_identifier': [None, None],
        'process_at_date': [None, None]
    })
    df.to_sql('myfonts_shopify_data', engine, index=False)
    
    return MyFontsShopifyToDemoETL(source_url, target_url)

def test_transform(pipeline):
    df = pd.DataFrame({
        'id': [1],
        'email': ['test@example.com'],
        'skuid': ['s1'],
        'eula_id': ['e1'],
        'process_at': ['now']
    })
    transformed = pipeline.transform(df)
    assert 'domain' in transformed.columns
    assert transformed['domain'].iloc[0] == 'example.com'
    assert 'skuid' not in transformed.columns
    assert 'eula_id' not in transformed.columns

def test_validate(pipeline):
    df = pd.DataFrame({'id': [1], 'process_at': ['now']})
    assert pipeline.validate(df) is True
    
    df_invalid = pd.DataFrame({'id': [None], 'process_at': ['now']})
    with pytest.raises(ValueError):
        pipeline.validate(df_invalid)
