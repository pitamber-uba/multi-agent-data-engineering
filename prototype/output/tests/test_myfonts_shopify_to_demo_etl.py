import pandas as pd
from pipelines.myfonts_shopify_to_demo_etl import MyFontsShopifyToDemoETL

def test_transform():
    # Mock data
    data = {
        'id': [1, 2],
        'process_at': ['2023-01-01', '2023-01-02'],
        'skuid': ['s1', 's2'],
        'eula_id': ['e1', 'e2'],
        'email': ['test@example.com', 'invalid_email'],
        'created_at': [None, None],
        'order_id': [None, None],
        'order_name': [None, None],
        'variant_title': [None, None],
        'font_name': [None, None],
        'md5': [None, None],
        'source_name': [None, None],
        'status': [None, None],
        'attempt_count': [0, 0],
        'last_attempt_on': [None, None],
        'is_reprocessed': [False, False],
        'product_type': [None, None],
        'font_identifier': [None, None],
        'process_at_date': [None, None]
    }
    df = pd.DataFrame(data)
    
    # Initialize with dummy URLs (not used in transform)
    etl = MyFontsShopifyToDemoETL('sqlite:///:memory:', 'sqlite:///:memory:')
    
    transformed_df = etl.transform(df)
    
    assert 'domain' in transformed_df.columns
    assert transformed_df.loc[0, 'domain'] == 'example.com'
    assert transformed_df.loc[1, 'domain'] is None
    assert 'skuid' not in transformed_df.columns
    assert len(transformed_df) == 2

def test_validate():
    data = {
        'email': ['test@example.com', 'no_domain'],
        'domain': ['example.com', None]
    }
    df = pd.DataFrame(data)
    etl = MyFontsShopifyToDemoETL('sqlite:///:memory:', 'sqlite:///:memory:')
    
    # Should return True (logging warning internally)
    assert etl.validate(df) is True
