import pytest
import pandas as pd
from pipelines.daily_sales_summary import DailySalesSummaryPipeline

def test_transform_and_validate():
    # Setup dummy data
    data = {
        'order_id': [1, 1, 2, 3],
        'customer_id': ['C1', 'C1', 'C2', 'C3'],
        'product_id': ['P1', 'P1', 'P2', 'P3'],
        'amount': [100.0, 100.0, -5.0, 50.0],
        'currency': ['USD', 'USD', 'USD', 'USD'],
        'order_date': ['2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01']
    }
    df = pd.DataFrame(data)
    
    # Initialize pipeline with dummy db_url
    pipeline = DailySalesSummaryPipeline(db_url='sqlite:///:memory:', execution_date='2023-01-01')
    
    # Transform
    transformed_df = pipeline.transform(df)
    
    # Assertions
    # Deduplication: order_id 1 should appear once.
    # Filter: order_id 2 (amount -5) should be removed.
    # Aggregation: C1 should have 100.0, C3 should have 50.0
    assert len(transformed_df) == 2
    assert transformed_df.loc[transformed_df['customer_id'] == 'C1', 'total_amount'].iloc[0] == 100.0
    assert transformed_df.loc[transformed_df['customer_id'] == 'C3', 'total_amount'].iloc[0] == 50.0
    
    # Validate
    assert pipeline.validate(transformed_df) is True

def test_validate_fails_on_empty():
    pipeline = DailySalesSummaryPipeline(db_url='sqlite:///:memory:', execution_date='2023-01-01')
    df = pd.DataFrame(columns=['customer_id', 'order_date', 'total_amount', 'order_count'])
    with pytest.raises(ValueError, match="Row count must be greater than 0"):
        pipeline.validate(df)

def test_validate_fails_on_nulls():
    pipeline = DailySalesSummaryPipeline(db_url='sqlite:///:memory:', execution_date='2023-01-01')
    df = pd.DataFrame({
        'customer_id': ['C1', None],
        'order_date': ['2023-01-01', '2023-01-01'],
        'total_amount': [100.0, 50.0],
        'order_count': [1, 1]
    })
    with pytest.raises(ValueError, match="Column customer_id contains null values"):
        pipeline.validate(df)
