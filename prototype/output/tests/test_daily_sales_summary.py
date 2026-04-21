import pytest
import pandas as pd
from pipelines.daily_sales_summary import DailySalesSummaryPipeline

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'order_id': [1, 2, 3, 1],
        'customer_id': [101, 102, 103, 101],
        'product_id': [50, 51, 52, 50],
        'quantity': [2, 1, 5, 2],
        'unit_price': [10.0, 20.0, 5.0, 10.0],
        'region': ['US', 'US', 'EU', 'US'],
        'order_date': ['2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01'],
        'payment_method': ['credit', 'credit', 'cash', 'credit']
    })

def test_transform(sample_data):
    pipeline = DailySalesSummaryPipeline()
    transformed = pipeline.transform(sample_data)
    
    assert len(transformed) == 2  # US and EU
    assert 'total_revenue' in transformed.columns
    assert 'order_count' in transformed.columns
    assert 'avg_order_value' in transformed.columns
    
    # Check US revenue: (2*10) + (1*20) = 40
    us_data = transformed[transformed['region'] == 'US']
    assert us_data['total_revenue'].iloc[0] == 40.0
    assert us_data['order_count'].iloc[0] == 2
    
    # Check EU revenue: (5*5) = 25
    eu_data = transformed[transformed['region'] == 'EU']
    assert eu_data['total_revenue'].iloc[0] == 25.0
    assert eu_data['order_count'].iloc[0] == 1

def test_validate(sample_data):
    pipeline = DailySalesSummaryPipeline()
    transformed = pipeline.transform(sample_data)
    assert pipeline.validate(transformed) is True

def test_validate_empty():
    pipeline = DailySalesSummaryPipeline()
    with pytest.raises(ValueError, match="Dataframe is empty"):
        pipeline.validate(pd.DataFrame())

def test_validate_null_revenue():
    pipeline = DailySalesSummaryPipeline()
    df = pd.DataFrame({'total_revenue': [None]})
    with pytest.raises(ValueError, match="Revenue column contains null values"):
        pipeline.validate(df)
