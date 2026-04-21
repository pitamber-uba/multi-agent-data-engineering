import pytest
import pandas as pd
from pipelines.user_activity_tracker import UserActivityTrackerPipeline

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'user_id': [1, 1, 2, 1],
        'event_type': ['click', 'click', 'click', 'click'],
        'page_url': ['/home', '/about', '/home', '/contact'],
        'timestamp': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 10:10:00', '2023-01-01 10:00:00', '2023-01-01 11:00:00']),
        'device_type': ['mobile', 'mobile', 'desktop', 'mobile'],
        'referrer': ['google', 'direct', 'direct', 'direct']
    })

@pytest.fixture
def users_data():
    return pd.DataFrame({
        'user_id': [1, 2],
        'account_type': ['premium', 'free'],
        'signup_date': ['2022-01-01', '2022-02-01'],
        'country': ['US', 'UK']
    })

def test_transform(sample_data, users_data):
    pipeline = UserActivityTrackerPipeline()
    transformed = pipeline.transform(sample_data, users_data)
    
    assert not transformed.empty
    assert 'session_id' in transformed.columns
    assert 'page_views' in transformed.columns
    assert transformed.loc[transformed['user_id'] == 1, 'session_id'].nunique() == 2
    assert transformed.loc[transformed['user_id'] == 2, 'session_id'].nunique() == 1

def test_validate(sample_data, users_data):
    pipeline = UserActivityTrackerPipeline()
    transformed = pipeline.transform(sample_data, users_data)
    assert pipeline.validate(transformed) is True

def test_validate_fails_on_empty():
    pipeline = UserActivityTrackerPipeline()
    with pytest.raises(ValueError, match="Dataframe is empty"):
        pipeline.validate(pd.DataFrame())

def test_filter_bots(sample_data, users_data):
    data_with_bots = sample_data.copy()
    data_with_bots.loc[0, 'device_type'] = 'bot'
    
    pipeline = UserActivityTrackerPipeline()
    transformed = pipeline.transform(data_with_bots, users_data)
    
    assert 'bot' not in transformed['device_type'].values
