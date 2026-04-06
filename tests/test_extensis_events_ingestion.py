import pytest
import pandas as pd
from pipelines.extensis_events_ingestion import ExtensisEventsIngestionPipeline

@pytest.fixture
def pipeline():
    return ExtensisEventsIngestionPipeline(config={})

def test_transform(pipeline):
    # Test with real DataFrame
    input_df = pd.DataFrame([
        {"mtfid": "M1", "timestamp": "2023-10-01T10:00:00Z", "eventName": "permanentActivation", "userEmail": "test@example.com", "adobeFonts": {"L": [{"S": "ps:XTS:1:2:3:4:5:6"}]}},
        {"mtfid": "M1", "timestamp": "2023-10-01T10:05:00Z", "eventName": "permanentActivation", "userEmail": "test@example.com", "adobeFonts": {"L": [{"S": "ps:XTS:1:2:3:4:5:6"}]}}
    ])
    
    transformed = pipeline.transform(input_df)
    
    assert len(transformed) == 1
    assert transformed.iloc[0]['event_count'] == 2
    assert 'gcid' in transformed.columns
    assert 'profile_id' in transformed.columns

def test_validate(pipeline):
    df = pd.DataFrame([{'gcid': 'G1', 'profile_id': 'P1'}])
    assert pipeline.validate(df) is True
    
    df_invalid = pd.DataFrame([{'wrong': 'column'}])
    assert pipeline.validate(df_invalid) is False
