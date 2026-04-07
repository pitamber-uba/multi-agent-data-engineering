import pytest
import pandas as pd
from pipelines.extensis_events_ingestion import ExtensisEventsIngestionPipeline

def test_transform():
    config = {}
    pipeline = ExtensisEventsIngestionPipeline(config)
    
    input_df = pd.DataFrame({'raw_data': ['{"eventName": "permanentActivation", "userEmail": "test@example.com", "mtfid": "123", "timestamp": "2023-01-01"}']})
    transformed = pipeline.transform(input_df)
    
    assert 'event_type' in transformed.columns
    assert transformed['event_type'].iloc[0] == 'permanentActivation'
    assert 'gcid' in transformed.columns
    assert transformed['gcid'].iloc[0] == 'GCID123'

def test_validate():
    config = {}
    pipeline = ExtensisEventsIngestionPipeline(config)
    
    valid_df = pd.DataFrame({
        'event_type': ['permanentActivation'],
        'gcid': ['GCID123'],
        'profile_id': ['UUID-123']
    })
    
    assert pipeline.validate(valid_df) is True
    
    invalid_df = pd.DataFrame({'wrong_col': [1]})
    with pytest.raises(ValueError):
        pipeline.validate(invalid_df)
