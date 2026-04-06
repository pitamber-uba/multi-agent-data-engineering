import pytest
import pandas as pd
from pipelines.extensis_events_ingestion import ExtensisEventsIngestionPipeline

@pytest.fixture
def pipeline():
    return ExtensisEventsIngestionPipeline(config={})

def test_transform(pipeline):
    df = pd.DataFrame({'raw_data': ['{"eventName": "permanentActivation"}']})
    transformed = pipeline.transform(df)
    assert 'eventName' in transformed.columns
    assert transformed['eventName'].iloc[0] == 'permanentActivation'

def test_validate_success(pipeline):
    df = pd.DataFrame({'mtf_id': ['123'], 'user': ['test@example.com'], 'timestamp': ['2023-01-01'], 'eventName': ['permanentActivation']})
    assert pipeline.validate(df) is True

def test_validate_failure(pipeline):
    df = pd.DataFrame({'wrong_column': [1]})
    with pytest.raises(ValueError):
        pipeline.validate(df)
