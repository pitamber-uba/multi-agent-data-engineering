import pytest
import pandas as pd
from pipelines.monotype_customer_to_personal_details import MonotypeCustomerToPersonalDetails

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'name': ['John Doe', 'Jane Smith'],
        'email': ['john@example.com', 'jane@test.org'],
        'address': ['123 Main St', '456 Oak Ave'],
        'orgName': ['Org A', 'Org B']
    })

def test_transform(sample_data):
    # Mocking engine is not required if we don't call extract/load
    # We just test the transform logic
    pipeline = MonotypeCustomerToPersonalDetails("sqlite:///:memory:", "sqlite:///:memory:")
    pipeline.df = sample_data
    pipeline.transform()
    
    assert 'domain' in pipeline.df.columns
    assert pipeline.df.loc[0, 'domain'] == 'example.com'
    assert pipeline.df.loc[1, 'domain'] == 'test.org'
    assert list(pipeline.df.columns) == ['name', 'email', 'address', 'orgName', 'domain']

def test_validate_passes(sample_data):
    pipeline = MonotypeCustomerToPersonalDetails("sqlite:///:memory:", "sqlite:///:memory:")
    pipeline.df = sample_data
    assert pipeline.validate() is True

def test_validate_fails_on_nulls():
    df = pd.DataFrame({
        'name': ['John Doe', None],
        'email': ['john@example.com', 'jane@test.org'],
        'address': ['123 Main St', '456 Oak Ave'],
        'orgName': ['Org A', 'Org B']
    })
    pipeline = MonotypeCustomerToPersonalDetails("sqlite:///:memory:", "sqlite:///:memory:")
    pipeline.df = df
    with pytest.raises(ValueError, match="name contains nulls"):
        pipeline.validate()
