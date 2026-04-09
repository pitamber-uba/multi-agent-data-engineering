import pytest
import pandas as pd
from pipelines.monotype_customer_to_personal_details import MonotypeCustomerToPersonalDetails

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'email': ['alice@example.com', 'bob@test.org'],
        'address': ['123 St', '456 Ave'],
        'orgName': ['OrgA', 'OrgB']
    })

def test_transform(sample_data):
    # Mocking engine is not allowed, but we can test logic by passing dummy URLs
    # The class uses engines, so we need to ensure it doesn't try to connect during transform
    pipeline = MonotypeCustomerToPersonalDetails("sqlite:///:memory:", "sqlite:///:memory:")
    pipeline.df = sample_data
    pipeline.transform()
    
    assert 'domain' in pipeline.df.columns
    assert pipeline.df.loc[0, 'domain'] == 'example.com'
    assert pipeline.df.loc[1, 'domain'] == 'test.org'
    assert list(pipeline.df.columns) == ['name', 'email', 'address', 'orgName', 'domain']

def test_validate_success(sample_data):
    pipeline = MonotypeCustomerToPersonalDetails("sqlite:///:memory:", "sqlite:///:memory:")
    pipeline.df = sample_data
    pipeline.validate() # Should not raise

def test_validate_failure_nulls():
    df = pd.DataFrame({
        'name': ['Alice', None],
        'email': ['alice@example.com', 'bob@test.org'],
        'address': ['123 St', '456 Ave'],
        'orgName': ['OrgA', 'OrgB']
    })
    pipeline = MonotypeCustomerToPersonalDetails("sqlite:///:memory:", "sqlite:///:memory:")
    pipeline.df = df
    with pytest.raises(ValueError, match="Field name cannot contain nulls"):
        pipeline.validate()

def test_validate_failure_empty():
    df = pd.DataFrame(columns=['name', 'email', 'address', 'orgName'])
    pipeline = MonotypeCustomerToPersonalDetails("sqlite:///:memory:", "sqlite:///:memory:")
    pipeline.df = df
    with pytest.raises(ValueError, match="Row count must be greater than 0"):
        pipeline.validate()
