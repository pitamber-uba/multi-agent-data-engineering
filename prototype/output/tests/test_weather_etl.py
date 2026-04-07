import pytest
import pandas as pd
from pipelines.weather_etl import WeatherETL

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'date': ['2023-01-01', '2023-01-02', None],
        'city': ['New York', 'London', 'Paris'],
        'temp_f': [32.0, 68.0, 50.0],
        'humidity': [50, 60, 70],
        'wind_speed': [10, 5, 8]
    })

def test_transform(sample_data):
    etl = WeatherETL()
    transformed = etl.transform(sample_data)
    
    assert 'temp_c' in transformed.columns
    assert transformed['temp_c'].iloc[0] == 0.0
    assert transformed['temp_c'].iloc[1] == 20.0
    assert len(transformed) == 2  # One row dropped due to None in date

def test_validate_success(sample_data):
    etl = WeatherETL()
    transformed = etl.transform(sample_data)
    assert etl.validate(transformed) is True

def test_validate_failure():
    etl = WeatherETL()
    df = pd.DataFrame({'date': [None], 'city': ['Test'], 'temp_c': [0]})
    with pytest.raises(ValueError, match="Required columns contain null values"):
        etl.validate(df)

def test_run_integration(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    file_path = d / "weather.csv"
    df = pd.DataFrame({
        'date': ['2023-01-01'],
        'city': ['New York'],
        'temp_f': [32.0],
        'humidity': [50],
        'wind_speed': [10]
    })
    df.to_csv(file_path, index=False)
    
    db_path = f"sqlite:///{tmp_path}/test.db"
    etl = WeatherETL(db_path=db_path)
    etl.run(file_path)
    
    # Verify load
    from sqlalchemy import create_engine
    engine = create_engine(db_path)
    result = pd.read_sql("SELECT * FROM daily_weather", engine)
    assert len(result) == 1
    assert result['temp_c'].iloc[0] == 0.0
