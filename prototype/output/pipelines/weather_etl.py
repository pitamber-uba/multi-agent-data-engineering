import pandas as pd
from sqlalchemy import create_engine

class WeatherETL:
    def __init__(self, db_path="sqlite:///weather.db"):
        self.db_path = db_path
        self.engine = create_engine(self.db_path)

    def extract(self, file_path):
        return pd.read_csv(file_path)

    def transform(self, df):
        # Convert temperature
        df['temp_c'] = (df['temp_f'] - 32) * 5 / 9
        
        # Drop incomplete
        df = df.dropna(subset=['date', 'city', 'temp_f'])
        
        return df[['date', 'city', 'temp_c', 'humidity', 'wind_speed']]

    def validate(self, df):
        if len(df) <= 0:
            raise ValueError("Row count must be greater than 0")
        
        required_columns = ['date', 'city', 'temp_c']
        if df[required_columns].isnull().any().any():
            raise ValueError("Required columns contain null values")
        
        return True

    def load(self, df, table_name="daily_weather"):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

    def run(self, input_file):
        df = self.extract(input_file)
        df = self.transform(df)
        self.validate(df)
        self.load(df)
        return df
