import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime

class UserActivityTrackerPipeline:
    def __init__(self, db_url: str = None):
        self.logger = logging.getLogger(__name__)
        self.db_url = db_url
        self.engine = create_engine(db_url) if db_url else None

    def extract(self, data: pd.DataFrame = None):
        self.logger.info("Extracting data...")
        # In a real scenario, this would consume from Kafka
        return data if data is not None else pd.DataFrame()

    def transform(self, df: pd.DataFrame, users_df: pd.DataFrame = None):
        self.logger.info("Transforming data...")
        if df.empty:
            return df

        # 1. Filter bots
        df = df[df['device_type'] != 'bot'].copy()

        # 2. Sessionize
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(['user_id', 'timestamp'])
        df['time_diff'] = df.groupby('user_id')['timestamp'].diff()
        df['new_session'] = (df['time_diff'] > pd.Timedelta(minutes=30)) | (df['time_diff'].isna())
        df['session_id'] = df.groupby('user_id')['new_session'].cumsum()
        df['session_id'] = df['user_id'].astype(str) + '_' + df['session_id'].astype(str)

        # 3. Enrich
        if users_df is not None:
            df = df.merge(users_df, on='user_id', how='left')

        # 4. Aggregate
        agg_df = df.groupby(['user_id', 'session_id']).agg(
            page_views=('page_url', 'count'),
            session_duration_sec=('timestamp', lambda x: (x.max() - x.min()).total_seconds()),
            unique_pages=('page_url', 'nunique'),
            device_type=('device_type', 'first'),
            country=('country', 'first'),
            account_type=('account_type', 'first')
        ).reset_index()
        
        agg_df['load_date'] = datetime.now().date()
        return agg_df

    def validate(self, df: pd.DataFrame):
        self.logger.info("Validating data...")
        if df.empty:
            raise ValueError("Dataframe is empty")
        
        required_fields = ['user_id', 'session_id', 'page_views']
        for field in required_fields:
            if df[field].isnull().any():
                raise ValueError(f"Field {field} contains null values")
        
        if len(df) <= 0:
            raise ValueError("Row count must be greater than 0")
        
        return True

    def load(self, df: pd.DataFrame):
        self.logger.info("Loading data...")
        if self.engine:
            df.to_sql('user_sessions', self.engine, schema='analytics', if_exists='append', index=False, chunksize=500)
        return True

    def run(self, raw_data: pd.DataFrame, users_df: pd.DataFrame):
        df = self.extract(raw_data)
        transformed_df = self.transform(df, users_df)
        self.validate(transformed_df)
        self.load(transformed_df)
        self.logger.info("Pipeline completed successfully.")
        return transformed_df
