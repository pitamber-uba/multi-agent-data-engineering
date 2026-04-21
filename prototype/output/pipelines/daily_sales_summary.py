import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine

class DailySalesSummaryPipeline:
    def __init__(self, db_url=None):
        self.logger = logging.getLogger(__name__)
        self.db_url = db_url

    def extract(self, file_path: str):
        self.logger.info(f"Extracting data from {file_path}")
        return pd.read_csv(file_path)

    def transform(self, df: pd.DataFrame):
        self.logger.info("Transforming data...")
        
        # Deduplicate
        df = df.copy()
        df = df.drop_duplicates(subset=['order_id'], keep='first')
        
        # Calculate revenue
        df['revenue'] = df['quantity'] * df['unit_price']
        
        # Aggregate
        agg_df = df.groupby(['region', 'order_date']).agg(
            total_revenue=('revenue', 'sum'),
            order_count=('order_id', 'count'),
            avg_order_value=('revenue', 'mean')
        ).reset_index()
        
        agg_df['load_timestamp'] = datetime.now()
        return agg_df

    def validate(self, df: pd.DataFrame):
        self.logger.info("Validating data...")
        if df.empty:
            raise ValueError("Dataframe is empty")
        
        # Note: This validation is typically done on raw data before aggregation
        # But per spec, it's a pre-load check.
        
        if df['total_revenue'].isnull().any():
            raise ValueError("Revenue column contains null values")
            
        return True

    def load(self, df: pd.DataFrame):
        self.logger.info("Loading data to PostgreSQL...")
        if not self.db_url:
            self.logger.warning("No DB URL provided, skipping load.")
            return True
        
        engine = create_engine(self.db_url)
        # Upsert logic would go here, using pandas to_sql with a custom method or raw SQL
        df.to_sql('daily_sales_summary', engine, if_exists='append', index=False, schema='public')
        return True

    def run(self, file_path: str):
        raw_data = self.extract(file_path)
        transformed_data = self.transform(raw_data)
        self.validate(transformed_data)
        self.load(transformed_data)
        self.logger.info("Pipeline completed successfully.")
