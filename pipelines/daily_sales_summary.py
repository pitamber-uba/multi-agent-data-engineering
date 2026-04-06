import pandas as pd
import logging
from sqlalchemy import create_engine

class DailySalesSummaryPipeline:
    def __init__(self, db_url, execution_date):
        self.db_url = db_url
        self.execution_date = execution_date
        self.logger = logging.getLogger(__name__)
        self.engine = create_engine(self.db_url)

    def extract(self):
        self.logger.info(f"Extracting data for {self.execution_date}...")
        query = f"""
            SELECT order_id, customer_id, product_id, amount, currency, order_date
            FROM sales.transactions
            WHERE order_date = '{self.execution_date}'
        """
        return pd.read_sql(query, self.engine)

    def transform(self, df):
        self.logger.info("Transforming data...")
        # Deduplicate
        df = df.drop_duplicates(subset=['order_id'])
        # Filter
        df = df[df['amount'] > 0].copy()
        # Cast
        df['amount'] = pd.to_numeric(df['amount']).astype(float)
        df['order_date'] = pd.to_datetime(df['order_date']).dt.date
        
        # Aggregate
        summary = df.groupby(['customer_id', 'order_date']).agg(
            total_amount=('amount', 'sum'),
            order_count=('order_id', 'count')
        ).reset_index()
        
        return summary

    def validate(self, df):
        self.logger.info("Validating data...")
        if len(df) <= 0:
            raise ValueError("Row count must be greater than 0")
        
        required_columns = ['customer_id', 'order_date', 'total_amount', 'order_count']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Missing column: {col}")
            if df[col].isnull().any():
                raise ValueError(f"Column {col} contains null values")
        return True

    def load(self, df):
        self.logger.info("Loading data to reporting.daily_sales_summary...")
        df.to_sql('daily_sales_summary', self.engine, schema='reporting', if_exists='append', index=False)
        return True

    def run(self):
        try:
            raw_data = self.extract()
            transformed_data = self.transform(raw_data)
            self.validate(transformed_data)
            self.load(transformed_data)
            self.logger.info("Pipeline completed successfully.")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
