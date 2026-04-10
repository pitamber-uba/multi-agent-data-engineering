import logging
import pandas as pd
from sqlalchemy import create_engine

class ShopifyOrderSummaryEtlDataAnalytics:
    def __init__(self, source_url, target_url):
        self.logger = logging.getLogger(__name__)
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)
        self.row_limit = 3000

    def extract(self):
        self.logger.info("Starting extraction from myfonts_shopify_data")
        query = f"SELECT * FROM myfonts_shopify_data LIMIT {self.row_limit}"
        df = pd.read_sql(query, self.source_engine)
        self.logger.info(f"Extracted {len(df)} rows from myfonts_shopify_data")
        return df

    def transform(self, df):
        self.logger.info("Starting transformation")
        
        # 1. drop_columns
        df = df.drop(columns=['skuid', 'eula_id'], errors='ignore')
        self.logger.info(f"Dropped columns. Rows: {len(df)}")

        # 2. derive_domain
        def get_domain(email):
            if pd.isna(email) or '@' not in str(email):
                return None
            return str(email).split('@')[1]
        
        df['domain'] = df['email'].apply(get_domain)
        self.logger.info(f"Derived 'domain' column. Rows: {len(df)}")

        # 3. select_columns
        cols = [
            'id', 'created_at', 'process_at', 'order_id', 'order_name', 'email', 
            'domain', 'variant_title', 'font_name', 'md5', 'source_name', 'status', 
            'attempt_count', 'last_attempt_on', 'is_reprocessed', 'product_type', 
            'font_identifier', 'process_at_date'
        ]
        for col in cols:
            if col not in df.columns:
                df[col] = None
        df = df[cols]
        self.logger.info(f"Selected columns. Rows: {len(df)}")
        
        return df

    def validate(self, df):
        self.logger.info("Starting quality checks")
        if len(df) <= 0:
            raise ValueError("row_count_gt check failed: len(df) <= 0")
        
        for col in ['id', 'process_at']:
            if df[col].isnull().any():
                raise ValueError(f"required_fields_not_null check failed: {col} has nulls")
        
        self.logger.info("Quality checks passed")

    def load(self, df):
        self.logger.info("Starting load to testTable")
        df.to_sql('testTable', self.target_engine, if_exists='append', index=False, chunksize=1000)
        self.logger.info(f"Successfully loaded {len(df)} rows to testTable")

    def run(self):
        import datetime
        start = datetime.datetime.now()
        self.logger.info(f"Pipeline started at {start}")
        
        df = self.extract()
        df = self.transform(df)
        self.validate(df)
        self.load(df)
        
        end = datetime.datetime.now()
        self.logger.info(f"Pipeline finished at {end}. Duration: {end - start}")
