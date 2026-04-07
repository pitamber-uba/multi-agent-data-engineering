import pandas as pd
from sqlalchemy import create_engine
import logging

class MyFontsShopifyToDemoETL:
    def __init__(self, source_db_url, target_db_url):
        self.source_engine = create_engine(source_db_url, pool_pre_ping=True, pool_recycle=3600)
        self.target_engine = create_engine(target_db_url, pool_pre_ping=True, pool_recycle=3600)
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def extract(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.source_engine)

    def transform(self, df):
        # Pre-load: required fields check
        required_fields = ['id', 'process_at', 'skuid', 'eula_id']
        df = df.dropna(subset=required_fields)

        # Transform: derive domain
        def get_domain(email):
            if pd.isna(email) or '@' not in str(email):
                return None
            return str(email).split('@')[1]

        df['domain'] = df['email'].apply(get_domain)

        # Transform: drop columns
        df = df.drop(columns=['skuid', 'eula_id'], errors='ignore')

        # Transform: select columns
        target_cols = [
            'id', 'created_at', 'process_at', 'order_id', 'order_name', 'email',
            'domain', 'variant_title', 'font_name', 'md5', 'source_name', 'status',
            'attempt_count', 'last_attempt_on', 'is_reprocessed', 'product_type',
            'font_identifier', 'process_at_date'
        ]
        # Ensure all columns exist, fill missing with None
        for col in target_cols:
            if col not in df.columns:
                df[col] = None
        
        return df[target_cols]

    def validate(self, df):
        # Quality check: domain populated
        invalid_domains = df[df['email'].notna() & df['domain'].isna()]
        if not invalid_domains.empty:
            self.logger.warning(f"Found {len(invalid_domains)} rows with email but no domain.")
        return True

    def load(self, df, table_name):
        df.to_sql(table_name, self.target_engine, if_exists='append', index=False, chunksize=1000)

    def run(self, source_table, target_table):
        df = self.extract(source_table)
        if df.empty:
            self.logger.error("Source table is empty.")
            return
        
        transformed_df = self.transform(df)
        self.validate(transformed_df)
        self.load(transformed_df, target_table)
        self.logger.info("ETL job completed successfully.")
