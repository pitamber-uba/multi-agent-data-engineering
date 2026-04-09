import logging
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class EcommerceOrderAnalyticsETL:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

        source_user = "root"
        source_pass = os.environ.get("SOURCE_DB_PASSWORD", "")
        source_host = "localhost"
        source_port = 3306
        source_db = "MyFonts_Legacy"
        self.source_url = f"mysql+pymysql://{source_user}:{source_pass}@{source_host}:{source_port}/{source_db}"

        target_user = "root"
        target_pass = os.environ.get("TARGET_DB_PASSWORD", "")
        target_host = "localhost"
        target_port = 3306
        target_db = "MyEtlDemo"
        self.target_url = f"mysql+pymysql://{target_user}:{target_pass}@{target_host}:{target_port}/{target_db}"

        self.source_engine = create_engine(self.source_url)
        self.target_engine = create_engine(self.target_url)

    def extract(self):
        self.logger.info("Extracting data from MyFonts_Legacy.myfonts_shopify_data")
        query = "SELECT * FROM myfonts_shopify_data LIMIT 5000"
        df = pd.read_sql(query, self.source_engine)
        self.logger.info(f"Extracted {len(df)} rows.")
        return df

    def transform(self, df: pd.DataFrame):
        initial_count = len(df)
        self.logger.info(f"Starting transformation. Initial row count: {initial_count}")

        # 1. Cast types
        df = df.astype({
            'attempt_count': 'int',
            'email': 'str',
            'id': 'int',
            'order_id': 'str',
            'order_name': 'str'
        })
        self.logger.info("Transformation: Cast types completed.")

        # 2. Clean strings
        for col in ['email', 'font_name', 'variant_title', 'source_name', 'product_type']:
            df[col] = df[col].astype(str).str.strip().str.lower()
        self.logger.info("Transformation: Cleaned strings.")

        # 3. Parse dates
        for col, fmt in [('created_at', '%Y-%m-%d %H:%M:%S'), ('last_attempt_on', '%Y-%m-%d %H:%M:%S'), ('process_at', '%Y-%m-%d %H:%M:%S')]:
            df[col] = pd.to_datetime(df[col], format=fmt, errors='coerce')
        self.logger.info("Transformation: Parsed dates.")

        # 4. Derive email_domain
        df['email_domain'] = df['email'].apply(lambda x: x.split('@')[1] if isinstance(x, str) and '@' in x else None)
        self.logger.info("Transformation: Derived email_domain.")

        # 5. email_provider
        def get_provider(domain):
            if domain == 'gmail.com':
                return 'Gmail'
            if domain in ['outlook.com', 'hotmail.com', 'live.com']:
                return 'Microsoft'
            if domain and 'yahoo' in domain:
                return 'Yahoo'
            return 'Corporate'
        df['email_provider'] = df['email_domain'].apply(get_provider)
        self.logger.info("Transformation: Derived email_provider.")

        # 6. attempt_bucket
        def get_bucket(count):
            if count == 0:
                return 'no_attempts'
            if count == 1:
                return 'single'
            if count in [2, 3]:
                return 'moderate'
            return 'high'
        df['attempt_bucket'] = df['attempt_count'].apply(get_bucket)
        self.logger.info("Transformation: Derived attempt_bucket.")

        # 7. processing_lag_days
        df['processing_lag_days'] = (df['process_at'] - df['created_at']).dt.total_seconds() / 86400
        self.logger.info("Transformation: Derived processing_lag_days.")

        # 8. is_reprocessed_flag
        df['is_reprocessed_flag'] = (df['is_reprocessed'] == 1) | (df['attempt_count'] > 1)
        self.logger.info("Transformation: Derived is_reprocessed_flag.")

        # 9. fill_nulls
        fill_map = {
            'attempt_count': 0, 'email': 'unknown@unknown.com', 'email_domain': 'unknown',
            'email_provider': 'Unknown', 'font_name': 'N/A', 'processing_lag_days': -1.0,
            'product_type': 'N/A', 'source_name': 'N/A', 'variant_title': 'N/A'
        }
        df.fillna(value=fill_map, inplace=True)
        self.logger.info("Transformation: Filled nulls.")

        # 10. deduplicate
        df = df.sort_values('created_at').drop_duplicates(subset=['order_id', 'email'], keep='last')
        self.logger.info("Transformation: Deduplicated.")

        # 11. filter_rows
        df = df[df['status'] != 'failed']
        self.logger.info(f"Transformation: Filtered rows. New count: {len(df)}")

        # 12. drop_columns
        df.drop(columns=['skuid', 'eula_id', 'is_reprocessed', 'md5'], inplace=True)
        self.logger.info("Transformation: Dropped columns.")

        # 13. rename_columns
        rename_map = {
            'attempt_count': 'total_attempts', 'created_at': 'order_created_at',
            'font_identifier': 'font_id', 'id': 'order_record_id',
            'last_attempt_on': 'last_attempt_date', 'order_id': 'shopify_order_id',
            'order_name': 'shopify_order_name', 'process_at': 'order_processed_at'
        }
        df.rename(columns=rename_map, inplace=True)
        # Ensure processed_date exists
        if 'processed_date' not in df.columns:
            df['processed_date'] = df['order_processed_at']
        self.logger.info("Transformation: Renamed columns.")

        # 14. sort_rows
        df.sort_values(by=['order_created_at', 'shopify_order_id'], ascending=[False, True], inplace=True)
        self.logger.info("Transformation: Sorted rows.")

        # 15. select_columns
        cols = ['order_record_id', 'order_created_at', 'order_processed_at', 'shopify_order_id',
                'shopify_order_name', 'email', 'email_domain', 'email_provider', 'variant_title',
                'font_name', 'font_id', 'source_name', 'product_type', 'status', 'total_attempts',
                'attempt_bucket', 'last_attempt_date', 'is_reprocessed_flag', 'processing_lag_days', 'processed_date']
        df = df[cols]
        self.logger.info(f"Transformation: Selected columns. Final row count: {len(df)}")
        return df

    def validate(self, df: pd.DataFrame):
        self.logger.info("Starting validation.")
        if len(df) == 0:
            raise ValueError("Validation failed: Empty dataframe.")
        
        for col in ['order_record_id', 'order_processed_at', 'email']:
            if df[col].isnull().any():
                raise ValueError(f"Validation failed: Nulls in {col}")
        
        if df['order_record_id'].duplicated().any():
            raise ValueError("Validation failed: Duplicates in order_record_id")
            
        if not df['total_attempts'].between(0, 100).all():
            raise ValueError("Validation failed: total_attempts out of range")
            
        allowed = ['Gmail', 'Microsoft', 'Yahoo', 'Corporate', 'Unknown']
        if not df['email_provider'].isin(allowed).all():
            raise ValueError("Validation failed: Invalid email_provider")
            
        self.logger.info(f"Validation passed: {len(df)} rows.")

    def load(self, df: pd.DataFrame):
        self.logger.info(f"Loading {len(df)} rows into order_analytics.")
        try:
            df.to_sql('order_analytics', self.target_engine, if_exists='replace', index=False, chunksize=2000)
            self.logger.info("Load successful.")
        except Exception as e:
            self.logger.error(f"Load failed: {e}")
            raise

    def run(self):
        start = datetime.now()
        self.logger.info(f"Pipeline started at {start}")
        try:
            df = self.extract()
            df = self.transform(df)
            self.validate(df)
            self.load(df)
            end = datetime.now()
            self.logger.info(f"Pipeline finished successfully. Duration: {end - start}")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
