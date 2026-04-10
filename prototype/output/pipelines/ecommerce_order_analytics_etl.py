import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

class EcommerceOrderAnalyticsEtl:
    def __init__(self, source_url, target_url):
        self.logger = logging.getLogger(__name__)
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)
        self.row_limit = 5000

    def extract(self):
        self.logger.info("Starting extraction from myfonts_shopify_data")
        query = f"SELECT * FROM myfonts_shopify_data LIMIT {self.row_limit}"
        df = pd.read_sql(query, self.source_engine)
        self.logger.info(f"Extracted {len(df)} rows from source.")
        return df

    def transform(self, df):
        self.logger.info("Starting transformation steps.")
        
        # 1. Cast types
        df = df.astype({
            'attempt_count': 'int',
            'email': 'str',
            'id': 'int',
            'order_id': 'str',
            'order_name': 'str'
        })
        self.logger.info(f"Cast types. Rows: {len(df)}")

        # 2. Clean strings
        cols = ['email', 'font_name', 'variant_title', 'source_name', 'product_type']
        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.lower()
        self.logger.info(f"Cleaned strings. Rows: {len(df)}")

        # 3. Parse dates
        date_cols = {
            'created_at': '%Y-%m-%d %H:%M:%S',
            'last_attempt_on': '%Y-%m-%d %H:%M:%S',
            'process_at': '%Y-%m-%d %H:%M:%S'
        }
        for col, fmt in date_cols.items():
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=fmt, errors='coerce')
        self.logger.info(f"Parsed dates. Rows: {len(df)}")

        # 4. Derive email_domain
        df['email_domain'] = df['email'].apply(lambda x: x.split('@')[1] if isinstance(x, str) and '@' in x else None)
        self.logger.info(f"Derived email_domain. Rows: {len(df)}")

        # 5. Conditional column email_provider
        def get_provider(row):
            domain = row['email_domain']
            if domain == 'gmail.com':
                return 'Gmail'
            if domain in ['outlook.com', 'hotmail.com', 'live.com']:
                return 'Microsoft'
            if domain and 'yahoo' in domain:
                return 'Yahoo'
            return 'Corporate'
        df['email_provider'] = df.apply(get_provider, axis=1)
        self.logger.info(f"Derived email_provider. Rows: {len(df)}")

        # 6. Bucket attempt_count
        bins = [-1, 0, 1, 3, np.inf]
        labels = ['no_attempts', 'single', 'moderate', 'high']
        df['attempt_bucket'] = pd.cut(df['attempt_count'], bins=bins, labels=labels)
        self.logger.info(f"Bucketed attempt_count. Rows: {len(df)}")

        # 7. Derive processing_lag_days
        df['processing_lag_days'] = df.apply(lambda r: (r['process_at'] - r['created_at']).total_seconds() / 86400 if pd.notna(r['created_at']) and pd.notna(r['process_at']) else None, axis=1)
        self.logger.info(f"Derived processing_lag_days. Rows: {len(df)}")

        # 8. Derive is_reprocessed_flag
        df['is_reprocessed_flag'] = df.apply(lambda r: True if (r.get('is_reprocessed') == 1 or r['attempt_count'] > 1) else False, axis=1)
        self.logger.info(f"Derived is_reprocessed_flag. Rows: {len(df)}")

        # 9. Fill nulls
        defaults = {
            'attempt_count': 0, 'email': 'unknown@unknown.com', 'email_domain': 'unknown',
            'email_provider': 'Unknown', 'font_name': 'N/A', 'processing_lag_days': -1.0,
            'product_type': 'N/A', 'source_name': 'N/A', 'variant_title': 'N/A'
        }
        df = df.fillna(defaults)
        self.logger.info(f"Filled nulls. Rows: {len(df)}")

        # 10. Deduplicate
        df = df.sort_values('created_at').drop_duplicates(subset=['order_id', 'email'], keep='last')
        self.logger.info(f"Deduplicated. Rows: {len(df)}")

        # 11. Filter rows
        df = df.query("status != 'failed'")
        self.logger.info(f"Filtered rows. Rows: {len(df)}")

        # 12. Drop columns
        df = df.drop(columns=['skuid', 'eula_id', 'is_reprocessed', 'md5'], errors='ignore')
        self.logger.info(f"Dropped columns. Rows: {len(df)}")

        # 13. Rename columns
        rename_map = {
            'attempt_count': 'total_attempts', 'created_at': 'order_created_at',
            'font_identifier': 'font_id', 'id': 'order_record_id',
            'last_attempt_on': 'last_attempt_date', 'order_id': 'shopify_order_id',
            'order_name': 'shopify_order_name', 'process_at': 'order_processed_at',
            'process_at_date': 'processed_date'
        }
        df = df.rename(columns=rename_map)
        self.logger.info(f"Renamed columns. Rows: {len(df)}")

        # 14. Sort rows
        df = df.sort_values(['order_created_at', 'shopify_order_id'], ascending=[False, True])
        self.logger.info(f"Sorted rows. Rows: {len(df)}")

        # 15. Select columns
        cols = ['order_record_id', 'order_created_at', 'order_processed_at', 'shopify_order_id', 'shopify_order_name', 'email', 'email_domain', 'email_provider', 'variant_title', 'font_name', 'font_id', 'source_name', 'product_type', 'status', 'total_attempts', 'attempt_bucket', 'last_attempt_date', 'is_reprocessed_flag', 'processing_lag_days', 'processed_date']
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
        self.logger.info(f"Selected columns. Rows: {len(df)}")
        
        return df

    def validate(self, df):
        self.logger.info("Starting quality checks.")
        if len(df) <= 0:
            raise ValueError("row_count_gt check failed")
        for col in ['order_record_id', 'order_processed_at', 'email']:
            if df[col].isnull().any():
                raise ValueError(f"Required field {col} has nulls")
        if df['order_record_id'].duplicated().any():
            raise ValueError("Unique check failed on order_record_id")
        if not df['total_attempts'].between(0, 100).all():
            raise ValueError("Value range check failed on total_attempts")
        allowed = ['Gmail', 'Microsoft', 'Yahoo', 'Corporate', 'Unknown']
        if not df['email_provider'].isin(allowed).all():
            raise ValueError("Allowed values check failed on email_provider")
        self.logger.info("Quality checks passed.")

    def load(self, df):
        self.logger.info("Starting load to order_analytics.")
        df.to_sql('order_analytics', self.target_engine, if_exists='replace', index=False, chunksize=2000)
        self.logger.info(f"Loaded {len(df)} rows to order_analytics.")

    def run(self):
        import time
        start = time.time()
        try:
            df = self.extract()
            df = self.transform(df)
            self.validate(df)
            self.load(df)
            self.logger.info(f"Pipeline finished successfully in {time.time() - start:.2f}s")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
