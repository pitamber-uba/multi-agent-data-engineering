import logging
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

class ShopifyOrderSummaryEtl:
    def __init__(self, source_url, target_url):
        self.logger = logging.getLogger(__name__)
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)
        self.row_limit = 500
        self.table = "myfonts_shopify_data"
        self.target_table = "order_summary"

    def extract(self):
        self.logger.info(f"Extracting from {self.table} with limit {self.row_limit}")
        query = f"SELECT * FROM {self.table} LIMIT {self.row_limit}"
        df = pd.read_sql(query, self.source_engine)
        self.logger.info(f"Extracted {len(df)} rows")
        return df

    def transform(self, df):
        self.logger.info("Starting transformations")
        
        # 1. clean_strings
        df['email'] = df['email'].astype(str).str.strip().str.lower()
        self.logger.info(f"Cleaned email, rows: {len(df)}")

        # 2. derive_column: email_domain
        df['email_domain'] = df['email'].apply(lambda x: x.split('@')[1] if isinstance(x, str) and '@' in x else 'unknown')
        self.logger.info(f"Derived email_domain, rows: {len(df)}")

        # 3. derive_column: order_year
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['order_year'] = df['created_at'].apply(lambda x: x.year if pd.notna(x) else None)
        self.logger.info(f"Derived order_year, rows: {len(df)}")

        # 4. derive_column: order_month
        df['order_month'] = df['created_at'].apply(lambda x: x.month if pd.notna(x) else None)
        self.logger.info(f"Derived order_month, rows: {len(df)}")

        # 5. conditional_column: status_group
        conditions = [
            (df['status'] == 'success'),
            (df['status'].isin(['load', 'pending', 'processing']))
        ]
        choices = ['completed', 'in_progress']
        df['status_group'] = np.select(conditions, choices, default='other')
        self.logger.info(f"Derived status_group, rows: {len(df)}")

        # 6. drop_columns
        df = df.drop(columns=['skuid', 'eula_id', 'md5', 'font_identifier'], errors='ignore')
        self.logger.info(f"Dropped columns, rows: {len(df)}")

        # 7. select_columns
        cols = ['id', 'created_at', 'order_year', 'order_month', 'order_id', 'order_name', 'email', 'email_domain', 'font_name', 'product_type', 'status', 'status_group', 'attempt_count', 'is_reprocessed']
        for col in cols:
            if col not in df.columns:
                df[col] = None
        df = df[cols]
        self.logger.info(f"Selected columns, rows: {len(df)}")
        
        return df

    def validate(self, df):
        self.logger.info("Starting quality checks")
        if len(df) <= 0:
            raise ValueError("row_count_gt: 0 failed")
        self.logger.info("row_count_gt: 0 passed")

        for field in ['id', 'email', 'status']:
            if df[field].isnull().any():
                raise ValueError(f"required_fields_not_null: {field} failed")
        self.logger.info("required_fields_not_null passed")

        if df['id'].isnull().any():
            raise ValueError("column_not_null: id failed")
        self.logger.info("column_not_null: id passed")

        allowed = ['completed', 'in_progress', 'other']
        if not df['status_group'].isin(allowed).all():
            raise ValueError("allowed_values: status_group failed")
        self.logger.info("allowed_values: status_group passed")

    def load(self, df):
        self.logger.info(f"Loading to {self.target_table}")
        try:
            df.to_sql(self.target_table, self.target_engine, if_exists='replace', index=False, chunksize=500)
            self.logger.info(f"Loaded {len(df)} rows to {self.target_table}")
        except Exception as e:
            self.logger.error(f"Load failed: {e}")
            raise

    def run(self):
        import time
        start = time.time()
        self.logger.info("Pipeline started")
        df = self.extract()
        df = self.transform(df)
        self.validate(df)
        self.load(df)
        end = time.time()
        self.logger.info(f"Pipeline finished in {end - start:.2f}s")
