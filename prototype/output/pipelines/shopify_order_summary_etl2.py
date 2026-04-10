import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

class ShopifyOrderSummaryEtl2:
    def __init__(self, source_url, target_url):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)
        self.row_limit = 500

    def extract(self):
        self.logger.info("Starting extraction from myfonts_shopify_data")
        query = f"SELECT * FROM myfonts_shopify_data LIMIT {self.row_limit}"
        df = pd.read_sql(query, self.source_engine)
        self.logger.info(f"Extracted {len(df)} rows from source.")
        return df

    def transform(self, df):
        self.logger.info("Starting transformation steps.")
        
        # 1. clean_strings
        df['email'] = df['email'].str.strip().str.lower()
        self.logger.info(f"Cleaned email column. Rows: {len(df)}")

        # 2. derive_column: email_domain
        df['email_domain'] = df['email'].apply(
            lambda x: x.split('@')[1] if isinstance(x, str) and '@' in x else 'unknown'
        )
        self.logger.info("Derived email_domain.")

        # 3. derive_column: order_year
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['order_year'] = df['created_at'].apply(lambda x: x.year if pd.notna(x) else None)
        self.logger.info("Derived order_year.")

        # 4. derive_column: order_month
        df['order_month'] = df['created_at'].apply(lambda x: x.month if pd.notna(x) else None)
        self.logger.info("Derived order_month.")

        # 5. conditional_column: status_group
        conditions = [
            (df['status'] == 'success'),
            (df['status'].isin(['load', 'pending', 'processing']))
        ]
        choices = ['completed', 'in_progress']
        df['status_group'] = np.select(conditions, choices, default='other')
        self.logger.info("Derived status_group.")

        # 6. drop_columns
        df = df.drop(columns=['skuid', 'eula_id', 'md5', 'font_identifier'], errors='ignore')
        self.logger.info("Dropped unnecessary columns.")

        # 7. select_columns
        cols = ['id', 'created_at', 'order_year', 'order_month', 'order_id', 'order_name', 
                'email', 'email_domain', 'font_name', 'product_type', 'status', 
                'status_group', 'attempt_count', 'is_reprocessed']
        for col in cols:
            if col not in df.columns:
                df[col] = None
        df = df[cols]
        self.logger.info(f"Selected final columns. Final row count: {len(df)}")
        return df

    def validate(self, df):
        self.logger.info("Running quality checks.")
        if len(df) <= 0:
            raise ValueError("row_count_gt check failed: 0 rows")
        
        for field in ['id', 'email', 'status']:
            if df[field].isnull().any():
                raise ValueError(f"required_fields_not_null check failed for {field}")
        
        if df['id'].isnull().any():
            raise ValueError("column_not_null check failed for id")
            
        allowed = ['completed', 'in_progress', 'other']
        if not df['status_group'].isin(allowed).all():
            raise ValueError("allowed_values check failed for status_group")
        
        self.logger.info("All quality checks passed.")

    def load(self, df):
        self.logger.info("Loading data into order_summary.")
        try:
            df.to_sql('order_summary', self.target_engine, if_exists='replace', index=False, chunksize=500)
            self.logger.info(f"Successfully loaded {len(df)} rows into order_summary.")
        except Exception as e:
            self.logger.error(f"Load failed: {e}")
            raise

    def run(self):
        import time
        start = time.time()
        self.logger.info("Pipeline run started.")
        try:
            df = self.extract()
            df = self.transform(df)
            self.validate(df)
            self.load(df)
            duration = time.time() - start
            self.logger.info(f"Pipeline run completed successfully in {duration:.2f}s.")
        except Exception as e:
            self.logger.error(f"Pipeline run failed: {e}")
            raise
