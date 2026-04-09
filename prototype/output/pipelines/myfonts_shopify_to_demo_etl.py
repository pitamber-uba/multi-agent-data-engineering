import pandas as pd
import logging
from sqlalchemy import create_engine

class MyFontsShopifyToDemoETL:
    def __init__(self, source_url, target_url):
        self.logger = logging.getLogger(__name__)
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)

    def extract(self):
        self.logger.info("Extracting data from MyFonts_Legacy.myfonts_shopify_data")
        query = "SELECT * FROM myfonts_shopify_data LIMIT 3000"
        df = pd.read_sql(query, self.source_engine)
        self.logger.info(f"Extracted {len(df)} rows from MyFonts_Legacy.myfonts_shopify_data")
        return df

    def transform(self, df):
        self.logger.info("Starting transformation")
        
        # Drop columns
        df = df.drop(columns=['skuid', 'eula_id'], errors='ignore')
        self.logger.info("Dropped columns: skuid, eula_id")
        
        # Derive domain
        # expression: email.split('@')[1] if '@' in email else None
        def get_domain(email):
            if pd.isna(email) or '@' not in str(email):
                return None
            return str(email).split('@')[1]
        
        df['domain'] = df['email'].apply(get_domain)
        self.logger.info("Derived 'domain' column")
        
        # Select columns
        columns = [
            'id', 'created_at', 'process_at', 'order_id', 'order_name', 'email', 
            'domain', 'variant_title', 'font_name', 'md5', 'source_name', 'status', 
            'attempt_count', 'last_attempt_on', 'is_reprocessed', 'product_type', 
            'font_identifier', 'process_at_date'
        ]
        # Ensure all columns exist, fill missing with None
        for col in columns:
            if col not in df.columns:
                df[col] = None
        
        df = df[columns]
        self.logger.info(f"Transformation complete. Row count: {len(df)}")
        return df

    def validate(self, df):
        self.logger.info("Starting quality checks")
        if len(df) <= 0:
            self.logger.error("Validation failed: Row count is 0")
            raise ValueError("Row count is 0")
        self.logger.info("Validation passed: Row count > 0")
        
        # Check required fields
        required_fields = ['id', 'process_at']
        for field in required_fields:
            if field not in df.columns:
                self.logger.error(f"Validation failed: {field} column missing")
                raise ValueError(f"{field} column missing")
            if df[field].isnull().any():
                self.logger.error(f"Validation failed: {field} column contains nulls")
                raise ValueError(f"{field} column contains nulls")
            self.logger.info(f"Validation passed: {field} column not null")
        
        return True

    def load(self, df):
        self.logger.info("Loading data into MyEtlDemo.testTable")
        df.to_sql('testTable', self.target_engine, if_exists='append', index=False, chunksize=1000)
        self.logger.info(f"Loaded {len(df)} rows into MyEtlDemo.testTable")

    def run(self):
        import time
        start_time = time.time()
        self.logger.info("Pipeline run started")
        try:
            df = self.extract()
            df = self.transform(df)
            self.validate(df)
            self.load(df)
            self.logger.info("Pipeline run completed successfully")
        except Exception as e:
            self.logger.error(f"Pipeline run failed: {e}")
            raise
        finally:
            end_time = time.time()
            self.logger.info(f"Pipeline run finished in {end_time - start_time:.2f} seconds")
