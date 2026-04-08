import pandas as pd
from sqlalchemy import create_engine

class MyFontsShopifyToDemoETL:
    def __init__(self, source_url, target_url):
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)

    def extract(self):
        query = "SELECT * FROM myfonts_shopify_data LIMIT 100"
        return pd.read_sql(query, self.source_engine)

    def transform(self, df):
        # Drop columns
        df = df.drop(columns=['skuid', 'eula_id'], errors='ignore')
        
        # Derive domain
        def get_domain(email):
            if pd.isna(email) or '@' not in str(email):
                return None
            return str(email).split('@')[1]
        
        df['domain'] = df['email'].apply(get_domain)
        
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
        return df[columns]

    def validate(self, df):
        if len(df) <= 0:
            raise ValueError("Row count is 0")
        if df['id'].isnull().any():
            raise ValueError("id column contains nulls")
        if df['process_at'].isnull().any():
            raise ValueError("process_at column contains nulls")
        return True

    def load(self, df):
        df.to_sql('testTable', self.target_engine, if_exists='append', index=False, chunksize=1000)

    def run(self):
        df = self.extract()
        df = self.transform(df)
        self.validate(df)
        self.load(df)
