import logging
import pandas as pd
from sqlalchemy import create_engine

class MonotypeCustomerToPersonalDetails:
    def __init__(self, source_url, target_url):
        self.logger = logging.getLogger(__name__)
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)
        self.df = None

    def extract(self):
        self.logger.info("Starting extraction from customer table")
        query = "SELECT * FROM customer LIMIT 3000"
        self.df = pd.read_sql(query, self.source_engine)
        self.logger.info(f"Extracted {len(self.df)} rows from customer")
        return self.df

    def transform(self):
        self.logger.info("Starting transformation")
        
        # Step 1: derive_domain
        self.logger.info(f"Applying derive_domain. Rows before: {len(self.df)}")
        self.df['domain'] = self.df['email'].apply(
            lambda x: x.split('@')[1] if isinstance(x, str) and '@' in x else None
        )
        self.logger.info(f"Applied derive_domain. Rows after: {len(self.df)}")

        # Step 2: select_columns
        self.logger.info(f"Applying select_columns. Rows before: {len(self.df)}")
        cols = ['name', 'email', 'address', 'orgName', 'domain']
        for col in cols:
            if col not in self.df.columns:
                self.df[col] = None
        self.df = self.df[cols]
        self.logger.info(f"Applied select_columns. Rows after: {len(self.df)}")
        
        return self.df

    def validate(self):
        self.logger.info("Starting quality checks")
        
        # row_count_gt: 0
        if len(self.df) <= 0:
            raise ValueError("Quality check failed: row_count_gt 0")
        self.logger.info("Passed: row_count_gt 0")

        # required_fields_not_null
        for field in ['name', 'email']:
            if self.df[field].isnull().any():
                raise ValueError(f"Quality check failed: {field} contains nulls")
        self.logger.info("Passed: required_fields_not_null")

        return True

    def load(self):
        self.logger.info("Starting load to personalDetails")
        try:
            self.df.to_sql(
                'personalDetails',
                con=self.target_engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            self.logger.info(f"Successfully loaded {len(self.df)} rows to personalDetails")
        except Exception as e:
            self.logger.error(f"Failed to load data: {e}")
            raise

    def run(self):
        import time
        start = time.time()
        self.logger.info("Pipeline run started")
        
        self.extract()
        self.transform()
        self.validate()
        self.load()
        
        end = time.time()
        self.logger.info(f"Pipeline run finished. Duration: {end - start:.2f}s")
