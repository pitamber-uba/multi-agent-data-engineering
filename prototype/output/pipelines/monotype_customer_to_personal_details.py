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
        self.logger.info("Extracting data from source table: customer")
        query = "SELECT * FROM customer"
        self.df = pd.read_sql(query, self.source_engine)
        self.logger.info(f"Extraction complete. Rows extracted: {len(self.df)}")

    def transform(self):
        self.logger.info("Starting transformation steps.")
        
        # Step 1: derive_domain
        self.logger.info("Applying transformation: derive_domain")
        self.df['domain'] = self.df['email'].apply(
            lambda x: x.split('@')[1] if isinstance(x, str) and '@' in x else None
        )
        self.logger.info(f"Transformation 'derive_domain' complete. Rows: {len(self.df)}")

        # Step 2: select_columns
        self.logger.info("Applying transformation: select_columns")
        columns = ['name', 'email', 'address', 'orgName', 'domain']
        for col in columns:
            if col not in self.df.columns:
                self.df[col] = None
        self.df = self.df[columns]
        self.logger.info(f"Transformation 'select_columns' complete. Rows: {len(self.df)}")

    def validate(self):
        self.logger.info("Starting quality checks.")
        
        # row_count_gt: 0
        if len(self.df) <= 0:
            self.logger.error("Quality check failed: row_count_gt")
            raise ValueError("Row count must be greater than 0")
        self.logger.info("Quality check passed: row_count_gt")

        # required_fields_not_null / column_not_null
        fields = ['name', 'email']
        for field in fields:
            if self.df[field].isnull().any():
                self.logger.error(f"Quality check failed: {field} contains nulls")
                raise ValueError(f"Field {field} cannot contain nulls")
            self.logger.info(f"Quality check passed: {field} not null")

    def load(self):
        self.logger.info("Starting load process.")
        try:
            self.df.to_sql(
                'personalDetails',
                self.target_engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            self.logger.info(f"Load complete. Rows loaded: {len(self.df)}")
        except Exception as e:
            self.logger.error(f"Load failed: {e}")
            raise

    def run(self):
        import time
        start_time = time.time()
        self.logger.info("Pipeline run started.")
        try:
            self.extract()
            self.transform()
            self.validate()
            self.load()
            self.logger.info(f"Pipeline run finished successfully. Duration: {time.time() - start_time:.2f}s")
        except Exception as e:
            self.logger.error(f"Pipeline run failed: {e}")
            raise
