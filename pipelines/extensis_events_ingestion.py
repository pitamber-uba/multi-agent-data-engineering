import pandas as pd
import logging

class ExtensisEventsIngestionPipeline:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def extract(self):
        # Placeholder for S3 extraction logic
        self.logger.info("Extracting data from S3...")
        return pd.DataFrame({'raw_data': ['{"eventName": "permanentActivation", "userEmail": "test@example.com", "mtfid": "123", "timestamp": "2023-01-01"}']})

    def transform(self, df):
        self.logger.info("Transforming data...")
        # Simulate parsing
        df['eventName'] = 'permanentActivation'
        df['user'] = 'test@example.com'
        df['mtf_id'] = '123'
        df['timestamp'] = '2023-01-01'
        return df

    def validate(self, df):
        self.logger.info("Validating data...")
        if df.empty:
            raise ValueError("DataFrame is empty")
        # Updated required fields based on spec: mtf_id, user, timestamp, eventName
        required_fields = ['mtf_id', 'user', 'timestamp', 'eventName']
        for field in required_fields:
            if field not in df.columns:
                raise ValueError(f"Missing field: {field}")
        
        # New quality check: event_type_in
        allowed_events = [
            'permanentActivation',
            'temporaryActivation',
            'riskScan',
            'autoActivation',
            'addedToLibrary'
        ]
        # Filter out invalid records
        invalid_mask = ~df['eventName'].isin(allowed_events)
        if invalid_mask.any():
            self.logger.warning(f"Skipping {invalid_mask.sum()} records with invalid event types")
            df = df[~invalid_mask].copy()
            
        return df

    def load(self, df):
        self.logger.info("Loading data to target...")
        # Placeholder for DB load logic
        return True

    def run(self):
        try:
            raw_data = self.extract()
            transformed_data = self.transform(raw_data)
            validated_data = self.validate(transformed_data)
            self.load(validated_data)
            self.logger.info("Pipeline completed successfully.")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
