import os
import logging
from pipelines.ecommerce_order_analytics_etl import EcommerceOrderAnalyticsEtl

logging.basicConfig(level=logging.INFO)

def main():
    source_pass = os.environ.get("SOURCE_DB_PASSWORD")
    target_pass = os.environ.get("TARGET_DB_PASSWORD")
    
    if not source_pass or not target_pass:
        raise ValueError("Missing DB password environment variables")

    source_url = f"mysql+pymysql://root:{source_pass}@localhost:3306/MyFonts_Legacy"
    target_url = f"mysql+pymysql://root:{target_pass}@localhost:3306/MyEtlDemo"

    pipeline = EcommerceOrderAnalyticsEtl(source_url, target_url)
    pipeline.run()

if __name__ == "__main__":
    main()
