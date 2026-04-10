import os
import logging
from pipelines.shopify_order_summary_etl import ShopifyOrderSummaryEtl

logging.basicConfig(level=logging.INFO)

def main():
    source_user = "root"
    source_pass = os.environ.get("SOURCE_DB_PASSWORD")
    source_host = "localhost"
    source_port = 3306
    source_db = "MyFonts_Legacy"
    
    target_user = "root"
    target_pass = os.environ.get("TARGET_DB_PASSWORD")
    target_host = "localhost"
    target_port = 3306
    target_db = "MyEtlDemo"

    source_url = f"mysql+pymysql://{source_user}:{source_pass}@{source_host}:{source_port}/{source_db}"
    target_url = f"mysql+pymysql://{target_user}:{target_pass}@{target_host}:{target_port}/{target_db}"

    pipeline = ShopifyOrderSummaryEtl(source_url, target_url)
    pipeline.run()

if __name__ == "__main__":
    main()
