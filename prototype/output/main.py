import os
from pipelines.shopify_order_summary_etl2 import ShopifyOrderSummaryEtl2

def main():
    source_pass = os.environ.get("SOURCE_DB_PASSWORD")
    target_pass = os.environ.get("TARGET_DB_PASSWORD")
    
    if not source_pass or not target_pass:
        raise ValueError("Environment variables SOURCE_DB_PASSWORD and TARGET_DB_PASSWORD must be set.")

    source_url = f"mysql+pymysql://root:{source_pass}@localhost:3306/MyFonts_Legacy"
    target_url = f"mysql+pymysql://root:{target_pass}@localhost:3306/MyEtlDemo"

    pipeline = ShopifyOrderSummaryEtl2(source_url, target_url)
    pipeline.run()

if __name__ == "__main__":
    main()
