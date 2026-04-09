import os
from pipelines.myfonts_shopify_to_demo_etl import MyFontsShopifyToDemoETL

def main():
    # Ensure environment variables are set
    if "SOURCE_DB_PASSWORD" not in os.environ or "TARGET_DB_PASSWORD" not in os.environ:
        raise EnvironmentError("SOURCE_DB_PASSWORD and TARGET_DB_PASSWORD must be set.")
    
    source_password = os.environ["SOURCE_DB_PASSWORD"]
    target_password = os.environ["TARGET_DB_PASSWORD"]
    
    source_url = f"mysql+pymysql://root:{source_password}@localhost:3306/MyFonts_Legacy"
    target_url = f"mysql+pymysql://root:{target_password}@localhost:3306/MyEtlDemo"
    
    etl = MyFontsShopifyToDemoETL(source_url, target_url)
    etl.run()

if __name__ == "__main__":
    main()
