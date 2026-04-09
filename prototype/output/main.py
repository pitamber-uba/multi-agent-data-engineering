import os
import logging
from pipelines.monotype_customer_to_personal_details import MonotypeCustomerToPersonalDetails

logging.basicConfig(level=logging.INFO)

def main():
    source_user = "root"
    source_pass = os.environ.get("SOURCE_DB_PASSWORD")
    source_host = "localhost"
    source_port = 3306
    source_db = "Monotype"
    
    target_user = "root"
    target_pass = os.environ.get("TARGET_DB_PASSWORD")
    target_host = "localhost"
    target_port = 3306
    target_db = "Monotype"

    if not source_pass or not target_pass:
        raise ValueError("SOURCE_DB_PASSWORD and TARGET_DB_PASSWORD must be set")

    source_url = f"mysql+mysqldb://{source_user}:{source_pass}@{source_host}:{source_port}/{source_db}"
    target_url = f"mysql+mysqldb://{target_user}:{target_pass}@{target_host}:{target_port}/{target_db}"

    pipeline = MonotypeCustomerToPersonalDetails(source_url, target_url)
    pipeline.run()

if __name__ == "__main__":
    main()
