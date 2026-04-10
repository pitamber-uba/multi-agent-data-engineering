import os
import logging
from pipelines.monotype_customer_to_personal_details import MonotypeCustomerToPersonalDetails

logging.basicConfig(level=logging.INFO)

def main():
    source_pass = os.environ.get("SOURCE_DB_PASSWORD")
    target_pass = os.environ.get("TARGET_DB_PASSWORD")
    
    if not source_pass or not target_pass:
        raise ValueError("Missing DB password environment variables")

    source_url = f"mysql+mysqldb://root:{source_pass}@localhost:3306/Monotype"
    target_url = f"mysql+mysqldb://root:{target_pass}@localhost:3306/Monotype"

    pipeline = MonotypeCustomerToPersonalDetails(source_url, target_url)
    pipeline.run()

if __name__ == "__main__":
    main()
