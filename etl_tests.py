import logging
import os
import sqlite3
import logging

# Configure logging
logging.basicConfig(
    filename='/var/log/etl_tests.log',  # Log file location
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


directory = "patients_fhir_100"



def test_extraction(file_path):
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
    try:
        with open(file_path, 'r') as f:
            if not f.read():
                raise ValueError("File is empty")
        logging.info("Extraction test passed")
    except Exception as e:
        logging.error(f"Extraction test failed: {e}")




database_path = "fhir_data.db"

def test_loading(database_path):
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM Patients")  # Replace `target_table` with your table
        if cursor.fetchone()[0] == 0:
            raise ValueError("No patients loaded into the database")
        logging.info("Patients Loading test passed")
    except Exception as e:
        logging.error(f"Patients Loading test failed: {e}")
        
        cursor.execute("SELECT COUNT(*) FROM MedicationRequests")  # Replace `target_table` with your table
        if cursor.fetchone()[0] == 0:
            raise ValueError("No Medication Requests loaded into the database")
        logging.info("Medication Requests Loading test passed")
    except Exception as e:
        logging.error(f"Medication Requests Loading test failed: {e}")
        


def main():
    """
    Main function to run the tests.
    """
    logging.info("Starting ETL tests...")
    
    # Test extraction
    test_extraction(directory)
    
    # Test loading
    test_loading(database_path)
    
    logging.info("ETL tests completed.")

if __name__ == "__main__":
    main()     
    
