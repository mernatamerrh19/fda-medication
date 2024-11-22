import requests
import sqlite3
import time
import regex as re
import logging

logging.basicConfig(
    filename='/var/log/FDA.log',  # Log file location
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Connect to the SQLite database
conn = sqlite3.connect("fhir_data.db")
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='FDA_Drug_Info'")
if cursor.fetchone():
    # Delete all rows from the FDA_Drug_Info table, if it's already created
    cursor.execute("DELETE FROM FDA_Drug_Info")
    conn.commit()
    # Optionally, reset the auto-increment counter (if any) for the table
    cursor.execute("DELETE FROM sqlite_sequence WHERE name='FDA_Drug_Info'")
    conn.commit()
else:
    print("Table does not exist")

# Create a table for storing FDA Drug information
cursor.execute("""
CREATE TABLE IF NOT EXISTS FDA_Drug_Info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    medication TEXT UNIQUE,
    brand_name TEXT,
    generic_name TEXT,
    manufacturer TEXT,
    active_ingredients TEXT,
    route TEXT,
    warnings TEXT,
    indications_usage TEXT
)
""")


# Function to query the FDA Drug Label API
def query_fda_api(medication):
    api_url = f"https://api.fda.gov/drug/label.json?search=openfda.generic_name:\"{medication}\""
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "results" in data:
                drug_data = data["results"][0]  # Use the first result
                return {
                    "brand_name": ", ".join(drug_data.get("openfda", {}).get("brand_name", [])),
                    "generic_name": ", ".join(drug_data.get("openfda", {}).get("generic_name", [])),
                    "manufacturer": ", ".join(drug_data.get("openfda", {}).get("manufacturer_name", [])),
                    "active_ingredients": ", ".join(drug_data.get("openfda", {}).get("substance_name", [])),
                    "dosage_form": ", ".join(drug_data.get("openfda", {}).get("dosage_form", [])),
                    "route": ", ".join(drug_data.get("openfda", {}).get("route", [])),
                    "warnings": drug_data.get("warnings", ["Unknown"])[0],
                    "indications_usage": drug_data.get("indications_and_usage", ["Unknown"])[0],
                }
        return None
    except Exception as e:
        print(f"Error querying FDA API for {medication}: {e}")
        return None

# Fetch distinct medication names from MedicationRequests table
cursor.execute("SELECT DISTINCT medication FROM MedicationRequests WHERE medication != 'Unknown' ")
medications = cursor.fetchall()
print(len(medications))

regex= r"(\d+ HR )?([a-zA-Z ,]+)(?=\d| [\dA-Za-z]+\/)"

logging.info("Starting data loading...")

# Process each medication
for med in medications:
    medication_names = med[0].split(" / ")

    
    for medication_name in medication_names:
        # medication_name = medication_name.replace(" ", "+")  # Remove any extra whitespace
        match= re.search(regex, medication_name)
        if match: 
            medication_name=match.group(2).strip()
            medication_name = medication_name.replace(",", "+")
            medication_name = medication_name.replace(" ", "+")
            medication_name = medication_name.replace("++", "+")
            print(f"Processing medication: {medication_name}")
            # Query the FDA API
            drug_info = query_fda_api(medication_name)
        else:
            print(f"Regex did not match for: {medication_name}")
            continue  # Skip this iteration if no match is found
    # medication_name = med[0].split(" ")  # Extract the medication name
    # print(f"Processing medication: {medication_name}")

    
    # Delete all rows from the FDA_Drug_Info table

        if drug_info:
            try:
                # Insert the drug information into the FDA_Drug_Info table
                cursor.execute("""
                INSERT INTO FDA_Drug_Info (medication, brand_name, generic_name, manufacturer, active_ingredients,  route, warnings, indications_usage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    medication_name,
                    drug_info["brand_name"],
                    drug_info["generic_name"],
                    drug_info["manufacturer"],
                    drug_info["active_ingredients"],
                    drug_info["route"],
                    drug_info["warnings"],
                    drug_info["indications_usage"]
                ))
                conn.commit()
            except sqlite3.Error as db_err:
                print(f"Database error for {medication_name}: {db_err}")

    # Pause to respect API rate limits
    time.sleep(1)

# Commit changes and close the database connection
conn.execute("SELECT * FROM FDA_Drug_Info")
conn.close()
logging.info("Loading completed successfully!")