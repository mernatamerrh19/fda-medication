import os
import json
import sqlite3
from datetime import datetime
import logging

logging.basicConfig(
    filename='/var/log/extract-json.log',  # Log file location
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# Function to clean missing or null fields
def clean_field(value, default="Unknown"):
    return value if value is not None and value != "" else default

# Function to standardize date formats
def standardize_date(date_str, default="0000-00-00"):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return default

# Extract the first coding's code from the category array
def extract_category_code(category_field):
    try:
        # Take the first category object, then the first coding object within it
        return category_field[0].get("coding", [{}])[0].get("code", "Unknown")
    except (IndexError, AttributeError, TypeError):
        return "Unknown"

def extract_date(date_str, default="0000-00-00"):
    try:
        # Split the string and extract the date part before 'T'
        return date_str.split("T")[0]
    except AttributeError:
        # If date_str is not a string, return the default
        return default
    
directory = "patients_fhir_100"

# Initialize lists to store extracted data
patients = []
medication_requests = []


# Load and parse all JSON files
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        file_path = os.path.join(directory, filename)
        with open(file_path, "r") as file:
            try:
                data = json.load(file)
                for entry in data.get("entry", []):
                    resource = entry.get("resource", {})
                    if resource.get("resourceType") == "Patient":
                            ssn = ""
                            # identifiers = []
                            for identifier in resource.get("identifier", []):
                                # Check for SSN
                                if "type" in identifier and "coding" in identifier["type"]:
                                    for coding in identifier["type"]["coding"]:
                                        if coding.get("code") == "SS":
                                            ssn = identifier.get("value", "")
                                # Collect other identifiers
                                else:
                                    continue
                            address = resource.get("address", [{}])[0]  # Default to an empty dict if no address is present
                            patients.append({
                            "id": resource["id"],
                            "name": " ".join(
                                [name.get("given", [""])[0] + " " + name.get("family", "") for name in resource.get("name", [])]
                            ),
                            "ssn": ssn,
                            "gender": clean_field(resource.get("gender")),
                            "birthDate": standardize_date(resource.get("birthDate")),
                            "country": address.get("country"), 
                            "state": address.get("state"),
                            "city": address.get("city"),
                            "maritalStatus": clean_field(resource.get("maritalStatus", {}).get("text", "")),
                            
                        })

                    if resource.get("resourceType") == "MedicationRequest":
                        uuid =resource["subject"]["reference"].split("/")[-1]
                        medication_requests.append({
                            "id": resource["id"],
                            "patient_id": uuid.split(":")[-1],
                            "medication": clean_field(resource.get("medicationCodeableConcept", {}).get("text", "")),
                            "status": clean_field(resource.get("status")),
                            "dosageInstruction": clean_field(resource.get("dosageInstruction", [{}])[0].get("text", "")),
                            "category": extract_category_code(resource.get("category", [])),
                            "date": extract_date(resource.get("authoredOn")),
                        })
            except json.JSONDecodeError:
                logging.info("Extraction Failed for file : {filename}")
                continue

logging.info("Extraction of medication requests completed successfully!")

# Create and populate the SQLite database
conn = sqlite3.connect("fhir_data.db")
cursor = conn.cursor()

# Create Patients table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Patients (
    id TEXT PRIMARY KEY,
    name TEXT,
    gender TEXT,
    ssn TEXT,
    birthDate DATE,
    country TEXT,
    state TEXT,
    city TEXT,
    MaritalStatus TEXT
)
""")

# Create MedicationRequests table
cursor.execute("""
CREATE TABLE IF NOT EXISTS MedicationRequests (
    id TEXT PRIMARY KEY,
    patient_id TEXT,
    medication TEXT,
    status TEXT,
    dosageInstruction TEXT,
    category TEXT,
    date DATE,
    FOREIGN KEY(patient_id) REFERENCES Patients(id)
)
""")

# Insert Patients data
for patient in patients:
    cursor.execute("""
    INSERT OR REPLACE INTO Patients (id, name, gender, ssn, birthDate, country, state, city, maritalStatus)
    VALUES (?, ?, ?, ?, ?,?, ?, ?, ?)
    """, (
        patient["id"], patient["name"],
        patient["gender"], patient["ssn"], patient["birthDate"], patient["country"],
        patient["state"], patient["city"], patient["maritalStatus"]
    ))

# Insert MedicationRequests data
for med_request in medication_requests:
    cursor.execute("""
    INSERT OR REPLACE INTO MedicationRequests (id, patient_id, medication, status, dosageInstruction, category, date)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        med_request["id"], med_request["patient_id"], med_request["medication"],
        med_request["status"], med_request["dosageInstruction"], 
        med_request["category"], med_request["date"]
    ))

# Commit changes and close connection
conn.commit()
conn.close()

print("Data successfully stored in fhir_data.db")
