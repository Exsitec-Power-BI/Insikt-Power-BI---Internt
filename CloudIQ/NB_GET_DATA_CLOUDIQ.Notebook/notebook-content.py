# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a97f71ea-0fd9-457c-b72b-1086fbcea57b",
# META       "default_lakehouse_name": "LH_CloudIQ",
# META       "default_lakehouse_workspace_id": "ad503b56-680c-45f3-b7b9-8b31dfa2da20",
# META       "known_lakehouses": [
# META         {
# META           "id": "a97f71ea-0fd9-457c-b72b-1086fbcea57b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
token_url = "https://api.crayon.com/api/v1/connect/token/"

token_headers = {
    "Authorization" : "Basic NmY2MmYxNGUtZTAyMS00NzhmLTg0ZjctYTA3YjcwMDdlYmM4OmVmMWI3ZjFmLWExZGQtNGM4Mi1iNzJiLTRlNTZiMGJiNjQzNA=="
}

token_data = {
    "grant_type" : "password",
    "username" : "alonso.brito@exsitec.se",
    "password" : notebookutils.credentials.getSecret('https://insiktakv.vault.azure.net', 'PASSWORD-ALONSO-CLOUDIQ'),
    "scope" : "CustomerApi"
}

resp = requests.post(token_url, headers=token_headers, data=token_data).json()

bearer_token = resp["AccessToken"]
# print(bearer_token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------------------------------------------------
# 0. SETUP: Define OrganizationID & "InvoiceGroup"
# --------------------------------------------------------------------------
invoiceProfileId = 100627
organizationId = 4022233

# --------------------------------------------------------------------------
# 1. SETUP: Import necessary libraries
# --------------------------------------------------------------------------
import requests
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

print("Notebook 1: Fetching Billing Statement IDs.")
print("---")


# --------------------------------------------------------------------------
# 2. DEFINE SCHEMA for the initial API call
# --------------------------------------------------------------------------
# This ensures Spark correctly interprets the data types.
initial_item_schema = StructType([
    StructField("Id", LongType(), True)
    # We only need the ID, so we don't have to define the rest of the schema
])

# --------------------------------------------------------------------------
# 3. API CALL: Fetch the list of billing statements
# --------------------------------------------------------------------------
api_url = f"https://api.crayon.com/api/v1/billingstatements/grouped?invoiceProfileId={invoiceProfileId}&organizationId={organizationId}"
headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {bearer_token}'
}
api_data = None
try:
    print(f"Making API call to: {api_url}")
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    api_data = response.json()
    print("Successfully fetched list of billing statements.")
except Exception as err:
    print(f"An error occurred during API call: {err}")
    raise

# --------------------------------------------------------------------------
# 4. PROCESS AND SAVE IDs
# --------------------------------------------------------------------------
if api_data and 'Items' in api_data and api_data['Items']:
    items_list = api_data['Items']
    
    # Create a DataFrame with just the IDs
    # We apply a minimal schema because we only care about the 'Id' field
    df = spark.createDataFrame(items_list, schema=initial_item_schema)
    
    # Select only the 'Id' column for clarity
    statement_ids_df = df.select(col("Id").alias("BillingStatementId"))
    
    count_of_ids = statement_ids_df.count()
    print(f"Found {count_of_ids} Billing Statement ID(s) to process.")
    
    # Define the name for our intermediate table
    intermediate_table_name = "BillingStatementIDs_ToProcess"
    
    print(f"Saving IDs to intermediate table: '{intermediate_table_name}'")
    
    # Save the IDs to a Delta table in the Lakehouse
    statement_ids_df.write.format("delta").mode("overwrite").save(f"Tables/{intermediate_table_name}")
    
    print("---")
    print("Notebook 1 finished successfully.")
else:
    print("No 'Items' found in the API response. No IDs to process.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------------------------------------------------
# 1. SETUP: Import necessary libraries
# --------------------------------------------------------------------------
import requests
import json
import base64 # Import the base64 library for decoding
import time
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

print("Notebook 2: Fetching Detailed Billing Records.")
print("---")

# --------------------------------------------------------------------------
# 2. LOAD IDs FROM LAKEHOUSE
# --------------------------------------------------------------------------
intermediate_table_name = "BillingStatementIDs_ToProcess"
final_table_name = "LH_CloudIQ"

try:
    print(f"Reading IDs from intermediate table: '{intermediate_table_name}'")
    statement_ids_df = spark.read.table(intermediate_table_name)
    
    # Collect the IDs into a Python list to loop over them.
    # .rdd.flatMap(lambda x: x).collect() is an efficient way to do this.
    ids_to_process = statement_ids_df.rdd.flatMap(lambda x: x).collect()
    
    if not ids_to_process:
        print("No IDs found in the intermediate table. Exiting.")
        # dbutils.notebook.exit("No IDs to process") # Use this to stop the pipeline run
    
    print(f"Found {len(ids_to_process)} IDs to process.")

except Exception as e:
    print(f"Error reading from table '{intermediate_table_name}'. Make sure the first notebook ran successfully. Error: {e}")
    raise

# --------------------------------------------------------------------------
# 3. LOOP THROUGH IDs AND FETCH DETAILED DATA (with Base64 Decoding)
# --------------------------------------------------------------------------
all_billing_records = []
headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {bearer_token}'
}

for statement_id in ids_to_process:
    api_url = f"https://api.crayon.com/api/v1/billingstatements/{statement_id}/billingrecordsfile/"
    try:
        print(f"Fetching details for ID: {statement_id}...")
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        
        detail_data = response.json()
        
        # The detailed records are in a Base64 encoded string inside the 'Data' key.
        # We need to decode it first, then parse the resulting string as JSON.
        if detail_data and detail_data.get('Data'):
            encoded_data = detail_data['Data']
            
            # Decode the Base64 string
            decoded_bytes = base64.b64decode(encoded_data)
            decoded_json_string = decoded_bytes.decode('utf-8')
            
            # Now parse the decoded string
            records_list = json.loads(decoded_json_string)
            
            # Add the parent BillingStatementId to each record for lineage/traceability
            for record in records_list:
                record['ParentBillingStatementId'] = statement_id
            
            # Add the processed records to our main list
            all_billing_records.extend(records_list)
            print(f" -> Successfully processed {len(records_list)} records for ID {statement_id}.")

    except requests.exceptions.HTTPError as http_err:
        print(f" -> WARNING: HTTP error for ID {statement_id}: {http_err}. Skipping this ID.")
    except (json.JSONDecodeError, base64.binascii.Error) as decode_err:
        print(f" -> WARNING: Could not parse or decode JSON data for ID {statement_id}: {decode_err}. Skipping this ID.")
    except Exception as err:
        print(f" -> WARNING: An unexpected error occurred for ID {statement_id}: {err}. Skipping this ID.")

# --------------------------------------------------------------------------
# 4. CREATE FINAL DATAFRAME AND SAVE TO LAKEHOUSE
# --------------------------------------------------------------------------
if not all_billing_records:
    print("---")
    print("No billing records were successfully fetched. Final table will not be created.")
    # dbutils.notebook.exit("No billing records fetched")
else:
    print("---")
    print(f"Total records fetched across all IDs: {len(all_billing_records)}")
    
    # It's best practice to define a schema, but for a very wide and complex JSON like this,
    # letting Spark infer the schema from the complete dataset is often a practical start.
    # For production, you would want to define this explicitly.
    final_df = spark.createDataFrame([Row(**record) for record in all_billing_records])
    
    # Reorder the DataFrame to have the ParentBillingStatementId as the first column
    all_columns = final_df.columns
    # Ensure the ParentBillingStatementId exists before trying to move it
    if 'ParentBillingStatementId' in all_columns:
        all_columns.remove('ParentBillingStatementId')
        new_column_order = ['ParentBillingStatementId'] + all_columns
        final_df = final_df.select(new_column_order)
    
    print("Final DataFrame Schema:")
    final_df.printSchema()

    print(f"Saving final data to Delta table: '{final_table_name}'")
    final_df.write.format("delta").mode("overwrite").save(f"Tables/{final_table_name}")
    
    print("---")
    print("Notebook 2 finished successfully. Final data is in the Lakehouse.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
