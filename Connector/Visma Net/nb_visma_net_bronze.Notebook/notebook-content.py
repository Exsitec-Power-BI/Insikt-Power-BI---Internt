# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0bf70ad1-5b1b-4c95-bd9a-cd6997f968a2",
# META       "default_lakehouse_name": "lh_visma_net",
# META       "default_lakehouse_workspace_id": "ad503b56-680c-45f3-b7b9-8b31dfa2da20",
# META       "known_lakehouses": [
# META         {
# META           "id": "0bf70ad1-5b1b-4c95-bd9a-cd6997f968a2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, to_json
from pyspark.sql.types import StructType, ArrayType
from notebookutils import mssparkutils
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_secret(vault, secret_name):
    """
    Retrieves a secret from the linked Key Vault using Fabric Utilities.
    """
    try:
        return mssparkutils.credentials.getSecret(vault, secret_name)
        print(f"Returned secret {secret_name}")
    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        return None
        
def get_visma_auth_header(token):
    """
    Constructs the required headers for Visma Net API calls.
    Note: Some endpoints require 'ipp-company-id' (Tenant ID).
    """
    return {
        "Authorization": f"Bearer {token}",
        "ipp-company-id": TENANT_ID,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

# Fetches a short-lived (60 min) access token using Client Credentials
def fetch_access_token():
    """
    Exchanges Client ID and Client Secret for a temporary access token.
    """
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": VISMA_SCOPE,
        "tenant_id": TENANT_ID
    }
    
    try:
        response = requests.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        token_data = response.json()
        return token_data.get("access_token")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch access token: {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_visma_data(endpoint, access_token, params=None):
    if not access_token:
        print("Error: Could not obtain access token. Aborting fetch.")
        return []

    headers = get_visma_auth_header(access_token)
    
    all_records = []
    url = f"{VISMA_NET_API_URL}/{endpoint}"
    
    print(f"Starting data fetch from: {url}")
    
    # Simple pagination example
    # Visma Net often uses 'skipNumber' and 'numberToRead'
    limit = 1000
    skip = 0
    
    while True:
        current_params = params.copy() if params else {}
        current_params.update({"skipNumber": skip, "numberToRead": limit})
        
        response = requests.get(url, headers=headers, params=current_params)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break
            
        data = response.json()
        
        if not data or len(data) == 0:
            break
            
        all_records.extend(data)
        print(f"Fetched {len(all_records)} records...")
        
        if len(data) < limit:
            break
            
        skip += limit
        
    return all_records

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_df(nested_df):
    """
    Recursively flattens StructTypes and serializes ArrayTypes to JSON strings.
    This ensures compatibility with Delta tables without exploding rows.
    """
    stack = [((), nested_df)]
    columns = []

    while stack:
        parents, df = stack.pop()
        for field in df.schema:
            name = field.name
            full_name = ".".join(parents + (name,))
            
            # If it's a Nested Object (Struct)
            if isinstance(field.dataType, StructType):
                stack.append((parents + (name,), df.select(f"{name}.*")))
            
            # If it's an Array (List of objects or primitives)
            elif isinstance(field.dataType, ArrayType):
                clean_name = full_name.replace(".", "_")
                # Convert Array to a JSON string to avoid schema/saving issues
                columns.append(to_json(col(full_name)).alias(clean_name))
            
            # Standard Primitive types
            else:
                clean_name = full_name.replace(".", "_")
                columns.append(col(full_name).alias(clean_name))
    
    return nested_df.select(columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vl = notebookutils.variableLibrary.getLibrary("vl_visma_net")

VISMA_NET_API_URL = "https://api.finance.visma.net/v1/"
VISMA_PAGE_SIZE = vl.VISMA_PAGE_SIZE
VISMA_SCOPE = vl.VISMA_SCOPE
AKV_NAME = "https://pbi-akv.vault.azure.net/"
CLIENT_ID = get_secret(AKV_NAME, vl.CLIENT_ID_SECRET)
CLIENT_SECRET = get_secret(AKV_NAME, vl.CLIENT_SECRET_SECRET)
TENANT_ID = get_secret(AKV_NAME, vl.TENANT_ID_SECRET)
TOKEN_URL = "https://connect.visma.com/connect/token"

INGESTION_TASKS = [
    {"endpoint": "generalLedgerTransactions?Ledger=1", "table": "stg_visma_gl_transactions", "params": {"lastModifiedDateTime": "2023-01-01"}},
    {"endpoint": "customer", "table": "stg_visma_customers", "params": {}},
    {"endpoint": "supplier", "table": "stg_visma_suppliers", "params": {}},
    {"endpoint": "dimension", "table": "stg_visma_dimensions", "params": {}}
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

access_token = fetch_access_token()

if access_token:
    for task in INGESTION_TASKS:
        endpoint = task["endpoint"]
        table_name = task["table"]
        params = task["params"]
        
        print(f"--- Processing {endpoint} -> {table_name} ---")
        
        raw_data = fetch_visma_data(endpoint, access_token, params)
        
        if raw_data:

            # Convert your raw_data (list of dicts) to a list of JSON strings
            json_rdd = sc.parallelize([json.dumps(record) for record in raw_data])

            # Let Spark read the JSON RDD
            # This allows Spark to use its internal JSON-to-Struct logic which 
            # is more forgiving than the Pandas-to-Spark conversion.
            df_raw = spark.read.json(json_rdd)

            # Convert to Spark DF and Flatten
            # df_raw = spark.createDataFrame(pd.DataFrame(raw_data))
            df_flat = flatten_df(df_raw)
            df_final = df_flat.withColumn("ingested_at", current_timestamp())
            
            # Write to Lakehouse
            try:
                df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
                print(f"Successfully saved {table_name}")
            except Exception as e:
                print(f"Failed to write {table_name}: {e}")
        else:
            print(f"No data found for {endpoint}")
else:
    print("Failed to authenticate with Visma Net.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
