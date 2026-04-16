# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
import json
import time
import pandas as pd
from notebookutils import mssparkutils
from pyspark.sql.functions import current_timestamp, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_config_value(var_name, default=None):
    return globals().get(var_name, default)

def resolve_secret(secret_name_var, fallback_var):
    vault_name = get_config_value("https://pbi-akv.vault.azure.net/")
    secret_key = get_config_value(secret_name_var)
    
    if vault_name and secret_key:
        try:
            return mssparkutils.credentials.getSecret(vault_name, secret_key)
        except Exception as e:
            print(f"Warning: Could not fetch {secret_key} from {vault_name}. Error: {e}")
    
    return get_config_value(fallback_var)

def parse_list_config(var_name):
    val = get_config_value(var_name)
    if not val:
        return []
    try:
        parsed = json.loads(val)
        return parsed if isinstance(parsed, list) else [parsed]
    except:
        return [item.strip() for item in str(val).split(',') if item.strip()]

CONFIG = {
    "auth_url": get_config_value("VISMA_AUTH_URL", "https://connect.visma.com/connect/token"),
    "base_url": get_config_value("VISMA_BASE_URL", "https://integration.visma.net/API/controller/api/v1"),
    "scope": get_config_value("VISMA_SCOPE", "vismanet_erp_service_api:read"),
    "page_size": int(get_config_value("VISMA_PAGE_SIZE", 1000)),
    "endpoint": get_config_value("VISMA_ENDPOINT", "customer"),
    
    # Secrets resolved via Key Vault or Variable Library fallback
    "client_id": resolve_secret("CLIENT_ID_SECRET_NAME", "VISMA_CLIENT_ID"),
    "client_secret": resolve_secret("CLIENT_SECRET_SECRET_NAME", "VISMA_CLIENT_SECRET"),

    "tenants": parse_list_config("VISMA_TENANT_IDS"),
    "load_type": parse_list_config("VISMA_LOAD_TYPE")
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_SESSION_STATE = {
    "access_token": None,
    "token_expiry": 0
}

def get_visma_token():
    global _SESSION_STATE
    now = time.time()
    
    if _SESSION_STATE["access_token"] and now < (_SESSION_STATE["token_expiry"] - 60):
        return _SESSION_STATE["access_token"]

    print("Refreshing Visma Connect Token...")
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CONFIG['client_id'],
        'client_secret': CONFIG['client_secret'],
        'scope': CONFIG['scope'],
        'tenant_id': CONFIG['tenant_id']
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    for attempt in range(5):
        try:
            response = requests.post(CONFIG['auth_url'], data=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            _SESSION_STATE["access_token"] = data['access_token']
            _SESSION_STATE["token_expiry"] = now + data['expires_in']
            
            return _SESSION_STATE["access_token"]
        except Exception as e:
            wait = 2 ** attempt
            print(f"Auth failed. Retrying in {wait}s... Error: {e}")
            time.sleep(wait)
    
    raise ConnectionError("Failed to authenticate with Visma Connect.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_last_modified_date(table_name):
    spark = SparkSession.builder.getOrCreate()
    try:
        # We assume the column name is 'lastModifiedDateTime' per Visma Net standards
        max_date = spark.read.table(table_name).select(spark_max("lastModifiedDateTime")).collect()[0][0]
        if max_date:
            # Format usually needs to be ISO 8601 for Visma API
            return max_date.isoformat() if hasattr(max_date, 'isoformat') else str(max_date)
    except:
        print(f"Table {table_name} not found or empty. Performing full load.")
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_and_save_visma_data(tenant_id, endpoint_suffix, table_name, load_type):
    
    last_modified = None
    if load_type == 1: # incremental
        last_modified = get_last_modified_date(table_name)

    page_number = 1
    has_more = True
    total_records = 0
    
    headers = {
        'ipp-tenant-id': tenant_id,
        'Content-Type': 'application/json'
    }

    while has_more:
        token = get_visma_token()
        headers['Authorization'] = f'Bearer {token}'
        
        url = f"{CONFIG['base_url']}/{endpoint_suffix}"
        params = {'pageSize': CONFIG['page_size'], 'pageNumber': page_number}
        
        print(f"Processing page {page_number} for {table_name} in {tenant_id}...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            current_batch = data if isinstance(data, list) else data.get('value', [])
            
            if not current_batch:
                has_more = False
            else:
                pdf = pd.DataFrame(current_batch)
                batch_df = spark.createDataFrame(pdf)
                
                batch_df = batch_df.withColumn("extraction_timestamp", current_timestamp()) \
                                   .withColumn("source_system", lit("VismaNet")) \
                                   .withColumn("page_number", lit(page_number))

                if load_type == 0 and page_number == 1:
                    write_mode = "overwrite"
                elif load_type == 1 and page_number == 1 and not last_modified:
                    write_mode = "overwrite"
                else:
                    write_mode = "append"


                batch_df.write.format("delta") \
                    .mode(write_mode) \
                    .option("overwriteSchema", "true" if page_number == 1 else "false") \
                    .saveAsTable(table_name)
                
                total_records += len(current_batch)
                if len(current_batch) < CONFIG['page_size']:
                    has_more = False
                else:
                    page_number += 1
        else:
            print(f"Error {response.status_code}: {response.text}")
            has_more = False
            
    print(f"Total records processed and saved for {table_name} in {tenant_id}: {total_records}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_extraction():
    if not CONFIG['client_id'] or not CONFIG['client_secret']:
        print("ERROR: Missing Client ID or Secret. Check Key Vault or Variable Library.")
        return
        
    for tenant in CONFIG['tenants']:
        print(f"--- PROCESSING TENANT: {tenant} ---")

        try:
            table_name = f"visma_{CONFIG['endpoint'].split('/')[-1]}_raw"
            fetch_and_save_visma_data(CONFIG['endpoint'], table_name)
            
            spark = SparkSession.builder.getOrCreate()
            display(spark.read.table(table_name).limit(10))
        except Exception as e:
            print(f"Extraction Pipeline Failed: {str(e)}")
            raise

if __name__ == "__main__":
    run_extraction()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
