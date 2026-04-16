# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08d8e28f-64e4-4b94-a480-688442e09297",
# META       "default_lakehouse_name": "lh_bnxt",
# META       "default_lakehouse_workspace_id": "ad503b56-680c-45f3-b7b9-8b31dfa2da20",
# META       "known_lakehouses": [
# META         {
# META           "id": "08d8e28f-64e4-4b94-a480-688442e09297"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Change Log
# 
# <div style="display: flex; justify-content: flex-start; margin-left: 0; width:fit-content">
# 
# | Version | Datum | Utvecklare | Beskrivning |
# | :--- | :--- | :--- | :--- |
# | v1.0 | 2026-03-16 | Gustav Gjesdahl | Initial version: Full load, laddning av samtliga tabeller ifrån config eller enkel tabell, automatisk refresh vid utlöpt token. |
# 
# </div>

# MARKDOWN ********************

# ### Info & Setup
# Denna Notebook är framtagen för att utgöra en standardiserad lösning för hämtning av data från Visma Business NXT.
# 
# Lösningen utgörs av fyra filer:
# - Excel-fil med standardtabeller
# - Config-notebook för att skapa upp config-tabell
# - Extract-notebook för att hämta ut samtliga tabeller angivna i config-tabellen
# - Pipeline för körning
# 
# Arbetsflöde:
# 1. Fyll i Excel-filen som ligger till grund för config-tabellen med bolag och tabeller för extract
# 2. Kör config-Notebook för att skapa upp configtabellen
# 3. Fyll i kundspecifika uppgifter i extract-Notebook
#     - AKV-adress
#     - Client-ID
#     - Client Secret
# 4. Kör Pipeline
# 
# Notebooken körs i en pipeline med en config-sträng innehållande bolag och tabellinformation som parameter. Config-strängen skapas av en lookup-aktivitetet. Exekveringsloopen kör en gång per rad i config-tabellen och sparar ner resultaten som Delta-tabell i Lakehouse.

# CELL ********************

# import av moduler
import requests
import json
import pandas as pd
from notebookutils import credentials
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import NullType, StringType
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Funktioner

# CELL ********************

# Metadata
# returnerar en lista på samtliga fält i angiven tabell i Business NXT

def get_all_fields(object_type, company_no, access_token):
  
    # BNXT typer börjar ofta med stor bokstav i schemat (t.ex. Associate)
    type_name = object_type[0].upper() + object_type[1:]
    
    # GraphQL-query
    introspection_query = """
    query($type: String!) {
      __type(name: $type) {
        fields {
          name
          type {
            kind
            name
            ofType {
              kind
              name
            }
          }
        }
      }
    }
    """
    
    headers = {"Authorization": f"Bearer {access_token}"}
    variables = {"type": type_name}
    
    # passar header, GQL-query och variabler till request
    response = requests.post(GQL_URL, json={'query': introspection_query, 'variables': variables}, headers=headers)
    
    # om token går ut under körning förnyas den
    if response.status_code == 401:
        access_token = get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.post(GQL_URL, json={'query': introspection_query, 'variables': variables}, headers=headers)

    if response.status_code != 200:
        print(f"API ERROR: Kunde inte hämta metadata för {object_type}, {company_no}.")
        return None
    
    # hämtar ut __type-nivån ur svaret
    data = response.json().get('data', {}).get('__type')
    
    # om __type är None returnernar funktionen None för att hanteras i exekveringsloopen
    if data is None:
        print(f"TYPE ERROR: Ingen tillgänglig metadata för {object_type}, {company_no}.")
        return None
    
    # fält plockas ut ur metadatan
    fields_data = data.get('fields', [])  

    # filtrera fram skalärer och enums
    allowed_kinds = ['SCALAR', 'ENUM', 'NON_NULL']
    scalar_fields = []
    
    for f in fields_data:
        # väckla ut NON-NULL
        type_info = f['type']
        if type_info['kind'] == 'NON_NULL':
            type_info = type_info['ofType']
        
        # behåll endast skalärer och enums
        if type_info['kind'] in ['SCALAR', 'ENUM']:
            scalar_fields.append({
                "name": f['name'],
                "kind": type_info['kind'],
                "type_name": type_info['name']
            })
            
    return scalar_fields

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Schema
# funktion för att skapa upp ett schema för sparks dataframe
def create_schema(fields_metadata):
    type_map = {
        "String": StringType(),
        "Int": IntegerType(),
        "Float": DoubleType(),
        "Boolean": BooleanType(),
        "ID": StringType(),
        "DateTime": StringType()
    }

    fields = []

    for f in fields_metadata:
        spark_type = type_map.get(f['type_name'], StringType()) if f['kind'] == 'SCALAR' else StringType()
        fields.append(StructField(f['name'], spark_type, True))

    return StructType(fields)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Kolumner
# hjälpfunktion för att skapa upp fältsträngen för GQL-queryn
def create_fields_list(fields_metadata):
    fields_string = " ".join([f['name'] for f in fields_metadata])
    return fields_string

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract
# hämtar data utifrån givet tabellnamn, tabellfält och bolagsnummer

def extract_data(table_name, fields, company_no, access_token):
    all_rows = []
    has_next = True
    after_cursor = None
    
    # GraphQL-template, fylls i med obj (tabellnamn) och fields i pagineringsloopen
    gql_template = """
    query($company: Int!, $cursor: String) {{
      useCompany(no: $company) {{
        {obj}(first: 500, after: $cursor) {{
          pageInfo {{ hasNextPage endCursor }}
          items {{ {fields} }}
        }}
      }}
    }}
    """
    
    # pagineringsloop som körs så länge det finns en "next page"
    while has_next:
        query = gql_template.format(obj=table_name, fields=fields) # fyller i GQL-queryn
        
        # anrop
        headers = {"Authorization": f"Bearer {access_token}"}
        variables = {"company": company_no, "cursor": after_cursor}
        response = requests.post(GQL_URL, json={'query': query, 'variables': variables}, headers=headers)
        
        # om token går ut under körning förnyas den
        if response.status_code == 401:
            access_token = get_access_token()
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.post(GQL_URL, json={'query': query, 'variables': variables}, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"API Error ({table_name}): {response.text}")
            
        res_json = response.json()

        # om GraphQL-queryn returnerar error skrivs detta ut, och funktionen returnerar None för att plockas upp i exekveringsloopen
        if 'errors' in res_json:
            print(f"GraphQL ERROR ({table_name}, {company_no}): {res_json['errors']}")
            return None
            
        data = res_json.get('data', {}).get('useCompany', {}).get(table_name)
        
        # om tabellen saknar data avbryts loopen och all_rows returnas tom för att plockas upp i exekveringsloopen
        if not data:
          print(f"DATA ERROR: Ingen data för {table_name}, {company_no}.")
          break

        items = data.get('items')

        # om det finns rader data på sidan läggs dessa till all_rows
        if items:
            all_rows.extend(items)
        
        has_next = data['pageInfo']['hasNextPage']
        after_cursor = data['pageInfo']['endCursor']
        
    return all_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Access token
def get_access_token():
    
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'business-graphql-service-api:access-group-based' # scope måste vara tilldelat i Visma Developer Portal
    }

    # anropar Visma connect med payload
    response = requests.post(TOKEN_URL, data=payload)
    response.raise_for_status() # kontrollerar att anropet lyckades
    
    # hämtar och returnerar token ur svaret
    return response.json().get("access_token")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parametrar

# PARAMETERS CELL ********************

# secret från Visma Connect (ersätts)
KEY_VAULT_NAME = "https://pbi-akv.vault.azure.net/"
SECRET_NAME = "Visma-Client-Secret" 
ID_NAME = "Visma-Client-ID"

# inloggningsuppgifter för Visma Connect
CLIENT_ID = credentials.getSecret(KEY_VAULT_NAME, ID_NAME) # hämtar secret från AKV
CLIENT_SECRET = credentials.getSecret(KEY_VAULT_NAME, SECRET_NAME)
TOKEN_URL = "https://connect.visma.com/connect/token"

# GraphQL API-adress
GQL_URL = "https://business.visma.net/api/graphql-service"

# ska hämtning göras för en specifik tabell specas tabell och bolag här
table_name = 'associate'
company_no = 4960258

# formatering av config-sträng - skrivs över vid körning av pipeline
config_list = f"""
[
    {{
        "CompanyNo": {company_no}, 
        "Table": "{table_name}"
    }}
]
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# gör om config-strängen till en lista av dicts och skriver över config_list-variabeln
try:
    config_list = json.loads(config_list)
    print(config_list)
except json.JSONDecodeError as e:
    print("Error parsing JSON: ", e)
    config_list = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Hämta och skriv

# CELL ********************

# hämta access-token för körning
access_token = get_access_token()

# för varje rad i config-tabellen
for config in config_list:
    company_no = config.get("CompanyNo")
    print(company_no)
    table_name = config.get("Table")
    print(table_name)
    # hämta kolumnmetadata från Business NXT
    fields_metadata = get_all_fields(table_name, company_no, access_token)

    if not fields_metadata:
        print(f"Metadata kunde inte hämtas för tabellen '{table_name}'. Kontrollera namnet. Hoppar över.")
        continue # går till nästa varv i for-loopen

    columns = create_fields_list(fields_metadata)

    schema = create_schema(fields_metadata)

    # hämta data från Business NXT
    raw_data = extract_data(table_name, columns, company_no, access_token)

    if not raw_data:
        print(f"Ingen data för '{table_name}'. Hoppar över.")
        continue # går till nästa varv i for-loopen

    # skapa Spark DataFrame från JSON-datan
    df_spark = spark.createDataFrame(pd.DataFrame(raw_data), schema=schema)

    # lägg till kolumn för laddningstidpunkt
    df_spark = df_spark.withColumn("ingested_at", F.current_timestamp())

    # skriv till Bronze 
    df_spark.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f'{table_name}_{company_no}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
