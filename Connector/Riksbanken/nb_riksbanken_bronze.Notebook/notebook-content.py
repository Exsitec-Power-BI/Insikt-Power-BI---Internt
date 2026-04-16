# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6bbefa69-0ee3-46e8-ac9c-44c15237c40d",
# META       "default_lakehouse_name": "lh_riksbanken_bronze",
# META       "default_lakehouse_workspace_id": "ad503b56-680c-45f3-b7b9-8b31dfa2da20",
# META       "known_lakehouses": [
# META         {
# META           "id": "6bbefa69-0ee3-46e8-ac9c-44c15237c40d"
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
# | 1.0.0 | 2026-03-17 | Gustav Gjesdahl | Initial version: Hämtning via API utifrån config-lista. |
# | 1.0.1 | 2026-03-30 | Gustav Gjesdahl | Mindre namngivningsuppdateringar och kommentering. |
# 
# </div>

# MARKDOWN ********************

# ## Extract från Riksbanken API
# 
# Den här Notebooken är framtagen för att hämta valutakurser från Riksbankens API. Lösningen består av:
# 
# - Config-fil (Excel) där det specas om en valutakurs ska hämtas, samt om den ska användas som basvaluta vid konvertering
# - Notebook för uppskapning av config-tabell från config-fil
# - Notebook för hämtning från API till bronze (körs i pipeline utifrån config-tabellen)
# - Notebook för transform och konvertering till silver (körs i pipeline utifrån config-tabellen)

# CELL ********************

import time
import json
import requests as r
from datetime import date, datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# parameter-cell

base_url = "https://api.riksbank.se/swea/v1/Observations/"

start_date = date(2020, 1, 1)

config_list = """{"currency": "EUR", "SeriesId": "SEKEURPMI"}""" # för test, skrivs över vid pipeline-körning

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# konverterar config-strängen till en lista av dicts och skriver över config_list-variabeln
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

# CELL ********************

# exekveringsloop för varje valuta i config-tabellen

results = []

for line in config_list:
    success = False
    series_id = line["series_id"]
    currency = line['currency']
    include = 

    # bygger API-adressen
    path = f"{base_url}{series_id}/{start_date}"

    print(f"Fetching {currency} starting from {start_date}")

    # anropar API:et tills den returnerar data
    while not success:
        try:
            response = r.get(path)
            
            # 60 sek väntetid vid rate limit
            if response.status_code == 429:
                print(f"Rate limited. Sleeping 60s...")
                time.sleep(60)
                continue
            
            if response.status_code == 204:
                print(f"No content (204) for {currency}.")
                success = True
                break

            if response.status_code == 400:
                print(f"Bad Request (400) for {currency}. Path: {path}")
                break

            # om status 200 kolla om svaret innehåller data
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    for item in data:
                        item['currency'] = currency
                        results.append(item) # lägger till valuta-datan i resultatlistan
                    success = True # bryter loopen
                    time.sleep(1)
                else:
                    print(f"Empty data for {currency}")
                    break
            else:
                print(f"Error {response.status_code} for {currency}")
                break
                
        except Exception as e:
            print(f"Request failed for {currency}: {e}")
            break

# om vi fått in data skriver vi den till delta-tabellen, annars skriver vi ut att ingen data hämtades
if results:
    df_new_rates = spark.createDataFrame(results)
    
    df_new_rates.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable('riksbanken_currency_rates')
else:
    print("No new data retrieved.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
