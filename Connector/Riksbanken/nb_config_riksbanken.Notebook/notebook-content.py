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

# CELL ********************

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# länk till Excel (behöver skrivas om om i Sharepoint)
# excel_file_path = "/lakehouse/default/Files/riksbanken_valutakurser.xlsx"

# df_pd = pd.read_excel(excel_file_path)

# skapa upp schemat explicit
schema = StructType([
    StructField("currency", StringType(), True),
    StructField("series_id", StringType(), True),
    StructField("include", IntegerType(), True), # vilka kurser ska hämtas
    StructField("base", IntegerType(), True) # vilka valutor ska användas som bas
])

# specificera mål- och basvalutor (SEK är alltid basvaluta från Riksbanken, konvertering sker i silver)
data = [
    ("AUD", "SEKAUDPMI", 0, 0), ("BGN", "SEKBGNPMI", 0, 0), ("BRL", "SEKBRLPMI", 0, 0),
    ("CAD", "SEKCADPMI", 0, 0), ("CHF", "SEKCHFPMI", 0, 0), ("CNY", "SEKCNYPMI", 0, 0),
    ("CZK", "SEKCZKPMI", 0, 0), ("DKK", "SEKDKKPMI", 0, 0), ("EUR", "SEKEURPMI", 0, 0),
    ("GBP", "SEKGBPPMI", 0, 0), ("HKD", "SEKHKDPMI", 0, 0), ("HUF", "SEKHUFPMI", 0, 0),
    ("IDR", "SEKIDRPMI", 0, 0), ("ILS", "SEKILSPMI", 0, 0), ("INR", "SEKINRPMI", 0, 0),
    ("ISK", "SEKISKPMI", 0, 0), ("JPY", "SEKJPYPMI", 0, 0), ("KRW", "SEKKRWPMI", 0, 0),
    ("MXN", "SEKMXNPMI", 0, 0), ("MYR", "SEKMYRPMI", 0, 0), ("NOK", "SEKNOKPMI", 0, 0),
    ("NZD", "SEKNZDPMI", 0, 0), ("PHP", "SEKPHPPMI", 0, 0), ("PLN", "SEKPLNPMI", 0, 0),
    ("RON", "SEKRONPMI", 0, 0), ("SGD", "SEKSGDPMI", 0, 0), ("THB", "SEKTHBPMI", 0, 0),
    ("TRY", "SEKTRYPMI", 0, 0), ("USD", "SEKUSDPMI", 0, 0), ("ZAR", "SEKZARPMI", 0, 0)
]


spark = SparkSession.builder.getOrCreate()
df_spark = spark.createDataFrame(data, schema)

table_name = "riksbanken_config"

# skapa upp Delta-tabell
try:
    df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Successfully saved Delta table: {table_name}")
except Exception as e:
    print(f"Error saving Delta table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
