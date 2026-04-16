# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08d8e28f-64e4-4b94-a480-688442e09297",
# META       "default_lakehouse_name": "lh_graphql",
# META       "default_lakehouse_workspace_id": "ad503b56-680c-45f3-b7b9-8b31dfa2da20",
# META       "known_lakehouses": [
# META         {
# META           "id": "08d8e28f-64e4-4b94-a480-688442e09297"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import SparkSession

# länk till Excel (behöver skrivas om om i Sharepoint)
excel_file_path = "/lakehouse/default/Files/bnxt_default_config.xlsx"

df_pd = pd.read_excel(excel_file_path)

# filtrera bort de tabeller som inte ska läsas
df_pd = df_pd[df_pd['Fabric'] == 1]

spark = SparkSession.builder.getOrCreate()
df_spark = spark.createDataFrame(df_pd)

table_name = "bnxt_config"

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
