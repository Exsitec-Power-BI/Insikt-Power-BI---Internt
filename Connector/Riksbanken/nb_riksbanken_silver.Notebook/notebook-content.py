# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "22f8f0e3-1351-4cc7-a230-6d2e57202ee3",
# META       "default_lakehouse_name": "lh_riksbanken_silver",
# META       "default_lakehouse_workspace_id": "ad503b56-680c-45f3-b7b9-8b31dfa2da20",
# META       "known_lakehouses": [
# META         {
# META           "id": "6bbefa69-0ee3-46e8-ac9c-44c15237c40d"
# META         },
# META         {
# META           "id": "22f8f0e3-1351-4cc7-a230-6d2e57202ee3"
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
# | 1.0.0 | 2026-03-17 | Gustav Gjesdahl | Initial version: Forward fill på helgdagar, SEK till SEK, konvertering till basvalutor specade i config-tabellen |
# | 1.0.1 | 2026-03-27 | Gustav Gjesdahl | Speciferat schema vid write för att spara korrekt till Delta, uppdaterad variabellogik|
# 
# </div>

# MARKDOWN ********************

# ## Transform från Riksbanken API
# 
# Den här Notebooken är framtagen för transform av valutadata från Riksbankens API. Hela lösningen består av:
# 
# - Config-fil (Excel) där det specas om en valutakurs ska hämtas, samt om den ska användas som basvaluta vid konvertering
# - Notebook för uppskapning av config-tabell från config-fil
# - Notebook för hämtning från API till bronze (körs i pipeline utifrån config-tabellen)
# - Notebook för transform och konvertering till silver (körs i pipeline utifrån config-tabellen)

# CELL ********************

import pandas as pd
import polars as pl
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

# parameter-cell
config_list = """[{"currency": "USD", "base": 1}]""" # skrivs över vid körning av notebook med config-strängen

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# läs in från bronze och konvertera datumen
df = pl.read_delta("abfss://ad503b56-680c-45f3-b7b9-8b31dfa2da20@onelake.dfs.fabric.microsoft.com/6bbefa69-0ee3-46e8-ac9c-44c15237c40d/Tables/dbo/riksbanken_currency_rates")

df = df.with_columns(pl.col("date").str.to_datetime())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# generera date range

date_range = df.select([
    pl.col("date").min().alias("min_date"),
    pl.col("date").max().alias("max_date")
])

# plocka första raden ur "date_range"-tabellen
min_date = date_range["min_date"][0]
max_date = date_range["max_date"][0]

# skapa upp en-dimensionell datumtabell från min till max
dates_df = pl.datetime_range(
        start=min_date,
        end=max_date,
        interval="1d",
        eager=True
    ).to_frame("date")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# joina på valutor på datumtabellen med cross join
currencies_df = df.select("currency").unique()

grid_df = dates_df.join(currencies_df, how="cross")

# joina på daglig kurs med forward fill på dagar som saknar kurs
filled_df = (
        grid_df.join(df, on=["date", "currency"], how="left")
        .sort(["currency", "date"])
        .with_columns([
            pl.col("value").forward_fill().over("currency")
        ])
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# skapa upp värden för sek-sek (saknas i källdatan)
sek_df = dates_df.with_columns(
    pl.lit("SEK").alias("currency"),
    pl.lit(1.00).alias("value")
)

df = pl.concat([sek_df, filled_df])

df = df.rename({"currency": "target_currency"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# lägg till target currency (i källdatan är all SEK)

df = df.with_columns(
    pl.lit("SEK").alias("base_currency") # target och base har fått fel plats (uppdateras i 1.1)
)

df = df.select(["target_currency", "date", "value", "base_currency"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# skapa upp en lista som vi kommer att fylla på med dataframes för varje base currency
base_currencies = [df] # initialiserar med SEK

# loopar över konfigurationslistan för varje base currency
for curr in config_list:
    currency = curr['currency']
    is_base = curr['base']

    if is_base:
        # spara base currency som referenstabell
        curr_rates = (
            df.filter(pl.col("base_currency") == currency)
            .select([
                pl.col("date"),
                pl.col("value").alias("target_rate")
            ])
        )

        # för varje rad i grundtabellen, dela kursen med target_rate i referenstabellen för att få konverterade kurser
        converted_rates = (
            df.join(curr_rates, on="date")
            .with_columns(
                (pl.col("value") / pl.col("target_rate")).alias("converted_rate")
            )
        )

        # skapa en kolumn base_currency
        appended_data = (
            converted_rates.select([
                pl.col("target_currency").alias("target_currency"),
                pl.col("date"),
                pl.col("converted_rate").alias("value")
            ])
            .with_columns(
                pl.lit(currency).alias("base_currency")
            )
        ) 

        # lägg till i listan av dataframes som ska konkateneras
        base_currencies.append(appended_data)

df = pl.concat(base_currencies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# casta om från datetime till date (för att matcha datatyper i källdatan)
df = df.with_columns(pl.col("date").cast(pl.Date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# specificera path
lakehouse_id = '22f8f0e3-1351-4cc7-a230-6d2e57202ee3'
lakehouse_workspace_id = 'ad503b56-680c-45f3-b7b9-8b31dfa2da20'
table_name = 'currencies'
path = f"abfss://{lakehouse_workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/silver/{table_name}"

df.write_delta(target=path, mode="overwrite", delta_write_options={"schema_mode": "overwrite"}, storage_options={"allow_http": "true"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
