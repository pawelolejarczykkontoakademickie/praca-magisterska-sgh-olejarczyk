# -*- coding: utf-8 -*-
from google.colab import auth
from google.cloud import bigquery
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

auth.authenticate_user()

PROJECT_ID = 'porownanie-elt-etl' 
client = bigquery.Client(project=PROJECT_ID)

print("Środowisko utworzone")

# @title 1. Audyt unikalności i wolumenu przejazdów
import matplotlib.ticker as ticker

query = """
SELECT
  EXTRACT(YEAR FROM trip_start_timestamp) as rok,
  COUNT(*) as wszystkie_przejazdy,
  COUNT(DISTINCT unique_key) as unikalne_przejazdy,
  ROUND((COUNT(*) - COUNT(DISTINCT unique_key)) / COUNT(*) * 100, 5) as pct_duplikatow
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
GROUP BY 1 ORDER BY 1
"""
df = client.query(query).to_dataframe()

pd.options.display.float_format = '{:,.5f}'.format
print(df.to_string(index=False))

print("\n")

sns.set_style("white")
plt.figure(figsize=(10, 5))

ax = sns.barplot(x='rok', y='unikalne_przejazdy', data=df, color='#4c72b0')

fmt = ticker.FuncFormatter(lambda x, p: f'{x/1_000_000:.1f} mln')
ax.yaxis.set_major_formatter(fmt)
ax.bar_label(ax.containers[0], fmt=fmt, padding=3)

plt.title('Liczba unikalnych przejazdów (2013-2023)', loc='left', pad=15)
plt.ylabel('')
plt.grid(axis='y', alpha=0.2)
sns.despine(left=True)

plt.savefig('wykres_unikalne_przejazdy.png')

plt.show()

# @title 2. Analiza kompletności danych 

import matplotlib.ticker as ticker

query_null = """
SELECT
  COUNTIF(pickup_community_area IS NULL) / COUNT(*) * 100 as pickup_community_area,
  COUNTIF(dropoff_community_area IS NULL) / COUNT(*) * 100 as dropoff_community_area,
  COUNTIF(trip_start_timestamp IS NULL) / COUNT(*) * 100 as trip_start_timestamp,
  COUNTIF(trip_seconds IS NULL) / COUNT(*) * 100 as trip_seconds,
  COUNTIF(trip_miles IS NULL) / COUNT(*) * 100 as trip_miles,
  COUNTIF(trip_total IS NULL) / COUNT(*) * 100 as trip_total,
  COUNTIF(tips IS NULL) / COUNT(*) * 100 as tips,
  COUNTIF(payment_type IS NULL) / COUNT(*) * 100 as payment_type
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`

"""

df_nulls = client.query(query_null).to_dataframe()
df_melted = df_nulls.melt(var_name='kolumna', value_name='procent_NULL').sort_values('procent_NULL', ascending=False)

plt.figure(figsize=(10, 5))
ax = sns.barplot(x='kolumna', y='procent_NULL', data=df_melted, color='#0000ff')

plt.xticks(rotation=45, ha='right', fontsize=10)

ax.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=100))
ax.bar_label(ax.containers[0], fmt='%.2f%%', padding=3)

plt.title('Odsetek brakujących wartości (NULL)', loc='left', pad=15)
plt.ylabel('Braki danych [%]')
plt.grid(axis='y', alpha=0.2)
sns.despine(left=True)

plt.savefig('roklad_wartosci_null.png', bbox_inches='tight')
plt.show()

# @title 3. Rozkłady i anomalie

query_dist = """
SELECT trip_seconds, trip_miles, trip_total
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE RAND() < 0.01
"""
df_dist = client.query(query_dist).to_dataframe()

stats = df_dist.describe(percentiles=[.01, .5, .99]).T
print(stats[['min', '1%', '50%', '99%', 'max']])

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
cols = ['trip_seconds', 'trip_miles', 'trip_total']
titles = ['Czas trwania (s)', 'Dystans (mile)', 'Kwota ($)']

for i, col in enumerate(cols):
    limit = stats.loc[col, '99%']
    data_filtered = df_dist[df_dist[col] <= limit]
    sns.histplot(data_filtered[col], bins=30, kde=True, ax=axes[i], color='#94e3ff', edgecolor=None)
    axes[i].set_title(titles[i])
    axes[i].set_xlabel('')
    axes[i].set_ylabel('')
    axes[i].grid(axis='y', alpha=0.2)

sns.despine(left=True)
plt.tight_layout()

plt.savefig('rysunek_rozklady.png', bbox_inches='tight')
plt.show()