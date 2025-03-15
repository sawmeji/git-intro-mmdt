import pandas as pd
import requests 

covid19_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"

cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"

countries_raw_data = requests.get(covid19_url).json()

cities_raw_data = requests.get(cities_url).json()

cities_df = pd.DataFrame(cities_raw_data)

all_countrie_df = pd.DataFrame()

for country_short_code in countries_raw_data.keys():
    single_country_df = pd.json_normalize(countries_raw_data[country_short_code])
    single_country_df['iso_country_code'] = country_short_code
    all_countrie_df = pd.concat([all_countrie_df, single_country_df], ignore_index=True)

print(all_countrie_df.head())
print(all_countrie_df.columns)
print(all_countrie_df.shape)

print(cities_df.head())
print(cities_df.columns)
print(cities_df.shape)

cities_join_data = pd.merge(cities_df, all_countrie_df, left_on='country_name', right_on='location', how='inner')

print(cities_join_data.head())
print(cities_join_data.columns)
print(cities_join_data.shape)




