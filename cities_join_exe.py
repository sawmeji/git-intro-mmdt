import pandas as pd
import requests 

url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"

cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"

raw_data = requests.get(url).json()

city_raw_data = requests.get(cities_url).json()

cities = pd.DataFrame(city_raw_data)

all_countries = pd.DataFrame()

for country_short_code in raw_data.keys():
    single_country_df = pd.json_normalize(raw_data[country_short_code])
    single_country_df['iso_country_code'] = country_short_code
    all_countries = pd.concat([all_countries, single_country_df], ignore_index=True)

print(all_countries.head())
print(all_countries.columns)
print(all_countries.shape)

print(cities.head())
print(cities.columns)
print(cities.shape)

cities_join_data = pd.merge(cities, all_countries, left_on='country_name', right_on='location', how='inner')

print(cities_join_data.head())
print(cities_join_data.columns)
print(cities_join_data.shape)




