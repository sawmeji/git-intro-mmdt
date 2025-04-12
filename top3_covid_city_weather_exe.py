import pandas as pd
import requests
import os

WEATHER_KEY = os.getenv("WEATHER_API_KEY")




# countries_url = "https://github.com/dr5hn/countries-states-cities-database/blob/master/json/countries.json"

# countries_json = requests.get(countries_url).json()

# weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={cities}&appid={WEATHER_KEY}"

covid19_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"

covid19_json = requests.get(covid19_url).json()

all_covid_countries_df = pd.DataFrame()

all_covid_countries_list = []
for iso_country_code in covid19_json.keys():
    single_country_df = pd.json_normalize(covid19_json[iso_country_code]).copy()
    single_country_df['iso_country_code'] = iso_country_code
    if single_country_df['continent'].isna().all():
        continue
    single_country_df = single_country_df.dropna(axis=1, how='all')

    all_covid_countries_list.append(single_country_df)

all_covid_countries_df = pd.concat(all_covid_countries_list, ignore_index=True)

covid19_select_columns = ["iso_country_code", "continent", "location", "last_updated_date", "total_cases", "total_deaths"]

covid19_select_df = all_covid_countries_df[covid19_select_columns]

top_3_deaths_countries = covid19_select_df.nlargest(3, 'total_deaths')

print(top_3_deaths_countries)


cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"

cities_json = requests.get(cities_url).json()

cities_df = pd.DataFrame(cities_json)

select_city_columns = ["name", "country_name", "latitude", "longitude"]

selected_cities_df = cities_df[select_city_columns]

filter_cities_df = selected_cities_df[selected_cities_df['country_name'].isin(top_3_deaths_countries['location'])].copy()

filtered_cities_df = filter_cities_df.groupby('country_name').head(10)

# print(filtered_cities_df)

cities_weather_df = pd.DataFrame()

# for i in filtered_cities_df.index:
for i in filtered_cities_df.index:
        cities = filtered_cities_df.loc[i, 'name']
        # lat = filtered_cities_df.loc[i, 'latitude']
        # lon = filtered_cities_df.loc[i, 'longitude']
        # lat_weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={WEATHER_KEY}"
        weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={cities}&appid={WEATHER_KEY}"
        weather_raw_data = requests.get(weather_url).json()
        if weather_raw_data['cod'] == '404':
            lat = filtered_cities_df.loc[i, 'latitude']
            lon = filtered_cities_df.loc[i, 'longitude']
            lat_weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={WEATHER_KEY}"
            weather_raw_data = requests.get(lat_weather_url).json()
            
        weather_raw_df = pd.json_normalize(weather_raw_data, record_path='weather', meta=[['name'], ['main', 'temp_min'], ['main', 'temp_max']])

        filtered_weather_df = weather_raw_df[['name', 'description', 'main.temp_min', 'main.temp_max']]

        cities_weather_df = pd.concat([cities_weather_df, filtered_weather_df], ignore_index = True)



cities_weather_df.columns = [['Name', 'Condition', 'Temperature Minimum', 'Temperature Maximum']]

print(cities_weather_df)

