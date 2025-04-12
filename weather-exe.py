import pandas as pd
import requests


WEATHER_KEY = "da2e91ea9dc60eb53e0ddb41350c2e98"

cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"

cities_raw_data = requests.get(cities_url).json()

cities_df = pd.DataFrame(cities_raw_data)

cities_names_list = cities_df['name'].to_list()

cities_weather_df = pd.DataFrame()

for cities in cities_names_list[:101]:

    weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={cities}&appid={WEATHER_KEY}"
    
    weather_raw_data = requests.get(weather_url).json()

    if weather_raw_data['cod'] == '404':
        continue

    weather_raw_df = pd.json_normalize(weather_raw_data, record_path='weather', meta=[['name'], ['main', 'temp_min'], ['main', 'temp_max']])

    filtered_weather_df = weather_raw_df[['name', 'description', 'main.temp_min', 'main.temp_max']]

    cities_weather_df = pd.concat([cities_weather_df, filtered_weather_df], ignore_index = True)


cities_weather_df.columns = [['Name', 'Condition', 'Temperature Minimum', 'Temperature Maximum']]

cities_weather_df.to_csv("cities_weather.csv", index = False)