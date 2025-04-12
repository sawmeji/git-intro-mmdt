
import json
import numpy as np
import pandas as pd
import requests
import os


covid_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"
cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/countries%2Bstates%2Bcities.json"



def extract_json(url:str):
    return requests.get(url).json()

def get_covid_data(url:str)->pd.DataFrame:
    covid_json = extract_json(url)
    all_covid_countries_list = []
    for keys in covid_json.keys():
            if 'OWID' not in keys:
                    single_country_df = {
                                    'iso_country_code' : keys,
                                    'continent' : covid_json[keys].get('continent', np.nan),
                                    'location' : covid_json[keys].get('location',np.nan),
                                    'total_cases' : covid_json[keys].get('total_cases', np.nan),
                                    'total_deaths' : covid_json[keys].get('total_deaths', np.nan)
                            }
                    all_covid_countries_list.append(single_country_df)
                    all_covid_countries_df = pd.DataFrame(all_covid_countries_list)    

    top3_country_df = all_covid_countries_df.nlargest(3,'total_deaths')

    return top3_country_df
    
def get_city_data(url:str,covid_df:pd.DataFrame)->pd.DataFrame:
    city_json= extract_json(url)
    countries_code = covid_df['iso_country_code'].tolist()
    selected_country=[]
    for i in range(len(city_json)):
        if city_json[i]['iso3'] in countries_code:
            selected_country.append(city_json[i])

    city_raw_df = pd.json_normalize(selected_country, record_path=['states', 'cities'], meta=['name', ['states', 'name']],meta_prefix='state_')
    filter_df = city_raw_df.groupby('state_states.name').head(1).groupby('state_name').head(10).reset_index(drop=True)
    city_df=filter_df.drop(['id'],axis=1)
    columns=['city_name','latittude','longitude','country_name','state_name']
    city_df.columns=columns

    return city_df
    
def get_weather_data(city_df:pd.DataFrame)->pd.DataFrame:
    api_key = os.getenv("WEATHER_API_KEY")
    cities_weather_df=pd.DataFrame()
    for count,city in enumerate(city_df['city_name']):
            weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
            weather_raw_data = extract_json(weather_url)

            if weather_raw_data['cod'] == '404':
                lat = city_df.loc[count,'latittude']
                lon = city_df.loc[count,'longitude']
                lat_weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
                weather_raw_data = extract_json(lat_weather_url)
                
            weather_raw_df = pd.json_normalize(weather_raw_data, record_path='weather', meta=[['name'], ['main', 'temp_min'], ['main', 'temp_max']])

            filtered_weather_df = weather_raw_df[['name', 'description', 'main.temp_min', 'main.temp_max']]
            filtered_weather_df = filtered_weather_df.copy()
            filtered_weather_df['name'] = city

            cities_weather_df = pd.concat([cities_weather_df, filtered_weather_df], ignore_index = True)
            cities_weather_df.drop_duplicates(inplace=True)
            cities_weather_df.reset_index(drop=True, inplace=True)

    cities_weather_df.columns = ['city_name', 'Condition', 'Temperature Minimum', 'Temperature Maximum']

    return cities_weather_df
    

def transform()-> pd.DataFrame:

        covid_df = get_covid_data(covid_url)
        city_df = get_city_data(cities_url, covid_df)
        weather_df = get_weather_data(city_df)
        
        city_covid_df = pd.merge(city_df, covid_df, how='inner', left_on='country_name', right_on='location')

        weather_city_covid_df = pd.merge(weather_df, city_covid_df, how='inner', on='city_name')
       
        return weather_city_covid_df

def load(load_df:pd.DataFrame):
    try:
        load_df.to_csv("covid_cities_weather.csv", index=False)
        print(f"Data successfully loaded to covid_cities_weather.csv")
    except Exception as e:
        print(f"Error while saving CSV: {e}")