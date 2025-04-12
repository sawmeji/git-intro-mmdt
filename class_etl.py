import requests
import pandas as pd
import numpy as np
import logging
import os
from sqlalchemy import create_engine




def extract_json_from_url(url:str) -> dict:
    try:
        return requests.get(url).json()
    except Exception as e:
        raise ConnectionError(f"Error : {e}")
    

def print_df(df:pd.DataFrame):
    print(df.shape) 
    print(df.columns)
    print(df.head())

    
def extract_covid_data():
    covid_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"
    covid_json = extract_json_from_url(covid_url)

    country_df_list = []
    for country_short_code in covid_json.keys():
        singile_country_df = pd.json_normalize(covid_json[country_short_code])
        singile_country_df['iso_country_code'] = country_short_code
        country_df_list.append(singile_country_df)

    all_df = pd.concat(country_df_list, ignore_index=True)
    
    select_cols = ["iso_country-code", "continent", "location", "last_updated_date", "total_cases", "new_cases"]
    select_df = all_df[select_cols]

    select_df.replace(r"^\s$", np.nan, regex=True)
    return select_df

def extract_city_data():
    cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"
    cities_lst_json = extract_json_from_url(cities_url)
    all_data = []
    for country in cities_lst_json:
        selected_data_dict = { 
            "country_id" : country.get("id", np.nan),
            "country_name" : country.get("name", np.nan),
            "country_iso3" : country.get("iso3", np.nan),
            "cuuntry_capital" : country.get("capital", np.nan),
            "subregion" : country.get("subregion", np.nan),
            "region" : country.get("region", np.nan),
            "cities" : [],
        }
        all_data.append(selected_data_dict)
        logging.info(f"Collected data from {selected_data_dict['country_name']} : {selected_data_dict['cuuntry_capital']}")

    all_df = pd.DataFrame(all_data)
    return all_df
    # print_df(all_df)

def extract_single_city_weather(city_name:str):
    api_key = os.getenv("WEATHER_KEY")
    # weather_api = f"https://api.openweathermap.org/data/2.5/weather?q={cities}&appid={api_key}"
    weather_api = f"https://api.openweathermap.org/data/2.5/weather?q=Yangon&appid=da2e91ea9dc60eb53e0ddb41350c2e98"

    weather_json = extract_json_from_url(weather_api)
    try: 
        city_weather_dict = {
            # "weather_condition": weather_json['weather'][-1]['description'],
            "weather_condition": weather_json['weather'][-1].get("description", "Unknown Condition"),
            "temp_min" : weather_json['main'].get('temp_min', np.nan),
            "temp_max" : weather_json['main'].get('temp_max', np.nan),
        }
        city_weather_dict['city'] = city_name
    
        return city_weather_dict
    except ConnectionError:
        ##collect error ices and try i lat, long
        logging.info("Connection Error to check requests.get()")
        return None
    except Exception as e:
        logging.info("Other Error : {e}")
        return None

def extract_all_cities_weather(city_names: list):
    all_result_list = []
    for city_name in city_names:
        # if city_dict := extract_single_city_weather(city_name):
        city_dict = extract_single_city_weather(city_name)
        if city_dict:
            all_result_list.append(city_dict)
        else:
            print("Result is None for {city_name}")
    all_df = pd.DataFrame(all_result_list)
    # print_df(all_df)
    return all_df                                                                                        

def transform_data():
    ##get weather fro all cap citi
    city_df = extract_city_data()
    capital_names_lst = city_df['country_capital'].to_list()
    weather_df = extract_all_cities_weather(capital_names_lst)

    ##Join city + weather
    city_weather_df = city_df.merge(weather_df, how='left', left_on="country_capital", right_on="city")

    ##Join covid + result (from above join)
    covid_df = extract_covid_data()
    final_df = covid_df.merge(city_weather_df, how="left", left_on="iso_country_code", right_on="country_iso3")
    print_df(final_df)


def load_data(df:pd.DataFrame, table_name:str):
    engine = create_engine("sqlite:///class_demo.db")
    df.to_sql(table_name, con = engine, index=False, if_exists="replace")

## covid top 3 dead countries all cities 

# if __name__ == "__main__":
#     transform()

    # city_df = extract_city_data()
    # capital_names_lst = city_df['country_capital'].to_list()
    # weather_df = extract_all_cities_weather(capital_names_lst)
    # print_df(weather_df)

    # print(data.shape)
    # print(data.columns)
    # print("--x--")

