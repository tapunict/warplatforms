import json
import urllib
import requests
import pandas as pd
import re


def fetchDataset():
    url = 'https://parseapi.back4app.com/classes/City?limit=1512&keys=name,population,location,cityId'
    headers = {
        # This is the fake app's application id
        'X-Parse-Application-Id': 'WHhatLdoYsIJrRzvkD0Y93uKHTX49V9gmHgp8Rw3',
        # This is the fake app's readonly master key
        'X-Parse-Master-Key': 'iyzSAHUmPKUzceWRIIitUD1OKAGHvVUzEYb5DCpj'
    }
    dict = json.loads(requests.get(
        url, headers=headers).content.decode('utf-8'))
    df = pd.DataFrame(dict["results"])
    # df = pd.from_dict(dict)
    print(f"COUNT: {df.count()}")
    print(f"COLUMNS: {df.columns}")
    print(df.head())
    df.to_csv('ukraine_cities.csv', encoding='utf-8')


def modifyDataset():
    pattern = r'[0-9.]+'
    df = pd.read_csv('ukraine_cities.csv', engine='python')
    df['name'] = df['name'].apply(lambda x: x.lower())
    df['latitude'] = df['location'].apply(lambda x: re.findall(pattern, x)[0])
    df['longitude'] = df['location'].apply(lambda x: re.findall(pattern, x)[1])
    new_df = df[["name", "population", "latitude", "longitude"]]
    new_df.to_csv('ukraine_cities_cleaned.csv', encoding='utf-8')


def LoadUkraineCities():
    datasets = ['ukraine_cities_cleaned.csv']  # ,'ukraine_cities.csv'
    for dataset in datasets:
        try:
            return pd.read_csv(dataset, engine='python')
        except:
            print(f"Dataset {dataset} non è stato trovato")
    print("Nessun dataset trovato")
    return False


def findCity(cityname):
    df = LoadUkraineCities()
    search = df[df["name"] == cityname.lower()]
    # if search.empty:
    #     print("Non è stato trovato il campo")
    return search


def getLocation(cityname):
    city_df = findCity(cityname)
    if city_df.empty:
        return False
    location = {}
    location['latitude'] = city_df.iloc[0]['latitude']
    location['longitude'] = city_df.iloc[0]['longitude']
    return location

# tipo "41.12,-71.34"


def getLocationAsString(cityname):
    location = getLocation(cityname)
    if location == False:
        return False
    return f"POINT({location['longitude']} {location['latitude']})"      
# [float('%.5f' % location['latitude']), float('%.5f' % location['longitude'])]

# POINT(lon lat)

def getLocationsAsString(list):
    # for city in list:
    #     return getLocationAsString(city)
    points_list = []
    for city in list:
        points_list.append(getLocationAsString(city))
    if len(points_list) == 0:
        return None
    return points_list


def findCitiesInText(text, minlen=4, maxlen=10):
    cities = []
    tokens = [word for word in text.split() if len(
        word) >= minlen and len(word) <= maxlen]
    print(tokens)
    for token in tokens:
        result = findCity(token)
        if not result.empty:
            cities.append(result.iloc[0]['name'])
    return cities


def test():
    text = "ciao questa non ZybIny è una zlynka città zolochiv"
    result = findCitiesInText(text)
    print(result)
    print()
    print(getLocationsAsString(result))


if __name__ == '__main__':
    fetchDataset()
    modifyDataset()
    test()
