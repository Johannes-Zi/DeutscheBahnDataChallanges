import json
import numpy as np
import pandas
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from geopy.geocoders import Nominatim
import geopandas as gpd


class TweetMapper:

    def __init__(self, json_file):
        self.json_file = json_file
        self.locations_file = None
        self.tweet_data = self.get_data(self.json_file)
        self.geo_tweets = []
        self.locations = {}

    @staticmethod
    def get_data(json_file):
        with open(json_file, 'r') as in_file:
            data = json.load(in_file)

        return data

    def extract_geo_data(self):
        self.geo_tweets = {}
        for tweet_id in self.tweet_data:
            tweet = self.tweet_data[tweet_id]

            if all(tweet['Geo'].values()):
                self.geo_tweets[tweet_id] = tweet

    def save_data(self, data_file=None):
        if data_file is not None:
            with open(data_file, 'a', encoding='utf-8') as out_file:
                json.dump(self.geo_tweets, out_file, ensure_ascii=False, indent=4)
        else:
            time = datetime.now().strftime("%d-%m-%Y_%H-%M")
            with open('Geo_Tweets' + time + '.json', 'w', encoding='utf-8') as out_file:
                json.dump(self.geo_tweets, out_file, ensure_ascii=False, indent=4)

    def update_locations(self):
        df = pd.read_json(self.json_file, orient='index', convert_axes=False)

        """
        # Latitude between -90.00 and 90.00
        # Longitude between -180.00 and 180.00
        min_longitude = 90
        min_latitude = 180
        max_longitude = -90
        max_latitude = -180
        for i in range(len(df['Geo'])):
            if df['Geo'][i]['Country_Code'] == 'DE':
                if df['Geo'][i]['Geo']['bbox'][0] < min_longitude:
                    min_longitude = df['Geo'][i]['Geo']['bbox'][0]
                if df['Geo'][i]['Geo']['bbox'][1] < min_latitude:
                    min_latitude = df['Geo'][i]['Geo']['bbox'][1]
                if df['Geo'][i]['Geo']['bbox'][2] > max_longitude:
                    max_longitude = df['Geo'][i]['Geo']['bbox'][2]
                if df['Geo'][i]['Geo']['bbox'][3] > max_latitude:
                    max_latitude = df['Geo'][i]['Geo']['bbox'][3]

        bounding_box = (min_longitude, min_latitude, max_longitude, max_latitude)
        """

        for i in range(len(df['Geo'])):
            if df['Geo'][i]['Country_Code'] == 'DE':
                if df['Geo'][i]['Name'] not in self.locations:
                    self.locations[df['Geo'][i]['Name']] = None

        for place in self.locations.keys():
            if self.locations[place] is None:
                geolocator = Nominatim(user_agent="s9349604@stud.uni-frankfurt.de")
                location = geolocator.geocode(place, country_codes='de')
                print(location)
                if location is None:
                    self.locations[place] = {"Latitude": "n/a", "Longitude": "n/a"}
                else:
                    self.locations[place] = {"Latitude": location.latitude, "Longitude": location.longitude}

    def get_locations(self, file_path):
        self.locations_file = file_path
        with open(file_path, "r", encoding="utf-8") as in_file:
            self.locations = json.load(in_file)

    def save_locations(self):
        if self.locations_file is not None:
            with open(self.locations_file, "a", encoding='utf-8') as out_file:
                json.dump(self.locations, out_file, indent=4)
        else:
            time = datetime.now().strftime("%d-%m-%Y_%H-%M")
            with open('location_database_' + time + '.json', 'w', encoding='utf-8') as out_file:
                json.dump(self.locations, out_file, ensure_ascii=False, indent=4)

    @staticmethod
    def plot_data(self, json_file):
        df = pd.read_json(json_file, orient='index')

        to_drop = []
        for i in range(len(df.index)):
            if df['Latitude'][i] == "n/a" or df['Longitude'][i] == "n/a":
                row = df.iloc[[i]]
                to_drop.append(row.index[0])

        df = df.drop(to_drop)

        pd.set_option('display.max_rows', None)
        print(df)

        fig, ax = plt.subplots(figsize=(8, 6))
        countries = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
        countries[countries["name"] == "Germany"].plot(color="lightgrey", ax=ax)
        df.plot(x="Longitude", y="Latitude", kind="scatter", colormap="YlOrRd", title=f"Test", ax=ax)
        plt.show()


def main():
    json_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_21-07-2022_16-05_general.json"
    geo_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/Geo_Tweets21-07-2022_17-30.json"
    locations = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/location_database_24-07-2022_11-38.json"

    new_mapper = TweetMapper(geo_file)
    # new_mapper.extract_geo_data()
    # new_mapper.save_data()
    # new_mapper.update_locations()
    # new_mapper.save_locations()
    # new_mapper.plot_data(locations)


if __name__ == '__main__':
    main()
