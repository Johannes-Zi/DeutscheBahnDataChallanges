import json
import numpy as np
import pandas
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from geopy.geocoders import Nominatim
import geopandas as gpd


class TweetMapper:

    def __init__(self, twitter_json_file=None, geo_tweets_json_file=None):
        self.twitter_json_file = twitter_json_file
        self.geo_tweets_json_file = geo_tweets_json_file
        self.tweet_data = {}
        self.geo_tweets = []
        self.locations = {}

    def get_locations(self, location_json_file):
        with open(location_json_file, 'r', encoding='utf-8') as in_file:
            self.locations = json.load(in_file)

    @staticmethod
    def get_tweets(json_file):
        """
        Gets a dataset filled with Tweets and saves them in a dictionary.

        :param json_file: JSON file containing Twitter data
        """

        with open(json_file, 'r', encoding='utf-8') as in_file:
            data = json.load(in_file)

        return data

    def extract_geo(self):
        """
        Extracts all Tweets from the dataset that have a Geo object attached.
        """

        self.geo_tweets = {}
        for tweet_id in self.tweet_data:
            tweet = self.tweet_data[tweet_id]

            if all(tweet['Geo'].values()):
                self.geo_tweets[tweet_id] = tweet

    def save_geo_tweets(self, data_file=None):
        """
        Saves the extracted Tweets into a JSON file that can be expanded.

        :param data_file: If a data file is given, the values are attached to the file
        """

        if data_file is not None:
            with open(data_file, 'a', encoding='utf-8') as out_file:
                json.dump(self.geo_tweets, out_file, ensure_ascii=False, indent=4)
        else:
            time = datetime.now().strftime("%d-%m-%Y_%H-%M")
            with open('Geo_Tweets' + time + '.json', 'w', encoding='utf-8') as out_file:
                json.dump(self.geo_tweets, out_file, ensure_ascii=False, indent=4)

    def extract_geo_tweets(self):
        """
        Handles the extraction process of geo Tweets.
        """

        self.tweet_data = self.get_tweets(self.twitter_json_file)
        self.extract_geo()
        self.save_geo_tweets()

    def get_locations(self, json_file):
        """
        Gets the location da
        :param json_file: JSON file that contains the information
        """

        with open(json_file, "r", encoding="utf-8") as in_file:
            self.locations = json.load(in_file)

    def update_locations(self):
        """
        Creates a location file with latitude and longitude for all locations found in the geo Tweets.
        """

        self.geo_tweets = pd.read_json(self.geo_tweets_json_file, orient='index', convert_axes=False)

        for i in range(len(self.geo_tweets['Geo'])):
            if self.geo_tweets['Geo'][i]['Country_Code'] == 'DE':
                if self.geo_tweets['Geo'][i]['Name'] not in self.locations:
                    self.locations[self.geo_tweets['Geo'][i]['Name']] = None

        for place in self.locations.keys():
            if self.locations[place] is None:
                geolocator = Nominatim(user_agent="s9349604@stud.uni-frankfurt.de")
                location = geolocator.geocode(place, country_codes='de')
                print(location)
                if location is None:
                    self.locations[place] = {"Latitude": "n/a", "Longitude": "n/a"}
                else:
                    self.locations[place] = {"Latitude": location.latitude, "Longitude": location.longitude}

    def save_locations(self, locations_database=None):
        """
        Saves the locations in a JSON file.
        """

        if locations_database is not None:
            with open(locations_database, "a", encoding='utf-8') as out_file:
                json.dump(self.locations, out_file, indent=4)
        else:
            time = datetime.now().strftime("%d-%m-%Y_%H-%M")
            with open('locations_database_' + time + '.json', 'w', encoding='utf-8') as out_file:
                json.dump(self.locations, out_file, ensure_ascii=False, indent=4)

    def add_locations(self):
        with open(self.geo_tweets_json_file, "r", encoding="utf-8") as in_file:
            self.geo_tweets = json.load(in_file)

            for entry in self.geo_tweets.keys():
                location = self.geo_tweets[entry]['Geo']['Name']
                if location in self.locations.keys():
                    self.geo_tweets[entry]['Geo']['Place'] = self.locations[location]

        with open(self.geo_tweets_json_file, 'w', encoding="utf-8") as out_file:
            json.dump(self.geo_tweets, out_file, indent=4)

    def extract_locations(self, locations_database):
        """
        Handles the location extraction process.

        :param locations_database: Path to locations database file
        """

        if locations_database:
            self.get_locations(locations_database)
            self.update_locations()
            self.save_locations(locations_database)
        else:
            self.update_locations()
            self.save_locations()

    @staticmethod
    def plot_geo_data(json_file):

        with open(json_file, 'r', encoding='utf-8') as in_file:
            data = json.load(in_file)

            df = {}
            for entry in data.keys():
                values = data[entry]
                if values['Geo']['Country_Code'] == 'DE':
                    df[values['Data']['Id']] = {'User': values['User']['Id'],
                                                'Latitude': values['Geo']['Place']['Latitude'],
                                                'Longitude': values['Geo']['Place']['Longitude'],
                                                'Sentiment': values['Data']['Sentiment'],
                                                'Created_At': values['Data']['Created_At']}

            df = pd.DataFrame.from_dict(df, orient='Index')

            to_drop = []
            for index, row in df.iterrows():
                if row['Latitude'] == "n/a" or row['Longitude'] == "n/a":
                    to_drop.append(index)
            df = df.drop(to_drop)

            pandas.set_option('display.max_rows', None)
            print(df)

            cm = 1 / 2.54  # centimeters in inches
            fig, ax = plt.subplots(figsize=(20*cm, 28*cm))
            countries = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
            countries[countries["name"] == "Germany"].plot(color="lightgrey", ax=ax)
            df.plot(x="Longitude", y="Latitude", kind="scatter", c="Sentiment", colormap="YlOrRd",
                    title=f"Distribution Germany", ax=ax)
            plt.savefig("Distribution Germany.pdf")

    def plot_new_data(self, csv):
        pass


def main():
    json_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_21-07-2022_16-05_general.json"
    geo_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/Geo_Tweets21-07-2022_17-30.json"
    locations = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/location_database_24-07-2022_11-38.json"

    new_mapper = TweetMapper(json_file, geo_file)
    # new_mapper.extract_geo_tweets()
    # new_mapper.extract_locations(locations)
    new_mapper.plot_geo_data(geo_file)
    # new_mapper.get_locations(locations)
    # new_mapper.add_locations()


if __name__ == '__main__':
    main()
