import tweepy
from tweepy.parsers import JSONParser
import configparser
import pandas
import json
from datetime import datetime


class SentimentAnalyser:

    def __init__(self):
        pass

    @staticmethod
    def read_csv(path_to_file):
        dataframe = pandas.read_csv(path_to_file)

        condensed_data = dataframe[["id",
                                    "created_at",
                                    "text",
                                    "entities",
                                    "user",
                                    "geo",
                                    "coordinates",
                                    "place",
                                    "retweet_count",
                                    "favorite_count",
                                    "lang"]]

        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        condensed_data.to_json("/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/condensed_tweets_" + time + ".json", orient='records', indent=4)

        return condensed_data


def main():
    sa = SentimentAnalyser()
    data = sa.read_csv("/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_09-06-2022_12-24.csv")


if __name__ == '__main__':
    main()
