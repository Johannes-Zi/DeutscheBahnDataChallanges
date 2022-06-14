import json

import tweepy
import configparser
import pandas
import os
from datetime import datetime
#import logging

# Basic configuration
#logging.basicConfig(level=logging.DEBUG)


class DownloadHandler:
    """ Creates API access.
    """

    def __init__(self):
        """ Constructor.
        """
        self.api_key = None
        self.api_key_secret = None
        self.access_token = None
        self.access_token_secret = None
        self.bearer_token = None
        self.api = None

    def read_config_file(self, path_to_file):
        """ Reads the config file.
        """
        config = configparser.RawConfigParser()
        config.read(path_to_file)

        # Assign values
        self.api_key = config["twitter"]["api_key"]
        self.api_key_secret = config["twitter"]["api_key_secret"]
        self.access_token = config["twitter"]["access_token"]
        self.access_token_secret = config["twitter"]["access_token_secret"]
        self.bearer_token = config["twitter"]["bearer_token"]

    def create_api_interface(self):
        """ Uses authentication details to create an API interface.
        """
        # Authentication
        authentication = tweepy.OAuthHandler(self.api_key, self.api_key_secret)
        authentication.set_access_token(self.access_token, self.access_token_secret)

        # Create API interface
        self.api = tweepy.API(authentication)

    def get_recent_tweets(self, query):
        """ Method to download the recent tweets.
        """
        # Create client
        client = tweepy.Client(bearer_token=self.bearer_token)

        # Die folgenden Daten sollten wir für jeden tweet speichern, den unteschied zwischen tweet geo und place geo hab
        # ich noch nicht ganz verstanden
        """response = client.search_recent_tweets(query=query, max_results=10,
                                               tweet_fields=["id", "created_at", "author_id", "text", "entities", "geo",
                                                             "in_reply_to_user_id", "lang", "possibly_sensitive",
                                                             "public_metrics", "source"],
                                               user_fields=["id", "created_at", "location", "public_metrics"],
                                               place_fields=["id", "contained_within", "geo", "name", "place_type"])"""


        # Ich glaube mit Paginator kann man auf die user_fileds us zugreifen
        """for tweet in tweepy.Paginator(client.search_recent_tweets, "Tweepy",
                                      max_results=100).flatten(limit=250):
            print(tweet.id)"""

        """response = client.search_recent_tweets(query=query, max_results=10,
                                               expansions=["author_id", "media_keys", "place_id"],
                                               tweet_fields=["source", "entities"],
                                               user_fields=["public_metrics"],
                                               place_fields=["place_type"])"""

        response = client.search_recent_tweets(query=query, max_results=20,
                                               tweet_fields=["source", "lang", "geo"],
                                               user_fields=["id", "public_metrics", "location"],
                                               place_fields=["geo"],
                                               expansions=["author_id", "geo.place_id"])

        tweet_data = []


        # mit Paginator kann ma mehr als 100 Tweets auf einmal ziehen

        # Define dictionary with  users in list from the includes object
        users = {u["id"]: u for u in response.includes['users']}

        # Dict out of list of places from includes object
        places = {p["id"]: p for p in response.includes['places']}

        # Extract data
        for tweet in response.data:
            print("###########")
            print(tweet.author_id)
            print(tweet.text.strip())

            if users[tweet.author_id]:
                user = users[tweet.author_id]
                print(user.id)
                #print(user.geo)

            if tweet.geo:   # Not all tweets have geo data
                print("### Has geo")
                if places[tweet.geo['place_id']]:
                    place = places[tweet.geo['place_id']]
                    print(place.geo)

            print("\n")

        exit()

        for tweet in response.data:
            # Extract relevant data
            """tweet_data.append([tweet.id, tweet.created_at, tweet.author_id, tweet.text.strip(), tweet.entities,
                               tweet.geo, tweet.in_reply_to_user_id, tweet.lang, tweet.possibly_sensitive,
                               tweet.public_metrics, tweet.source])

            attributes = dir(tweet)
            print(attributes)
            exit()"""
            print("#############")
            print(tweet.text.strip())
            print(tweet.lang)
            #print(tweet.entities)
            #tweet_data.append([tweet.public_metrics])

        print("XXXXXXXXXXXXXXXXXXXXXX")

        """attributes = dir(response.includes)
        print(attributes)
        exit()"""


        for users in response.includes:
            print(users)

        return tweet_data

    @staticmethod
    def save_tweets(tweets, columns):
        """ Method to store input tweets in a csv file.
        """
        data_frame = pandas.DataFrame(tweets, columns=columns)

        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        data_frame.to_csv("Data/tweets_" + time + ".csv")


def main():

    download_handler = DownloadHandler()
    key_location = "Data/config.ini"
    download_handler.read_config_file(key_location)
    download_handler.create_api_interface()

    query = "@DB OR Deutsche Bahn OR Die Bahn -is:retweet"      # excludes retweets

    # -RT to exclude the russian Trolls (serious problem)
    query_nine_euro_de = "(#9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR #NeunEuroTickets OR neun-euro-ticket " \
                         "OR neun-euro-tickets OR (9 euro ticket) OR (9 euro tickets)) lang:de -RT"
    query_nine_euro_en = "(#9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR #NeunEuroTickets OR neun-euro-ticket " \
                         "OR neun-euro-tickets OR (9 euro ticket) OR (9 euro tickets)) lang:en -RT"
    query_db_general_de = "((@DB_Bahn OR @DB_Info OR @DB_Presse OR (deutsche bahn) OR #DeutscheBahn OR " \
                          "#DBNavigator) -(RT OR #9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR " \
                          "#NeunEuroTickets OR #9euro OR neun-euro OR (9 euro) OR 9€)) lang:de"
    query_db_general_en = "((@DB_Bahn OR @DB_Info OR @DB_Presse OR (deutsche bahn) OR #DeutscheBahn OR " \
                          "#DBNavigator) -(RT OR #9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR " \
                          "#NeunEuroTickets OR #9euro OR neun-euro OR (9 euro) OR 9€)) lang:en"
    test_query = "deutsche bahn lang:en"

    tweets = download_handler.get_recent_tweets(query_nine_euro_en)

    columns = ["Time", "Tweet"]
    #download_handler.save_tweets(tweets, columns)


if __name__ == '__main__':
    main()
