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

    def get_recent_tweets(self, query, verbosefunc):
        """ Method to download the recent tweets.
        """
        # Start client
        client = tweepy.Client(bearer_token=self.bearer_token)

        # Retrieve queries
        response = client.search_recent_tweets(query=query, max_results=10,
                                               tweet_fields=["id", "created_at", "text", "source", "public_metrics",
                                                             "entities", "lang", "geo"],
                                               user_fields=["id", "name", "location", "created_at"],
                                               place_fields=["place_type", "geo", "id", "name", "country_code"],
                                               expansions=["author_id", "geo.place_id"])

        # Define dictionary with  users in list from the includes object
        users = {u["id"]: u for u in response.includes["users"]}

        # There has to be at least one tweet with geo info
        # Dict out of list of places from includes object
        if "places" in response.includes:
            places = {p["id"]: p for p in response.includes["places"]}

        tweet_data = []     # Stores data of all tweets

        # Extract data
        for tweet in response.data:

            # Checks if hashtags data is availiable:
            if tweet.entities and "hashtags" in tweet.entities:
                hashtag_data = tweet.entities["hashtags"]
            else:
                hashtag_data = None

            # Create list with data of current tweet
            current_tweet_data = [tweet.id, tweet.created_at, tweet.text.strip().replace("\n", " "), tweet.source,
                                  tweet.public_metrics["retweet_count"], tweet.public_metrics["reply_count"],
                                  tweet.public_metrics["like_count"], tweet.public_metrics["quote_count"],
                                  hashtag_data, tweet.lang]
            # Extract tweet data
            if verbosefunc:
                print("###############################################\n###############################################")
                print("##### Tweet object data #####")
                print("tweet.id: ", tweet.id)
                print("tweet.created_at", tweet.created_at)
                print("tweet.text.strip()", tweet.text.strip())
                print("tweet.source", tweet.source)
                print("tweet.public_metrics", tweet.public_metrics)
                print("tweet.entities", tweet.entities)
                print("tweet.lang", tweet.lang, "\n")

            if users[tweet.author_id]:  # Extract user data
                user = users[tweet.author_id]

                # Append user data to current tweet data
                current_tweet_data += [user.id, user.name, user.location, user.created_at]

                if verbosefunc:
                    print("##### User object data #####")
                    print("user.id", user.id)
                    print("user.name", user.name)
                    print("user.location", user.location)
                    print("user.created_at", user.created_at, "\n")

            if tweet.geo:   # Not all tweets have geo data
                if places[tweet.geo["place_id"]]:
                    place = places[tweet.geo["place_id"]]

                    current_tweet_data += [place.id, place.name, place.country_code, place.geo, place.place_type]

                    if verbosefunc:
                        print("##### Geo object data #####")
                        print("place.id", place.id)
                        print("place.name", place.name)
                        print("place.country_code", place.country_code)
                        print("place.geo", place.geo)
                        print("place.place_type", place.place_type, "\n")

            # Append empty element when there is no geo data
            else:
                current_tweet_data += [None, None, None, None, None]

            print(current_tweet_data)
            # Format elements to list and replace potential false separators
            current_tweet_data = list(map(lambda x: x.replace("$", "€"), list(map(str, current_tweet_data))))

            print(current_tweet_data)
            # Append formatted tweet data to final list
            tweet_data.append(current_tweet_data)

        # zum testen all position mit $ printen +-10 Positionen um zu schauen ob man vlt was wichtiges raus kickt

        return tweet_data

    @staticmethod
    def save_tweets(tweets, columns):
        """ Method to store input tweets in a csv file.
        """
        data_frame = pandas.DataFrame(tweets, columns=columns)

        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        data_frame.to_csv("Data/tweets_" + time + ".csv", sep="$")


def main():

    download_handler = DownloadHandler()
    key_location = "Data/config.ini"
    download_handler.read_config_file(key_location)
    download_handler.create_api_interface()

    query = "@DB OR Deutsche Bahn OR Die Bahn -is:retweet"      # excludes retweets

    # -RT to exclude the russian Trolls (serious problem)
    query_nine_euro = "(#9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR #NeunEuroTickets OR neun-euro-ticket OR" \
                      " neun-euro-tickets OR (9 euro ticket) OR (9 euro tickets)) (lang:en OR lang:de) -RT"
    query_db_general = "((@DB_Bahn OR @DB_Info OR @DB_Presse OR (deutsche bahn) OR #DeutscheBahn OR #DBNavigator) " \
                       "-(RT OR #9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR #NeunEuroTickets OR #9euro OR " \
                       "neun-euro OR (9 euro) OR 9€)) (lang:de OR lang:en)"

    test_query = "deutsche bahn lang:en"

    tweets = download_handler.get_recent_tweets(query_nine_euro, True)

    columns = ["tweet.id", "tweet.created_at", "tweet.text", "tweet.source", "tweet.retweet_count", "tweet.reply_count",
               "tweet.like_count", "tweet.quote_count", "tweet.hashtags", "tweet.lang", "user.id", "user.name",
               "user.location", "user.created_at", "place.id", "place.name", "place.country_code", "place.geo",
               "place.place_type"]

    download_handler.save_tweets(tweets, columns)


if __name__ == '__main__':
    main()
