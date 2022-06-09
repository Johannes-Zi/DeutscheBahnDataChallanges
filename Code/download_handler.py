import tweepy
from tweepy.parsers import JSONParser
import configparser
import pandas
import json
from datetime import datetime


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

    def create_api_interface(self):
        """ Uses authentication details to create an API interface.
        """
        # Authentication
        authentication = tweepy.OAuthHandler(self.api_key, self.api_key_secret)
        authentication.set_access_token(self.access_token, self.access_token_secret)

        # Create API interface
        self.api = tweepy.API(authentication, parser=tweepy.parsers.JSONParser())

    def get_tweets(self, query, until=None):
        """ Method to download the recent tweets.
        """

        if until == None:
            response = self.api.search_tweets(q=query)
        else:
            response = self.api.search_tweets(q=query, until=until)

        json.dumps(response)
        tweet_list = []
        for tweet in response["statuses"]:
            tweet_list.append(tweet)

        return tweet_list

    @staticmethod
    def save_tweets(tweets):
        """ Method to store input tweets in a csv file.
        """
        data_frame = pandas.DataFrame.from_records(tweets)

        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        data_frame.to_csv("Data/tweets_" + time + ".csv", encoding='utf-8')


def main():
    query = "(Deutsche Bahn) OR Bahn OR @DB OR #DeutscheBahn OR (9 Euro Ticket) OR (#9EuroTicket)"

    download_handler = DownloadHandler()
    download_handler.read_config_file("Data/config.ini")
    download_handler.create_api_interface()
    tweets = download_handler.get_tweets(query)
    download_handler.save_tweets(tweets)


if __name__ == '__main__':
    main()
