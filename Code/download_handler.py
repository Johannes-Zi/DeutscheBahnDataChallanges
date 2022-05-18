import tweepy
import configparser
import pandas
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

        response = client.search_recent_tweets(query=query, tweet_fields=["created_at"])
        # Format the data
        tweet_data = []
        for tweet in response.data:
            text = tweet.text.strip()
            tweet_data.append([tweet.created_at, text])

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
    download_handler.read_config_file("Data/config.ini")
    download_handler.create_api_interface()

    query = "@DB OR Deutsche Bahn OR Die Bahn -is:retweet"
    tweets = download_handler.get_recent_tweets(query)
    columns = ["Time", "Tweet"]
    download_handler.save_tweets(tweets, columns)


if __name__ == '__main__':
    main()
