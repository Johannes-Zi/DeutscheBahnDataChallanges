import tweepy
import configparser
import pandas

# Read config file
config = configparser.ConfigParser()
config.read("config.ini")

api_key = config["twitter"]["api_key"]
api_key_secret = config["twitter"]["api_key_secret"]
access_token = config["twitter"]["access_token"]
access_token_secret = config["twitter"]["access_token_secret"]

# Authentication
authentication = tweepy.OAuthHandler(api_key, api_key_secret)
authentication.set_access_token(access_token, access_token_secret)

api = tweepy.API(authentication)

tweets = api.search_tweets(q="Deutsche Bahn")


def main():
    print(tweets)


if __name__ == '__main__':
    main()
