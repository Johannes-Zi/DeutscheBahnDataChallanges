# coding=utf-8
# !python3

"""Functions for twitter retrieval """
__author__ = "6947325: Johannes Zieres"
__credits__ = ""
__email__ = "johannes.zieres@gmail.com"

import tweepy
import os

print("test")

key_file_path = r"C:\Users\19joh\OneDrive - Johann Wolfgang Goethe Universität\Master\Semester_1\Data_Challenges\twitter_key.txt"

# Read in key for API access
key_file = open(key_file_path)
key_data = []

for line in key_file:
    if line[0] != "#":
        key_data.append(line)

auth = tweepy.OAuth1UserHandler(
   consumer_key, consumer_secret, access_token, access_token_secret
)

api = tweepy.API(auth)

public_tweets = api.home_timeline()
for tweet in public_tweets:
    print(tweet.text)

