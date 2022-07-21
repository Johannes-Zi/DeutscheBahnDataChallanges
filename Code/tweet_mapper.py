import json
from datetime import datetime


class TweetMapper:

    def __init__(self, json_file):
        self.json_file = json_file
        self.tweet_data = self.get_data()
        self.geo_tweets = []

    def get_data(self):
        with open(self.json_file, 'r') as in_file:
            data = json.load(in_file)

        return data

    def extract_geo_data(self):
        self.geo_tweets = []
        for tweet_id in self.tweet_data:
            tweet = self.tweet_data[tweet_id]

            if all(tweet['Geo'].values()):
                self.geo_tweets.append(tweet)

    def save_data(self, data_file=None):
        if data_file is not None:
            with open(data_file, 'a', encoding='utf-8') as out_file:
                json.dump(self.geo_tweets, out_file, ensure_ascii=False, indent=4)
        else:
            time = datetime.now().strftime("%d-%m-%Y_%H-%M")
            with open('Geo_Tweets' + time + '.json', 'w', encoding='utf-8') as out_file:
                json.dump(self.geo_tweets, out_file, ensure_ascii=False, indent=4)


def main():
    json_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_21-07-2022_16-05_general.json"

    new_mapper = TweetMapper(json_file)
    new_mapper.extract_geo_data()
    new_mapper.save_data()


if __name__ == '__main__':
    main()
