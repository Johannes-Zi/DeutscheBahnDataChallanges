
import download_handler
import data_processing


def main():

    # Read in storage files
    storage_dir_path = "C://Users//19joh//Desktop//testdir//"
    # File with city key names
    city_key_file_path = "C://Users//19joh//Desktop//deutschland_gemeinden_short.txt"

    # Create tweet processing instance
    tweet_processing = data_processing.DataProcessing()
    # Load city key names
    tweet_processing.load_city_key_data(city_key_file_path)

    # Read in the tweets of the last 6 weeks
    tweet_processing.create_df_with_storage_data(input_dir_path=storage_dir_path)
    # Reduce the dataframe to drop unnecessary information and detect city names
    tweet_processing.create_short_tweet_df()

    # Extract relevant user ids for history search
    user_id_list = tweet_processing.extract_individual_user_ids()

    # Created download instance
    download_handler_1 = download_handler.DownloadHandler()
    key_location = "Data/config.ini"
    # Create API connection
    download_handler_1.read_config_file(key_location)
    download_handler_1.create_api_interface()

    # Pull the history tweets
    #tweets = download_handler_1.pull_user_histories([1003746587842105346,
    #                                                 1003746587842105346], verbosefunc=False, max_results=10)
    tweets = download_handler_1.pull_user_histories(user_id_list=user_id_list,
                                                    verbosefunc=False, max_results=100)

    # Save the tweets as file
    columns = ["tweet.id", "tweet.created_at", "tweet.text", "tweet.source", "tweet.retweet_count", "tweet.reply_count",
               "tweet.like_count", "tweet.quote_count", "tweet.hashtags", "tweet.lang", "user.id", "user.name",
               "user.location", "user.created_at", "place.id", "place.name", "place.country_code", "place.geo",
               "place.place_type"]
    download_handler_1.save_tweets(tweets, columns)

    # Next steps:
    # 1. Load history tweets
    # 2. filter not db related out
    # 3. save the tweets as files

    # 4. Load all tweet files as single df
    # 5. create short df
    # 6. Extract top 20 city combinations
    # 7. Extract tweets of top 20 combinations
    # 7.1 Assign users to top 20 twenty combinations
    # 8. Save tweets ready for annotation (all db tweets of user that are part of the top combinations)
    # 9. manually annotate tweets
    # 10. read in tweets
    # 11. determine sentiment difference before and after 9euro ticket for each user, an then reference to the
    # combinations


if __name__ == '__main__':
    main()
