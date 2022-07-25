
import download_handler
import data_processing


def main():

    # Pull relevant user histories
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

    # Create download instance
    download_handler_1 = download_handler.DownloadHandler()
    key_location = "Data/config.ini"
    # Create API connection
    download_handler_1.read_config_file(key_location)
    download_handler_1.create_api_interface()

    # Pull the history tweets
    #tweets = download_handler_1.pull_user_histories_deep([1003746587842105346,
    #                                                      1003746587842105346], max_results=10)
    tweets = download_handler_1.pull_user_histories_deep(user_id_list=user_id_list[250:300], max_results=2000)

    # Save the tweets as file
    columns = ["tweet.id", "tweet.created_at", "tweet.text", "user.id"]
    download_handler_1.save_tweets(tweets, columns)
    #"""

    # Extract relevant history files and save them as csv
    """# Read in storage files
    storage_dir_path = "C://Users//19joh//Desktop//history_tweets//"
    # Create tweet processing instance
    tweet_processing = data_processing.DataProcessing()
    # Read in the tweets of user histories
    tweet_processing.create_df_with_storage_data(input_dir_path=storage_dir_path)
    # Extract DB related tweets and save them as csv
    tweet_processing.save_db_related_tweets_for_annotation()
    #"""

    # Overrepresented railway line extraction out of short 650er df
    # 4. frequencuies von cities bestimmen
    # 5. alle kombinationen von häufigen städten erstellen
    # 6. häufigkeit aller kombinationen zählen
    # 7. Extract tweets of top 20 combinations user ids,
    # dict funktion bekommt liste mit zusätzlicher bool spalte, rest droppen

    # 9. manually annotate tweets
    # 10. read in tweets

    # 11. determine sentiment difference before and after 9euro ticket for each user, an then reference to the
    # combinations
    # history df und 9euro getrennt anschauen stimmung für jeder der 20 Strecken bestimmen, und dann die dfs vergleichen

    # aufkerte einzeichnen und präsentation bauen

    # Präsentation erstellen


if __name__ == '__main__':
    main()
