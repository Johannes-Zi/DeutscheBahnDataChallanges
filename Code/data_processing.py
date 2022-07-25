import pandas as pd
import glob
from datetime import datetime


class DataProcessing:
    """
    imports, filter and save data in geopandas dataframes
    """

    def __init__(self):
        """
        Constructor
        """
        self.dataframe = None
        self.input_directory = None
        self.city_location_count = None
        self.tweet_df = pd.DataFrame([])
        self.history_tweet_df = pd.DataFrame([])
        self.city_key_dict = {}
        self.short_tweet_df = pd.DataFrame
        self.history_short_tweet_df = pd.DataFrame

    def create_df_with_storage_data(self, input_dir_path):
        """
        Reads in the tweet storage csv files and combines them to a single pandas dataframe. All files in the handed
        over directory, which are formatted like a storage file are considered.
        :param input_dir_path: path to input directory
        :return:
        """
        # Get all relevant files that are stored in the input dir
        # Returns every file in the directory with .out at the end
        input_file_list = glob.glob(input_dir_path + "/tweets_*.csv")
        pd.set_option('display.max_columns', None)  # prints all columns

        self.city_location_count = 0     # Saves the number of tweets with city location data

        entry_count = 0

        # Read in tweet data from files
        for current_file_path in input_file_list:
            #print("Current file path: ", current_file_path)

            current_tweet_df = pd.read_csv(current_file_path, sep="$", index_col=0)
            #print(len(current_tweet_df))
            entry_count += len(current_tweet_df)
            #print(sum(current_tweet_df["place.name"] != "None"))
            #print(sum(current_tweet_df["place.place_type"] == "city"))
            self.tweet_df = pd.concat([self.tweet_df, current_tweet_df])

        # Drop duplicates
        #print(self.tweet_df.head())
        #self.tweet_df = self.tweet_df.set_index("tweet.id", inplace=False) # Sets index column to tweeti
        self.tweet_df = self.tweet_df.drop_duplicates(subset="tweet.id")
        #print("???", len(self.tweet_df[self.tweet_df.index.duplicated()]))  # gives back indexes that are present multiple times
        #print("???", self.tweet_df.index)  # prints all present indexes

        print("Tweets with geo object: ", sum(self.tweet_df["place.name"] != "None"))
        print("Tweets with city geo object: ", sum(self.tweet_df["place.place_type"] == "city"))
        self.city_location_count += sum(self.tweet_df["place.place_type"] == "city")

        print("length tweet.df (without duplicates)", len(self.tweet_df))

        print("Counted csv entries (with duplications):", entry_count)

    def load_city_key_data(self, city_key_file_path):
        """
        Load the file with all key parts of all cities and smaller tows in germany. And creates a dictionary for a fast
        key search.
        :param city_key_file_path: path to city key file
        :return:
        """

        city_key_file = open(city_key_file_path, "r", encoding="utf-8")

        # Append key parts of the city names in the key list
        for line in city_key_file:
            converted = line.strip().split()[0]
            self.city_key_dict[converted] = converted

    def text_city_key_extraction(self, tweet_text):
        """
        Filters for a given tweet text the keywords out. And checks if the city keys are used in combination with
        other keywords, which might indicate an assignment as start or end position of the traveling.
        :param tweet_text: stripped text of a tweet
        :return: isolated_keys_list, start_keys_list, end_keys_list
        """

        # Split tweet text, to search for words not substrings
        tweet_text_split = tweet_text.split()

        # Remove all  hashtags, dots, commas in front of the words to enable key search
        tweet_text_split = list(map(lambda x: x.replace("#", "").replace(".", "").replace(",", ""), tweet_text_split))

        # Stores isolated city key words found in the tweet text
        isolated_keys_list = []

        # Stores city key words that are found in combination with start key words in the tweet text
        start_keys_list = []
        # Stores city key words that are found in combination with end key words in the tweet text
        end_keys_list = []
        # saves the position of the current word in the text to check previous words as keys
        current_list_position = 0

        # Check for each word, if it is a key in the city key name dict
        for current_word in tweet_text_split:

            # Checks if word is a key
            if current_word in self.city_key_dict:

                previous_pos = tweet_text_split[current_list_position - 1]

                # Check if the key is a start position key
                if ((previous_pos == "von") or (previous_pos == "Von")
                    or (previous_pos == "aus") or (previous_pos == "Aus") or (previous_pos == "from") or
                                                  (previous_pos == "From")) and current_list_position != 0:
                    start_keys_list.append(current_word)

                # Check if the key is an end position key
                elif ((previous_pos == "nach") or (previous_pos == "Nach") or (previous_pos == "to") or
                                                (previous_pos == "To")) and current_list_position != 0:
                    end_keys_list.append(current_word)

                # Isolated key found
                else:
                    isolated_keys_list.append(current_word)

            current_list_position += 1

        return start_keys_list, end_keys_list, isolated_keys_list

    def create_short_tweet_df(self):
        """
        Write function which kicks out unessesary columns and add columns for city key storage
        The information of the geo location name should also be considered, when type is city
        The information of the enteties can be included, but i think they are based on the tweet text.
        english tweets should be excluded from the analysis
        Account location as start
        :return:
        """

        # Remove dots form column names, they are obstructive
        self.tweet_df.columns = self.tweet_df.columns.str.replace('.', '_')

        # Drop unnecessary columns
        self.short_tweet_df = self.tweet_df.drop(["tweet_source", "tweet_retweet_count", "tweet_reply_count",
                                                  "tweet_like_count", "tweet_quote_count", "tweet_hashtags",
                                                  "user_name", "tweet_lang"], axis=1)

        # New columns
        self.short_tweet_df["hometowns"] = self.short_tweet_df.apply(lambda x: [], axis=1)
        self.short_tweet_df["destinations"] = self.short_tweet_df.apply(lambda x: [], axis=1)
        self.short_tweet_df["unassigned_locations"] = self.short_tweet_df.apply(lambda x: [], axis=1)

        # transfer user.location in hometowns with city key detection
        self.short_tweet_df["hometowns"] = self.short_tweet_df.apply(
            lambda name_list: name_list.hometowns + self.text_city_key_extraction(name_list.user_location)[2], axis=1)

        # Transfer tagged geo data in unassigned column with city key detection
        self.short_tweet_df["unassigned_locations"] = self.short_tweet_df.apply(
            lambda name_list: name_list.unassigned_locations + self.text_city_key_extraction(name_list.place_name)[2],
            axis=1)

        # Drop unnecessary geo columns
        self.short_tweet_df = self.short_tweet_df.drop(["place_name", "place_country_code", "place_id",
                                                        "place_geo", "place_place_type", "user_location"], axis=1)

        # City name extraction from tweet texts
        # For hometowns
        self.short_tweet_df["hometowns"] = self.short_tweet_df.apply(
            lambda name_list: name_list.hometowns + self.text_city_key_extraction(name_list.tweet_text)[0],
            axis=1)
        # For travel destinations
        self.short_tweet_df["destinations"] = self.short_tweet_df.apply(
            lambda name_list: name_list.destinations + self.text_city_key_extraction(name_list.tweet_text)[1],
            axis=1)
        # For city names without route assignment
        self.short_tweet_df["unassigned_locations"] = self.short_tweet_df.apply(
            lambda name_list: name_list.unassigned_locations + self.text_city_key_extraction(name_list.tweet_text)[2],
            axis=1)

        """#print(self.short_tweet_df.head())
        print("Hometown count distribution: \n", self.short_tweet_df["hometowns"].value_counts())
        print("Destination count distribution: \n", self.short_tweet_df["destinations"].value_counts())
        print("Unassigned_locations count distribution: \n", self.short_tweet_df["unassigned_locations"].value_counts())
        """

        # Drop all rows, where no geolocation was assigned
        self.short_tweet_df = self.short_tweet_df[(self.short_tweet_df["hometowns"].map(lambda x: len(x)) > 0) |
                                                  (self.short_tweet_df["destinations"].map(lambda x: len(x)) > 0) |
                                                  (self.short_tweet_df["unassigned_locations"].map(lambda x: len(x))
                                                   > 0)]

        """print("###########################################")
        print(len(self.short_tweet_df))
        print(self.short_tweet_df.head())


        print("Hometown count distribution: \n", self.short_tweet_df["hometowns"].value_counts())
        print("Destination count distribution: \n", self.short_tweet_df["destinations"].value_counts())
        print("Unassigned_locations count distribution: \n", self.short_tweet_df["unassigned_locations"].value_counts())

        # Print specific index
        #print(self.short_tweet_df.loc[[3]])"""

    def extract_individual_user_ids(self):
        """
        Extracts individual user id of the short_tweet_df for the user history pulls.
        :return: user_id_df
        """
        print("\n")
        print("(Short_df) Number of geo tweets in df without duplicates", len(self.short_tweet_df))

        # Extract tweets with assigned hometown and travel destination
        self.short_tweet_df = self.short_tweet_df[(self.short_tweet_df["hometowns"].map(lambda x: len(x)) > 0) &
                                                  (self.short_tweet_df["destinations"].map(lambda x: len(x)) > 0)]
        # Drop user id duplicates
        user_id_df = self.short_tweet_df.drop_duplicates(subset="user_id")
        user_id_list = user_id_df['user_id'].tolist()
        print("Indidual users with assigned geo data", len(user_id_list))

        #print(user_id_list[:10])

        return user_id_list

    @staticmethod
    def db_key_extraction(self, tweet_text):
        """
        Function determines if a tweet text is related to the deutsch bahn
        :param tweet_text: trivial
        :return: db_related
        """

        # True if tweet text is db related
        db_related = False

        # Split tweet text, to search for words not substrings
        tweet_text_split = tweet_text.split()

        # Remove all  hashtags, dots, commas in front of the words to enable key search
        tweet_text_split = list(map(lambda x: x.replace("#", "").replace(".", "").replace(",", ""), tweet_text_split))

        key_dict = {"@DB_Bahn": "@DB_Bahn", "@DB_Info": "@DB_Info", "@DB_Presse": "@DB_Presse", "bahn": "bahn",
                    "Bahn": "Bahn", "DeutscheBahn": "DeutscheBahn", "#DBNavigator": "#DBNavigator",
                    "#9EuroTicket": "#9EuroTicket", "#9EuroTickets": "#9EuroTickets",
                    "#NeunEuroTicket": "#NeunEuroTicket", "#NeunEuroTickets": "#NeunEuroTickets",
                    "neun-euro-ticket": "neun-euro-ticket", "neun-euro-tickets": "neun-euro-tickets",
                    }

        # Check for each word, if it is a key in the db key name dict
        for current_word in tweet_text_split:

            # Checks if word is a key
            if current_word in key_dict:
                db_related = True
                break

        return db_related

    def save_db_related_tweets(self):
        """
        Extracts the tweets out of the dataframe that are related to the the Deutsche Bahn and saves them as csv file.
        :return:
        """

        # Remove dots form column names, they are obstructive
        self.tweet_df.columns = self.tweet_df.columns.str.replace('.', '_')

        # Add column for DB relation assessment
        self.tweet_df["db_related"] = False

        # Determine if the tweet texts are DB related
        self.tweet_df["db_related"] = self.tweet_df.apply(lambda db_related:
                                                          self.db_key_extraction(tweet_text=db_related.tweet_text),
                                                          axis=1)

        # Drop tweets that are not db related
        self.tweet_df = self.tweet_df[(self.tweet_df["db_related"] == True)]

        # Drop unnecessary column
        self.tweet_df = self.tweet_df.drop(["db_related"], axis=1)

        print(self.tweet_df.head())

        print("DB related tweets in df:", len(self.tweet_df))

        # Count the number of individual users in data
        user_id_df = self.tweet_df.drop_duplicates(subset="user_id")
        print("Individual users with DB related Tweets in history", len(user_id_df))

        # Save tweets as csv file
        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        #self.tweet_df.to_csv("Data/tweets_" + time + ".csv", sep="$")


def main():
    # Read in storage files
    """storage_dir_path = "C://Users//19joh//Desktop//testdir//"

    city_key_file_path = "C://Users//19joh//Desktop//deutschland_gemeinden_short.txt"

    tweet_processing = DataProcessing()
    tweet_processing.load_city_key_data(city_key_file_path)

    tweet_processing.create_df_with_storage_data(input_dir_path=storage_dir_path)

    tweet_processing.create_short_tweet_df()

    tweet_processing.extract_individual_user_ids()"""


if __name__ == '__main__':
    main()