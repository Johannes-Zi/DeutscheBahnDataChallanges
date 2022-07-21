import pandas as pd
import geopandas as gpd


class TweetMapper:

    def __init__(self):
        self.data_frame = None
        self.geo_data_frame = None

    def extract_geo_data(self, csv_file):
        self.data_frame = pd.read_csv(csv_file, sep="$")

        new_frame = self.data_frame[0]

        print(new_frame)

    def get_data(self, csv_file):
        self.geo_data_frame = gpd.GeoDataFrame(csv_file)

        self.geo_data_frame.head()


def main():
    csv_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/06.07.2022/tweets_06-07-2022_15-07_nine.csv"

    new_mapper = TweetMapper()
    new_mapper.extract_geo_data(csv_file)


if __name__ == '__main__':
    main()
