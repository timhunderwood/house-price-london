import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig()
LOGGER = logging.getLogger(__file__)
LOGGER.setLevel("DEBUG")
LOGGER.info("data loader")
LOGGER.debug("data loader")


class DataLoader(object):
    """
    Responsible for loading aggregating and analysing the data set
    """

    def __init__(self):
        self._raw_df = None
        self.all_london_boroughs = self.get_all_london_boroughs()
        self.max_price = 100e6
        self._data_directory = os.path.join(os.getcwd(), "..", "data")
        self._cached_data_path = os.path.join(
            self._data_directory, "london_aggregated_cache.csv"
        )
        self._cached_yearly_data_path = os.path.join(
            self._data_directory, "yearly_london_aggregated_cache.csv"
        )
        self.default_data_path = os.path.join(
            self._data_directory, "pp-complete_20190112.csv.gz"
        )

    def get_all_london_boroughs(self):
        """
        Returns list of all london boroughs in upper case

        :return list(str): list of all london boroughs in upper case
        """

        inner_london_boroughs = [
            "CITY OF LONDON",
            "Camden",
            "Greenwich",
            "Hackney",
            "Hammersmith and Fulham",
            "Islington",
            "Kensington and Chelsea",
            "Lambeth",
            "Lewisham",
            "Southwark",
            "Tower Hamlets",
            "Wandsworth",
            "City of Westminster",
        ]
        outer_london_boroughs = [
            "Barking and Dagenham",
            "Barnet",
            "Bexley",
            "Brent",
            "Bromley",
            "Croydon",
            "Ealing",
            "Enfield",
            "Haringey",
            "Harrow",
            "Havering",
            "Hillingdon",
            "Hounslow",
            "Kingston upon Thames",
            "Merton",
            "Newham",
            "Redbridge",
            "Richmond upon Thames",
            "Sutton",
            "Waltham Forest",
        ]
        inner_london_boroughs = list(map(str.upper, inner_london_boroughs))
        outer_london_boroughs = list(map(str.upper, outer_london_boroughs))
        return inner_london_boroughs + outer_london_boroughs

    def _load_data(self, path):
        """
        Reads the data from CSV and loads it into a dataframe. The private _data attribute is populated with the result.

        :param path:
        :return None:
        """
        names = [
            "transaction_id",
            "price_gbp",
            "date_time",
            "post_code",
            "property_type",
            "is_new_build",
            "estate_type",
            "address_number",
            "address_road",
            "address_locality",
            "address_town",
            "address_district",
            "address_county_1",
            "adress_county_2",
            "flag_4",
            "flag_5",
        ]
        dtypes = [
            str,
            np.int64,
            str,
            str,
            "category",
            "category",
            "category",
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
        ]
        use_cols = [0, 1, 2, 3, 4, 5, 6, 12]
        use_names = [names[i] for i in use_cols]
        use_dtypes = {names[i]: dtypes[i] for i in use_cols}
        raw_df = pd.read_csv(
            path,
            delimiter=",",
            names=use_names,
            index_col=0,
            usecols=use_cols,
            dtype=use_dtypes,
            engine="c",
            compression="gzip",
        )
        self._raw_df = raw_df
        LOGGER.debug("completed reading raw data")

    def _update_data_for_london_analysis(self):
        """
        Updates the _data DataFrame to only include london boroughs

        :return:
        """
        df = self._raw_df.copy()
        df["date_time"] = pd.to_datetime(df["date_time"])
        df["year"] = df["date_time"].dt.year
        df["month"] = df["date_time"].dt.month
        df["day"] = df["date_time"].dt.day
        df["epoch_seconds"] = df["date_time"].astype("int64")
        df["is_london_borough"] = df["address_county_1"].isin(self.all_london_boroughs)
        df["address_county_1"] = df["address_county_1"].astype("category")
        df = df.loc[
            df["price_gbp"] <= self.max_price
        ]  # not interested in prices over 100 MM
        self._raw_df = df.loc[df["is_london_borough"]]
        LOGGER.debug(self._raw_df["address_county_1"].unique())
        LOGGER.debug("filtered to london only")

    def _aggregate_data(self):
        """
        Aggregates the data to have an index of year, month, london borough and value price gbp --> mean or count

        :return:
        """
        df = self._raw_df.groupby(["year", "month", "address_county_1"]).aggregate(
            {"price_gbp": ["mean", "median", "count"], "date_time": "first"}
        )
        df.columns = ["_".join(col) for col in df.columns]
        df = df.dropna(subset=["price_gbp_count"])
        df["date_time_first"] = df["date_time_first"].values.astype("datetime64[M]")
        df = df.rename(columns={"date_time_first": "date_time"})

        aggregated_data = self._raw_df.groupby(["year", "month"]).aggregate(
            {"price_gbp": ["mean", "median", "count"], "date_time": "first"}
        )
        aggregated_data.columns = ["_".join(col) for col in aggregated_data.columns]
        aggregated_data = aggregated_data.dropna(subset=["price_gbp_count"])
        aggregated_data["date_time_first"] = aggregated_data[
            "date_time_first"
        ].values.astype("datetime64[M]")
        aggregated_data = aggregated_data.rename(
            columns={"date_time_first": "date_time"}
        )

        self._borough_data = df
        self._aggregated_data = aggregated_data

    def load_prepare_and_aggregate_data(self):
        """
        Loads prepares and aggregates the self._data attribute

        :return:
        """
        if self._cached_data_available():
            self._load_cached_data()
        else:
            self._load_data(self.default_data_path)
            self._update_data_for_london_analysis()
            self._aggregate_data()
            self._save_data_to_disk()

    def _cached_data_available(self):
        return os.path.exists(self._cached_data_path) and os.path.exists(
            self._cached_yearly_data_path
        )

    def get_mean_prices(self, year, month):
        LOGGER.debug("getting mean price for (%s, %s)", year, month)
        return self._borough_data.loc[(year, month)][("price_gbp_mean")]

    def get_median_prices(self, year, month):
        LOGGER.debug("getting mean price for (%s, %s)", year, month)
        return self._borough_data.loc[(year, month)][("price_gbp_median")]

    def _save_data_to_disk(self):
        """
        save data to disk

        :return:
        """
        self._borough_data.to_csv(self._cached_data_path, index=True)
        self._aggregated_data.to_csv(self._cached_yearly_data_path, index=True)

    def _load_cached_data(self):
        """
        Loads the data
        :return:
        """
        self._borough_data = pd.read_csv(self._cached_data_path, index_col=[0, 1, 2])
        self._aggregated_data = pd.read_csv(
            self._cached_yearly_data_path, index_col=[0, 1], parse_dates=["date_time"]
        )
        self._raw_data = None

    def get_line_data(self):
        x_data, y_data = (
            self._aggregated_data["date_time"],
            self._aggregated_data["price_gbp_median"],
        )
        return x_data.values, y_data.values


if __name__ == "__main__":
    data_loader = DataLoader()
    data_loader.load_prepare_and_aggregate_data()
    print(data_loader.get_mean_prices(1995, 1))
