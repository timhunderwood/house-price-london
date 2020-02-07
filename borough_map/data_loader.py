import os
import logging
from typing import Optional, List, Tuple

import pandas
import numpy

logging.basicConfig()
LOGGER = logging.getLogger(__file__)
LOGGER.setLevel("DEBUG")


class DataLoader:
    """Responsible for loading aggregating and analysing the data set.

    Reads the raw data file of Price paid data UK and creates an aggregated file
    that has all the data required for plotting. If it finds this file in future runs
    it does not read the raw file again (caching).

    The public method load_prepare_and_aggregate_data should be called once after instantiation
    to load the data frame. Then public methods can be called on the object to get data.
    """

    def __init__(self, price_paid_file_name):
        """Instantiate the DataLoader.

        All dataframes are initialised as None and then populated by calling load_prepare_and_aggregate_data.
        """
        self._raw_df: Optional[pandas.DataFrame] = None
        self._borough_data: Optional[pandas.DataFrame] = None
        self._aggregated_data: Optional[pandas.DataFrame] = None

        self.all_london_boroughs = self.get_all_london_boroughs()
        self.max_price: int = 100e6
        self._data_directory: str = os.path.join(os.getcwd(), "..", "data")
        self._cached_data_path: str = os.path.join(
            self._data_directory, "london_aggregated_cache.csv"
        )
        self._cached_yearly_data_path: str = os.path.join(
            self._data_directory, "yearly_london_aggregated_cache.csv"
        )
        self._price_paid_data_path: str = os.path.join(
            self._data_directory, price_paid_file_name
        )

    def get_all_london_boroughs(self) -> List[str]:
        """Returns list of all london boroughs in upper case

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
        return [
            str.upper(borough)
            for borough in inner_london_boroughs + outer_london_boroughs
        ]

    def _load_data(self, path: str) -> pandas.DataFrame:
        """Reads the data from CSV and loads it into a dataframe.

        The private _data attribute is populated with the result.

        :param path:
        :return: the raw full data frame of price paid data UK
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
            numpy.int64,
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
        raw_df = pandas.read_csv(
            path,
            delimiter=",",
            names=use_names,
            index_col=0,
            usecols=use_cols,
            dtype=use_dtypes,
            engine="c",
            compression="gzip",
        )
        LOGGER.debug("completed reading raw data")
        return raw_df

    def _update_data_for_london_analysis(self) -> pandas.DataFrame:
        """Updates the _data DataFrame to only include london boroughs.

        :return: the raw dataframe but filtered to only london boroughs
        """
        df = self._raw_df.copy()
        df["date_time"] = pandas.to_datetime(df["date_time"])
        df["year"] = df["date_time"].dt.year
        df["month"] = df["date_time"].dt.month
        df["day"] = df["date_time"].dt.day
        df["epoch_seconds"] = df["date_time"].astype("int64")
        df["is_london_borough"] = df["address_county_1"].isin(self.all_london_boroughs)
        df["address_county_1"] = df["address_county_1"].astype("category")
        df = df.loc[
            df["price_gbp"] <= self.max_price
        ]  # not interested in prices over 100 MM
        LOGGER.debug("filtered to london only")
        return df.loc[df["is_london_borough"]]

    def _aggregate_data(self) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
        """Get the borough df and aggregated df.

        Performs group bys to produce much smaller datasets required for the plots.
        :return: tuple of pandas Dataframes that have been aggregated.
        """
        borough_df = self._raw_df.groupby(
            ["year", "month", "address_county_1"]
        ).aggregate({"price_gbp": ["mean", "median", "count"], "date_time": "first"})
        borough_df.columns = ["_".join(col) for col in borough_df.columns]
        borough_df = borough_df.dropna(subset=["price_gbp_count"])
        borough_df["date_time_first"] = borough_df["date_time_first"].values.astype(
            "datetime64[M]"
        )
        borough_df = borough_df.rename(columns={"date_time_first": "date_time"})

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
        return borough_df, aggregated_data

    def load_prepare_and_aggregate_data(self) -> None:
        """Loads prepares and aggregates the self._data attribute.

        Updates the dataframe attributes of the object.
        """
        if self._cached_data_available():
            LOGGER.info("Reading cached aggregated data.")
            self._borough_data, self._aggregated_data = self._load_cached_data()
        else:
            LOGGER.info("Did not find cached aggregated data. Reading raw data.")
            self._raw_df = self._load_data(self._price_paid_data_path)
            self._raw_df = self._update_data_for_london_analysis()
            self._borough_data, self._aggregated_data = self._aggregate_data()
            self._save_data_to_disk()

    def _cached_data_available(self) -> bool:
        """Checks if the cached data is available."""
        return os.path.exists(self._cached_data_path) and os.path.exists(
            self._cached_yearly_data_path
        )

    def get_mean_prices(self, year: int, month: int) -> pandas.Series:
        """Get the mean price for that year, month as a series for all address_county_1s"""
        LOGGER.debug("getting mean price for (%s, %s)", year, month)
        return self._borough_data.loc[(year, month)][("price_gbp_mean")]

    def get_median_prices(self, year: int, month: int) -> pandas.Series:
        """Get the median price for that year, month as a series for all address_county_1s"""
        LOGGER.debug("getting mean price for (%s, %s)", year, month)
        return self._borough_data.loc[(year, month)][("price_gbp_median")]

    def _save_data_to_disk(self) -> None:
        """Save the borough and aggregated dataframes to disk for caching."""
        self._borough_data.to_csv(self._cached_data_path, index=True)
        self._aggregated_data.to_csv(self._cached_yearly_data_path, index=True)

    def _load_cached_data(self) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
        """Loads cached borough and aggregated dataframe files and loads them.

        :return: Tuple of the borough_df and aggregated_df
        """
        borough_data = pandas.read_csv(self._cached_data_path, index_col=[0, 1, 2])
        aggregated_data = pandas.read_csv(
            self._cached_yearly_data_path, index_col=[0, 1], parse_dates=["date_time"]
        )
        return borough_data, aggregated_data

    def get_line_data(self) -> Tuple[numpy.ndarray, numpy.ndarray]:
        """Gets the data necessary for the median line data below the cmap plot.

        :return: Tuple of the x (datetime) and y (median price) data for the plot.
        """
        x_data, y_data = (
            self._aggregated_data["date_time"],
            self._aggregated_data["price_gbp_median"],
        )
        return x_data.values, y_data.values


if __name__ == "__main__":
    data_loader = DataLoader()
    data_loader.load_prepare_and_aggregate_data()
    print(data_loader.get_mean_prices(1995, 1))
