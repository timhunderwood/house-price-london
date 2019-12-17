import pandas as pd
import numpy as np
import os


class DataLoader(object):
    """
    Responsible for loading aggregating and analysing the data set
    """

    def __init__(self):
        self._data = None
        self.all_london_boroughs = self.get_all_london_boroughs()
        self.max_price = 100E6
        self._data_directory = os.path.join(os.getcwd(), '..', '..', 'data')
        self._cached_data_path = os.path.join(self._data_directory,'london_aggregated_cache.csv' )
        self.default_data_path = os.path.join(self._data_directory, 'pp-complete.csv.gz')

    def get_all_london_boroughs(self):
        """
        Returns list of all london boroughs in upper case

        :return list(str): list of all london boroughs in upper case
        """

        inner_london_boroughs = ['CITY OF LONDON', 'Camden', 'Greenwich', 'Hackney', 'Hammersmith and Fulham',
                                 'Islington', 'Kensington and Chelsea', 'Lambeth', 'Lewisham', 'Southwark',
                                 'Tower Hamlets', 'Wandsworth', 'City of Westminster']
        outer_london_boroughs = ['Barking and Dagenham', 'Barnet', 'Bexley', 'Brent', 'Bromley', 'Croydon', 'Ealing',
                                 'Enfield', 'Haringey', 'Harrow', 'Havering', 'Hillingdon', 'Hounslow',
                                 'Kingston upon Thames', 'Merton', 'Newham', 'Redbridge', 'Richmond upon Thames',
                                 'Sutton', 'Waltham Forest']
        inner_london_boroughs = list(map(str.upper, inner_london_boroughs))
        outer_london_boroughs = list(map(str.upper, outer_london_boroughs))
        return inner_london_boroughs + outer_london_boroughs

    def _load_data(self, path):
        """
        Reads the data from CSV and loads it into a dataframe. The private _data attribute is populated with the result.

        :param path:
        :return None:
        """
        names = ['transaction_id', 'price_gbp', 'date_time', 'post_code', 'property_type', 'is_new_build',
                 'estate_type',
                 'address_number', 'address_road', 'address_locality', 'address_town', 'address_district',
                 'address_county_1', 'adress_county_2', 'flag_4', 'flag_5']
        dtypes = [str, np.int64, str, str, 'category', 'category', 'category', str, str, str, str, str, str, str, str,
                  str]
        use_cols = [0, 1, 2, 3, 4, 5, 6, 12]
        use_names = [names[i] for i in use_cols]
        use_dtypes = {names[i]: dtypes[i] for i in use_cols}
        raw_df = pd.read_csv(path, delimiter=',', names=use_names, index_col=0, usecols=use_cols,
                             dtype=use_dtypes, engine='c', compression='gzip')
        self._data = raw_df

    def _update_data_for_london_analysis(self):
        """
        Updates the _data DataFrame to only include london boroughs

        :return:
        """
        df = self._data.copy()
        df['date_time'] = pd.to_datetime(df['date_time'])
        df['year'] = df['date_time'].dt.year
        df['month'] = df['date_time'].dt.month
        df['day'] = df['date_time'].dt.day
        df['epoch_seconds'] = df['date_time'].astype('int64')
        df['address_county_1'] = df['address_county_1'].astype('category')
        df['is_london_borough'] = df['address_county_1'].isin(self.all_london_boroughs)
        df = df.loc[df['price_gbp'] <= self.max_price]  # not interested in prices over 100 MM
        self._data = df.loc[df['is_london_borough']].copy()

    def _aggregate_data(self):
        """
        Aggregates the data to have an index of year, month, london borough and value price gbp --> mean or count

        :return:
        """

        self._data = self._data.groupby(['year', 'month', 'address_county_1']).aggregate(
            {'price_gbp': ['mean', 'count']})
        self._data.columns = list(map('_'.join, self._data.columns.values))

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
        return os.path.exists(self._cached_data_path)

    def get_mean_prices(self, year, month):
        return self._data.loc[(year,month)][('price_gbp_mean')]

    def _save_data_to_disk(self):
        """
        save data to disk

        :return:
        """
        self._data.to_csv(self._cached_data_path, index=True)

    def _load_cached_data(self):
        """
        Loads the data
        :return:
        """
        self._data = pd.read_csv(self._cached_data_path, index_col=[0,1,2])


if __name__=='__main__':
    data_loader = DataLoader()
    data_loader.load_prepare_and_aggregate_data()
    print(data_loader.get_mean_prices(1995, 1))
