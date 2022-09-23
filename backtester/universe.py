import datetime
import os

import numpy as np
import pandas as pd
import tqdm

class Universe():
    def __init__(self, name, data_source_type, data_source_url, start_datetime, end_datetime, raw_interval, resample_interval, datetime_format, instrument_list=None):
        self.name=name
        self.data={}
        self.start_datetime=start_datetime
        self.end_datetime=end_datetime
        self.raw_interval=raw_interval
        self.resample_interval=resample_interval
        self.datetime_format=datetime_format
        self.instrument_list=instrument_list
        self.data_source_type=data_source_type
        self.data_source_url=data_source_url
        self.allowed_data_source_types=("parquet")
        self.allowed_datetime_formats={"date": "%Y-%m-%d", "datetime": "%Y-%m-%d %H:%M:%S"}
        self.__validate_datetime_format()

    def __validate_datetime_format(self):
        if self.datetime_format not in self.allowed_datetime_formats:
            raise ValueError("datetime_format can only be the following: {}".format(tuple(self.allowed_datetime_formats.keys())))

    def load(self):
        if self.data_source_type == "csv":
            raise NotImplementedError("Current system accepts only parquet!")
        elif self.data_source_type == "parquet":
            self.load_parquet_data()
        elif self.data_source_type == "database":
            raise NotImplementedError("Current system accepts only parquet!")
        else:
            raise ValueError("Invalid data_source_type specified! Allowed data source types are: {}".format(self.allowed_data_source_types))

    def load_parquet_data(self):
        rule = {
            "o": "first",
            "h": "max",
            "l": "min",
            "c": "last",
            "v": "sum",
            "a": "sum"
        }
        # If self.instrument_list is not specified, assume loading all symbols
        if self.instrument_list is None:
            instrument_list = self.get_all_instruments_from_directory()
            self.instrument_list = instrument_list
        else:
            instrument_list = self.instrument_list
        start_datetime_object = datetime.datetime.strptime(
            self.start_datetime, self.allowed_datetime_formats[self.datetime_format]
        )
        end_datetime_object = datetime.datetime.strptime(
            self.end_datetime, self.allowed_datetime_formats[self.datetime_format]
        )
        # Need add 1 day as if date is specified, will only take at time 00:00:00
        # E.g. if end_datetime is 2021-01-31, date will be taken only up till 2021-01-31 00:00:00
        # but the subsequent time belonging to 2021-01-31 will be ignored. Hence, time delta is added
        # and as a result, the filtering function subsequently look for lesser than rather than
        # lesser than or equals to
        end_datetime_object += datetime.timedelta(days=1)
        pbar = tqdm.tqdm(instrument_list)
        for symbol in pbar:
            pbar.set_description("Loading parquet data for {}".format(symbol))
            df = pd.read_parquet(self.data_source_url + "/" + symbol + "_" + self.raw_interval + ".parquet")
            df = self.utc_to_datetime_index_conversion(df)
            df = df.loc[(df.index.to_pydatetime() >= start_datetime_object) & (df.index.to_pydatetime() < end_datetime_object)]
            df = df.resample(self.resample_interval).agg(rule)
            df = self.create_custom_columns(df)
            self.data[symbol] = df

    def create_custom_columns(self, df):
        # Custom values derived from raw
        # log_o represents the log open price
        df["log_o"] = np.log(df["o"])
        # log_h represents the log high price
        df["log_h"] = np.log(df["h"])
        # log_l represents the log low price
        df["log_l"] = np.log(df["l"])
        # log_c represents the log close price
        df["log_c"] = np.log(df["c"])
        # p represents p&l which is the difference between current closing price and previous period closing price
        df["p"] = df["c"].diff()
        # r represents the returns percentage via decimals which is the (current closing price - previous closing price) / previous closing price
        df["r"] = df["c"].pct_change()
        # d represents spread between the highest price and lowest price
        df["d"] = df["h"] - df["l"]
        # s represents the spread between the open and closing price
        df["s"] = df["c"] - df["o"]
        # log_r represents the log returns
        df["log_r"] = np.log(df["c"] / df["c"].shift(1))
        return df

    def get_all_instruments_from_directory(self):
        all_instrument_files = os.listdir(self.data_source_url)
        return list(map(lambda x : x.split('_')[0], all_instrument_files))

    def utc_to_datetime_index_conversion(self, df):
        df.index = df.index.map(lambda x : datetime.datetime.fromtimestamp(x))
        return df