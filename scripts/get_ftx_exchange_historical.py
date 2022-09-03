import requests
import os
import time
import datetime
import json
import string
import random
import argparse

import pandas as pd

### Global Variables ###
FTX_BASE_API_ENDPOINT = "https://ftx.com/api/markets"
FTX_REQUEST_LIMIT = 1500


# TEMP NOTE: 2021-11-16 00:00:00 is used to extract data

def generate_symbol_filemame(symbol, timeframe, save_extension):
    ALLOWED_EXTENSIONS = {
        "csv": ".csv",
        "parquet": ".parquet",
        "pickle": ".pkl"
    }
    if save_extension not in ALLOWED_EXTENSIONS:
        raise ValueError("Invalid file format specified! Allowed file formats: {}".format(ALLOWED_EXTENSIONS.keys()))
    return symbol + "_" + timeframe + ALLOWED_EXTENSIONS[save_extension]


def recouncile_timestamp_for_existing_data(save_filepath, end_extraction_timestamp):
    if os.path.isfile(save_filepath):
        print("Existing data file found @ {}".format(save_filepath))
        if save_filepath.endswith("csv"):
            symbol_df = pd.read_csv(save_filepath)
        elif save_filepath.endswith("parquet"):
            symbol_df = pd.read_parquet(save_filepath)
        elif save_filepath.endswith("pkl"):
            symbol_df = pd.read_pickle(save_filepath)
        else:
            raise ValueError("Invalid file found! Please re-save existing data into the correct format (csv, parquet, pkl)!")
        earliest_timestamp_found = symbol_df.index[0]
        latest_timestamp_found = symbol_df.index[-1]
        return earliest_timestamp_found, latest_timestamp_found
    else:
        earliest_timestamp_found = end_extraction_timestamp
        return earliest_timestamp_found, None


def convert_raw_to_df(new_raw_data):
    def convert_to_dict(single_candlestick_list):
        output = {
            "timestamp": int(single_candlestick_list[0]), # need convert this string to int
            "o": single_candlestick_list[1],
            "c": single_candlestick_list[2],
            "h": single_candlestick_list[3],
            "l": single_candlestick_list[4],
            "v": single_candlestick_list[5],
        }
        return output
    def data_conversion(df):
        df.o = df.o.astype("float32")
        df.c = df.c.astype("float32")
        df.h = df.h.astype("float32")
        df.l = df.l.astype("float32")
        df.v = df.v.astype("float32")
        return df
    new_raw_data_json_list = list(map(convert_to_dict, new_raw_data))
    # Reindex backwards as daata are extracted with latest timestamp at the top
    new_raw_data_json_list = new_raw_data_json_list[::-1]
    new_raw_df = pd.DataFrame(new_raw_data_json_list)
    # Set timestamp as index
    new_raw_df.set_index("timestamp", inplace=True)
    new_raw_df = data_conversion(new_raw_df)
    return new_raw_df


def update_data_for_symbol_file(filepath, new_raw_data):
    new_raw_df = convert_raw_to_df(new_raw_data)
    if os.path.isfile(filepath):
        if filepath.endswith("csv"):
            symbol_df = pd.read_csv(filepath)
        elif filepath.endswith("parquet"):
            symbol_df = pd.read_parquet(filepath)
        elif filepath.endswith("pkl"):
            symbol_df = pd.read_pickle(filepath)
        else:
            raise ValueError("Invalid file found! Please re-save existing data into the correct format (csv, parquet, pkl)!")
        # new_raw_df is calling append as symbol_df timestamps should be later than new raw
        # given that this is extracting backwards
        symbol_df = new_raw_df.append(symbol_df)
    else:
        symbol_df = new_raw_df
    # Drop duplicated rows
    symbol_df = symbol_df[~symbol_df.index.duplicated(keep='first')]
    # Sort timestamp index
    symbol_df.sort_index(inplace=True)
    # Save updated df
    if filepath.endswith("csv"):
        symbol_df.to_csv(filepath)
    elif filepath.endswith("parquet"):
        symbol_df.to_parquet(filepath)
    elif filepath.endswith("pkl"):
        symbol_df.to_pickle(filepath)


def extract_all_klines_for_current_symbol(
        exchange_market_url,
        symbol,
        timeframe,
        end_extraction_timestamp,
        save_directory,
        save_file_extension,
        sleep_duration=10,
        request_limit=1500
    ):
    """Retrieve klines backwards from specified timestamp
    """
    ALLOWED_TIMEFRAME = {
        "1min": 60,
        "5min": 300,
        "15min": 900,
        "1hour": 3600,
        "4hour": 14400,
        "1day": 86400
    }
    if timeframe not in ALLOWED_TIMEFRAME:
        raise ValueError("Invalid timeframe specified. Allowed Timeframes: {}".format(ALLOWED_TIMEFRAME))
    save_filename = generate_symbol_filemame(symbol, timeframe, save_file_extension)
    save_filepath = os.path.join(save_directory, save_filename)
    klines = []
    current_unix_timestamp = int(end_extraction_timestamp)
    reached_start_of_data = False
    # Recouncile data if data filepath is found
    current_unix_timestamp, latest_existing_timestamp = recouncile_timestamp_for_existing_data(save_filepath, current_unix_timestamp)
    print("Begin extraction of symbol {} of timeframe {} at end timestamp {}".format(symbol, timeframe, end_extraction_timestamp))
    while not reached_start_of_data:
        try:
            result_limit_start_timestamp = int(current_unix_timestamp - request_limit * ALLOWED_TIMEFRAME[timeframe])
            response = requests.get(f"{exchange_market_url}/{symbol}/candles?resolution={ALLOWED_TIMEFRAME[timeframe]}&start_time={result_limit_start_timestamp}&end_time={current_unix_timestamp}")
            retrieved_klines = response.json()["result"]
            retrieved_klines = [
                [
                    single_json["time"],
                    single_json["open"],
                    single_json["high"],
                    single_json["low"],
                    single_json["close"],
                    single_json["volume"],
                ]
                for single_json in retrieved_klines
            ]
            if isinstance(retrieved_klines, list):
                klines += retrieved_klines
                current_unix_timestamp = int(retrieved_klines[-1][0])
                # Max number of rows retrieved is request_limit. Hence lesser than request_limit means reached first instance of data
                if len(retrieved_klines) < request_limit:
                    reached_start_of_data = True
                    print("Start of first historical data reached for symbol {}. Writing remaining data to filepath @ {}...".format(symbol, save_filepath))
                    update_data_for_symbol_file(save_filepath, klines)
                    print("Successfully updated {}!".format(save_filepath))
                    print("")
            else:
                reached_start_of_data = True
                print("No updates required for symbol {} @ filepath {}!".format(symbol, save_filepath))
                print("")
        except:
            data_rows_extracted = len(klines)
            print("Max request reached for requesting {} data (Current timestamp: {}, {} rows extracted)! While waiting for limit to be removed, writing extracted data to filepath @ {}...".format(symbol, datetime.datetime.fromtimestamp(current_unix_timestamp), data_rows_extracted, save_filepath))
            if data_rows_extracted > 0:
                update_data_for_symbol_file(save_filepath, klines)
                klines = []
                print("Successfully updated {}. Sleeping for {} seconds to wait for limit removal...".format(save_filepath, sleep_duration))
            else:
                print("Nothing to update @ {} as 0 rows extracted. Sleeping for {} seconds to wait for limit removal...".format(save_filepath, sleep_duration))
            time.sleep(sleep_duration)
            print("Sleep has ended!")
            print("")
    if latest_existing_timestamp is not None:
        print("Latest timestamp in dataset is earlier than requested timestamp to extract to. Extracting remaining data...")
        current_latest_existing_unix_timestamp = latest_existing_timestamp
        reached_requested_timestamp = False
        klines = []
        while not reached_requested_timestamp:
            try:
                response = requests.get(f"{exchange_market_url}/{symbol}/candles?resolution={ALLOWED_TIMEFRAME[timeframe]}&start_time={current_latest_existing_unix_timestamp}&end_time={end_extraction_timestamp}")
                retrieved_klines = response.json()["result"]
                retrieved_klines = [
                    [
                        single_json["time"],
                        single_json["open"],
                        single_json["high"],
                        single_json["low"],
                        single_json["close"],
                        single_json["volume"],
                    ]
                    for single_json in retrieved_klines
                ]
                if isinstance(retrieved_klines, list):
                    klines += retrieved_klines
                    current_latest_existing_unix_timestamp = int(retrieved_klines[0][0])
                    # Max number of rows retrieved is request_limit. Hence lesser than request_limit means reached first instance of data
                    if len(retrieved_klines) < request_limit:
                        reached_requested_timestamp = True
                        print("Successfully extracted historical data up to requested timestamp ({}) for symbol {}. Writing remaining data to filepath @ {}...".format(end_extraction_timestamp, symbol, save_filepath))
                        update_data_for_symbol_file(save_filepath, klines)
                        print("Successfully updated {}!".format(save_filepath))
                        print("")
                else:
                    reached_start_of_data = True
                    print("No updates required for symbol {} @ filepath {}!".format(symbol, save_filepath))
                    print("")
            except:
                data_rows_extracted = len(klines)
                print("Max request reached for requesting {} data (Current timestamp: {}, {} rows extracted)! While waiting for limit to be removed, writing extracted data to filepath @ {}...".format(symbol, datetime.datetime.fromtimestamp(current_latest_existing_unix_timestamp), data_rows_extracted, save_filepath))
                if data_rows_extracted > 0:
                    update_data_for_symbol_file(save_filepath, klines)
                    klines = []
                    print("Successfully updated {}. Sleeping for {} seconds to wait for limit removal...".format(save_filepath, sleep_duration))
                else:
                    print("Nothing to update @ {} as 0 rows extracted. Sleeping for {} seconds to wait for limit removal...".format(save_filepath, sleep_duration))
                time.sleep(sleep_duration)
                print("Sleep has ended!")
                print("")
    print("Extraction finished for symbol {} of timeframe {} at end timestamp {}. Data can be found at {}".format(symbol, timeframe, end_extraction_timestamp, save_filepath))
    print("")
    return klines


def extract_all_klines_for_all_symbols(
        exchange_market_url,
        timeframe,
        end_extraction_timestamp,
        save_directory,
        save_file_extension,
        sleep_duration,
        request_limit
    ):
    symbols_response = requests.get(exchange_market_url)
    list_of_symbols_json = symbols_response.json()
    spot_list_of_symbols_json = list(filter(lambda x : x["type"] == "spot", list_of_symbols_json["result"]))
    symbols_list = list(map(lambda x : x['name'], spot_list_of_symbols_json))
    print("Extraction job started!")
    for symbol in symbols_list:
        extract_all_klines_for_current_symbol(
            exchange_market_url,
            symbol,
            timeframe,
            end_extraction_timestamp,
            save_directory,
            save_file_extension,
            sleep_duration,
            request_limit
        )
    print("Extraction job has been successfully executed!")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract all symbols historical data from ftx based on specified timeframe')
    parser.add_argument('--timeframe', metavar='t', type=str, default="1min",
                        help='Timeframe of data to extract')
    parser.add_argument('--end_extraction_timestamp', metavar='d', type=str,
                        help='Last date to extract data from. Data will be extracted from the start to the specified date. Format is YYYY-MM-DD HH:MM:SS')
    parser.add_argument('--save_directory', metavar='s', type=str, default="data/parquet/1min",
                        help='Directory to save data')
    parser.add_argument('--save_file_extension', metavar='e', type=str, default='parquet',
                        help='File extension of saved data (csv, parquet, pkl)')
    parser.add_argument('--sleep_duration', metavar='p', type=int, default=10,
                        help='Number of seconds to sleep on timeout request')
    args = parser.parse_args()
    timeframe = args.timeframe
    end_extraction_timestamp = args.end_extraction_timestamp
    save_directory = args.save_directory
    save_file_extension = args.save_file_extension
    sleep_duration = args.sleep_duration
    # Process end_extraction_timestamp
    if end_extraction_timestamp is None:
        # Take timestamp of script execution
        end_extraction_timestamp = int(time.time())
    else:
        end_extraction_timestamp = datetime.datetime.strptime(end_extraction_timestamp, "%Y-%m-%d %H:%M:%S")
        end_extraction_timestamp = int(end_extraction_timestamp.timestamp())
    extract_all_klines_for_all_symbols(FTX_BASE_API_ENDPOINT, timeframe, end_extraction_timestamp, save_directory, save_file_extension, sleep_duration, FTX_REQUEST_LIMIT)