import os

import pandas as pd
import numpy as np


class MetricsCalculator():
    def __init__(self):
        self.allowed_returns_level=("strategy", "universe", "symbol")

    def run_simple(self, strat_returns, returns, returns_level, start_backtest_datetime_string, strategy_name, universe_name, symbols_name, risk_free_rate, mode, compounded, periods_per_year, database_url, tearsheet_directory, output_returns=False):
        strat_cumulative_returns = self.calculate_cumulative_strategy_returns(strat_returns)
        cumulative_returns = self.calculate_cumulative_returns(returns)
        # Generate results and save
        self.generate_results_simple(strat_returns, returns, returns_level, start_backtest_datetime_string, strategy_name, universe_name, symbols_name, risk_free_rate, mode, compounded, periods_per_year, database_url, tearsheet_directory)
        if output_returns:
            return returns, cumulative_returns, strat_returns, strat_cumulative_returns
    
    ### Helpers ###
    def calculate_strategy_returns(self, returns, positions):
        return np.array(returns) * np.array(positions)
    
    def calculate_cumulative_strategy_returns(self, strategy_returns):
        return np.array(strategy_returns).cumsum()
    
    def calculate_returns(self, prices):
        price = np.array(prices)
        return np.nan_to_num(np.diff(prices)) / prices[1:]
    
    def calculate_cumulative_returns(self, returns):
        return np.array(returns).cumsum()
    
    def generate_results_simple(self, strat_returns, benchmark_returns, returns_level, start_backtest_datetime_string, strategy_name, universe_name, symbols_name, risk_free_rate, mode, compounded, periods_per_year, database_url, tearsheet_directory):
        # Create directory if needed
        self.__create_tearsheet_directory(tearsheet_directory, start_backtest_datetime_string)
        # Create necessary filename and plot title
        plot_title = self.__create_tearsheet_plot_title(
            start_backtest_datetime_string,
            strategy_name,
            universe_name,
            symbols_name
        )
        tearsheet_filename = self.__create_tearsheet_filename(
            start_backtest_datetime_string,
            strategy_name,
            universe_name,
            symbols_name
        )
        self.generate_results_tearsheet_simple(strat_returns, benchmark_returns, start_backtest_datetime_string, risk_free_rate, compounded, periods_per_year, plot_title, tearsheet_directory, tearsheet_filename)
        self.generate_results_database_simple(strat_returns, benchmark_returns, returns_level, start_backtest_datetime_string, strategy_name, universe_name, symbols_name, risk_free_rate, mode, compounded, periods_per_year, database_url, tearsheet_directory, tearsheet_filename)
    
    def generate_results_tearsheet_simple(self, strat_returns, benchmark_returns, start_backtest_datetime_string, risk_free_rate, compounded, periods_per_year, plot_title, tearsheet_directory, tearsheet_filename):
        # Output argument will cause it to write directly rather than to trigger a web download.
        # If download_filename and output specified, output is priortised
        qs.reports.html(
            returns=strat_returns,
            benchmark=benchmark_returns,
            rf=risk_free_rate,
            title=plot_title,
            compounded=compounded,
            periods_per_year=periods_per_year,
            download_filename=tearsheet_filename,
            output=tearsheet_directory + "/" + start_backtest_datetime_string + "/" + tearsheet_filename,
            grayscale=False,
            figfmt='svg',
            template_path=None,
            match_dates=False,
        )

    def generate_results_database_simple(self, strat_returns, benchmark_returns, returns_level, start_backtest_datetime_string, strategy_name, universe_name, symbols_name, risk_free_rate, mode, compounded, periods_per_year, database_url, tearsheet_directory, tearsheet_filename):
        metrics_df = qs.reports.metrics(
            returns=pd.DataFrame(strat_returns),
            benchmark=pd.DataFrame(benchmark_returns),
            rf=risk_free_rate,
            mode=mode,
            compounded=compounded,
            periods_per_year=periods_per_year,
            display=False,
            sep=False,
            prepare_returns=True,
            match_dates=False,
        )
        metrics_df_transposed = metrics_df.T
        # Step 1: Extract the Strategy and Benchmark row separately
        strategy_metrics = metrics_df_transposed.loc["Strategy"]
        benchmark_metrics = metrics_df_transposed.loc["Benchmark"]
        # Step 2: Convert both to dataframes
        strategy_metrics_df = pd.DataFrame(strategy_metrics)
        benchmark_metrics_df = pd.DataFrame(benchmark_metrics)
        # Step 3: Given extraction of these data changes them to Series, transposed them
        strategy_metrics_df = strategy_metrics_df.T
        benchmark_metrics_df = benchmark_metrics_df.T
        # Step 4: Rename column headers to include "Strategy" or "Benchmark" for identification
        strategy_metrics_df.columns = ["Strategy_" + i  for i in strategy_metrics_df.columns]
        benchmark_metrics_df.columns = ["Benchmark_" + i for i in benchmark_metrics_df.columns]
        # Step 5: reset_index as there is "Strategy" or "Benchmark" in the index and drop the column generated from resetting
        strategy_metrics_df = strategy_metrics_df.reset_index().drop(columns=["index"])
        benchmark_metrics_df = benchmark_metrics_df.reset_index().drop(columns=["index"])
        # Step 6: Combine the 2 dataframes together
        metrics_df = pd.concat([strategy_metrics_df, benchmark_metrics_df], axis=1)
        # Step 7: Create additional columns for meta data
        metrics_df["start_backtest_datetime"] = returns_level
        metrics_df["returns_level"] = returns_level
        metrics_df["strategy_name"] = strategy_name
        metrics_df["universe_name"] = universe_name
        metrics_df["symbols_name"] = symbols_name
        metrics_df["tearsheet_filepath"] = tearsheet_directory + "/" + tearsheet_filename
        # Step 8: Save output
        if database_url.endswith(".csv"):
            if os.path.isfile(database_url):
                database_df = pd.read_csv(database_url)
                database_df = database_df.append(metrics_df, ignore_index=True)
                database_df.to_csv(database_url, index=False)
            else:
                database_df = metrics_df
                database_df.to_csv(database_url)
        else:
            raise NotImplementedError("Only supports csv for now!")

    def __create_tearsheet_directory(self, tearsheet_directory, start_backtest_datetime_string):
        os.makedirs(tearsheet_directory + "/" + start_backtest_datetime_string, exist_ok=True)
        
    def __create_tearsheet_plot_title(self, start_backtest_datetime_string, strategy_name, universe_name, symbols_name):
        plot_title = "({}) Strategy {}".format(start_backtest_datetime_string, strategy_name)
        if universe_name is not None:
            plot_title += " for universe {}".format(universe_name)
            if symbols_name is not None:
                plot_title += " for symbols {}".format(symbols_name)
        plot_title += " tearsheet"
        return plot_title
    
    def __create_tearsheet_filename(self, start_backtest_datetime_string, strategy_name, universe_name, symbols_name):
        tearsheet_filename = f"{start_backtest_datetime_string}_{strategy_name}"
        if universe_name is not None:
            tearsheet_filename += f"_{universe_name}"
            if symbols_name is not None:
                tearsheet_filename += f"_{symbols_name}"
        tearsheet_filename += ".html"
        return tearsheet_filename