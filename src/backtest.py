import datetime
import pandas as pd
import numpy as np
import tqdm

from metrics_calculator import MetricsCalculator
from portfolio import Portfolio

class BacktestSimulator():
    def __init__(self, metrics_database_url, metrics_tearsheet_directory, metrics_mode, metrics_periods_per_year, metrics_risk_free_rate=0.0, metrics_compounded=True):
        self.metrics_database_url=metrics_database_url
        self.metrics_tearsheet_directory=metrics_tearsheet_directory
        self.metrics_mode=metrics_mode
        self.metrics_risk_free_rate=metrics_risk_free_rate
        self.metrics_compounded=metrics_compounded
        self.metrics_periods_per_year=metrics_periods_per_year
        self.portfolio=None
        self.metrics_calculator=MetricsCalculator()
        self.strategy={}
        self.universe={}
        self.universe_json_constructor={}
        # Default attributes
        self.allowed_metrics_mode=("basic", "full")
        self.allowed_metrics_periods_per_year=(12, 252, 365)
        self.datetime_format="%Y%m%d_%H%M%S"
        self.start_backtest_datetime_string=None
        # Validation methods
        self.__validate_metrics_periods_per_year()
        self.__validate_metrics_mode()
        
        
    def register_strategy(self, strategy):
        if strategy.name in self.strategy:
            raise ValueError("Cannot have strategies of the same name! (Duplicated strategy name: {})".format(strategy.name))
        self.strategy[strategy.name] = strategy
        
    def create_portfolio(self, portfolio_name, portfolio_env, autobalance=False):
        self.portfolio = Portfolio(portfolio_name, portfolio_env, autobalance)
        for strategy in self.strategy:
            self.portfolio.register_strategy(strategy)
    
    def register_universe(self, universe, universe_json_constructor):
        self.universe[universe.name] = universe
        self.universe_json_constructor[universe.name] = universe_json_constructor
        
    def run(self, universe_name=None, symbols=None):
        # Assert all inputs are created first
        self.__validate_inputs()
        # Create datetimestamp
        start_run_datetime = datetime.datetime.now()
        start_run_datetime_string = datetime.datetime.strftime(start_run_datetime, "%Y%m%d_%H%M%S")
        self.start_backtest_datetime_string = start_run_datetime_string
        print("Backtest has started at {}".format(datetime.datetime.strftime(start_run_datetime, "%Y-%m-%d_%H:%M:%S")))
        # Run actual simulation
        if universe_name is None and symbols is None:
            self.run_all_exchanges_and_all_symbols()
        elif universe_name is None and symbols is not None:
            self.run_all_exchanges_and_specific_symbols()
        elif universe_name is not None and symbols is None:
            self.run_specific_exchange_and_all_symbols(universe_name)
        else:
            self.run_specific_exchange_and_specific_symbols(universe_name, symbols)
        # Compute metrics
        print("Generating metrics results...")
        self.generate_metrics_and_save()
        end_run_datetime = datetime.datetime.now()
        print("Backtest has successfully completed at {}".format(datetime.datetime.strftime(end_run_datetime, "%Y-%m-%d_%H:%M:%S")))
            
    def run_all_exchanges_and_all_symbols(self):
        print("All exchanges all symbols backtest mode selected!")
        for universe_name in self.universe:
            for symbol in self.universe[universe_name].data.keys():
                self.run_single(universe_name, symbol)

    def run_all_exchanges_and_specific_symbols(self, symbols):
        print("All exchanges specific symbols backtest mode selected!")
        for universe_name in self.universe:
            for symbol in symbols:
                self.run_single(universe_name, symbol)
    
    def run_specific_exchange_and_all_symbols(self, universe_name):
        print("Specific exchange all symbols backtest mode selected!")
        universe_data = self.universe[universe_name]
        for symbol in self.universe[universe_name].data:
            self.run_single(universe_name, symbol)
            
    def run_specific_exchange_and_specific_symbols(self, universe_name, symbols):
        print("Specific exchanges specific symbols backtest mode selected!")
        for symbol in symbols:
            self.run_single(universe_name, symbol)

    def run_single(self, universe_name, symbol):
        universe_data = self.universe[universe_name].data
        symbol_data = universe_data[symbol]
        strategy_names = list(self.strategy.keys())
        number_of_strategies = len(strategy_names)
        print("Running simulation for universe '{}' on symbol '{}' for all strategies...".format(universe_name, symbol))
        for i in range(len(strategy_names)):
            strategy = strategy_names[i]
            print("Strategy {}/{}".format(i + 1, len(self.strategy.keys())))
            print("Backtesting strategy: '{}'".format(strategy))
            for index, row in tqdm.tqdm(symbol_data.iterrows(), total=len(symbol_data)):
                constructed_json = self.universe_json_constructor[universe_name].construct_json(symbol, index, row, self.universe[universe_name].interval)
                if symbol in self.strategy[strategy].symbols:
                    closing_period_position = self.strategy[strategy].symbols[symbol]["position"]
                else:
                    closing_period_position = None
                position_json = self.strategy[strategy].run(constructed_json)
                position_json["universe_name"] = universe_name
                # Because portfolio tracks position at close while the position json tracks the change in position after close
                # Hence, the portfolio needs to take in the previous position when a change in position occurs
                # As such, replace position_json["position"] with closing_period_position
                position_json["position"] = closing_period_position
                self.portfolio.update_position(position_json)
        print("Finished simulation!")
        print("")

    def generate_metrics_and_save(self):
        # TODO: Calculate weighted metrics ###
        datetime_index = None
        for strategy in self.portfolio.strategies:
            strategy_returns = None
            strategy_cumulative_returns = None
            strategy_strat_returns = None
            strategy_strat_cumulative_returns = None
            for universe_name in self.portfolio.strategies[strategy]["universe"]:
                universe_returns = None
                universe_cumulative_returns = None
                universe_strat_returns = None
                universe_strat_cumulative_returns = None
                # Extract some meta data to be used when generating report for universe level
                universe_interval = self.universe[universe_name].interval
                universe_start_datetime = self.universe[universe_name].start_datetime
                universe_end_datetime = self.universe[universe_name].end_datetime
                universe_datetime_index = pd.date_range(universe_start_datetime, universe_end_datetime, freq=universe_interval)
                for symbol in self.portfolio.strategies[strategy]["universe"][universe_name]["symbols"]:
                    symbol_data = self.universe[universe_name].data[symbol]
                    symbol_datetime_indexes = symbol_data.index
                    if datetime_index is None:
                        datetime_index = symbol_datetime_indexes
                    returns = np.nan_to_num(symbol_data["r"])
                    strat_positions = self.portfolio.strategies[strategy]["universe"][universe_name]["symbols"][symbol]["position"]
                    strat_returns = self.metrics_calculator.calculate_strategy_returns(returns, strat_positions)
                    returns = pd.Series(returns, index=datetime_index)
                    strat_returns = pd.Series(strat_returns, index=datetime_index)
                    # Generate metrics and save for symbol level results
                    returns, cumulative_returns, strat_returns, strat_cumulative_returns  =\
                        self.metrics_calculator.run_simple(
                            strat_returns=strat_returns,
                            returns=returns,
                            returns_level="symbol",
                            start_backtest_datetime_string=self.start_backtest_datetime_string,
                            strategy_name=strategy,
                            universe_name=universe_name,
                            symbols_name=symbol,
                            risk_free_rate=self.metrics_risk_free_rate,
                            mode=self.metrics_mode,
                            compounded=self.metrics_compounded,
                            periods_per_year=self.metrics_periods_per_year,
                            database_url=self.metrics_database_url,
                            tearsheet_directory=self.metrics_tearsheet_directory,
                            output_returns=True
                    )
                    # Add to universe for both buy and hold returns and strat returns
                    self.portfolio.strategies[strategy]["universe"][universe_name]["symbols"][symbol]["metrics"]["returns"] = returns
                    self.portfolio.strategies[strategy]["universe"][universe_name]["symbols"][symbol]["metrics"]["strat_returns"] = strat_returns
                    self.portfolio.strategies[strategy]["universe"][universe_name]["symbols"][symbol]["metrics"]["cum_returns"] = cumulative_returns
                    self.portfolio.strategies[strategy]["universe"][universe_name]["symbols"][symbol]["metrics"]["strat_cum_returns"] = strat_cumulative_returns
                    # Formula is simple addition to get cumulative
                    if universe_returns is None:
                        universe_returns = np.array(returns)
                    else:
                        universe_returns = universe_returns + np.array(returns)
                    if universe_cumulative_returns is None:
                        universe_cumulative_returns = np.array(cumulative_returns)
                    else:
                        universe_cumulative_returns = universe_cumulative_returns + np.array(cumulative_returns)
                    if universe_strat_returns is None:
                        universe_strat_returns = np.array(strat_returns)
                    else:
                        universe_strat_returns = universe_strat_returns + np.array(strat_returns)
                    if universe_strat_cumulative_returns is None:
                        universe_strat_cumulative_returns = np.array(strat_cumulative_returns)
                    else:
                        universe_strat_cumulative_returns = universe_strat_cumulative_returns + np.array(strat_cumulative_returns)
                # Calcuate metrics for universe level returns
                self.metrics_calculator.run_simple(
                        strat_returns=pd.Series(universe_strat_returns, index=datetime_index),
                        returns=pd.Series(universe_returns, index=datetime_index),
                        returns_level="universe",
                        start_backtest_datetime_string=self.start_backtest_datetime_string,
                        strategy_name=strategy,
                        universe_name=universe_name,
                        symbols_name=None,
                        risk_free_rate=self.metrics_risk_free_rate,
                        mode=self.metrics_mode,
                        compounded=self.metrics_compounded,
                        periods_per_year=self.metrics_periods_per_year,
                        database_url=self.metrics_database_url,
                        tearsheet_directory=self.metrics_tearsheet_directory,
                        output_returns=False
                )
                # Create universe returns
                self.portfolio.strategies[strategy]["universe"][universe_name]["metrics"]["returns"] = universe_returns
                self.portfolio.strategies[strategy]["universe"][universe_name]["metrics"]["cum_returns"] = universe_cumulative_returns
                self.portfolio.strategies[strategy]["universe"][universe_name]["metrics"]["strat_returns"] = universe_strat_returns
                self.portfolio.strategies[strategy]["universe"][universe_name]["metrics"]["strat_cum_returns"] = universe_strat_cumulative_returns
                # Add to strategy for both buy and hold returns and strat returns
                # Formula is simple addition to get cumulative
                if strategy_returns is None:
                    strategy_returns = np.array(universe_returns)
                else:
                    strategy_returns = strategy_returns + np.array(universe_returns)
                if strategy_cumulative_returns is None:
                    strategy_cumulative_returns = np.array(universe_cumulative_returns)
                else:
                    strategy_cumulative_returns = strategy_cumulative_returns + np.array(universe_cumulative_returns)
                if strategy_strat_returns is None:
                    strategy_strat_returns = np.array(universe_strat_returns)
                else:
                    strategy_strat_returns = strategy_strat_returns + np.array(universe_strat_returns)
                if strategy_strat_cumulative_returns is None:
                    strategy_strat_cumulative_returns = np.array(universe_strat_cumulative_returns)
                else:
                    strategy_strat_cumulative_returns = strategy_strat_cumulative_returns + np.array(universe_strat_cumulative_returns)
            # Calcuate metrics for strategy level returns
            self.metrics_calculator.run_simple(
                    strat_returns=pd.Series(strategy_strat_returns, index=datetime_index),
                    returns=pd.Series(strategy_returns, index=datetime_index),
                    returns_level="strategy",
                    start_backtest_datetime_string=self.start_backtest_datetime_string,
                    strategy_name=strategy,
                    universe_name=None,
                    symbols_name=None,
                    risk_free_rate=self.metrics_risk_free_rate,
                    mode=self.metrics_mode,
                    compounded=self.metrics_compounded,
                    periods_per_year=self.metrics_periods_per_year,
                    database_url=self.metrics_database_url,
                    tearsheet_directory=self.metrics_tearsheet_directory,
                    output_returns=False
            )
            # Create strategy returns
            self.portfolio.strategies[strategy]["metrics"]["returns"] = strategy_returns
            self.portfolio.strategies[strategy]["metrics"]["cum_returns"] = strategy_cumulative_returns
            self.portfolio.strategies[strategy]["metrics"]["strat_returns"] = strategy_strat_returns
            self.portfolio.strategies[strategy]["metrics"]["strat_cum_returns"] = strategy_strat_cumulative_returns

    def get_exchanges(self):
        return [self.exchanges[i].name for i in self.exchanges]

    def __validate_inputs(self):
        try:
            assert len(self.strategy.keys()) > 0, "No strategy was loaded into the environment! Please ensure at least one strategy is loaded into the enivornment!"
            assert len(self.universe.keys()) > 0, "No universe data was loaded into the environment! Please ensure at least one data source is loaded into the enivornment!"
            assert self.portfolio is not None, "No portfolio was created! Please create portfolio before running simulation!"
        except AssertionError as err:
            print(err)
            
    def __validate_metrics_mode(self):
        if self.metrics_mode not in self.allowed_metrics_mode:
            raise ValueError("Invalid metrics_mode specified! Allowed values are: {}".format(self.allowed_metrics_mode))
            
    def __validate_metrics_periods_per_year(self):
        if self.metrics_periods_per_year not in self.allowed_metrics_periods_per_year:
            raise ValueError("Invalid metrics_periods_per_year specified! Allowed values are: {}".format(self.allowed_metrics_periods_per_year))