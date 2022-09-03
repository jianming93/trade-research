import yaml

from src.universe import Universe
from src.backtest import BacktestSimulator

CONFIG_FILE = "config.yaml"

if __name__ == "__main__":
    with open(CONFIG_FILE, "r") as file_stream:
        try:
            config = yaml.safe_load(file_stream)
        except yaml.YAMLError as exc:
            print(exc)
    # Initiate Backtest Environment
    backtest_simulator = BacktestSimulator(
        metrics_database_url=config["backtest"]["database_url"],
        metrics_tearsheet_directory=config["backtest"]["tearsheet_directory"],
        metrics_mode=config["backtest"]["mode"],
        metrics_periods_per_year=config["backtest"]["periods_per_year"],
        metrics_risk_free_rate=config["backtest"]["risk_free_rate"],
        metrics_compounded=config["backtest"]["compounded"]
    )
    # Register universes
    for universe_name in config["universe"]["universe_names"]:
        universe = Universe(
            name=universe_name,
            data_source_type=config["universe"]["universe_names"][universe_name]["data_source_type"],
            data_source_url=config["universe"]["universe_names"][universe_name]["data_source_url"],
            start_datetime=config["universe"]["configs"]["start_datetime"],
            end_datetime=config["universe"]["configs"]["end_datetime"],
            interval=config["universe"]["configs"]["interval"],
            datetime_format=config["universe"]["configs"]["datetime_format"],
            instrument_list=config["universe"]["configs"]["instrument_list"]
        )
        backtest_simulator.register_universe(universe)
    # Register Strategy
    for stategy_class_name in config["backtest"]["strategy"]:
        pass

