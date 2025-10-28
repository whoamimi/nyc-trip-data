# src/checks.py

import json
import pandas as pd
from datetime import datetime
import great_expectations as gx
from dataclasses import dataclass
from typing import Literal

from utils.setting import setting

# Definingt he Data Source
NYC_DATA_SOURCE = "nyc_taxi"
# Defining the Data assets as the types of data under this data source
NYC_DATA_ASSET = "nyc_taxi_data"
NYC_FARE_ASSET = "nyc_taxi_fare"

@dataclass
class QualityConfig:
    # In prod, this would be prefixed with timestamp
    trip_batch_name: str # e.g. trip_data_1
    fare_batch_name: str # e.g. trip_data_2
    nyc_data_asset: str = NYC_DATA_ASSET
    nyc_fare_asset: str = NYC_FARE_ASSET
    datasource_name: str = NYC_DATA_SOURCE

    input_trip_data_path = setting.trip_data_path
    input_trip_fare_path = setting.trip_fare_path
    output_dir = setting.output_report_path

    def _get_batch_file(self, base_path, name):
        path = base_path / f"{name}.csv"
        if not path.exists():
            raise FileExistsError(f"File not found: {path}")
        return path

    def output_file_name(self, suite: str):
        return self.output_dir / f"{datetime.now().isoformat()}_{suite}_{self.trip_batch_name}.json"

    @property
    def trip_batch_file(self):
        return self._get_batch_file(self.input_trip_data_path, self.trip_batch_name)

    @property
    def fare_batch_file(self):
        return self._get_batch_file(self.input_trip_fare_path, self.fare_batch_name)

class NYCTripQualityBuilder:
    """ In-memory Data Quality Testing. """

    def __init__(self, config: QualityConfig, suites_config: dict = setting.checks_config):
        self.config = config
        self.suites = suites_config

        self.ctx = gx.get_context() # type: ignore
        # Setup Data source
        self.data_source = self.ctx.data_sources.add_pandas(name=self.config.datasource_name)
        # Setup Data assets
        self.trip_data_asset = self.data_source.add_dataframe_asset(name=self.config.nyc_data_asset)
        self.trip_fare_asset = self.data_source.add_dataframe_asset(name=self.config.nyc_fare_asset)

    def configure(self):
        """ Load and prepare trip and fare datasets as in-memory Great Expectations batches."""

        # Load into memory as dataframe
        data = pd.read_csv(self.config.trip_batch_file)
        fare = pd.read_csv(self.config.fare_batch_file)

        # Remove any whitestrings from the column headers before running checks
        data.columns = data.columns.str.strip()
        fare.columns = fare.columns.str.strip()

        # Defines the current dataset as batches
        self.data_batch_param = {"dataframe": data}
        self.fare_batch_param = {"dataframe": fare}
        self.data_batch = self.trip_data_asset.add_batch_definition(self.config.trip_batch_name).get_batch(self.data_batch_param)
        self.fare_batch = self.trip_fare_asset.add_batch_definition(self.config.fare_batch_name).get_batch(self.fare_batch_param)

    def _run_suites(self, suite_name: str, data_type: Literal["trip_data", "trip_fare"]):
        attr = ["data_batch", "fare_batch", "suites"]
        for i in attr:
            if not hasattr(self, i):
                raise AttributeError

        if suite_build := self.suites.get(suite_name):
            if isinstance(suite_build, dict):
                expectations = list(suite_build.values())
                suite = gx.ExpectationSuite(name=suite_name, expectations=expectations)  # type: ignore

                if data_type == "trip_data":
                    return self.data_batch.validate(suite)
                else:
                    return self.fare_batch.validate(suite)

    def _save_report(self, suite_name: str, output):
        """ Stores results after evaluation. """

        output_path = self.config.output_file_name(suite=suite_name)
        json_result = output.to_json_dict()  # safer for JSON dumping

        with open(output_path, "w") as file:
            json.dump(json_result, file, indent=2)

        print(f"Completed saving report to path: {output_path}")

    def _run_quality_test(self, suite_name: str, data_type: Literal["trip_data", "trip_fare"], save_report: bool = True):
        """ Runs Quality tests given suite_name for input dataset. """

        report = self._run_suites(suite_name, data_type)

        if save_report:
            self._save_report(suite_name, report)

    def quality_test_trip_data(self, save_report: bool = True):
        """ Runs all checks for trip_data datasets. """

        for suite_name in (label for label in self.suites.keys() if label.startswith("trip_data")):
            self._run_quality_test(suite_name, "trip_data", save_report)

    def quality_test_trip_fare(self, save_report: bool = True):
        """ Runs all checks for trip_fare datasets. """

        for suite_name in (label for label in self.suites.keys() if label.startswith("trip_fare")):
            self._run_quality_test(suite_name, "trip_fare", save_report)

if __name__ == "__main__":
    # Usage example
    config = QualityConfig(trip_batch_name="trip_data_1", fare_batch_name="trip_fare_1")
    quality = NYCTripQualityBuilder(config=config)
    quality.configure()
    quality.quality_test_trip_data()