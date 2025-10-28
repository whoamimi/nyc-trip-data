# src/utils.py

from pathlib import Path
from dataclasses import dataclass, field

from utils.loaders import load_checks

ROOT_PATH = "elula-group-interview"
OUTPUT_DATA_PATH = "data.parquet"
OUTPUT_FARE_PATH = "fare.parquet"

@dataclass
class WorkspaceSetting:
    root_path: Path = field(init=False)
    data_path: Path = field(init=False)
    trip_data_path: Path = field(init=False)
    trip_fare_path: Path = field(init=False)
    output_path: Path = field(init=False)
    output_data_path: Path = field(init=False)
    output_report_path: Path = field(init=False)

    checks_path: Path = field(init=False)
    _checks_config_path: Path = field(init=False)

    checks_config: dict = field(init=False)

    def __post_init__(self):
        """ Workspace project setup. """

        if Path().absolute().stem.endswith(ROOT_PATH):
            self.root_path = Path()
            self.data_path = self.root_path / "data"
            self.trip_fare_path = self.data_path / "input" / "trip_fare"
            self.trip_data_path = self.data_path / "input" / "trip_data"
            self.output_path = self.data_path / "output"
            self.output_data_path = self.output_path / "data"
            self.output_report_path = self.output_path / "reports"
            self.checks_path = self.root_path / "checks"

            self.output_path.mkdir(parents=True, exist_ok=True)
            self.output_data_path.mkdir(parents=True, exist_ok=True)
            self.output_report_path.mkdir(parents=True, exist_ok=True)

            self.checks_config = load_checks(check_config_dir=self.checks_path)
        else:
            raise ValueError(f"Running workspace expected at root but got: {Path().absolute()}")

    @property
    def all_checks(self):
        return list(self.checks_config)

setting = WorkspaceSetting()

print("Number of Validations loaded for:", {key: len(val) for key, val in setting.checks_config.items()})