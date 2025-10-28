# utils/loaders.py

import yaml
from pathlib import Path
import great_expectations as gx

def load_checks(check_config_dir: Path):
    """ Quick quality checks config loader """

    all_checks = {}
    print(f"Checks dir: {check_config_dir}")

    for f in check_config_dir.iterdir():
        label = f.stem

        with open(f, "r") as file:
            check_info = yaml.safe_load(file)

        if isinstance(check_info, list) and len(check_info) != 0:

            for info in check_info:
                if label not in all_checks:
                    all_checks[label] = {}

                expectation_type = info.get('expectation_type', None)
                kwargs = info.get('kwargs', {})
                meta = info.get("meta", {})
                description = info.get("description", "")

                if meta.get("label", None) is None:
                    raise ValueError
                else:
                    validation_name = meta.get("label", None)

                all_checks[label][validation_name] = getattr(gx.expectations, expectation_type)(**kwargs, meta=meta, description=description)

    return all_checks
