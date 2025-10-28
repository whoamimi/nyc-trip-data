# src/shchema.py

from dataclasses import dataclass, field
from typing import List, Dict, Optional
import json
import statistics

# ---- Base Units ----
@dataclass
class ExpectationConfig:
    """Configuration metadata of a single expectation test."""
    type: str
    column: str
    description: str
    label: str
    severity: str
    quality_score: int
    success: bool

@dataclass
class ExpectationResult:
    """Outcome and metrics of each test."""
    element_count: int
    unexpected_count: int
    unexpected_percent: float
    missing_percent: float

@dataclass
class QualityTest:
    """Encapsulates one expectation test, linking config + result."""
    config: ExpectationConfig
    result: ExpectationResult

    def weighted_score(self) -> float:
        """Calculate weighted contribution of this test to field quality."""
        base = self.config.quality_score
        penalty = 0 if self.config.success else 1
        # A simple scaled adjustment: lower penalty = better pass rate
        return max(0, base - penalty * (base / 2))

# ---- Aggregation by Field ----
@dataclass
class FieldQualityPanel:
    """Represents a single field's quality across multiple checks."""
    column: str
    tests: List[QualityTest] = field(default_factory=list)

    def field_score(self) -> float:
        """Average score from all tests for this column."""
        if not self.tests:
            return 0.0
        return statistics.mean(test.weighted_score() for test in self.tests)

    def checks_summary(self) -> Dict[str, float]:
        passed = sum(test.config.success for test in self.tests)
        total = len(self.tests)
        return {
            "total_checks": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": round(100 * passed / total, 2),
            "avg_quality_score": round(self.field_score(), 2)
        }

# ---- Overall Dashboard ----
@dataclass
class QualityDashboard:
    """Full data quality report container."""
    panels: List[FieldQualityPanel]

    def overall_score(self) -> float:
        if not self.panels:
            return 0.0
        return statistics.mean(p.field_score() for p in self.panels)

    def to_summary(self) -> Dict[str, Dict]:
        """Convert into summary for dashboard rendering."""
        return {
            panel.column: panel.checks_summary()
            for panel in self.panels
        }


def main():
    """ Main function to run. """
    # Assuming your JSON blob is stored in `data.json`
    with open("data/output/reports/2025-10-28T10:33:57.255983_trip_data_schema_raw_validation_trip_data_1.json") as f:
        data = json.load(f)

    tests_by_field = {}

    for r in data["results"]:
        cfg = r["expectation_config"]
        res = r.get("result", {})
        test = QualityTest(
            config=ExpectationConfig(
                type=cfg["type"],
                column=cfg["kwargs"].get("column") or cfg["kwargs"].get("column_A"),
                description=cfg["description"],
                label=cfg["meta"]["label"],
                severity=cfg["severity"],
                quality_score=cfg["meta"]["quality_score"],
                success=r["success"]
            ),
            result=ExpectationResult(
                element_count=res.get("element_count", 0),
                unexpected_count=res.get("unexpected_count", 0),
                unexpected_percent=res.get("unexpected_percent", 0.0),
                missing_percent=res.get("missing_percent", 0.0)
            )
        )

        tests_by_field.setdefault(test.config.column, []).append(test)

    dashboard = QualityDashboard(
        panels=[FieldQualityPanel(column=c, tests=t) for c, t in tests_by_field.items()]
    )

    # Print summary
    from pprint import pprint
    pprint(dashboard.to_summary())

    print("\nOverall dataset quality score:", dashboard.overall_score())

if __name__ == "__main__":
    main()