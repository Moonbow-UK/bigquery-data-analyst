from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..services.dataset_summary import DatasetSummaryOptions


@dataclass
class _ColumnAccumulator:
    name: str
    max_top_values: int
    total_rows: int = 0
    null_count: int = 0
    numeric_possible: bool = True
    numeric_count: int = 0
    numeric_sum: float = 0.0
    numeric_min: float | None = None
    numeric_max: float | None = None
    top_counter: Counter[str] | None = None

    def __post_init__(self) -> None:
        if self.top_counter is None:
            self.top_counter = Counter()

    def add(self, raw_value: str | None) -> None:
        self.total_rows += 1
        if raw_value is None:
            self.null_count += 1
            return

        value = raw_value.strip()
        if value == "" or value.lower() in {"null", "none", "nan"}:
            self.null_count += 1
            return

        self.top_counter[value] += 1

        if self.numeric_possible:
            try:
                number = float(value)
            except ValueError:
                self.numeric_possible = False
                self.numeric_count = 0
                self.numeric_sum = 0.0
                self.numeric_min = None
                self.numeric_max = None
            else:
                self.numeric_count += 1
                self.numeric_sum += number
                if self.numeric_min is None or number < self.numeric_min:
                    self.numeric_min = number
                if self.numeric_max is None or number > self.numeric_max:
                    self.numeric_max = number

    def finalize(self) -> dict[str, Any]:
        non_null = self.total_rows - self.null_count
        column_type = "numeric" if self.numeric_possible and self.numeric_count > 0 else "categorical"
        average = (
            self.numeric_sum / self.numeric_count if column_type == "numeric" and self.numeric_count else None
        )
        top_values = []
        for value, count in self.top_counter.most_common(self.max_top_values):
            ratio = count / non_null if non_null else 0.0
            top_values.append(
                {
                    "value": value,
                    "count": count,
                    "ratio": ratio,
                }
            )
        return {
            "name": self.name,
            "type": column_type,
            "null_count": self.null_count,
            "null_ratio": (self.null_count / self.total_rows) if self.total_rows else 0.0,
            "non_null_count": non_null,
            "numeric": {
                "min": self.numeric_min,
                "max": self.numeric_max,
                "avg": average,
            }
            if column_type == "numeric"
            else None,
            "top_values": top_values,
        }


def build_csv_summary(csv_path: Path, *, max_top_values: int) -> dict[str, Any]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found at {csv_path}")

    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise RuntimeError("CSV file is missing headers; cannot summarise")

        accumulators = {
            name: _ColumnAccumulator(name=name, max_top_values=max_top_values)
            for name in reader.fieldnames
        }
        total_rows = 0
        for row in reader:
            total_rows += 1
            for name, accumulator in accumulators.items():
                accumulator.add(row.get(name))

    column_summaries = [acc.finalize() for acc in accumulators.values()]
    return {
        "file_path": str(csv_path),
        "file_name": csv_path.name,
        "total_rows": total_rows,
        "columns_count": len(accumulators),
        "columns": column_summaries,
    }


def render_csv_summary(summary: dict[str, Any], options: DatasetSummaryOptions) -> str:
    total_rows = summary["total_rows"]
    lines: list[str] = []
    lines.append(f"CSV summary for {summary['file_name']}")
    lines.append(f"Total rows: {format_number(total_rows)}")
    lines.append(f"Columns: {summary['columns_count']}")

    numeric_columns = [col for col in summary["columns"] if col["type"] == "numeric"]
    categorical_columns = [col for col in summary["columns"] if col["type"] == "categorical"]

    if numeric_columns:
        lines.append("")
        lines.append("Numeric columns:")
        limit = options.max_numeric_columns or len(numeric_columns)
        for column in numeric_columns[:limit]:
            numeric = column["numeric"] or {}
            lines.append(
                f"- {column['name']} (nulls: {format_number(column['null_count'])}"  # noqa: ISC003
                f" / {format_percent(column['null_ratio'])})"
            )
            lines.append(
                "  min={min}, max={max}, avg={avg}".format(
                    min=format_float(numeric.get("min")),
                    max=format_float(numeric.get("max")),
                    avg=format_float(numeric.get("avg")),
                )
            )
        if len(numeric_columns) > limit:
            remaining = len(numeric_columns) - limit
            lines.append(f"  … {remaining} additional numeric column(s) omitted.")

    if categorical_columns:
        lines.append("")
        lines.append("Categorical columns:")
        limit = options.max_categorical_columns or len(categorical_columns)
        for column in categorical_columns[:limit]:
            lines.append(
                f"- {column['name']} (nulls: {format_number(column['null_count'])}"
                f" / {format_percent(column['null_ratio'])})"
            )
            top_values = column["top_values"] or []
            if top_values:
                for entry in top_values:
                    lines.append(
                        "    • {value} — {count} ({ratio})".format(
                            value=entry["value"],
                            count=format_number(entry["count"]),
                            ratio=format_percent(entry["ratio"]),
                        )
                    )
            else:
                lines.append("    • (no frequent values)")
        if len(categorical_columns) > limit:
            remaining = len(categorical_columns) - limit
            lines.append(f"  … {remaining} additional categorical column(s) omitted.")

    return "\n".join(lines)


def summarize_csv_file(csv_path: Path, options: DatasetSummaryOptions) -> tuple[dict[str, Any], str]:
    summary = build_csv_summary(csv_path, max_top_values=options.max_top_values)
    text = render_csv_summary(summary, options)
    return summary, text


def format_number(value: int | float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,}"


def format_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.2f}"


def format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"
