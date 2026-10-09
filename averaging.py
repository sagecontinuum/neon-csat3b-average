"""Averages over the latest window of sensor readings.

Pure functions on pandas tables: no Kafka or Sage connection is needed, so each
step can be checked on its own with a small hand-made table.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import pandas as pd
from neon_sage_edge import expand_json, to_utc_datetimes

@dataclass(frozen=True)
class FieldAverage:
    field: str     # the record's field name, e.g. "ux_wind_speed" (nested fields as "data.ux_wind_speed")
    mean: float
    samples: int   # readings in the window that had a value for this field


@dataclass(frozen=True)
class WindowSummary:
    start: pd.Timestamp  # exclusive
    end: pd.Timestamp    # inclusive: the newest reading time
    readings: int
    averages: Tuple[FieldAverage, ...]
    group: Optional[str] = None  # the sensor this window belongs to, when grouping


def readings_table(messages: pd.DataFrame, time_field: str) -> pd.DataFrame:
    """One row per reading: each field of the record as a column, with `time_field`
    converted to UTC times. Readings without a usable time are dropped."""
    table = expand_json(messages)
    if time_field not in table.columns:
        raise ValueError(f"No {time_field!r} field in the messages. Are the values deserialized?")
    table[time_field] = to_utc_datetimes(table[time_field])
    return table.dropna(subset=[time_field])


def require_fields(table: pd.DataFrame, fields: Sequence[str]) -> None:
    """Fail clearly if the readings lack any of the fields to average."""
    missing = [f for f in fields if f not in table.columns]
    if missing:
        raise ValueError(f"Fields not in the readings: {', '.join(missing)}")


def latest_window(times: pd.Series, minutes: float) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """The window of `minutes` ending at the newest time: (start, end]."""
    end = times.max()
    return end - pd.Timedelta(minutes=minutes), end


def summarize_window(rows: pd.DataFrame, time_field: str, minutes: float, fields: Sequence[str],
                     group: Optional[str] = None) -> WindowSummary:
    """Average every field over the latest window of `rows`."""
    start, end = latest_window(rows[time_field], minutes)
    in_window = rows[(rows[time_field] > start) & (rows[time_field] <= end)]
    averages = []
    for field in fields:
        values = pd.to_numeric(in_window[field], errors="coerce").dropna()
        if not values.empty:
            averages.append(FieldAverage(field, float(values.mean()), int(values.size)))
    return WindowSummary(start, end, len(in_window), tuple(averages), group)


def summarize(table: pd.DataFrame, time_field: str, minutes: float, fields: Sequence[str],
              group_by: str = "") -> List[WindowSummary]:
    """One summary for the whole table, or one per sensor when `group_by` names the field
    that tells sensors apart (each sensor's window then ends at its own newest reading)."""
    if not group_by:
        return [summarize_window(table, time_field, minutes, fields)]
    if group_by not in table.columns:
        raise ValueError(f"No {group_by!r} field to group by.")
    return [summarize_window(rows, time_field, minutes, fields, group=str(key))
            for key, rows in table.groupby(group_by, sort=True)]


def measurement_name(prefix: str, field: str, statistic: str = "mean") -> str:
    """A Sage-style measurement name: lowercase and dot-separated, with camelCase field
    names turned into snake_case, e.g. ("neon.csat3b", "uVelo") -> "neon.csat3b.u_velo.mean"."""
    snake = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", field).lower()
    clean = re.sub(r"[^a-z0-9_.]+", "_", snake).strip("._")
    return f"{prefix}.{clean}.{statistic}"
