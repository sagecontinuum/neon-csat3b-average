"""Sage plugin: every 30 minutes, publish the averages of the CSAT3B's three wind speeds
(ux_wind_speed, uy_wind_speed, uz_wind_speed) over the latest 30 minutes of data,
ending at the newest readout_time, read from the sage-kafka mirror with the
neon_sage_edge client."""
from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import sys
import time
from typing import List, Optional, Sequence

from neon_sage_edge import (KafkaSettings, MessageDecoder, MirrorReader, RegistrySettings,
                            make_registry_client, show_notes)
from waggle.plugin import Plugin

from averaging import WindowSummary, measurement_name, readings_table, require_fields, summarize

log = logging.getLogger("neon-csat3b-rolling-average")

WIND_SPEEDS = "ux_wind_speed,uy_wind_speed,uz_wind_speed"


def comma_list(text: str) -> List[str]:
    return [item.strip() for item in text.split(",") if item.strip()]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka", default=os.getenv("MIRROR_BOOTSTRAP", "localhost:9094"),
                        help="the mirror's reader address (default: localhost:9094)")
    parser.add_argument("--topic", default=os.getenv("CSAT3B_TOPIC", "reading.sensor.csat3b"),
                        help="topic holding the CSAT3B readings (default: reading.sensor.csat3b)")
    parser.add_argument("--window-minutes", type=float, default=30,
                        help="length of the averaging window, ending at the newest reading (default: 30)")
    parser.add_argument("--interval-minutes", type=float, default=30,
                        help="how often to publish (default: 30)")
    parser.add_argument("--once", action="store_true",
                        help="publish one window and exit, for runs started by a Sage schedule")
    parser.add_argument("--time-field", default="readout_time",
                        help="field holding when each reading was taken (default: readout_time)")
    parser.add_argument("--fields", type=comma_list, default=comma_list(WIND_SPEEDS),
                        help=f"comma-separated fields to average (default: {WIND_SPEEDS})")
    parser.add_argument("--group-by", default="",
                        help="field telling sensors apart, if the topic holds several; each is averaged separately")
    parser.add_argument("--prefix", default="neon.csat3b",
                        help="start of the published measurement names (default: neon.csat3b)")
    parser.add_argument("--lookback-margin-minutes", type=float, default=10,
                        help="extra minutes read beyond the window, to include readings that "
                             "reached Kafka late (default: 10)")
    parser.add_argument("--max-messages", type=int, default=200_000,
                        help="most messages read per cycle (default: 200000; 30 minutes at 20 Hz is 36000)")
    parser.add_argument("--registry-url", default=os.getenv("SCHEMA_REGISTRY_URL", ""),
                        help="schema registry URL, for Avro or JSON Schema values")
    parser.add_argument("--registry-ip", default=os.getenv("SCHEMA_REGISTRY_IP", ""),
                        help="IP address to use for the registry's name, if it doesn't resolve")
    parser.add_argument("--value-format", default=os.getenv("VALUE_FORMAT", ""),
                        help="avro, json or string (default: avro with a registry, otherwise string)")
    args = parser.parse_args(argv)
    if not args.fields:
        parser.error("--fields needs at least one field name")
    return args


def connect(args: argparse.Namespace) -> MirrorReader:
    """A reader for the mirror, deserializing values through the registry if one is set."""
    kafka = dataclasses.replace(KafkaSettings.from_env(), bootstrap_servers=args.kafka)
    registry = RegistrySettings(url=args.registry_url, ip=args.registry_ip,
                                value_format=args.value_format or ("avro" if args.registry_url else "string"))
    decoder = MessageDecoder.from_settings(registry, make_registry_client(registry))
    log.info("Reading %s from %s; schema registry: %s", args.topic, kafka.describe(), registry.describe())
    return MirrorReader.connect(kafka, decoder)


def compute(reader: MirrorReader, args: argparse.Namespace) -> List[WindowSummary]:
    """Read the latest readings and average them over the window."""
    lookback = args.window_minutes + args.lookback_margin_minutes
    messages = reader.read_since(args.topic, minutes=lookback, max_messages=args.max_messages)
    if messages.empty:
        log.warning("No messages on %s in the last %g minutes; nothing to publish.", args.topic, lookback)
        return []
    table = readings_table(messages, args.time_field)
    require_fields(table, args.fields)
    return summarize(table, args.time_field, args.window_minutes, args.fields, group_by=args.group_by)


def publish(plugin: Plugin, summaries: List[WindowSummary], args: argparse.Namespace) -> None:
    """Publish one value per field, plus the number of readings, timestamped at the window's end."""
    for summary in summaries:
        timestamp = int(summary.end.value)  # nanoseconds since the epoch, as pywaggle expects
        meta = {"window_minutes": f"{args.window_minutes:g}",
                "window_start": summary.start.isoformat(),
                "window_end": summary.end.isoformat(),
                "topic": args.topic}
        if summary.group is not None:
            meta["group"] = f"{args.group_by}={summary.group}"
        # plugin.publish(f"{args.prefix}.readings", summary.readings, timestamp=timestamp, meta=meta)
        for average in summary.averages:
            plugin.publish(measurement_name(args.prefix, average.field), average.mean, timestamp=timestamp,
                           meta={**meta, "field": average.field, "samples": str(average.samples)})
        log.info("Window %s to %s%s: %d readings, %d fields averaged", summary.start, summary.end,
                 f" ({meta['group']})" if summary.group is not None else "", summary.readings,
                 len(summary.averages))


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for http_logger in ("httpx", "httpx2"):  # the registry client's HTTP library logs every request at INFO
        logging.getLogger(http_logger).setLevel(logging.WARNING)
    show_notes()  # print neon_sage_edge's notes, e.g. records it couldn't deserialize
    with connect(args) as reader, Plugin() as plugin:
        while True:
            started = time.monotonic()
            try:
                publish(plugin, compute(reader, args), args)
            except Exception:  # a bad cycle (broker unreachable, unexpected data) must not end the plugin
                log.exception("Couldn't publish this window.")
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(max(0.0, args.interval_minutes * 60 - (time.monotonic() - started)))


if __name__ == "__main__":
    sys.exit(main())
