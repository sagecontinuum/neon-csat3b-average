# neon-csat3b-average

A Sage plugin that publishes rolling averages of the three wind speeds measured by a NEON CSAT3B sonic anemometer: `ux_wind_speed`, `uy_wind_speed` and `uz_wind_speed`. Every 30 minutes it reads the newest readings from the sage-kafka mirror with the [neon_sage_edge](https://github.com/sagecontinuum/neon_sage_edge) client, averages each wind speed over the latest 30 minutes, and publishes the results through pywaggle.

## Science

The CSAT3B measures the wind along three axes (ux, uy and uz) at 20 Hz, about 36,000 readings every 30 minutes. Thirty-minute averages are the standard summary interval for these measurements. The plugin turns the raw stream into one value per wind speed every half hour, along with a count of the readings behind it, so gaps in the data are visible.

## How the window is chosen

The window ends at the **newest `readout_time`** found in the data, the time the newest reading was taken, and covers the 30 minutes before it: `(newest − 30 min, newest]`. Because the window follows the data rather than the clock, readings that reach Kafka in delayed batches are still averaged into the right window. To catch those batches, each cycle reads 10 minutes more than the window (`--lookback-margin-minutes`), going by when records reached Kafka.

## What it publishes

All values are timestamped at the window's end.

| Name | Value |8
|---|---|
| `neon.csat3b.ux_wind_speed.mean` | Mean of `ux_wind_speed` over the window |
| `neon.csat3b.uy_wind_speed.mean` | Mean of `uy_wind_speed` over the window |
| `neon.csat3b.uz_wind_speed.mean` | Mean of `uz_wind_speed` over the window |

Every value carries metadata: `window_start`, `window_end`, `window_minutes` and `topic`. Each mean also carries the original `field` name and its `samples` count, the readings in the window that had a value for that field. With `--group-by`, a `group` entry names the sensor.

To average different fields, list them with `--fields`. Each one is published as `<prefix>.<field>.mean`, with camelCase names turned into snake_case. If a listed field is missing from the readings, the plugin says which one instead of publishing partial results.

## Arguments

| Argument | Default | Meaning |
|---|---|---|
| `--kafka` | `localhost:9094` (or `MIRROR_BOOTSTRAP`) | The mirror's reader address; no login is needed |
| `--topic` | `reading.sensor.csat3b` (or `CSAT3B_TOPIC`) | Topic holding the CSAT3B readings |
| `--window-minutes` | `30` | Length of the averaging window |
| `--interval-minutes` | `30` | How often to publish |
| `--once` | off | Publish one window and exit, for runs started by a schedule |
| `--time-field` | `readout_time` | Field holding when each reading was taken |
| `--fields` | `ux_wind_speed,uy_wind_speed,uz_wind_speed` | Comma-separated fields to average |
| `--group-by` | none | Field telling sensors apart, if the topic holds several; each sensor gets its own window |
| `--prefix` | `neon.csat3b` | Start of the published names |
| `--lookback-margin-minutes` | `10` | Extra minutes read, for late batches |
| `--max-messages` | `200000` | Most messages read per cycle |
| `--registry-url` | `SCHEMA_REGISTRY_URL` | Schema registry, for Avro or JSON Schema values |
| `--registry-ip` | `SCHEMA_REGISTRY_IP` | IP to use for the registry's name if it doesn't resolve |
| `--value-format` | `avro` with a registry, else `string` | How values are deserialized: `avro`, `json` or `string` |

## Running it

**On a Sage node (withot publishing):**, with the mirror on the same machine. `--network host` lets `localhost:9094` reach it:

```bash
docker build -t neon-csat3b-average .
mkdir test-run
docker run -e PYWAGGLE_LOG_DIR=/tmp/test-run -v ./test-run:/tmp/test-run --rm --network host neon-csat3b-average
--registry-url url --registry-ip ip
```

**On a Sage node:**, with the mirror on the same machine. `--network host` lets `localhost:9094` reach it:

```bash
docker build -t neon-csat3b-average .
docker run --rm --network host neon-csat3b-average --registry-url url --registry-ip ip
```

**On a Sage node (with pluginctl):**

```bash
pluginctl build .
pluginctl run -n neon-csat3b-average 10.31.81.1:5000/local/neon-csat3b-average:latest -- \
  --registry-url url --registry-ip ip
```

## Files

| File | Contents |
|---|---|
| `main.py` | Arguments, the connection to the mirror, the 30-minute cycle and publishing |
| `averaging.py` | The averaging itself: plain functions on tables, independent of Kafka and Sage |
| `requirements.txt` | pywaggle, and neon_sage_edge pinned to a commit on GitHub |
| `Dockerfile`, `sage.yaml` | From the Sage app template; `sage.yaml` holds the app's metadata for the Sage portal |
