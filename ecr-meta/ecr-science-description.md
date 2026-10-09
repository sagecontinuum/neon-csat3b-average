# NEON CSAT3B Rolling Average

Publishes 30-minute averages of the three wind components measured by a NEON CSAT3B sonic anemometer, read from the site's Kafka mirror.

# Science

The Campbell Scientific CSAT3B is a three-dimensional sonic anemometer, the standard instrument for eddy-covariance measurements of how heat, water vapor and carbon dioxide move between the land surface and the atmosphere. It measures the wind along three orthogonal axes (ux, uy and uz) using ultrasonic pulses. At NEON sites it's read 20 times a second, about 36,000 readings every 30 minutes.

This plugin reduces the 20 Hz stream to one mean per wind component every half hour, together with the number of readings behind each mean. That makes the mean airflow at the tower easy to follow over time, and gaps in the data easy to spot.

The components are in the anemometer's own coordinate system: ux and uy lie along the sensor's horizontal axes, and uz is vertical. Turning the means into a horizontal wind speed and direction requires the anemometer's orientation on the tower.

# How it works

Every 30 minutes, the plugin reads the newest readings from the site's Kafka mirror with the [neon_sage_edge](https://github.com/sagecontinuum/neon_sage_edge) client, decoding them through the schema registry. Each averaging window ends at the time of the newest reading (`readout_time`) and covers the 30 minutes before it. Readings that reach Kafka late therefore still count toward the window they belong to.

# Ontology

| Name | Description | Unit |
|---|---|---|
| `neon.csat3b.ux_wind_speed.mean` | Mean wind along the anemometer's x axis | m/s |
| `neon.csat3b.uy_wind_speed.mean` | Mean wind along the anemometer's y axis | m/s |
| `neon.csat3b.uz_wind_speed.mean` | Mean vertical wind | m/s |
| `neon.csat3b.readings` | Number of readings in the window | count |

Each value is timestamped at the end of its window. It carries the metadata `window_start`, `window_end`, `window_minutes` and `topic`; the means also carry `field` and `samples`, the number of readings averaged.

# References

- Campbell Scientific, CSAT3B 3-D Sonic Anemometer: https://www.campbellsci.com/csat3b
- neon_sage_edge, the client used to read the Kafka mirror: https://github.com/sagecontinuum/neon_sage_edge
