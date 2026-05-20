# InfluxFlux

A simple Julia client to access InfluxDB based on the Flux query language.

Only supports read access

## Install

```Julia
] add https://github.com/tallakt/InfluxFlux#main
```

## Usage

```Julia
using Dates
using InfluxFlux

api_key = "...."
srv = influx_server("https://some.influxdb.endpoint.influxdata.com", "some@organization.com", api_token)

# raw query to string
raw = flux(srv, "buckets()") |> String

# raw query to dataframe — errors if query returns more than one table
table = flux_to_dataframe(srv, """
  from(bucket: "example-bucket")
    |> range(start: -1d)
    |> filter(fn: (r) => r._field == "foo")
    |> group(columns: ["sensorID"])
    |> mean()
  """)

# get all data for a measurement as a DataFrame
dataframe1 = measurement(srv, "example_bucket", "sensors", now(UTC) - Hour(1), now())

# get measurements with a reduced sample rate 1 minute
dataframe2 = aggregate_measurement(srv, "example_bucket", "sensors", now(UTC) - Hour(1), now(), Minute(1))

# raw query returning multiple tables, keyed by yield name
# add |> yield(name: "foo") to your query for a meaningful key, otherwise defaults to :_result
tables = flux_to_dataframes(srv, """
  from(bucket: "example-bucket")
    |> range(start: -1h)
    |> yield(name: "example")
  """)
tables.example

# discovery helpers for interactive exploration
buckets = list_buckets(srv)
measurements = list_measurements(srv, "example_bucket")
fields = list_fields(srv, "example_bucket")
fields = list_fields(srv, "example_bucket", "sensors")  # scoped to one measurement
```
