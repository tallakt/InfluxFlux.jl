# InfluxFlux

A simple Julia client to access InfluxDB based on the Flux query language.

Only supports read access

## Install

```Julia
] add https://github.com/tallakt/InfluxFlux#main
```

## Usage

```Julia
using InfluxFlux

api_token = "...."
srv = influx_server("https://some.influxdb.endpoint.influxdata.com", "some@organization.com", api_token)
```

### Discovery helpers

```Julia
buckets      = list_buckets(srv)
measurements = list_measurements(srv, "example_bucket")
fields       = list_fields(srv, "example_bucket")
fields       = list_fields(srv, "example_bucket", "sensors")
```

### Measurement helpers

InfluxDB groups data by tag set, so a measurement with multiple tag
combinations returns multiple tables. `measurement()` errors unless there is
exactly one; use `measurement_multi()` to get a `Vector{DataFrame}`, one per
table group.

```Julia
using Dates

df     = measurement(srv, "example_bucket", "sensors", now(UTC) - Hour(1), now())
tables = measurement_multi(srv, "example_bucket", "sensors", now(UTC) - Hour(1), now())
```

### Using Time Zones

Time bounds accept a `DateTime`, `ZonedDateTime`, or a plain `Int` (epoch nanoseconds).
`time_spec_to_epoc_ns()` converts any of those to an integer if you need the value directly.

Some IANA timezone names are classified as legacy in TimeZones.jl and require passing
`TimeZones.Class(:LEGACY)` explicitly:

```Julia
using Dates
using TimeZones

oslo = TimeZone("Europe/Oslo", TimeZones.Class(:LEGACY))
df = measurement(srv, "example_bucket", "sensors", ZonedDateTime(2025, 1, 1, oslo), ZonedDateTime(2025, 2, 1, oslo))

t_ns = time_spec_to_epoc_ns(now(UTC) - Hour(1))
```

### Aggregate helpers

`aggregate_measurement()` downsamples using a Flux `aggregateWindow`. The `window`
argument is any `Period` (e.g. `Second(30)`, `Minute(1)`, `Hour(6)`). The `fn` keyword
defaults to `"mean"` but accepts any Flux aggregate: `"min"`, `"max"`, `"sum"`,
`"median"`, etc.

```Julia
df     = aggregate_measurement(srv, "example_bucket", "sensors", now(UTC) - Hour(1), now(), Minute(1))
tables = aggregate_measurement_multi(srv, "example_bucket", "sensors", now(UTC) - Hour(1), now(), Minute(1))

# custom aggregate function
df = aggregate_measurement(srv, "example_bucket", "sensors", now(UTC) - Day(1), now(), Hour(1); fn="max")
```

### Raw queries

`flux()` returns the raw response body. `flux_to_dataframe()` parses it into a
DataFrame but errors if the query returns more than one table.

```Julia
raw = flux(srv, "buckets()") |> String

table = flux_to_dataframe(srv, """
  from(bucket: "example-bucket")
    |> range(start: -1d)
    |> filter(fn: (r) => r._field == "foo")
    |> group(columns: ["sensorID"])
    |> mean()
  """)
```

`flux_to_dataframe_multi()` handles queries that yield multiple named result sets. Add `|> yield(name: "foo")` to your query for a meaningful key; without it the key defaults to `:_result`. Each key holds a `Vector{DataFrame}`.

```Julia
tables = flux_to_dataframe_multi(srv, """
  from(bucket: "example-bucket")
    |> range(start: -1h)
    |> yield(name: "example")
  """)
tables.example
```

## Tips for Writing Flux

### Time column

`measurement()` and `aggregate_measurement()` map `_time` to `UInt64` nanoseconds so it
arrives as a plain integer column. For raw Flux queries, add this yourself:

```
|> map(fn: (r) => ({ r with _time: uint(v: r._time) }))
```

When doing so, also consider calling `clean_influx_df()` to drop the InfluxDB internal
columns (`result`, `table`, `_start`, `_stop`, `_measurement`) that the high-level helpers
strip automatically:

```Julia
df = clean_influx_df(flux_to_dataframe(srv, my_query))
```

### Row order

Row order within a table is not guaranteed. Sort explicitly if needed:

```Julia
sort!(df, :_time)
```

### Consolidating multiple tables into one

To avoid the `_multi` variants entirely, collapse all series into a single table in Flux
using `group()` followed by `sort()`. This is useful when tag differences don't matter:

```Julia
single = flux_to_dataframe(srv, """
  from(bucket: "example-bucket")
    |> range(start: -1h)
    |> filter(fn: (r) => r._measurement == "sensors")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> group()
    |> sort(columns: ["_time"])
  """)
```
