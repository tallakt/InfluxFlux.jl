# InfluxFlux.jl

A minimal Julia package for **read-only** access to an [InfluxDB v2](https://docs.influxdata.com/influxdb/v2/) server using the [Flux query language](https://docs.influxdata.com/flux/v0/).

## Installation

```julia
] add InfluxFlux
```

## Quick start

```julia
using InfluxFlux
using Dates

# Create a server handle
srv = influx_server("http://localhost:8086", "my-org", "my-token")

# List available buckets
list_buckets(srv)

# Fetch a measurement as a DataFrame
t0 = DateTime(2024, 1, 1)
t1 = DateTime(2024, 1, 2)
df = measurement(srv, "my-bucket", "temperature", t0, t1)

# Downsample to hourly means
df_hourly = aggregate_measurement(srv, "my-bucket", "temperature", t0, t1, Hour(1))

# Run an arbitrary Flux query
df = flux_to_dataframe(srv, """
    from(bucket: "my-bucket")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "cpu")
""")
```

## Time specifications

Functions that accept time bounds take a `TimeSpec`, which is any of:

| Type            | Meaning                          |
|:----------------|:---------------------------------|
| `Int`           | Epoch nanoseconds (Unix × 10⁹)  |
| `DateTime`      | Treated as UTC                   |
| `ZonedDateTime` | Converted to UTC automatically   |

Use `time_spec_to_epoc_ns` to convert any `TimeSpec` to an integer epoch-nanosecond value.

## API reference

### Server

```@docs
influx_server
```

### Query execution

```@docs
flux
flux_to_dataframe
flux_to_dataframe_multi
clean_influx_df
```

### High-level helpers

```@docs
measurement
measurement_multi
aggregate_measurement
aggregate_measurement_multi
```

### Schema inspection

```@docs
list_buckets
list_measurements
list_fields
```

### Utilities

```@docs
time_spec_to_epoc_ns
```

## Error handling

All non-200 responses throw an `InfluxFluxError` with the HTTP status code, the InfluxDB error code (if the response body is JSON), and the error message.

```julia
try
    df = measurement(srv, "no-such-bucket", "cpu", t0, t1)
catch e::InfluxFlux.InfluxFluxError
    println("HTTP $(e.status): $(e.message)")
end
```

## Notes

- **Read-only** — there is no write or delete API.
- Bucket and measurement names are passed directly into Flux queries without escaping; callers are responsible for trusting input against their own server.
- The `_time` column in DataFrames returned by `measurement` and `aggregate_measurement` is an integer representing epoch nanoseconds.
