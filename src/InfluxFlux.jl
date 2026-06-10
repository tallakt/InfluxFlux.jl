module InfluxFlux

using HTTP
using JSON
using CSV
using Dates
using DataFrames
using TimeZones

export time_spec_to_epoc_ns,
    influx_server,
    flux,
    flux_to_dataframe,
    flux_to_dataframe_multi,
    measurement,
    measurement_multi,
    aggregate_measurement,
    aggregate_measurement_multi,
    list_buckets,
    list_measurements,
    list_fields,
    clean_influx_df

const TimeSpec = Union{Int,DateTime,ZonedDateTime}

struct InfluxServer
    uri::String
    org::String
    api_token::String
end

struct InfluxFluxError <: Exception
    status::Int
    code::Union{String,Nothing}
    message::String
    raw::String
end

function Base.showerror(io::IO, e::InfluxFluxError)
    println(io, "InfluxFluxError (HTTP $(e.status))")
    println(io, "Code: ", something(e.code, "unknown"))
    println(io, e.message)
end

"""
    time_spec_to_epoc_ns(time_spec::TimeSpec) -> Int

Convert a `TimeSpec` value to an integer epoch-nanosecond timestamp (Unix × 10⁹).

Accepts an `Int` (returned as-is), a `DateTime` (treated as UTC), or a
`ZonedDateTime` (converted to UTC first).
"""
function time_spec_to_epoc_ns(time_spec::Int)
    time_spec
end

function time_spec_to_epoc_ns(time_spec::ZonedDateTime)
    time_spec_to_epoc_ns(DateTime(time_spec, UTC))
end

function time_spec_to_epoc_ns(time_spec::DateTime)
    Int(1_000_000_000 * datetime2unix(time_spec))
end

function uri_helper(srv::InfluxServer, path::String)
    HTTP.URI(HTTP.URI("$(srv.uri)/$path"); query=Dict("org" => srv.org))
end

function token_json_headers(srv::InfluxServer)
    Dict("Authorization" => "Token $(srv.api_token)", "Accept" => "application/json")
end

"""
    influx_server(uri, org, api_token) -> InfluxServer

Create a handle to an InfluxDB v2 server.

# Arguments
- `uri`: Base URL of the server, e.g. `"http://localhost:8086"`.
- `org`: InfluxDB organisation name.
- `api_token`: Read-access API token.
"""
function influx_server(uri::String, org::String, api_token::String)::InfluxServer
    InfluxServer(uri, org, api_token)
end

"""
    flux(srv, flux_query) -> Vector{UInt8}

Execute a raw Flux query against `srv` and return the response body as bytes.

Throws an `InfluxFluxError` on any non-200 HTTP response.
"""
function flux(srv::InfluxServer, flux_query::String)
    headers = merge(token_json_headers(srv), Dict("Content-Type" => "application/vnd.flux"))

    response = HTTP.post(
        uri_helper(srv, "api/v2/query"), headers, flux_query; status_exception=false
    )

    if response.status == 200
        return response.body
    end

    body_str = String(copy(response.body))

    # Try to parse JSON error
    err = try
        JSON.parse(body_str)
    catch
        nothing
    end

    if err !== nothing
        throw(
            InfluxFluxError(
                response.status,
                get(err, "code", nothing),
                get(err, "message", body_str),
                body_str,
            ),
        )
    else
        throw(InfluxFluxError(response.status, nothing, body_str, body_str))
    end
end

function parse_annotated_csv(body::Vector{UInt8})
    chunks = split(String(copy(body)), r"\r?\n\r?\n")
    result = Pair{Symbol,DataFrame}[]
    for chunk in chunks
        lines = split(chunk, r"\r?\n")
        default_line = findfirst(l -> startswith(l, "#default,"), lines)
        default_name = if !isnothing(default_line)
            raw = split(lines[default_line], ",")[2]
            isempty(raw) ? "_result" : raw
        else
            "_result"
        end
        data_lines = filter(!isempty, filter(l -> !startswith(l, "#"), lines))
        isempty(data_lines) && continue
        # second field of first data row is the result name when explicitly yielded
        name = if length(data_lines) >= 2
            raw = split(data_lines[2], ",")[2]
            isempty(raw) ? default_name : raw
        else
            default_name
        end
        push!(
            result,
            Symbol(name) =>
                (DataFrame(CSV.File(IOBuffer(join(data_lines, "\n")); delim=','))),
        )
    end
    result
end

"""
    flux_to_dataframe_multi(srv, flux_query) -> NamedTuple

Execute a Flux query and return a `NamedTuple` mapping each named result to a
`Vector{DataFrame}` (one element per table group).

Use this when a query yields multiple results with `yield(name: ...)`:

```julia
q = \"\"\"
    from(bucket: "sensors")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "temperature")
      |> yield(name: "temp")

    from(bucket: "sensors")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "humidity")
      |> yield(name: "hum")
\"\"\"
result = flux_to_dataframe_multi(srv, q)
result.temp   # Vector{DataFrame} for temperature tables
result.hum    # Vector{DataFrame} for humidity tables
```
"""
function flux_to_dataframe_multi(srv::InfluxServer, flux_query::String)
    pairs_list = parse_annotated_csv(flux(srv, flux_query))

    groups = Dict{Symbol,Vector{DataFrame}}()
    order = Symbol[]

    for (name, df) in pairs_list
        if !haskey(groups, name)
            groups[name] = DataFrame[]
            push!(order, name)
        end
        push!(groups[name], df)
    end

    NamedTuple(name => get(groups, name, DataFrame[]) for name in order)
end

"""
    flux_to_dataframe(srv, flux_query) -> DataFrame

Execute a Flux query and return the result as a single `DataFrame`.

```julia
df = flux_to_dataframe(srv, \"\"\"
    from(bucket: "sensors")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "cpu_load")
\"\"\")
# df has columns: result, table, _start, _stop, _time, _value, _field, _measurement, host, …
```

Throws if the query returns more than one result table. Use
[`flux_to_dataframe_multi`](@ref) when multiple results are expected.
"""
function flux_to_dataframe(srv::InfluxServer, flux_query::String)
    only(last.(parse_annotated_csv(flux(srv, flux_query))))
end

"""
    clean_influx_df(df) -> DataFrame

Drop the InfluxDB bookkeeping columns (`result`, `table`, `_start`, `_stop`,
`_measurement`, `Column1`) from a DataFrame, leaving only the time and field
columns.

```julia
raw = flux_to_dataframe(srv, "from(bucket: \\"env\\") |> range(start: -1h) |> pivot(...)")
# raw has columns: result, table, _start, _stop, _measurement, _time, temp, humidity
df = clean_influx_df(raw)
# df has columns: _time, temp, humidity
```
"""
function clean_influx_df(df::DataFrame)
    dropcols = Set(["result", "table", "_start", "_stop", "_measurement", "Column1"])
    keep = filter(c -> !(String(c) in dropcols), names(df))
    return df[:, keep]
end

"""
    measurement_multi(srv, bucket, measurement_name, from, to) -> Vector{DataFrame}

Fetch a measurement over a time range and return one `DataFrame` per table
group (typically one per tag-set combination). Each `DataFrame` has one column
per field plus a `_time` column of epoch-nanosecond integers.

```julia
dfs = measurement_multi(srv, "sensors", "temperature", now() - Minute(10), now())
# dfs[1] — columns: _time, value  (tag set A)
# dfs[2] — columns: _time, value  (tag set B)
```

See also [`measurement`](@ref) when only one table group is expected.
"""
function measurement_multi(
    srv::InfluxServer,
    bucket::String,
    measurement_name::String,
    from::TimeSpec,
    to::TimeSpec,
)
    q = """
    from(bucket: "$bucket")
    |> range(start: time(v: uint(v: $(time_spec_to_epoc_ns(from)))), stop: time(v: uint(v: $(time_spec_to_epoc_ns(to)))))
    |> filter(fn: (r) => r._measurement == "$measurement_name")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> map(fn: (r) => ({ r with _time: uint(v: r._time) }))
    |> drop(columns: ["result", "table", "_start", "_stop", "_measurement"])
    |> yield(name: "out")
    """
    result = flux_to_dataframe_multi(srv, q).out
    [clean_influx_df(df) for df in result]
end

"""
    measurement(srv, bucket, measurement_name, from, to) -> DataFrame

Fetch a measurement over a time range and return it as a single `DataFrame`
with one column per field and a `_time` column of epoch-nanosecond integers.

```julia
df = measurement(srv, "sensors", "temperature", now() - Hour(1), now())
# df columns: _time, indoor, outdoor
```

Throws if the query returns more than one table group (i.e. multiple tag-set
combinations). Use [`measurement_multi`](@ref) in that case.
"""
function measurement(
    srv::InfluxServer,
    bucket::String,
    measurement_name::String,
    from::TimeSpec,
    to::TimeSpec,
)
    only(measurement_multi(srv, bucket, measurement_name, from, to))
end

"""
    aggregate_measurement_multi(srv, bucket, measurement_name, from, to, window; fn="mean")
    -> Vector{DataFrame}

Like [`measurement_multi`](@ref) but downsamples each field with an aggregate
function applied over `window`-sized time buckets. Returns one `DataFrame` per
tag-set combination.

```julia
# 5-minute means over the last hour
dfs = aggregate_measurement_multi(srv, "sensors", "temperature",
                                  now() - Hour(1), now(), Minute(5))

# Maximum instead of mean
dfs = aggregate_measurement_multi(srv, "sensors", "temperature",
                                  now() - Hour(1), now(), Minute(5); fn="max")
```

`window` can be any `Period`, e.g. `Second(30)`, `Minute(5)`, `Hour(1)`.

Common values for `fn`: `"mean"`, `"median"`, `"sum"`, `"count"`, `"min"`, `"max"`.
See the [Flux aggregateWindow docs](https://docs.influxdata.com/flux/v0/stdlib/universe/aggregatewindow/)
for the full list.
"""
function aggregate_measurement_multi(
    srv::InfluxServer,
    bucket::String,
    measurement_name::String,
    from::TimeSpec,
    to::TimeSpec,
    window::Period;
    fn::String="mean",
)
    q = """
    from(bucket: "$bucket")
    |> range(start: time(v: uint(v: $(time_spec_to_epoc_ns(from)))), stop: time(v: uint(v: $(time_spec_to_epoc_ns(to)))))
    |> filter(fn: (r) => r._measurement == "$measurement_name")
    |> aggregateWindow(every: $(Nanosecond(window).value)ns, fn: $fn, createEmpty: false)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> map(fn: (r) => ({ r with _time: uint(v: r._time) }))
    |> drop(columns: ["result", "table", "_start", "_stop", "_measurement"])
    |> yield(name: "out")
    """
    result = flux_to_dataframe_multi(srv, q).out
    [clean_influx_df(df) for df in result]
end

"""
    aggregate_measurement(srv, bucket, measurement_name, from, to, window; fn="mean")
    -> DataFrame

Downsample a measurement into fixed-width time buckets and return a single
`DataFrame`. See [`aggregate_measurement_multi`](@ref) for supported `fn` values.

```julia
# Hourly means over the past day
df = aggregate_measurement(srv, "sensors", "temperature",
                           now() - Day(1), now(), Hour(1))
# df columns: _time, indoor, outdoor

# 10-minute maxima
df = aggregate_measurement(srv, "power", "consumption",
                           now() - Hour(6), now(), Minute(10); fn="max")
```

Throws if the query returns more than one table group. Use
[`aggregate_measurement_multi`](@ref) in that case.
"""
function aggregate_measurement(
    srv::InfluxServer,
    bucket::String,
    measurement_name::String,
    from::TimeSpec,
    to::TimeSpec,
    window::Period;
    fn::String="mean",
)
    only(
        aggregate_measurement_multi(srv, bucket, measurement_name, from, to, window; fn=fn)
    )
end

"""
    list_buckets(srv) -> Vector{String}

Return the names of all buckets visible to the configured API token.

```julia
list_buckets(srv)
# ["_monitoring", "_tasks", "sensors", "power"]
```
"""
function list_buckets(srv::InfluxServer)
    String.(flux_to_dataframe(srv, "buckets()")[:, :name])
end

"""
    list_measurements(srv, bucket) -> Vector{String}

Return the measurement names stored in `bucket`.

```julia
list_measurements(srv, "sensors")
# ["humidity", "pressure", "temperature"]
```
"""
function list_measurements(srv::InfluxServer, bucket::String)
    q = """
    import "influxdata/influxdb/schema"
    schema.measurements(bucket: "$bucket")
    """
    String.(flux_to_dataframe(srv, q)[:, "_value"])
end

@deprecate buckets(srv::InfluxServer) list_buckets(srv)
@deprecate measurements(srv::InfluxServer, bucket::String) list_measurements(srv, bucket)

"""
    list_fields(srv, bucket) -> Vector{String}
    list_fields(srv, bucket, measurement) -> Vector{String}

Return field key names in `bucket`, optionally filtered to a single `measurement`.

```julia
list_fields(srv, "sensors")
# ["humidity", "pressure", "temperature"]

list_fields(srv, "sensors", "temperature")
# ["indoor", "outdoor"]
```
"""
function list_fields(srv::InfluxServer, bucket::String)
    q = """
    import "influxdata/influxdb/schema"
    schema.fieldKeys(bucket: "$bucket")
    """
    String.(flux_to_dataframe(srv, q)[:, "_value"])
end

function list_fields(srv::InfluxServer, bucket::String, measurement::String)
    q = """
    import "influxdata/influxdb/schema"
    schema.measurementFieldKeys(bucket: "$bucket", measurement: "$measurement")
    """
    String.(flux_to_dataframe(srv, q)[:, "_value"])
end

end # module
