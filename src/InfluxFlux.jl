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
    measurements,
    buckets,
    list_buckets,
    list_measurements,
    list_fields


TimeSpec = Union{Int,DateTime,ZonedDateTime}


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

Base.showerror(io::IO, e::InfluxFluxError) = begin
    println(io, "InfluxFluxError (HTTP $(e.status))")
    println(io, "Code: ", something(e.code, "unknown"))
    println(io, e.message)
end


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
    HTTP.URI(HTTP.URI("$(srv.uri)/$path"), query = Dict("org" => srv.org))
end


function token_json_headers(srv::InfluxServer)
    Dict("Authorization" => "Token $(srv.api_token)", "Accept" => "application/json")
end


function influx_server(uri::String, org::String, api_token::String)::InfluxServer
    InfluxServer(uri, org, api_token)
end


function flux(srv::InfluxServer, flux_query::String)
    headers = merge(token_json_headers(srv), Dict("Content-Type" => "application/vnd.flux"))

    response = HTTP.post(uri_helper(srv, "api/v2/query"), headers, flux_query)

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
                (CSV.File(IOBuffer(join(data_lines, "\n")), delim = ',') |> DataFrame),
        )
    end
    result
end


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

function flux_to_dataframe(srv::InfluxServer, flux_query::String)
    only(last.(parse_annotated_csv(flux(srv, flux_query))))
end

function clean_influx_df(df::DataFrame)
    dropcols = Set(["result", "table", "_start", "_stop", "_measurement", "Column1"])
    keep = filter(c -> !(String(c) in dropcols), names(df))
    return df[:, keep]
end

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
    [clean_influx_df(df) for df in result] # remove influxdb internal columns
end


function measurement(
    srv::InfluxServer,
    bucket::String,
    measurement_name::String,
    from::TimeSpec,
    to::TimeSpec,
)
    only(measurement_multi(srv, bucket, measurement_name, from, to))
end


function aggregate_measurement_multi(
    srv::InfluxServer,
    bucket::String,
    measurement_name::String,
    from::TimeSpec,
    to::TimeSpec,
    window::Period;
    fn::String = "mean",
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
    [clean_influx_df(df) for df in result] # remove influxdb internal columns
end

function aggregate_measurement(
    srv::InfluxServer,
    bucket::String,
    measurement_name::String,
    from::TimeSpec,
    to::TimeSpec,
    window::Period;
    fn::String = "mean",
)
    only(
        aggregate_measurement_multi(
            srv,
            bucket,
            measurement_name,
            from,
            to,
            window;
            fn = fn,
        ),
    )
end



function measurements(srv::InfluxServer, bucket::String)
    q = """
    import "influxdata/influxdb/schema"
    schema.measurements(bucket: "$bucket")
    """
    String.(flux_to_dataframe(srv, q)[:, "_value"])
end


function buckets(srv::InfluxServer)
    String.(flux_to_dataframe(srv, "buckets()")[:, :name])
end


function list_buckets(srv::InfluxServer)
    buckets(srv)
end


function list_measurements(srv::InfluxServer, bucket::String)
    measurements(srv, bucket)
end


function list_fields(srv::InfluxServer, bucket::String)
    q = """
    import "influxdata/influxdb/schema"
    schema.fieldKeys(bucket: "$bucket")
    """
    String.(InfluxFlux.flux_to_dataframe(srv, q)[:, "_value"])
end


function list_fields(srv::InfluxServer, bucket::String, measurement::String)
    q = """
    import "influxdata/influxdb/schema"
    schema.measurementFieldKeys(bucket: "$bucket", measurement: "$measurement")
    """
    String.(InfluxFlux.flux_to_dataframe(srv, q)[:, "_value"])
end

end # module
