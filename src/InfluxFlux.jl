module InfluxFlux

using HTTP
using CSV
using Dates
using DataFrames
using TimeZones

export time_spec_to_epoc_ns, influx_server, flux, flux_to_dataframe,
       measurement, aggregate_measurement, measurements, buckets,
       list_buckets, list_measurements, list_fields


TimeSpec = Union{Int,DateTime,ZonedDateTime}


struct InfluxServer
    uri::String
    org::String
    api_token::String
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
    Dict("Authorization" => "Token $(srv.api_token)"
         , "Accept" => "application/json")
end


function influx_server(uri::String, org::String, api_token::String)::InfluxServer
    InfluxServer(uri, org, api_token)
end


function flux(srv::InfluxServer, flux_query::String)
    headers = merge(token_json_headers(srv), Dict("Content-Type" => "application/vnd.flux"))
    response = HTTP.post(uri_helper(srv, "api/v2/query"), headers, flux_query);
    if response.status == 200
        response.body
    else
        throw(response.status)
    end
end


function flux_to_dataframe(srv::InfluxServer, flux_query::String)
    CSV.File(flux(srv, flux_query), delim = ',') |> DataFrame
end


function measurement(srv::InfluxServer, bucket::String, measurement_name::String, from::TimeSpec, to::TimeSpec)
    q = """
    from(bucket: "$bucket")
    |> range(start: time(v: uint(v: $(time_spec_to_epoc_ns(from)))), stop: time(v: uint(v: $(time_spec_to_epoc_ns(to)))))
    |> filter(fn: (r) => r._measurement == "$measurement_name")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> map(fn: (r) => ({ r with _time: uint(v: r._time) }))
    """
    #println(q)
    result = flux_to_dataframe(srv, q)
    result[:,7:end] # remove influxdb internal columns
end


function aggregate_measurement(srv::InfluxServer, bucket::String, measurement_name::String, from::TimeSpec, to::TimeSpec, window::Period; fn::String = "mean")
    q = """
    from(bucket: "$bucket")
    |> range(start: time(v: uint(v: $(time_spec_to_epoc_ns(from)))), stop: time(v: uint(v: $(time_spec_to_epoc_ns(to)))))
    |> filter(fn: (r) => r._measurement == "$measurement_name")
    |> aggregateWindow(every: $(Nanosecond(window).value)ns, fn: $fn, createEmpty: false)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> map(fn: (r) => ({ r with _time: uint(v: r._time) }))
    """
    #println(q)
    result = flux_to_dataframe(srv, q)
    result[:,7:end] # remove influxdb internal columns
end


function measurements(srv::InfluxServer, bucket::String)
    q = """
    import "influxdata/influxdb/schema"
    schema.measurements(bucket: "$bucket")
    """
    String.(InfluxFlux.flux_to_dataframe(srv, q)[:, "_value"])
end


function buckets(srv::InfluxServer)
    String.(InfluxFlux.flux_to_dataframe(srv, "buckets()")[:, :name])
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
