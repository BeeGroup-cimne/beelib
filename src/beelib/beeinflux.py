import time
from datetime import datetime, timezone
import influxdb_client
import isodate
import pandas as pd


def connect_influx(influx_connection):
    client = influxdb_client.InfluxDBClient(
        url=influx_connection['connection']['url'],
        org=influx_connection['connection']['org'],
        token=influx_connection['connection']['token'],
        timeout=60000
    )
    return client


def run_query(influx_connection, query):
    client = connect_influx(influx_connection)
    query_api = client.query_api()
    return query_api.query_data_frame(query)


def get_timeseries_by_hash(d_hash, freq, influx_connection, ts_ini, ts_end):
    aggregation_window = int(isodate.parse_duration(freq).total_seconds() * 10**9)
    start = int(ts_ini.timestamp()) * 10**9
    end = int(ts_end.timestamp()) * 10**9
    query = f"""
        from(bucket: "{influx_connection['bucket']}")
        |> range(start: time(v:{start}), stop: time(v:{int(end)}))
        |> filter(fn: (r) => r["_measurement"] == "{influx_connection['measurement']}")
        |> filter(fn: (r) => r["hash"] == "{d_hash}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> filter(fn: (r) => r["is_null"]==0.0)
        |> keep(columns: ["_time", "value", "end", "isReal"])
    """
    client = connect_influx(influx_connection)
    query_api = client.query_api()
    df = query_api.query_data_frame(query)
    if df.empty:
        return pd.DataFrame()
    df['end'] = pd.to_datetime(df['end'], unit="s").dt.tz_localize("UTC")
    df.rename(columns={"_time": "start"}, inplace=True)
    df = df[["start", "end", "isReal", "value"]]
    df.set_index("start", inplace=True)
    return df

