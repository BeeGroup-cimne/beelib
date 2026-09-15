import time
from datetime import datetime, timezone
import influxdb_client
import isodate
import pandas as pd
import certifi
import urllib3
from influxdb_client.rest import ApiException

from beelib.beeretry import retry_with_backoff

# influxdb_client no llenca res en connectar (el client es lazy): els talls de
# xarxa/servei apareixen en fer la crida real (query/write). Aquestes son les
# excepcions que val la pena reintentar -- errors de transport i 5xx del
# servidor -- no els 4xx (query mal formada, auth, etc.) que fallarien igual
# per molt que s'esperi.
INFLUX_TRANSIENT_EXCEPTIONS = (urllib3.exceptions.HTTPError, ConnectionError, TimeoutError, ApiException)


def connect_influx(influx_connection):
    client = influxdb_client.InfluxDBClient(
        url=influx_connection['connection']['url'],
        org=influx_connection['connection']['org'],
        token=influx_connection['connection']['token'],
        ssl_ca_cert=certifi.where(),
        timeout=60000
    )
    return client


def run_query(influx_connection, query):
    client = connect_influx(influx_connection)
    query_api = client.query_api()
    return retry_with_backoff(query_api.query_data_frame, query, exceptions=INFLUX_TRANSIENT_EXCEPTIONS)


def get_timeseries_by_hash(d_hash, freq, influx_connection, ts_ini, ts_end, diff="final"):
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
        |> filter(fn: (r) => r["run_status"]=="{diff}")
        |> keep(columns: ["_time", "value", "end", "isReal"])
    """
    client = connect_influx(influx_connection)
    query_api = client.query_api()
    df = retry_with_backoff(query_api.query_data_frame, query, exceptions=INFLUX_TRANSIENT_EXCEPTIONS)
    if df.empty:
        return pd.DataFrame()
    df['end'] = pd.to_datetime(df['end'], unit="s").dt.tz_localize("UTC")
    df.rename(columns={"_time": "start"}, inplace=True)
    df = df[["start", "end", "isReal", "value"]]
    df.set_index("start", inplace=True)
    return df

