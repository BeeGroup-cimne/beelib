import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import re

from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement


# table_name = "modbus.timeseries_id_"
# options = {'partition_rows': {'rows': ['uri', 'year'], 'types': ['text', 'int']},"sort_row":{"row": "timestamp","type": "timestamp"}}
#cassandra_connection = {"auth": {'username': 'cassandra', 'password': 'trPQoVfVvn3W1R-v_cdgHuIPnxeaHa0hJ1KHdNxJBCpOMKiJ1AUYsg%'},
#                        "connection": {"contact_points": ['127.0.0.1'], "port": 9042}}


def get_session(cassandra_connection):
    auth_provider = PlainTextAuthProvider(username=cassandra_connection['auth']['username'], password=cassandra_connection['auth']['password'])
    cluster = Cluster(cassandra_connection['connection']['contact_points'], port=cassandra_connection['connection']['port'], auth_provider=auth_provider, connect_timeout=10)
    return cluster.connect()


def __create_table__(session, table_name, options):
    """
        cassandra_connection = cassandra connection config
        table_name = name of table
        options = {
                    'partition_rows': {
                        'rows': ['uri', 'mes'],
                        'types': ['text', 'int']
                    },
                    'sort_row': {
                        'row': 'timestamp',
                        'type': 'timestamp'
                    },
                    'columns': {
                        'column_name': ['column', 'column', 'all'],
                    }

                }

    """
    row_def = options['partition_rows']['rows'] + [options['sort_row']['row']]
    type_def = options['partition_rows']['types'] + [options['sort_row']['type']]
    rows = ",".join([f"{x[0]} {x[1]}" for x in zip(row_def, type_def)])
    colum_def = [k for k, _ in options['columns']]
    columns = ",".join([f"{c} map<text, text>" for c in colum_def])
    query_create_taula = f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            {rows}, 
            {columns}, 
            PRIMARY KEY (({",".join(options['partition_rows']['rows'])}), {options['sort_row']['row']})
        ) WITH CLUSTERING ORDER BY ({options['sort_row']['row']} DESC); 
    """
    try:
        session.execute(query_create_taula)
        print("Taula creada o ja existent.")
    except Exception as e:
        print(f"Error creant la taula: {e}")


def save_to_cassandra(documents, table_name, cassandra_connection, options):
    session = get_session(cassandra_connection)
    __create_table__(session, table_name, options)
    rows_keys = ",".join(options['partition_rows']['rows']+[options['sort_row']['row'], "info"])
    rows_values = ",".join(["?" for _ in options['partition_rows']['rows']+[options['sort_row']['row'], "info"]])
    insert_query = f"""
        INSERT INTO {table_name} 
            ({rows_keys}) 
        VALUES ({rows_values})
    """
    docs = []
    for doc in documents:
        keys = options['partition_rows']['rows'] + [options['sort_row']['row']]
        l = []
        for key in keys:
            l.append(doc.pop(key))
        l.append(doc)
        docs.append(tuple(l))
    try:
        insert_q = session.prepare(insert_query)
        results = execute_concurrent_with_args(session, insert_q, docs, concurrency=100)
        for (success, result) in results:
            if not success:
                print("Error en una inserció:", result)
    except Exception as e:
        print(f"Error creant la taula: {e}")


def __parse_query_parameters__(parameter):
    if isinstance(parameter, int):
        return parameter
    elif isinstance(parameter, str):
        return f"'{parameter}'"
    elif isinstance(parameter, datetime.datetime):
        return f"'{parameter.strftime('%Y-%m-%d %H:%M:%S')}'"
    else:
        raise TypeError(f"Parameter type '{type(parameter)}' is not supported")


def get_cassandra_data(table_name, fix_key, start_key, end_key, sort_key_start, sort_key_end, cassandra_connection, batch_size=10000):
    """
    :param table_name:
    :param fix_key: {"key1": ("val", str), "key2": ("val2", int), "key3": ("val3", str)}
    :param start_key: {"key4": (val3, int)}
    :param end_key: {"key4": (val5, str)}
    :param sort_key_start: {"key5": (val5, datetime)}
    :param sort_key_end: {"key5": (val6, datetime)}
    :param cassandra_connection:
    :param options:
    :return:
    """
    session = get_session(cassandra_connection)
    requests_db = []
    if start_key and end_key:
        k, sv = list(start_key.items())[0]
        k2, ev = list(end_key.items())[0]
        for x in range(sv, ev):
            curr_req = {k: {"v": v} for k,v in fix_key.items()}
            curr_req[k] = {"v": x}
            requests_db.append(curr_req)
        curr_req = {k: {"v": v} for k,v in fix_key.items()}
        curr_req[k] = {"v": ev}
        requests_db.append(curr_req)
        requests_db[0].update({k: {"v":v, "op":">"} for k,v in sort_key_start.items()})
        requests_db[-1].update({k: {"v":v, "op":"<"} for k,v in sort_key_end.items()})
    else:
        requests_db.append(fix_key)

    session.default_fetch_size = batch_size
    for req in requests_db:
        if req:
            where = " AND ".join([f"{k}{v['op'] if 'op' in v else '='}{__parse_query_parameters__(v['v'])}" for k,v in req.items()])
            query = f"""
                SELECT * FROM {table_name}
                WHERE {where};
            """
        else:
            query = f"""SELECT * FROM {table_name};"""

        statement = SimpleStatement(query, fetch_size=batch_size)
        result_set = session.execute(statement)
        current_batch = []
        for row in result_set:
            d = row._asdict()
            info = d.pop("info")
            d.update(info)
            current_batch.append(row)

        if len(current_batch) == batch_size:
            yield current_batch
            current_batch = []

        if current_batch:
            yield current_batch
    session.shutdown()
