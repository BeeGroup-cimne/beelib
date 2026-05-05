from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
import datetime


def get_session(cassandra_connection):
    if cassandra_connection['auth']['active'] == "true":
        auth_provider = PlainTextAuthProvider(
            username=cassandra_connection['auth']['username'],
            password=cassandra_connection['auth']['password']
        )
    else:
        auth_provider = None
    cluster = Cluster(
        cassandra_connection['connection']['contact_points'],
        load_balancing_policy=DCAwareRoundRobinPolicy(
            local_dc=cassandra_connection['connection']['local_dc']
        ),
        port=cassandra_connection['connection']['port'],
        auth_provider=auth_provider,
        connect_timeout=10,
        protocol_version=5
    )
    session = cluster.connect()
    return session, cluster


def __create_table__(session, table_name, options):
    """
        table_name = name of table
        options = {
                    'partition_rows': {
                        'rows': ['uri', 'mes'],
                        'types': ['text', 'int']
                    },
                    'sort_row': {
                        'rows': ['timestamp', 'tag'],
                        'types': ['timestamp', 'int']
                    },
                    'columns': {
                        'column_name': ['column', 'column', 'all'],
                    }
                }
    """
    row_def = options['partition_rows']['rows'] + options['sort_row']['rows']
    type_def = options['partition_rows']['types'] + options['sort_row']['types']
    rows = ",".join([f"{x[0]} {x[1]}" for x in zip(row_def, type_def)])
    colum_def = [k for k, _ in options['columns'].items()]
    columns = ",".join([f"{c} map<text, text>" for c in colum_def])
    partition_keys = ",".join(options['partition_rows']['rows'])
    sort_keys = ",".join(options['sort_row']['rows'])
    clustering_order = ",".join(f"{x} DESC" for x in options['sort_row']['rows'])
    query_create_taula = f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            {rows},
            {columns},
            PRIMARY KEY (({partition_keys}), {sort_keys})
        ) WITH CLUSTERING ORDER BY ({clustering_order});
    """
    try:
        session.execute(query_create_taula)
    except Exception as e:
        print(f"Error creant la taula: {e}")


def save_to_cassandra(documents, table_name, session, options):
    __create_table__(session, table_name, options)
    key_columns = [k for k, _ in options['columns'].items()]
    table_columns = options['partition_rows']['rows'] + options['sort_row']['rows'] + key_columns
    keys_query = ",".join(table_columns)
    rows_values = ",".join(["?" for _ in table_columns])
    insert_query = f"""
        INSERT INTO {table_name}
            ({keys_query})
        VALUES ({rows_values})
    """
    docs = []
    for doc in documents:
        keys = options['partition_rows']['rows'] + options['sort_row']['rows']
        col_map = options['columns']
        l = []
        for key in keys:
            l.append(doc.pop(key))
        for col, fields in col_map.items():
            col_value = {}
            for field in fields:
                if field == 'all':
                    col_value = doc
                    break
                else:
                    if field in doc:
                        col_value[field] = doc[field]
            l.append(col_value)
        docs.append(tuple(l))
    try:
        insert_q = session.prepare(insert_query)
        results = execute_concurrent_with_args(session, insert_q, docs, concurrency=100)
        for (success, result) in results:
            if not success:
                print("Error en una inserció:", result)
    except Exception as e:
        print(f"Error inserint les dades: {e}")


def __parse_query_parameters__(parameter):
    if isinstance(parameter, int):
        return parameter
    elif isinstance(parameter, str):
        return f"'{parameter}'"
    elif isinstance(parameter, datetime.datetime):
        return f"'{parameter.strftime('%Y-%m-%d %H:%M:%S')}'"
    else:
        raise TypeError(f"Parameter type '{type(parameter)}' is not supported")


def get_cassandra_data(
        table_name, fix_key, start_key, end_key,
        sort_key_start, sort_key_end, session, batch_size=10000):
    """
    :param table_name: name of the Cassandra table
    :param fix_key: {"key1": "val", "key2": 2} - partition key fields with fixed values
    :param start_key: {"year": 2023} - range partition key start (int), or None
    :param end_key: {"year": 2024} - range partition key end (int), must have same key as start_key
    :param sort_key_start: {"ts": 1234567890} - clustering key lower bound (int or datetime)
    :param sort_key_end: {"ts": 1234567890} - clustering key upper bound (int or datetime)
    :param session: Cassandra session
    :param batch_size: number of rows per batch (default 10000)
    :return: generator of row batches (list of dicts)
    """
    if bool(start_key) != bool(end_key):
        raise ValueError("start_key and end_key must both be provided or both be None")
    if start_key and end_key:
        k_start = list(start_key.keys())[0]
        k_end = list(end_key.keys())[0]
        if k_start != k_end:
            raise ValueError(
                f"start_key and end_key must have the same key, got '{k_start}' and '{k_end}'"
            )
    if bool(sort_key_start) != bool(sort_key_end):
        raise ValueError("sort_key_start and sort_key_end must both be provided or both be None")
    if sort_key_start and sort_key_end:
        k_sort_start = list(sort_key_start.keys())[0]
        k_sort_end = list(sort_key_end.keys())[0]
        if k_sort_start != k_sort_end:
            raise ValueError(
                f"sort_key_start and sort_key_end must have the same key, "
                f"got '{k_sort_start}' and '{k_sort_end}'"
            )
    requests_db = []
    if start_key and end_key:
        k, sv = list(start_key.items())[0]
        k2, ev = list(end_key.items())[0]
        for x in range(sv, ev):
            curr_req = {k: {"v": v} for k, v in fix_key.items()}
            curr_req[k] = {"v": x}
            requests_db.append(curr_req)
        curr_req = {k: {"v": v} for k, v in fix_key.items()}
        curr_req[k] = {"v": ev}
        requests_db.append(curr_req)
        requests_db[0].update({
            f"__sort_{k}_start": {"v": v, "op": ">", "col": k}
            for k, v in sort_key_start.items()
        })
        requests_db[-1].update({
            f"__sort_{k}_end": {"v": v, "op": "<", "col": k}
            for k, v in sort_key_end.items()
        })
    else:
        requests_db.append(fix_key)

    session.default_fetch_size = batch_size
    current_batch = []
    for req in requests_db:
        if req:
            conditions = [
                f"{v.get('col', k)}{v['op'] if 'op' in v else '='}{__parse_query_parameters__(v['v'])}"
                for k, v in req.items()
            ]
            query = f"SELECT * FROM {table_name} WHERE {' AND '.join(conditions)};"
        else:
            query = f"SELECT * FROM {table_name};"

        statement = SimpleStatement(query, fetch_size=batch_size)
        result_set = session.execute(statement)
        while True:
            for row in result_set.current_rows:
                d = row._asdict()
                info = d.pop("info")
                d.update(info)
                current_batch.append(d)
                if len(current_batch) == batch_size:
                    yield current_batch
                    current_batch = []
            if result_set.paging_state:
                result_set = session.execute(statement, paging_state=result_set.paging_state)
            else:
                break
    if current_batch:
        yield current_batch
