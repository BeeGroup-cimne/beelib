from pydruid.db import connect


def run_druid_query(druid_conf, query):
    druid = connect(**druid_conf)
    cursor = druid.cursor()
    cursor.execute(query)
    df_dic = cursor.fetchall()
    return[item._asdict() for item in df_dic]
