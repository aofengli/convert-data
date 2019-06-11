pg_username = ''
pg_passwd = ''
pg_host = ''
pg_port = 5432
pg_db_name = ''
pg_uri = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(pg_username, pg_passwd,
                                                       pg_host, pg_port,
                                                       pg_db_name)

hive_host = ''
hive_username = ''
hive_passwd = ''
hive_port = 21050
hive_db = ''


hive_all_data = []
kw = {
    'truncate': 1
}

hive_2_pg_type = {
    'TINYINT':'int2',
    'SMALLINT':'smallint',
    'INT':'int8',
    'BIGINT':'int8',
    'FLOAT':'double',
    'DOUBLE':'double',
    'DECIMAL':'decimal',
    'TIMESTAMP':'',
    'STRING':'text',
    'VARCHAR':'',
    'CHAR':'',
    'BOOLEAN':'',
    'BINARY':'',
    'arrays':'',
    'structs':'',
    'union':'',
}
