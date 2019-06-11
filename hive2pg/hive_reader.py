from contextlib import closing
from impala.dbapi import connect
from configs import hive_host, hive_username,\
    hive_passwd, hive_host, hive_db, hive_2_pg_type


class Table:
    def __init__(self, reader, name):
        self.reader = reader
        self.name = name
        self._columns_dict = {}
        self._columns = self._load_columns()

    def query_for(self):
        query = """
            SELECT
                {}
            FROM
                {}
        """.format(', '.join(c for c in self._columns), self.name)
        return query

    def _load_columns(self):
        fields = []
        with closing(self.reader.hive_conn.cursor()) as hive_cur:
            hive_cur.execute(f'describe {self.name};')
            for row in hive_cur:
                fields.append(row[0])
                self._columns_dict[row[0]] = row[1]
        return fields

    @property
    def columns(self):
        return self._columns

    @property
    def columns_dict(self):
        return self._columns_dict


class HiveReader:
    def __init__(self):
        self.hive_conn = connect(host=hive_host,
                                 port=21050,
                                 user=hive_username,
                                 password=hive_passwd,
                                 database=hive_db)

    def read(self, table):
        return self._query(table.query_for())

    def _query(self, sql):
        with closing(self.hive_conn.cursor()) as hive_cur:
            # hive_cur = self.hive_conn.cursor()
            hive_cur.execute(sql)
            for row in hive_cur:
                yield row

    def closing(self):
        self.hive_conn.close()