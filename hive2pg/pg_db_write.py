import psycopg2
from contextlib import closing

from configs import pg_db_name, pg_username, pg_passwd, pg_host, pg_port
from utils import FileObjFaker


class PGDBWrite:
    def __init__(self):
        self.pg_conn = psycopg2.connect(database=pg_db_name,
                                        user=pg_username,
                                        password=pg_passwd,
                                        host=pg_host,
                                        port=pg_port)

    def write_table(self, table):
        """
        创建表
        # TODO
        待完善，需要创建一个hive和postgres的映射表
        """
        query = f'CREATE TABLE {table.name} ( \n'
        i = 0
        lenth = len(table.columns) - 1
        for k, v in table.columns_dict.items():
            if i < lenth:
                query += '"' + k + '"' + ' text, \n'
            else:
                query += '"' + k + '"' + ' text \n'
            i += 1
        query += ');'
        with closing(self.pg_conn.cursor()) as pg_cur:
            pg_cur.execute(query)
        self.pg_conn.commit()

    def truncate(self, table):
        """
        清空表数据
        """
        with closing(self.pg_conn.cursor()) as pg_cur:
            pg_cur.execute(f'TRUNCATE TABLE {table.name};')
        self.pg_conn.commit()

    def writer_index(self, table):
        """
        创建索引
        # TODO
        """
        pass

    def write_contents(self, table, reader):
        f = FileObjFaker(reader.read(table))
        self.copy_from(f, f'{table.name}', table.columns)

    def copy_from(self, file_obj, table_name, columns):
        with closing(self.pg_conn.cursor()) as pg_cur:
            pg_cur.copy_from(file_obj, table_name, columns=columns)
        self.pg_conn.commit()

    def close(self):
        self.pg_conn.close()
