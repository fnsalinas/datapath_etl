from typing import Dict, Any
import psycopg2

class Database:

    def __init__(self, host: str, port: str, database: str, user: str, password: str, sslmode: str, sslcert: str, sslkey: str, sslrootcert: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.sslmode = sslmode
        self.sslcert = sslcert
        self.sslkey = sslkey
        self.sslrootcert = sslrootcert

    def _connect(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            sslmode=self.sslmode,
            sslcert=self.sslcert,
            sslkey=self.sslkey,
            sslrootcert=self.sslrootcert
            )

    def read(self, sql: str):

        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()
        except psycopg2.Error as e:
            raise e
        finally:
            conn.close()

    def insert(self, sql):

        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            return f"Number of rows affected: {cur.rowcount:,.0f}"
        except psycopg2.Error as e:
            raise e
        finally:
            conn.close()

if __name__ == "__main__":

    from common.files_reader import read_json
    connection_data: Dict[str, Any] = read_json("/home/FABIO/etl/private/postgres_data.json")
    testdb = Database(**connection_data)
    print("-"*30 + "> Connection Data:")
    print(connection_data)
    print("-"*30 + "> Testing query:")
    print(testdb.read("select * from public.etl_clients_resume limit 10;"))