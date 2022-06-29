from typing import Dict, Any
from sql.sql_reader import getSQLDict
from common.files_reader import read_json
from connection.connection import Database

def run_insert_clients_resume() -> str:
    """
    Executes the insert_clients_resume.sql script.
    Returns:
        A string with the number of rows affected.
    """
    sql_dict: Dict[str, str] = getSQLDict("/home/FABIO/etl/src/sql/etls.sql")
    connection_data: Dict[str, Any] = read_json("/home/FABIO/etl/private/postgres_data.json")
    db = Database(**connection_data)
    sql: str = sql_dict.get("CLIENTS_RESUME_INSERT")
    rows_affected = db.insert(sql)
    print(rows_affected)
    return rows_affected