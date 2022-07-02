# %%
import sys
import json
import datetime
from typing import List, Tuple, Dict, Any
from sql.sql_reader import getSQLDict
from common.files_reader import read_json
from connection.connection import Database

# %%
def run_insert_clients_resume(debug: bool = False) -> str:
    """
    Executes the insert_clients_resume.sql script.
    Returns:
        A string with the number of rows affected.
    """
    sql_dict: Dict[str, str] = getSQLDict("/home/FABIO/etl/src/sql/etls.sql")
    if debug: print(f"sql_dict: {sql_dict}\n\n")
    connection_data: Dict[str, Any] = read_json("/home/FABIO/etl/private/postgres_data.json")
    if debug: print(f"connection_data: {connection_data}\n\n")
    db = Database(**connection_data)
    sql: str = sql_dict.get("CLIENTS_RESUME_INSERT")
    if debug: print(f"sql: {sql}\n\n")
    rows_affected = db.insert(sql)
    print(rows_affected)
    return rows_affected

def read_data(sql_name: str, sql_vars: Dict[str, str], debug: bool = False) -> List[Dict[str, Any]]:
    """
    Executes the sql_name script.
    Args:
        sql_name: The name of the sql script to execute.
        sql_vars: The variables to replace in the sql script.
        debug: If True, prints the sql script and the variables.
    Returns:
        A list of dictionaries with the data.
    """
    if isinstance(sql_vars, str):
        if debug: print(f"isinstance(sql_vars, str) = {isinstance(sql_vars, str)}")
        sql_vars: Dict[str, str] = json.loads(sql_vars.replace("'", '"'))
    
    sql_dict: Dict[str, str] = getSQLDict("/home/FABIO/etl/src/sql/etls.sql")
    if debug: print(f"sql_dict: {sql_dict}\n\n")

    connection_data: Dict[str, Any] = read_json("/home/FABIO/etl/private/postgres_data.json")
    if debug: print(f"connection_data: {connection_data}\n\n")

    db = Database(**connection_data)
    sql: str = sql_dict.get(sql_name).format(**sql_vars)
    if debug: print(f"sql: {sql}\n\n")

    select: List[str] = [x for x in sql.split("\n") if "SELECT" in x.upper()]
    if select:
        fields: List[str] = select[0].split(",")
    else:
        fields: List[str] = []

    columns: List[str] = [x.split(" ")[1].strip() if "SELECT" in x.upper() else x.strip() for x in fields]

    data_list = db.read(sql)
    if debug: print(f"data_list: {data_list}\n\n")

    output_rows: List[Dict[str, Any]] = []
    timestamp: str = datetime.datetime.now().isoformat()
    for n, row in enumerate(data_list):
        row_dict: Dict[str, Any] = {"timestamp": timestamp, "result_no": n+1}
        for index, column_data in enumerate(row):
            if isinstance(column_data, datetime.date):
                column_data = column_data.strftime("%Y-%m-%d")
            row_dict[columns[index]] = column_data
        
        output_rows.append(row_dict)
    
    if debug: print(f"output_rows: {output_rows}\n\n")

    return output_rows

if __name__=="__main__":
    # run_insert_clients_resume(debug = True)
    rows = read_data(sys.argv[1], sys.argv[2], True)
    rows