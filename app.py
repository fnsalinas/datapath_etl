import json
from pkgutil import get_data
from typing import List, Dict, Any, Tuple
from common.files_reader import read_json
from connection.connection import Database
from flask import Flask, request, jsonify
from sql.sql_reader import getSQLDict
from etl_exec.postgres_execution import (
    read_data,
    run_insert_clients_resume,
    run_recreate_date_department_category_product_resume
    )

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.route('/')
def index():
    return "Hola mundo"

@app.route('/query_per_client_name', methods = ["GET"])
def query_per_client_name():
    input_data: Dict[str, str] = json.loads(request.get_data().decode())
    print(f"input_data: {input_data}")
    if request.method == "GET":
        try:
            client_name: str = input_data.get("client_name")
            data: Dict[str, str] = read_data("CLIENTS_RESUME_QUERY_PER_NAME", {"client_name": client_name}, False)
            
        except Exception as e:
            return str(e)
    
    return jsonify(data)

@app.route('/query_per_client_name_and_date', methods = ["GET"])
def query_per_client_name_and_date():
    input_data: Dict[str, str] = json.loads(request.get_data().decode())
    if request.method == "GET":
        try:
            data: Dict[str, str] = read_data("CLIENTS_RESUME_QUERY_PER_NAME_DATE", input_data, False)
            
        except Exception as e:
            return str(e)
    
    return jsonify(data)

@app.route('/run_etl', methods = ["GET"])
def etl_process():
    rows_affected: List[Dict[str, str]] = []
    if request.method == "GET":
        input_data: Dict[str, str] = json.loads(request.get_data().decode())
        functions: Dict[str, Any] = {
            "run_insert_clients_resume": run_insert_clients_resume,
            "run_recreate_date_department_category_product_resume": run_recreate_date_department_category_product_resume
            }
        try:
            if input_data.get("processes"):
                for process in input_data.get("processes"):
                    rows_affected.append({process: functions[process](False)})
            
        except Exception as e:
            return str(e)
    
    return jsonify(rows_affected)

if __name__ == "__main__":
    app.debug = True
    app.run()