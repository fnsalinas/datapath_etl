import json
from pkgutil import get_data
from typing import List, Dict, Any, Tuple
from common.files_reader import read_json
from connection.connection import Database
from flask import Flask, request, jsonify
from sql.sql_reader import getSQLDict
from etl_exec.postgres_execution import read_data

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
    
    return jsonify({"Data": data})

if __name__ == "__main__":
    app.debug = True
    app.run()