from typing import List, Dict, Any
import sys

def getSQLDict(etl_path: str = "etls.sql") -> Dict[str, str]:
    """
    Returns a dictionary of SQL statements.
    Args:
        etl_path: path to the etl.sql file
    Returns:
        A dictionary of SQL statements.
    """
    with open(etl_path, 'r') as f:
        sql = f.read()

    sqls_list: List[str] = [f"--{x}" for x in sql.split("--") if x != ""]
    sqls_dict: Dict[str, str] = {x.split("\n")[0].replace("-- ", ""): x for x in sqls_list}
    return sqls_dict

if __name__ == "__main__":
    print(getSQLDict(sys.argv[1]))