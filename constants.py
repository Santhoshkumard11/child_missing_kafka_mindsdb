QUERY_INSERT_TEMPLATE = """INSERT INTO child_missing.ml_iot_logs VALUES {}"""

QUERY_PREDICTION = """SELECT 
    missing, missing_explain
FROM 
    mindsdb.child_missing_model
WHERE 
    latitude = {}
    AND longitude={}
    AND vibration={}
    AND acceleration={};
"""
