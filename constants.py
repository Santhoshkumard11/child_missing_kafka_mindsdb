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
