QUERY_INSERT_TEMPLATE = """INSERT INTO child_missing.iot_logs (device_id, latitude, longitude, vibration, acceleration) VALUES ('12G40D9FT3328HS428', {} )"""
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
