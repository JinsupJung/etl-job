import requests
import json
import pymysql
import logging
from datetime import datetime

# MySQL database connection details
db_config = {
    "host": "175.196.7.45",
    "user": "nolboo",
    "password": "2024!puser",
    "database": "nolboo"
}

# API URL
api_url = "https://poson.easypos.net/servlet/EasyPosJsonChannelSVL?cmd=TlxSyncEasyposSaleCMD"

# API Request Data
request_data = {
    "s_code": "9",
    "hd_code": "I9X",
    "sp_code": "000001"
}

# API Request Headers
headers = {
    "Content-Type": "application/json"
}

# Set up log file with today's date
log_filename = f"/home/nolboo/etl-job/log/load_prod_data_log_{datetime.now().strftime('%Y%m%d')}.txt"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Connect to MySQL database
connection = pymysql.connect(
    host=db_config['host'],
    user=db_config['user'],
    password=db_config['password'],
    database=db_config['database']
)

try:
    with connection.cursor() as cursor:
        # Truncate the table before inserting new data
        logging.info("Truncating the kicc_product_list table.")
        cursor.execute("TRUNCATE TABLE kicc_product_list")

        # Send API request
        response = requests.post(api_url, headers=headers, data=json.dumps(request_data))
        
        if response.status_code == 200:
            response_data = response.json()
            ret_code = response_data.get("ret_code")

            if ret_code == "0000":
                data = response_data.get("data", [])
                for item in data:
                    fields = [
                        "hd_code", "sp_code", "item_code", "item_name", 
                        "large_scale_nm", "medium_scale_nm", "small_scale_nm", "item_cost", "item_price"
                    ]
                    
                    data_tuple = [item.get(field) for field in fields]

                    # Log the content of the data_tuple
                    logging.info(f"Data tuple content: {data_tuple}")

                    sql = """
                    INSERT INTO kicc_product_list (
                        hd_code, sp_code, item_code, item_name, 
                        large_scale_nm, medium_scale_nm, small_scale_nm, item_cost, item_price
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    logging.info(f"Inserting new data for item_code {item.get('item_code')}")
                    cursor.execute(sql, tuple(data_tuple))

                connection.commit()
                logging.info("Product data saved successfully.")
            else:
                logging.error(f"API Error: {ret_code}")
        else:
            logging.error(f"HTTP Request Failed: {response.status_code}")

finally:
    connection.close()
