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
    "s_code": "4",  # 고정
    "hd_code": "I9X",  # Replace with your hd_code
    "sp_code": "000001"  # Replace with your sp_code
}

# API Request Headers
headers = {
    "Content-Type": "application/json"
}

# Configure logging with daily log file
log_filename = f"/home/nolboo/etl-job/log/load_store_data_log_{datetime.now().strftime('%Y%m%d')}.txt"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Function to check if data already exists in the database
def data_exists(cursor, sp_code):
    query = "SELECT COUNT(*) FROM kicc_store_list WHERE sp_code = %s"
    cursor.execute(query, (sp_code,))
    result = cursor.fetchone()
    return result[0] > 0  # Returns True if the data exists

# Connect to MySQL database
connection = pymysql.connect(
    host=db_config['host'],
    user=db_config['user'],
    password=db_config['password'],
    database=db_config['database']
)

try:
    # Send API request
    response = requests.post(api_url, headers=headers, data=json.dumps(request_data))
    
    # Check if the request was successful
    if response.status_code == 200:
        response_data = response.json()
        ret_code = response_data.get("ret_code")

        if ret_code == "0000":
            data = response_data.get("data", [])
            with connection.cursor() as cursor:
                for item in data:
                    # Skip records with sp_code '000002'
                    if item.get("sp_code") == '000002':
                        logging.info(f"Skipping sp_code 000002.")
                        continue

                    # Check if the record already exists
                    if data_exists(cursor, item.get("sp_code")):
                        # Update existing record
                        sql = """
                        UPDATE kicc_store_list SET
                            sp_name = %s,
                            biz_no = %s,
                            open_flag = %s,
                            erp_sp_code = %s,
                            master_name = %s,
                            tel_no = %s,
                            sp_type = %s,
                            area_code = %s,
                            sale_class_code = %s,
                            sale_class_name = %s,
                            address1 = %s,
                            address2 = %s,
                            brand_code = %s,
                            brand_name = %s
                        WHERE sp_code = %s
                        """
                        data_tuple = (
                            item.get("sp_name"),
                            item.get("biz_no"),
                            item.get("open_flag"),
                            item.get("erp_sp_code"),
                            item.get("master_name"),
                            item.get("tel_no"),
                            item.get("sp_type"),
                            item.get("area_code"),
                            item.get("sale_class_code"),
                            item.get("sale_class_name"),
                            item.get("address1"),
                            item.get("address2"),
                            item.get("brand_code"),
                            item.get("brand_name"),
                            item.get("sp_code")
                        )
                        logging.info(f"Updating data for sp_code {item.get('sp_code')}.")
                    else:
                        # Insert new record
                        sql = """
                        INSERT INTO kicc_store_list (
                            hd_code, sp_code, sp_name, biz_no, open_flag, erp_sp_code, master_name, tel_no, sp_type, area_code, sale_class_code, sale_class_name, address1, address2, brand_code, brand_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        data_tuple = (
                            item.get("hd_code"),
                            item.get("sp_code"),
                            item.get("sp_name"),
                            item.get("biz_no"),
                            item.get("open_flag"),
                            item.get("erp_sp_code"),
                            item.get("master_name"),
                            item.get("tel_no"),
                            item.get("sp_type"),
                            item.get("area_code"),
                            item.get("sale_class_code"),
                            item.get("sale_class_name"),
                            item.get("address1"),
                            item.get("address2"),
                            item.get("brand_code"),
                            item.get("brand_name")
                        )
                        logging.info(f"Inserting new data for sp_code {item.get('sp_code')}.")

                    # Execute SQL query with data from API
                    cursor.execute(sql, data_tuple)

            # Commit the transaction
            connection.commit()
            logging.info("Data saved successfully.")
        else:
            logging.error(f"API Error: {ret_code}")
    else:
        logging.error(f"HTTP Request Failed: {response.status_code}")

finally:
    # Close the database connection
    connection.close()
    logging.info("Database connection closed.")
