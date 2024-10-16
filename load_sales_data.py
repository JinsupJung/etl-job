import requests
import json
import pymysql
import logging
from datetime import datetime, timedelta

# MySQL database connection details
db_config = {
    "host": "175.196.7.45",
    "user": "nolboo",
    "password": "2024!puser",
    "database": "nolboo"
}

# API URL
api_url = "https://poson.easypos.net/servlet/EasyPosJsonChannelSVL?cmd=TlxSyncEasyposSaleCMD"

# API Request Headers
headers = {
    "Content-Type": "application/json"
}

# Configure logging
today = datetime.today().strftime('%Y%m%d')
log_filename = f"/home/nolboo/etl-job/log/sales_data_log_{today}.txt"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

def data_exists_for_date_and_store(cursor, sp_code, sale_date):
    """Check if data already exists for the given sp_code and sale_date."""
    query = "SELECT COUNT(*) FROM kicc_sales_data WHERE sp_code = %s AND sale_date = %s"
    cursor.execute(query, (sp_code, sale_date))
    result = cursor.fetchone()
    return result[0] > 0

def update_sales_data(cursor, sp_code, sale_date, data_tuple):
    """Update existing sales data in the database."""
    sql = """
    UPDATE kicc_sales_data SET
        hd_code = %s, sp_name = %s, biz_no = %s,
        total_amt = %s, sale_amt = %s, net_amt = %s, total_dc_amt = %s,
        vat_amt = %s, bill_qty = %s, normal_qty = %s, normal_amt = %s,
        return_qty = %s, return_amt = %s, service_amt = %s, cash_qty = %s,
        cash_amt = %s, card_qty = %s, card_amt = %s, emoney_qty = %s, emoney_amt = %s
    WHERE sp_code = %s AND sale_date = %s
    """
    cursor.execute(sql, data_tuple + (sp_code, sale_date))

def insert_sales_data(cursor, data_tuple):
    """Insert new sales data into the database."""
    sql = """
    INSERT INTO kicc_sales_data (
        sp_code, sale_date, hd_code, sp_name, biz_no, 
        total_amt, sale_amt, net_amt, total_dc_amt, vat_amt, 
        bill_qty, normal_qty, normal_amt, return_qty,
        return_amt, service_amt, cash_qty, cash_amt, card_qty,
        card_amt, emoney_qty, emoney_amt
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        # Convert all elements in data_tuple to strings to avoid type issues
        data_tuple = tuple(str(element) if element is not None else None for element in data_tuple)
        cursor.execute(sql, data_tuple)
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        logging.error(f"SQL Query: {cursor.mogrify(sql, data_tuple)}")
        logging.error(f"Data Tuple: {repr(data_tuple)}")
        raise  # Re-raise the exception to stop execution if needed

def fetch_and_process_sales_data(sp_code, sale_date):
    """Fetch sales data from the API and update/insert it into the database as needed."""
    sale_date_str = str(sale_date)  # Ensure sale_date_str is defined early
    
    logging.info(f"Fetching sales data for store {sp_code} on date {sale_date_str}")
    request_data = {
        "s_code": "3",  # Sales data request code
        "hd_code": "I9X",  # Head code
        "sp_code": sp_code,  # Store code
        "sale_date": sale_date_str  # Date for which to fetch the data
    }

    connection = pymysql.connect(**db_config)

    try:
        with connection.cursor() as cursor:
            response = requests.post(api_url, headers=headers, data=json.dumps(request_data))

            if response.status_code == 200:
                response_data = response.json()
                ret_code = response_data.get("ret_code")

                if ret_code == "0000":
                    data = response_data.get("data", [])
                    logging.info(f"Received {len(data)} records from the API")

                    for item in data:
                        data_tuple = (
                            item.get("sp_code"),
                            sale_date_str,
                            item.get("hd_code"),
                            item.get("sp_name"),
                            item.get("biz_no"),
                            item.get("total_amt"),
                            item.get("sale_amt"),
                            item.get("net_amt"),
                            item.get("total_dc_amt"),
                            item.get("vat_amt"),
                            item.get("bill_qty"),
                            item.get("normal_qty"),
                            item.get("normal_amt"),
                            item.get("return_qty"),
                            item.get("return_amt"),
                            item.get("service_amt"),
                            item.get("cash_qty"),
                            item.get("cash_amt"),
                            item.get("card_qty"),
                            item.get("card_amt"),
                            item.get("emoney_qty"),
                            item.get("emoney_amt")
                        )

                        if data_exists_for_date_and_store(cursor, sp_code, sale_date_str):
                            logging.info(f"Updating existing data for store {sp_code} and date {sale_date_str}")
                            update_sales_data(cursor, sp_code, sale_date_str, data_tuple[2:])
                        else:
                            logging.info(f"Inserting new data: {repr(data_tuple)}")
                            insert_sales_data(cursor, data_tuple)

                    connection.commit()
                else:
                    logging.error(f"API Error: {ret_code} for date {sale_date_str} and store {sp_code}")
            else:
                logging.error(f"HTTP Request Failed: {response.status_code} for date {sale_date_str} and store {sp_code}")

    finally:
        connection.close()

if __name__ == "__main__":
    end_date = datetime.today()
    start_date = end_date - timedelta(days=1)

    logging.info(f"Starting data fetching and processing from {start_date.strftime('%Y%m%d')} to {end_date.strftime('%Y%m%d')}")

    connection = pymysql.connect(**db_config)

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT sp_code FROM kicc_store_list")
            sp_codes = cursor.fetchall()

            logging.info(f"Found {len(sp_codes)} stores in the database")

            for sp_code in sp_codes:
                current_date = start_date
                while current_date <= end_date:
                    fetch_and_process_sales_data(sp_code[0], current_date.strftime('%Y%m%d'))
                    current_date += timedelta(days=1)
    finally:
        connection.close()
        logging.info("Data fetching and processing completed")
