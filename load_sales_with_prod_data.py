import requests
import json
import pymysql
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

# Custom logging function using basic file I/O
def log_message(message):
    log_filename = f"/home/nolboo/etl-job/log/load_sales_with_prod_data_log_{datetime.now().strftime('%Y%m%d')}.txt"
    with open(log_filename, "a", encoding="utf-8") as log_file:
        log_file.write(f"{datetime.now()} - {message}\n")

def data_exists_for_product(cursor, sp_code, sale_date, item_code, item_name):
    query = "SELECT COUNT(*) FROM kicc_store_product_sales WHERE sp_code = %s AND sale_date = %s AND item_code = %s AND item_name = %s"
    cursor.execute(query, (sp_code, sale_date, item_code, item_name))
    result = cursor.fetchone()
    return result[0] > 0

def fetch_and_store_product_sales_data(sp_code, sale_date):
    request_data = {
        "s_code": "12",
        "hd_code": "I9X",
        "sp_code": sp_code,
        "sale_date": sale_date
    }

    connection = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )

    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(request_data))

        if response.status_code == 200:
            response_data = response.json()
            ret_code = response_data.get("ret_code")

            if ret_code == "0000":
                data = response_data.get("data", [])
                with connection.cursor() as cursor:
                    for item in data:
                        if data_exists_for_product(cursor, sp_code, sale_date, item.get("item_code"), item.get("item_name")):
                            # Update existing record
                            sql = """
                            UPDATE kicc_store_product_sales SET
                                sale_qty = %s,
                                total_amt = %s,
                                sale_amt = %s,
                                net_amt = %s,
                                total_dc_amt = %s,
                                vat_amt = %s
                            WHERE sp_code = %s AND sale_date = %s AND item_code = %s AND item_name = %s
                            """
                            data_tuple = (
                                item.get("sale_qty"),
                                item.get("total_amt"),
                                item.get("sale_amt"),
                                item.get("net_amt"),
                                item.get("total_dc_amt"),
                                item.get("vat_amt"),
                                sp_code,
                                sale_date,
                                item.get("item_code"),
                                item.get("item_name")
                            )
                            log_message(f"Updating data for store {sp_code}, date {sale_date}, item {item.get('item_code')}")
                        else:
                            # Insert new record
                            sql = """
                            INSERT INTO kicc_store_product_sales (
                                hd_code, sp_code, sp_name, biz_no, sale_date,
                                item_code, item_name, sale_qty, total_amt, sale_amt, net_amt,
                                total_dc_amt, vat_amt
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            data_tuple = (
                                item.get("hd_code"),
                                item.get("sp_code"),
                                item.get("sp_name"),
                                item.get("biz_no"),
                                item.get("sale_date"),
                                item.get("item_code"),
                                item.get("item_name"),
                                item.get("sale_qty"),
                                item.get("total_amt"),
                                item.get("sale_amt"),
                                item.get("net_amt"),
                                item.get("total_dc_amt"),
                                item.get("vat_amt")
                            )
                            log_message(f"Inserting new data for store {sp_code}, date {sale_date}, item {item.get('item_code')}")

                        cursor.execute(sql, data_tuple)

                connection.commit()
                log_message(f"Product sales data for {sale_date} for store {sp_code} saved successfully.")
            else:
                log_message(f"API Error: {ret_code} for date {sale_date} and store {sp_code}")
        else:
            log_message(f"HTTP Request Failed: {response.status_code} for date {sale_date} and store {sp_code}")

    finally:
        connection.close()

if __name__ == "__main__":
    # Define the date range
    yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    today_date = datetime.today().strftime('%Y%m%d')

    connection = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT sp_code FROM kicc_store_list")
            sp_codes = cursor.fetchall()

            for sp_code in sp_codes:
                # Fetch and store data for yesterday
                fetch_and_store_product_sales_data(sp_code[0], yesterday_date)
                # Fetch and store data for today
                fetch_and_store_product_sales_data(sp_code[0], today_date)
    finally:
        connection.close()
