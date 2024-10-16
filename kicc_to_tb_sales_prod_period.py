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

# Configure logging
now = datetime.now()
log_filename = now.strftime(f"/home/nolboo/etl-job/log/kicc_to_tb_sales_by_prod_log_%Y%m%d_%H%M%S.txt")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()]
)

def validate_date(input_date):
    """Validate input date format and convert to datetime object."""
    try:
        return datetime.strptime(input_date, '%Y%m%d')
    except ValueError:
        print(f"Invalid date format: {input_date}. Please enter the date in YYYYMMDD format.")
        return None

def data_exists(cursor, chain_no, sale_dy, prod_code):
    """Check if data exists in tb_sales_by_prod."""
    query = "SELECT COUNT(*) FROM tb_sales_by_prod WHERE chain_no = %s AND sale_dy = %s AND prod_code = %s"
    cursor.execute(query, (chain_no, sale_dy, prod_code))
    result = cursor.fetchone()
    return result[0] > 0

def update_data(cursor, data_tuple):
    """Update existing data in tb_sales_by_prod."""
    sql = """
    UPDATE tb_sales_by_prod SET
        chain_name = %s,
        prod_name = %s,
        maechool_count = %s,
        chong_maechool = %s,
        soon_maechool = %s,
        NET_maechool = %s,
        vat = %s,
        discount = %s
    WHERE chain_no = %s AND sale_dy = %s AND prod_code = %s
    """
    cursor.execute(sql, data_tuple)

def insert_data(cursor, data_tuple):
    """Insert new data into tb_sales_by_prod."""
    sql = """
    INSERT INTO tb_sales_by_prod (
        sale_dy, chain_no, chain_name, prod_code, prod_name,
        maechool_count, chong_maechool, soon_maechool, NET_maechool,
        vat, discount
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, data_tuple)

def transfer_data(start_date, end_date):
    """Transfer data from kicc_store_product_sales to tb_sales_by_prod for a date range."""
    connection = pymysql.connect(**db_config)
    
    try:
        with connection.cursor() as cursor:
            # Fetch data within the date range from kicc_store_product_sales
            cursor.execute("""
                SELECT 
                    sale_date, sp_code, sp_name, 
                    item_code, item_name, sale_qty, 
                    total_amt, sale_amt, net_amt, vat_amt, total_dc_amt
                FROM kicc_store_product_sales
                WHERE sale_date BETWEEN %s AND %s
            """, (start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')))
            records = cursor.fetchall()

            for record in records:
                sale_dy = datetime.strptime(record[0], "%Y%m%d").date()  # Convert sale_date from varchar to date
                data_tuple = (
                    sale_dy,
                    record[1],  # chain_no (sp_code)
                    record[2],  # chain_name (sp_name)
                    record[3],  # prod_code (item_code)
                    record[4],  # prod_name (item_name)
                    record[5],  # maechool_count (sale_qty)
                    record[6],  # chong_maechool (total_amt)
                    record[7],  # soon_maechool (sale_amt)
                    record[8],  # NET_maechool (net_amt)
                    record[9],  # vat (vat_amt)
                    record[10]  # discount (total_dc_amt)
                )

                if data_exists(cursor, record[1], sale_dy, record[3]):
                    update_data(cursor, data_tuple[2:] + (record[1], sale_dy, record[3]))
                    logging.info(f"Updated data for chain_no {record[1]} and prod_code {record[3]} on {sale_dy}.")
                else:
                    insert_data(cursor, data_tuple)
                    logging.info(f"Inserted new data for chain_no {record[1]} and prod_code {record[3]} on {sale_dy}.")
                    
            connection.commit()
    
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        connection.close()

def update_medium_scale_nm():
    """Update medium_scale_nm in tb_sales_by_prod based on kicc_product_list."""
    connection = pymysql.connect(**db_config)
    
    try:
        with connection.cursor() as cursor:
            # Update medium_scale_nm in tb_sales_by_prod using kicc_product_list
            update_query = """
            UPDATE tb_sales_by_prod prod
            JOIN kicc_product_list kpl ON prod.prod_code = kpl.item_code
            SET prod.medium_scale_nm = kpl.medium_scale_nm
            WHERE prod.medium_scale_nm IS NULL OR prod.medium_scale_nm = ''
            """
            cursor.execute(update_query)
            connection.commit()
            logging.info("Updated medium_scale_nm in tb_sales_by_prod based on kicc_product_list.")
    
    except Exception as e:
        logging.error(f"Error while updating medium_scale_nm: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    # 사용자로부터 시작일자와 종료일자 입력 받기
    start_date_input = input("Enter the start date (YYYYMMDD): ")
    end_date_input = input("Enter the end date (YYYYMMDD): ")

    # 입력한 날짜 검증
    start_date = validate_date(start_date_input)
    end_date = validate_date(end_date_input)

    if start_date and end_date:
        if start_date > end_date:
            print("Start date cannot be after end date.")
        else:
            # Transfer data from kicc_store_product_sales to tb_sales_by_prod for the specified date range
            transfer_data(start_date, end_date)

            # Update medium_scale_nm in tb_sales_by_prod based on kicc_product_list
            update_medium_scale_nm()

            logging.info(f"Data transfer and medium_scale_nm update completed from {start_date_input} to {end_date_input}")
    else:
        print("Invalid date format. Please enter the dates in YYYYMMDD format.")
