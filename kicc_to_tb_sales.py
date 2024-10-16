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
log_filename = now.strftime(f"/home/nolboo/etl-job/log/kicc_to_tb_sales_log_%Y%m%d_%H%M%S.txt")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

def data_exists(cursor, chain_no, sale_dy):
    """Check if data exists in tb_sales_by_store."""
    query = "SELECT COUNT(*) FROM tb_sales_by_store WHERE chain_no = %s AND sale_dy = %s"
    cursor.execute(query, (chain_no, sale_dy))
    result = cursor.fetchone()
    return result[0] > 0

def update_data(cursor, data_tuple):
    """Update existing data in tb_sales_by_store."""
    sql = """
    UPDATE tb_sales_by_store SET
        chain_name = %s,
        chong_maechool = %s,
        soon_maechool = %s,
        NET_maechool = %s,
        discount_amount = %s,
        vat = %s,
        cash_maechool = %s,
        card_maechool = %s,
        samsung_pay_maechool = %s,
        pay_count = %s
    WHERE chain_no = %s AND sale_dy = %s
    """
    cursor.execute(sql, data_tuple)

def insert_data(cursor, data_tuple):
    """Insert new data into tb_sales_by_store."""
    sql = """
    INSERT INTO tb_sales_by_store (
        chain_no, sale_dy, chain_name, chong_maechool, soon_maechool, NET_maechool,
        discount_amount, vat, cash_maechool, card_maechool, samsung_pay_maechool, pay_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, data_tuple)

def transfer_data():
    """Transfer data from kicc_sales_data to tb_sales_by_store."""
    connection = pymysql.connect(**db_config)
    
    try:
        with connection.cursor() as cursor:
            # Fetch data from kicc_sales_data
            cursor.execute("SELECT sp_code, sp_name, sale_date, total_amt, sale_amt, net_amt, total_dc_amt, vat_amt, cash_amt, card_amt, emoney_amt, bill_qty FROM kicc_sales_data")
            records = cursor.fetchall()

            for record in records:
                chain_no = record[0]
                sale_dy = datetime.strptime(record[2], "%Y%m%d").date()  # Convert sale_date from varchar to date
                data_tuple = (
                    record[1],  # chain_name
                    record[3],  # chong_maechool (total_amt)
                    record[4],  # soon_maechool (sale_amt)
                    record[5],  # NET_maechool (net_amt)
                    record[6],  # discount_amount (total_dc_amt)
                    record[7],  # vat
                    record[8],  # cash_maechool (cash_amt)
                    record[9],  # card_maechool (card_amt)
                    record[10], # samsung_pay_maechool (emoney_amt)
                    record[11], # pay_count (bill_qty)
                    chain_no,
                    sale_dy
                )

                if data_exists(cursor, chain_no, sale_dy):
                    update_data(cursor, data_tuple)
                    logging.info(f"Updated data for chain_no {chain_no} on {sale_dy}.")
                else:
                    insert_data(cursor, (chain_no, sale_dy) + data_tuple[:-2])
                    logging.info(f"Inserted new data for chain_no {chain_no} on {sale_dy}.")
                    
            connection.commit()
    
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        connection.close()

def execute_additional_queries():
    """Execute additional update queries after the transfer_data() process is complete."""
    connection = pymysql.connect(**db_config)
    
    try:
        with connection.cursor() as cursor:
            # Query 1: Update 'responsible' field from tb_store_temp
            query1 = """
            UPDATE tb_sales_by_store s
            JOIN tb_store_temp t ON s.chain_no = t.chain_no
            SET s.responsible = t.resp
            """
            cursor.execute(query1)
            logging.info("Query 1 executed: Updated 'responsible' field from tb_store_temp.")

            # Query 2: Update 'xy' field from tb_store_easypos
            query2 = """
            UPDATE tb_sales_by_store s
            JOIN tb_store_easypos e ON s.chain_no = e.chain_no
            SET s.xy = e.xy_degree
            """
            cursor.execute(query2)
            logging.info("Query 2 executed: Updated 'xy' field from tb_store_easypos.")

            # Query 3: Set 'responsible' to '직영' for specific chain_no values if responsible is NULL
            query3 = """
            UPDATE tb_sales_by_store
            SET responsible = '직영'
            WHERE responsible IS NULL
              AND chain_no IN ('000003', '000004', '000005', '000006', '000007', '000158')
            """
            cursor.execute(query3)
            logging.info("Query 3 executed: Set 'responsible' to '직영' for specific chain_no values.")
            
            connection.commit()
    
    except Exception as e:
        logging.error(f"Error executing additional queries: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    # Transfer data from kicc_sales_data to tb_sales_by_store
    transfer_data()

    # Execute the additional queries after the data transfer
    execute_additional_queries()

    logging.info("Data transfer and additional queries execution completed.")
