import os
import sys
import glob
import time
import datetime
import psycopg2
from psycopg2 import sql, OperationalError

def wait_for_postgres(max_retries=60, delay=2):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv("PG_HOST", "localhost"),
                port=os.getenv("PG_PORT", "28546"),
                dbname=os.getenv("PG_DATABASE", "main_db"),
                user=os.getenv("PG_USER", "postgres_user"),
                password=os.getenv("PG_PASSWORD", "postgres_pass")
            )
            print(f"{datetime.datetime.now()} ✅ Connected to PostgreSQL.")
            return conn
        except OperationalError:
            print(f"{datetime.datetime.now()} ❌ PostgreSQL not ready yet ({attempt + 1}/{max_retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise Exception(f"🚨 Could not connect to PostgreSQL after {max_retries * delay} seconds.")

def load_mock_data(conn):
    curr = conn.cursor()
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS mock_data
        (
            id BIGINT NULL,
            customer_first_name VARCHAR(64) NULL,
            customer_last_name VARCHAR(64) NULL,
            customer_age BIGINT NULL,
            customer_email VARCHAR(64) NULL,
            customer_country VARCHAR(64) NULL,
            customer_postal_code VARCHAR(64) NULL,
            customer_pet_type VARCHAR(64) NULL,
            customer_pet_name VARCHAR(64) NULL,
            customer_pet_breed VARCHAR(64) NULL,
            seller_first_name VARCHAR(64) NULL,
            seller_last_name VARCHAR(64) NULL,
            seller_email VARCHAR(64) NULL,
            seller_country VARCHAR(64) NULL,
            seller_postal_code VARCHAR(64) NULL,
            product_name VARCHAR(64) NULL,
            product_category VARCHAR(64) NULL,
            product_price decimal(12, 2) NULL,
            product_quantity BIGINT NULL,
            sale_date VARCHAR(64) NULL,
            sale_customer_id BIGINT NULL,
            sale_seller_id BIGINT NULL,
            sale_product_id BIGINT NULL,
            sale_quantity BIGINT NULL,
            sale_total_price decimal(14, 2) NULL,
            store_name VARCHAR(64) NULL,
            store_location VARCHAR(64) NULL,
            store_city VARCHAR(64) NULL,
            store_state VARCHAR(64) NULL,
            store_country VARCHAR(64) NULL,
            store_phone VARCHAR(64) NULL,
            store_email VARCHAR(64) NULL,
            pet_category VARCHAR(64) NULL,
            product_weight float4 NULL,
            product_color VARCHAR(64) NULL,
            product_size VARCHAR(64) NULL,
            product_brand VARCHAR(64) NULL,
            product_material VARCHAR(64) NULL,
            product_description varchar(1024) NULL,
            product_rating float4 NULL,
            product_reviews BIGINT NULL,
            product_release_date VARCHAR(64) NULL,
            product_expiry_date VARCHAR(64) NULL,
            supplier_name VARCHAR(64) NULL,
            supplier_contact VARCHAR(64) NULL,
            supplier_email VARCHAR(64) NULL,
            supplier_phone VARCHAR(64) NULL,
            supplier_address VARCHAR(64) NULL,
            supplier_city VARCHAR(64) NULL,
            supplier_country varchar(50) NULL
        );
        """
    )
    conn.commit()
    curr.close()

def load_csv_to_postgres(csv_path, conn):
    cur = conn.cursor()
    print(f"→ Loading {csv_path}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        cur.copy_expert(
            sql.SQL("COPY mock_data FROM STDIN WITH CSV HEADER DELIMITER ','"),
            f
        )
    conn.commit()
    cur.close()

def run_sql_file(conn, filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    cur = conn.cursor()
    cur.execute(sql_content)
    conn.commit()
    cur.close()

if __name__ == "__main__":
    try:
        path = "/source_data"

        if os.path.isdir(path):
            csv_files = glob.glob(os.path.join(path, '*.csv'))
        elif os.path.isfile(path) and path.lower().endswith('.csv'):
            csv_files = [path]
        else:
            print(f"Error: {path} is not a CSV file or directory")
            sys.exit(1)

        if not csv_files:
            print(f"No CSV files found in {path}")
            sys.exit(0)

        conn = wait_for_postgres()
        load_mock_data(conn)

        for csv_file in csv_files:
            load_csv_to_postgres(csv_file, conn)
        print(f"[✅] Loaded {len(csv_files)} file(s) into mock_data")


        # Здесь запускаем миграции
        print("Запускаем DDL миграции...")
        run_sql_file(conn, '/migrations/postgres_ddl.sql')
        print("DDL миграции выполнены.")

        # print("Запускаем DML миграции...")
        # run_sql_file(conn, '/migrations/postgres_dml.sql')
        # print("DML миграции выполнены.")

        conn.close()
    except Exception as e:
        print(f"🚨 Error: {e}")
        sys.exit(1)
