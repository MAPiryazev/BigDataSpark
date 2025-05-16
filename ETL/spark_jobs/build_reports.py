import os
from pyspark.sql import SparkSession

# Конфигурация PostgreSQL
PG_DATABASE      = os.getenv("PG_DATABASE", "main_db")
POSTGRE_HOST     = os.getenv("PG_HOST",     "bigdata-db")
POSTGRE_PORT     = int(os.getenv("PG_PORT",   "5432"))
POSTGRE_USER     = os.getenv("PG_USER",     "bigdata")
POSTGRE_PASSWORD = os.getenv("PG_PASSWORD", "bigdata")

# Конфигурация ClickHouse
CH_HOST          = os.getenv("CH_HOST",     "clickhouse-server")
CH_PORT          = os.getenv("CH_PORT",     "8123")
CH_DATABASE      = os.getenv("CH_DATABASE", "analytics")
CH_DRIVER        = os.getenv("CH_DRIVER",   "com.clickhouse.jdbc.ClickHouseDriver")

# JDBC URLs
pg_url   = f"jdbc:postgresql://{POSTGRE_HOST}:{POSTGRE_PORT}/{PG_DATABASE}"
ch_url   = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DATABASE}?user=default&password="

# Создаем Spark сессию
spark = (
    SparkSession.builder
        .appName('TestConnections')
        .config('spark.jars', '/opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar,/opt/bitnami/spark/jars/postgresql-42.7.3.jar')
        .getOrCreate()
)

print("=== Testing PostgreSQL Connection ===")
try:
    # Пробуем прочитать простую таблицу из PostgreSQL
    pg_df = spark.read.jdbc(
        url=pg_url,
        table='dim_dates',  # используем простую таблицу
        properties={
            'user': POSTGRE_USER,
            'password': POSTGRE_PASSWORD,
            'driver': 'org.postgresql.Driver'
        }
    )
    print("PostgreSQL connection successful!")
    print("Sample data from PostgreSQL:")
    pg_df.show(5)
except Exception as e:
    print("PostgreSQL connection failed!")
    print(f"Error: {str(e)}")

print("\n=== Testing ClickHouse Connection ===")
try:
    # Пробуем прочитать простую таблицу из ClickHouse
    ch_df = spark.read.jdbc(
        url=ch_url,
        table='analytics.sales_by_customer',  # используем существующую таблицу
        properties={
            'driver': CH_DRIVER
        }
    )
    print("ClickHouse connection successful!")
    print("Sample data from ClickHouse:")
    ch_df.show(5)
except Exception as e:
    print("ClickHouse connection failed!")
    print(f"Error: {str(e)}")

spark.stop()
