import os
from pyspark.sql import SparkSession

# Environment variables
CH_HOST = os.getenv("CH_HOST", "clickhouse-server")
CH_PORT = os.getenv("CH_PORT", "8123")
CH_DATABASE = os.getenv("CH_DATABASE", "analytics")
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DRIVER = os.getenv("CH_DRIVER", "com.clickhouse.jdbc.ClickHouseDriver")

# JDBC Configuration
JARS = os.getenv(
    "SPARK_JARS",
    "/opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar,"
    "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
)

jdbc_conf = {
    'ch.url': f"jdbc:clickhouse:http://{CH_HOST}:{CH_PORT}/{CH_DATABASE}?user={CH_USER}&password={CH_PASSWORD}&compress=false",
    'ch.driver': CH_DRIVER
}

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName('ClickHouseTest')
    .config('spark.jars', JARS)
    .config("spark.sql.catalog.clickhouse.host", CH_HOST)
    .config("spark.sql.catalog.clickhouse.protocol", "native")
    .config("spark.sql.catalog.clickhouse.port", CH_PORT)
    .config("spark.sql.catalog.clickhouse.database", CH_DATABASE)
    .getOrCreate()
)

print("Testing ClickHouse connection...")

try:
    # Простой тестовый запрос
    test_df = spark.read.format('jdbc')\
        .option('url', jdbc_conf['ch.url'])\
        .option('driver', jdbc_conf['ch.driver'])\
        .option('dbtable', '(SELECT 1 as test)')\
        .load()
    
    # Выводим результат
    test_df.show()
    print("Successfully connected to ClickHouse!")
    
except Exception as e:
    print(f"Error connecting to ClickHouse: {str(e)}")
finally:
    spark.stop()
