from pyspark.sql import SparkSession

# Создаём Spark-сессию
spark = SparkSession.builder \
    .appName("TestJDBCConnections") \
    .getOrCreate()

print("✅ SparkSession started")

# Чтение данных из PostgreSQL
try:
    pg_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "information_schema.tables") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    print(f"✅ PostgreSQL: {pg_df.count()} строк прочитано")
except Exception as e:
    print(f"❌ Ошибка при подключении к PostgreSQL: {e}")

# Чтение данных из ClickHouse
try:
    ch_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse_database:8123/analytics") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "system.tables") \
        .load()
    print(f"✅ ClickHouse: {ch_df.count()} строк прочитано")
except Exception as e:
    print(f"❌ Ошибка при подключении к ClickHouse: {e}")

spark.stop()
