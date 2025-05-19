import os
from pyspark.sql import SparkSession

# 🔧 Load env variables
CH_HOST = os.getenv("CH_HOST", "clickhouse_database")   # 🆗 имя сервиса
CH_PORT = os.getenv("CH_PORT", "8123")                  # ✅ HTTP порт
CH_DATABASE = os.getenv("CH_DATABASE", "analytics")
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DRIVER = os.getenv("CH_DRIVER", "com.clickhouse.jdbc.ClickHouseDriver")

JARS = os.getenv(
    "SPARK_JARS",
    "/opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar," +
    "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
)

# 📌 JDBC config (используем HTTP-протокол, как требует clickhouse-jdbc)
jdbc_url = (
    f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DATABASE}"
    f"?user={CH_USER}&password={CH_PASSWORD}&protocol=http"
    f"&compress=false&socket_timeout=300000&connect_timeout=300000"
)

# 🛠 Инициализация SparkSession
spark = (
    SparkSession.builder
    .appName("TestClickHouseConnection")
    .config("spark.jars", JARS)
    .getOrCreate()
)

print("\n✅ JDBC URL:")
print(jdbc_url)

try:
    print("🚀 Connecting to ClickHouse via JDBC...\n")

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", CH_DRIVER)
        .option("dbtable", "(SELECT now() AS current_time) AS t")
        .option("fetchsize", "1")
        .load()
    )

    df.show()
    print("✅ ClickHouse connection successful!")

except Exception as e:
    print("\n❌ Error connecting to ClickHouse:")
    print(str(e))
    print("\n🔍 Diagnose checklist:")
    print(f"1. Try: curl http://{CH_HOST}:{CH_PORT}")
    print("2. Check logs: docker logs clickhouse-server")
    print("3. Check DB: docker exec -it clickhouse-server clickhouse-client --query 'SHOW DATABASES'")
    print("4. Check port: docker exec -it clickhouse-server netstat -tulpn | grep 8123")

finally:
    spark.stop()
