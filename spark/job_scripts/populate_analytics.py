import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col, concat, year, month, lit, to_date, expr
from datetime import datetime, timedelta
import random

# Environment variables
CH_HOST = os.getenv("CH_HOST", "clickhouse-server")
CH_PORT = os.getenv("CH_PORT", "8123")
CH_DATABASE = os.getenv("CH_DATABASE", "analytics")
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DRIVER = os.getenv("CH_DRIVER", "com.clickhouse.jdbc.ClickHouseDriver")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# JDBC Configuration
JARS = os.getenv(
    "SPARK_JARS",
    "/opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar,"
    "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
)

# PostgreSQL connection
pg_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
pg_props = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ClickHouse connection
ch_url = f"jdbc:clickhouse:http://{CH_HOST}:{CH_PORT}/{CH_DATABASE}?user={CH_USER}&password={CH_PASSWORD}&compress=false"
ch_props = {
    "driver": CH_DRIVER
}

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName('AnalyticsETL')
    .config('spark.jars', JARS)
    .getOrCreate()
)

# Загружаем данные из PostgreSQL
fact_sales = spark.read.jdbc(pg_url, 'fact_sales', properties=pg_props)
dim_products = spark.read.jdbc(pg_url, 'dim_products', properties=pg_props)
dim_product_categories = spark.read.jdbc(pg_url, 'product_categories', properties=pg_props)
dim_customers = spark.read.jdbc(pg_url, 'dim_customers', properties=pg_props)
dim_stores = spark.read.jdbc(pg_url, 'dim_stores', properties=pg_props)
dim_suppliers = spark.read.jdbc(pg_url, 'dim_suppliers', properties=pg_props)
dim_countries = spark.read.jdbc(pg_url, 'countries', properties=pg_props)

# Проверяем все данные из fact_sales
print("Debug: Checking all fact_sales data...")
fact_sales.show(5)

# Проверяем схему fact_sales
print("Debug: Checking fact_sales schema...")
fact_sales.printSchema()

# 1. Витрина продаж по продуктам
sales_by_product = (
    fact_sales
    .join(dim_products, fact_sales.product_id == dim_products.product_id)
    .join(dim_product_categories, dim_products.category_id == dim_product_categories.category_id)
    .groupBy(
        dim_products.product_id,
        dim_products.name.alias('product_name'),
        dim_product_categories.category
    )
    .agg(
        sum('sale_total_price').alias('total_revenue'),
        sum('sale_quantity').alias('total_quantity'),
        avg('rating').alias('avg_rating'),
        sum('reviews').alias('review_count')
    )
)

# 2. Витрина продаж по клиентам
sales_by_customer = (
    fact_sales
    .join(dim_customers, fact_sales.customer_id == dim_customers.customer_id)
    .join(dim_countries, dim_customers.country_id == dim_countries.country_id)
    .groupBy(
        dim_customers.customer_id,
        concat(dim_customers.first_name, lit(' '), dim_customers.last_name).alias('customer_name'),
        dim_countries.country_name.alias('country')
    )
    .agg(
        sum('sale_total_price').alias('total_spent'),
        avg('sale_total_price').alias('avg_order_value')
    )
)

# 3. Витрина продаж по времени
print("Debug: Starting sales_by_time processing...")

# Генерируем случайные даты только для NULL значений
sales_with_dates = fact_sales.withColumn(
    'sell_date',
    expr("""
        CASE 
            WHEN sell_date IS NULL THEN date_add('2023-01-01', cast(rand() * 365 as int))
            ELSE sell_date
        END
    """)
)

sales_by_time = (
    sales_with_dates
    .withColumn('year', year('sell_date').cast('int'))
    .withColumn('month', month('sell_date').cast('int'))
    .groupBy('year', 'month')
    .agg(
        sum('sale_total_price').alias('total_revenue'),
        count('*').alias('total_orders'),
        avg('sale_total_price').alias('avg_order_size')
    )
)

# Проверяем финальные данные перед записью
print("Debug: Checking final sales_by_time data...")
sales_by_time.show(5)

# Записываем результаты в ClickHouse
print("Debug: Writing sales_by_time to ClickHouse...")
sales_by_time.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_time')\
    .options(**ch_props)\
    .mode("append")\
    .save()

print("Debug: sales_by_time processing completed.")

# 4. Витрина продаж по магазинам
sales_by_store = (
    fact_sales
    .join(dim_stores, fact_sales.store_id == dim_stores.store_id)
    .join(dim_countries, dim_stores.country_id == dim_countries.country_id)
    .groupBy(
        dim_stores.store_id,
        dim_stores.name.alias('store_name'),
        dim_stores.location.alias('city'),
        dim_countries.country_name.alias('country')
    )
    .agg(
        sum('sale_total_price').alias('total_revenue'),
        avg('sale_total_price').alias('avg_order_value')
    )
)

# 5. Витрина продаж по поставщикам
sales_by_supplier = (
    fact_sales
    .join(dim_suppliers, fact_sales.supplier_id == dim_suppliers.supplier_id)
    .join(dim_countries, dim_suppliers.country_id == dim_countries.country_id)
    .groupBy(
        dim_suppliers.supplier_id,
        dim_suppliers.name.alias('supplier_name'),
        dim_countries.country_name.alias('country')
    )
    .agg(
        sum('sale_total_price').alias('total_revenue'),
        avg('sale_total_price').alias('avg_price')
    )
)

# 6. Витрина качества продукции
product_quality = (
    fact_sales
    .join(dim_products, fact_sales.product_id == dim_products.product_id)
    .groupBy(
        dim_products.product_id,
        dim_products.name.alias('product_name'),
        dim_products.rating,
        dim_products.reviews.alias('review_count')
    )
    .agg(
        sum('sale_quantity').alias('total_quantity')
    )
)

# Записываем результаты в ClickHouse
sales_by_product.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_product')\
    .options(**ch_props)\
    .mode("append")\
    .save()

sales_by_customer.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_customer')\
    .options(**ch_props)\
    .mode("append")\
    .save()

sales_by_store.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_store')\
    .options(**ch_props)\
    .mode("append")\
    .save()

sales_by_supplier.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_supplier')\
    .options(**ch_props)\
    .mode("append")\
    .save()

product_quality.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.product_quality')\
    .options(**ch_props)\
    .mode("append")\
    .save()

spark.stop()
