import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, avg, count, col, concat, year, month, lit, to_date, expr, 
    rank, max, min, when, desc, asc, stddev, lag, coalesce, isnull,
    countDistinct, date_format, current_date, rand
)
from datetime import datetime, timedelta
import random
from pyspark.sql.window import Window

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
print("Loading data from PostgreSQL...")
fact_sales = spark.read.jdbc(pg_url, 'fact_sales', properties=pg_props)
dim_products = spark.read.jdbc(pg_url, 'dim_products', properties=pg_props)
dim_product_categories = spark.read.jdbc(pg_url, 'product_categories', properties=pg_props)
dim_customers = spark.read.jdbc(pg_url, 'dim_customers', properties=pg_props)
dim_stores = spark.read.jdbc(pg_url, 'dim_stores', properties=pg_props)
dim_suppliers = spark.read.jdbc(pg_url, 'dim_suppliers', properties=pg_props)
dim_countries = spark.read.jdbc(pg_url, 'countries', properties=pg_props)

print("\n=== Initial record counts ===")
print(f"fact_sales: {fact_sales.count()}")
print(f"dim_products: {dim_products.count()}")
print(f"dim_product_categories: {dim_product_categories.count()}")
print(f"dim_customers: {dim_customers.count()}")
print(f"dim_stores: {dim_stores.count()}")
print(f"dim_suppliers: {dim_suppliers.count()}")
print(f"dim_countries: {dim_countries.count()}")

# Заполняем NULL даты случайными датами из 2021 года
start_date = datetime(2021, 1, 1)
end_date = datetime(2021, 12, 31)
days_between = (end_date - start_date).days

fact_sales = fact_sales.withColumn(
    'sell_date',
    coalesce(
        col('sell_date'),
        to_date(
            expr(f"date_add('2021-01-01', cast(rand() * {days_between} as int))")
        )
    )
)

print("\n=== After date generation ===")
print(f"fact_sales count: {fact_sales.count()}")

# Проверяем данные
print("Debug: Checking fact_sales data...")
fact_sales.show(5)
print("Debug: Checking fact_sales schema...")
fact_sales.printSchema()

# Проверяем количество записей
print("Debug: Checking record counts...")
print(f"fact_sales count: {fact_sales.count()}")
print(f"dim_products count: {dim_products.count()}")
print(f"dim_customers count: {dim_customers.count()}")
print(f"dim_stores count: {dim_stores.count()}")
print(f"dim_suppliers count: {dim_suppliers.count()}")

# 1. Витрина продаж по продуктам
print("\nProcessing sales_by_product...")
sales_by_product = (
    fact_sales
    .join(dim_products, fact_sales.product_id == dim_products.product_id, 'left')
    .join(dim_product_categories, dim_products.category_id == dim_product_categories.category_id, 'left')
    .groupBy(
        dim_products.product_id,
        coalesce(dim_products.name, lit('Unknown')).alias('product_name'),
        coalesce(dim_product_categories.category, lit('Unknown')).alias('category')
    )
    .agg(
        coalesce(sum('sale_total_price'), lit(0)).alias('total_revenue'),
        coalesce(sum('sale_quantity'), lit(0)).alias('total_quantity'),
        coalesce(avg('rating'), lit(0)).alias('avg_rating'),
        coalesce(sum('reviews'), lit(0)).alias('review_count'),
        count('*').alias('total_orders'),
        coalesce(avg('sale_total_price'), lit(0)).alias('avg_order_value')
    )
    .withColumn('popularity_rank', rank().over(Window.partitionBy('category').orderBy(desc('total_quantity'))))
)

print(f"sales_by_product count: {sales_by_product.count()}")
print("Debug: sales_by_product sample...")
sales_by_product.show(5)

# 2. Витрина продаж по клиентам
print("\nProcessing sales_by_customer...")
sales_by_customer = (
    fact_sales
    .join(dim_customers, fact_sales.customer_id == dim_customers.customer_id, 'left')
    .join(dim_countries, dim_customers.country_id == dim_countries.country_id, 'left')
    .join(dim_products, fact_sales.product_id == dim_products.product_id, 'left')
    .join(dim_product_categories, dim_products.category_id == dim_product_categories.category_id, 'left')
    .groupBy(
        dim_customers.customer_id,
        concat(
            coalesce(dim_customers.first_name, lit('')),
            lit(' '),
            coalesce(dim_customers.last_name, lit(''))
        ).alias('customer_name'),
        coalesce(dim_countries.country_name, lit('Unknown')).alias('country')
    )
    .agg(
        coalesce(sum('sale_total_price'), lit(0)).alias('total_spent'),
        coalesce(avg('sale_total_price'), lit(0)).alias('avg_order_value'),
        count('*').alias('total_orders'),
        max('sell_date').alias('last_order_date'),
        coalesce(expr("max_by(category, sale_total_price)"), lit('Unknown')).alias('top_product_category')
    )
    .withColumn('customer_segment', 
        when(col('total_spent') > 10000, 'VIP')
        .when(col('total_spent') > 5000, 'Regular')
        .otherwise('Standard')
    )
)

print(f"sales_by_customer count: {sales_by_customer.count()}")
print("Debug: sales_by_customer sample...")
sales_by_customer.show(5)

# 3. Витрина продаж по времени
print("\nProcessing sales_by_time...")
sales_by_time = (
    fact_sales
    .withColumn('year', year('sell_date').cast('int'))
    .withColumn('month', month('sell_date').cast('int'))
    .groupBy('year', 'month')
    .agg(
        coalesce(sum('sale_total_price'), lit(0)).alias('total_revenue'),
        count('*').alias('total_orders'),
        coalesce(avg('sale_total_price'), lit(0)).alias('avg_order_size'),
        max('sell_date').alias('peak_sales_day'),
        coalesce(max('sale_total_price'), lit(0)).alias('peak_sales_revenue')
    )
    .withColumn('revenue_growth', 
        coalesce(
            (col('total_revenue') - lag('total_revenue').over(Window.orderBy('year', 'month'))) / 
            lag('total_revenue').over(Window.orderBy('year', 'month')) * 100,
            lit(0)
        )
    )
    .withColumn('order_growth',
        coalesce(
            (col('total_orders') - lag('total_orders').over(Window.orderBy('year', 'month'))) / 
            lag('total_orders').over(Window.orderBy('year', 'month')) * 100,
            lit(0)
        )
    )
)

print(f"sales_by_time count: {sales_by_time.count()}")
print("Debug: sales_by_time sample...")
sales_by_time.show(5)

# 4. Витрина продаж по магазинам
print("\nProcessing sales_by_store...")
sales_by_store = (
    fact_sales
    .join(dim_stores, fact_sales.store_id == dim_stores.store_id, 'left')
    .join(dim_countries, dim_stores.country_id == dim_countries.country_id, 'left')
    .join(dim_products, fact_sales.product_id == dim_products.product_id, 'left')
    .join(dim_product_categories, dim_products.category_id == dim_product_categories.category_id, 'left')
    .groupBy(
        dim_stores.store_id,
        coalesce(dim_stores.name, lit('Unknown')).alias('store_name'),
        coalesce(dim_stores.location, lit('Unknown')).alias('city'),
        coalesce(dim_countries.country_name, lit('Unknown')).alias('country')
    )
    .agg(
        coalesce(sum('sale_total_price'), lit(0)).alias('total_revenue'),
        coalesce(avg('sale_total_price'), lit(0)).alias('avg_order_value'),
        count('*').alias('total_orders'),
        countDistinct('customer_id').alias('customer_count'),
        coalesce(expr("max_by(category, sale_total_price)"), lit('Unknown')).alias('top_product_category')
    )
    .withColumn('performance_rank', rank().over(Window.orderBy(desc('total_revenue'))))
)

print(f"sales_by_store count: {sales_by_store.count()}")
print("Debug: sales_by_store sample...")
sales_by_store.show(5)

# 5. Витрина продаж по поставщикам
print("\nProcessing sales_by_supplier...")
sales_by_supplier = (
    fact_sales
    .join(dim_suppliers, fact_sales.supplier_id == dim_suppliers.supplier_id, 'left')
    .join(dim_countries, dim_suppliers.country_id == dim_countries.country_id, 'left')
    .join(dim_products, fact_sales.product_id == dim_products.product_id, 'left')
    .join(dim_product_categories, dim_products.category_id == dim_product_categories.category_id, 'left')
    .groupBy(
        dim_suppliers.supplier_id,
        coalesce(dim_suppliers.name, lit('Unknown')).alias('supplier_name'),
        coalesce(dim_countries.country_name, lit('Unknown')).alias('country')
    )
    .agg(
        coalesce(sum('sale_total_price'), lit(0)).alias('total_revenue'),
        coalesce(avg('sale_total_price'), lit(0)).alias('avg_price'),
        countDistinct(fact_sales.product_id).alias('total_products'),
        coalesce(expr("max_by(category, sale_total_price)"), lit('Unknown')).alias('top_product_category'),
        coalesce(avg('rating'), lit(0)).alias('delivery_success_rate')
    )
    .withColumn('performance_rank', rank().over(Window.orderBy(desc('total_revenue'))))
)

print(f"sales_by_supplier count: {sales_by_supplier.count()}")
print("Debug: sales_by_supplier sample...")
sales_by_supplier.show(5)

# 6. Витрина качества продукции
print("\nProcessing product_quality...")
product_quality = (
    fact_sales
    .join(dim_products, fact_sales.product_id == dim_products.product_id, 'left')
    .groupBy(
        dim_products.product_id,
        coalesce(dim_products.name, lit('Unknown')).alias('product_name'),
        coalesce(dim_products.rating, lit(0)).alias('rating'),
        coalesce(dim_products.reviews, lit(0)).alias('review_count')
    )
    .agg(
        coalesce(sum('sale_quantity'), lit(0)).alias('total_quantity'),
        sum(when(col('rating') >= 4, 1).otherwise(0)).alias('positive_reviews'),
        sum(when(col('rating') <= 2, 1).otherwise(0)).alias('negative_reviews'),
        coalesce(avg('rating'), lit(0)).alias('top_reviewer_rating')
    )
    .withColumn('quality_rank', rank().over(Window.orderBy(desc('rating'))))
    .withColumn('sales_correlation', 
        coalesce(
            (col('total_quantity') - avg('total_quantity').over(Window.partitionBy())) / 
            stddev('total_quantity').over(Window.partitionBy()) * 
            (col('rating') - avg('rating').over(Window.partitionBy())) / 
            stddev('rating').over(Window.partitionBy()),
            lit(0)
        )
    )
)

print(f"product_quality count: {product_quality.count()}")
print("Debug: product_quality sample...")
product_quality.show(5)

# Записываем результаты в ClickHouse
print("Writing data to ClickHouse...")

print("Writing sales_by_product...")
sales_by_product.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_product')\
    .options(**ch_props)\
    .mode("append")\
    .save()

print("Writing sales_by_customer...")
sales_by_customer.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_customer')\
    .options(**ch_props)\
    .mode("append")\
    .save()

print("Writing sales_by_time...")
sales_by_time.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_time')\
    .options(**ch_props)\
    .mode("append")\
    .save()

print("Writing sales_by_store...")
sales_by_store.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_store')\
    .options(**ch_props)\
    .mode("append")\
    .save()

print("Writing sales_by_supplier...")
sales_by_supplier.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.sales_by_supplier')\
    .options(**ch_props)\
    .mode("append")\
    .save()

print("Writing product_quality...")
product_quality.write.format('jdbc')\
    .option('url', ch_url)\
    .option('dbtable', 'analytics.product_quality')\
    .options(**ch_props)\
    .mode("append")\
    .save()

print("All data has been written to ClickHouse successfully!")

spark.stop()
