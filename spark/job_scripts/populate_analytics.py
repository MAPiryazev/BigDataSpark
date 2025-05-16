import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum as sum_, avg as avg_, col, count, max as max_,
    concat, date_format, to_date, when
)

# Environment variables
PG_DATABASE = os.getenv("PG_DATABASE", "main_db")
PG_HOST = os.getenv("PG_HOST", "bigdata-db")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "bigdata")
PG_PASSWORD = os.getenv("PG_PASSWORD", "bigdata")
CH_HOST = os.getenv("CH_HOST", "clickhouse-server")
CH_PORT = os.getenv("CH_PORT", "8123")
CH_DATABASE = os.getenv("CH_DATABASE", "analytics")
CH_DRIVER = os.getenv("CH_DRIVER", "com.clickhouse.jdbc.ClickHouseDriver")

# JDBC Configuration
JARS = os.getenv(
    "SPARK_JARS",
    "/opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar,"
    "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
)

jdbc_conf = {
    'pg.url': f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}",
    'pg.user': PG_USER,
    'pg.password': PG_PASSWORD,
    'ch.url': f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DATABASE}?user=default&password=",
    'ch.driver': CH_DRIVER
}

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName('AnalyticsDataLoader')
    .config('spark.jars', JARS)
    .config("spark.sql.catalog.clickhouse.host", CH_HOST)
    .config("spark.sql.catalog.clickhouse.protocol", "http")
    .config("spark.sql.catalog.clickhouse.http_port", CH_PORT)
    .config("spark.sql.catalog.clickhouse.database", CH_DATABASE)
    .getOrCreate()
)

def read_table(table_name):
    """Read table from PostgreSQL and check if it's empty"""
    df = spark.read.jdbc(jdbc_conf['pg.url'], table_name, properties={
        'user': jdbc_conf['pg.user'],
        'password': jdbc_conf['pg.password'],
        'driver': 'org.postgresql.Driver'
    })
    count = df.count()
    print(f"Table {table_name} has {count} rows")
    return df if count > 0 else None

# Load source data from PostgreSQL
sales = read_table('facts_sales')
if sales is None:
    print("No sales data available. Skipping analytics population.")
    spark.stop()
    exit(0)

products = read_table('dim_products')
categories = read_table('dim_product_categories')
customers = read_table('dim_customers')
dates = read_table('dim_dates')
stores = read_table('dim_stores')
suppliers = read_table('dim_suppliers')
countries = read_table('dim_countries')

if any(df is None for df in [products, categories, customers, dates, stores, suppliers, countries]):
    print("Some dimension tables are empty. Skipping analytics population.")
    spark.stop()
    exit(0)

# Rename country column
countries = countries.withColumnRenamed('name', 'country')

# 1. Product Sales Analysis
product_analysis = (
    sales.join(products, sales.product_id == products.id)
    .join(categories, products.category_id == categories.id)
    .join(dates, sales.date_id == dates.id)
    .groupBy(
        products.id.alias('product_id'),
        products.name.alias('product_name'),
        categories.name.alias('category')
    )
    .agg(
        sum_('total_price').alias('total_sales_amount'),
        sum_('product_quantity').alias('units_sold'),
        avg_('rating').alias('average_rating'),
        sum_('reviews').alias('review_count'),
        max_('date').alias('last_sale_date')
    )
)

# 2. Customer Purchase Analysis
customer_analysis = (
    sales.join(customers, sales.customer_id == customers.id)
    .join(dates, sales.date_id == dates.id)
    .join(countries, customers.country_id == countries.id)
    .groupBy(
        customers.id.alias('customer_id'),
        concat(customers.first_name, ' ', customers.last_name).alias('customer_name'),
        countries.country
    )
    .agg(
        sum_('total_price').alias('total_purchase_amount'),
        count('*').alias('purchase_count'),
        avg_('total_price').alias('average_order_value'),
        max_('date').alias('last_purchase_date')
    )
)

# 3. Time-based Sales Analysis
time_analysis = (
    sales.join(dates, sales.date_id == dates.id)
    .withColumn('year', date_format('date', 'yyyy').cast('int'))
    .withColumn('month', date_format('date', 'MM').cast('int'))
    .groupBy('year', 'month')
    .agg(
        sum_('total_price').alias('total_sales_amount'),
        count('*').alias('order_count'),
        avg_('total_price').alias('average_order_value'),
        max_('date').alias('peak_sales_day')
    )
)

# 4. Store Performance Analysis
store_analysis = (
    sales.join(stores, sales.store_id == stores.id)
    .join(dates, sales.date_id == dates.id)
    .join(countries, stores.country_id == countries.id)
    .groupBy(
        stores.id.alias('store_id'),
        stores.name.alias('store_name'),
        stores.location.alias('city'),
        countries.country
    )
    .agg(
        sum_('total_price').alias('total_sales_amount'),
        count('*').alias('order_count'),
        avg_('total_price').alias('average_order_value'),
        max_('date').alias('last_sale_date')
    )
)

# 5. Supplier Performance Analysis
supplier_analysis = (
    sales.join(suppliers, sales.supplier_id == suppliers.id)
    .join(dates, sales.date_id == dates.id)
    .join(countries, suppliers.country_id == countries.id)
    .groupBy(
        suppliers.id.alias('supplier_id'),
        suppliers.name.alias('supplier_name'),
        countries.country
    )
    .agg(
        sum_('total_price').alias('total_sales_amount'),
        count('*').alias('product_count'),
        avg_('total_price').alias('average_product_price'),
        max_('date').alias('last_supply_date')
    )
)

# 6. Product Quality Analysis
quality_analysis = (
    sales.join(products, sales.product_id == products.id)
    .join(dates, sales.date_id == dates.id)
    .groupBy(
        products.id.alias('product_id'),
        products.name.alias('product_name')
    )
    .agg(
        avg_('rating').alias('average_rating'),
        sum_('reviews').alias('review_count'),
        sum_('product_quantity').alias('total_units_sold'),
        (sum_(when(col('returned') == True, 1).otherwise(0)) / count('*')).alias('return_rate'),
        max_('date').alias('last_review_date')
    )
)

# Write to ClickHouse
def write_to_clickhouse(df, table_name):
    try:
        df.write.format('jdbc')\
            .option('url', jdbc_conf['ch.url'])\
            .option('dbtable', f'analytics.{table_name}')\
            .option('driver', jdbc_conf['ch.driver'])\
            .mode("append")\
            .save()
        print(f"Successfully wrote to {table_name}")
    except Exception as e:
        print(f"Error writing to {table_name}: {str(e)}")

# Write all analyses to ClickHouse
write_to_clickhouse(product_analysis, 'product_sales_analysis')
write_to_clickhouse(customer_analysis, 'customer_purchase_analysis')
write_to_clickhouse(time_analysis, 'time_sales_analysis')
write_to_clickhouse(store_analysis, 'store_performance_analysis')
write_to_clickhouse(supplier_analysis, 'supplier_performance_analysis')
write_to_clickhouse(quality_analysis, 'product_quality_analysis')

spark.stop() 