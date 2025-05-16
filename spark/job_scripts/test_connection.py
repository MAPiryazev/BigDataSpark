from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, expr
import os

# PostgreSQL connection parameters
PG_DATABASE = os.getenv("PG_DATABASE", "postgres_db")
PG_HOST = os.getenv("PG_HOST", "postgres_database")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "postgres_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres_pass")

# JDBC URL and properties
pg_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
pg_props = {
    'user': PG_USER,
    'password': PG_PASSWORD,
    'driver': 'org.postgresql.Driver'
}

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName('PostgreSQLDataTransfer')
    .config('spark.jars', '/opt/bitnami/spark/jars/postgresql-42.7.3.jar')
    .getOrCreate()
)

# Read mock_data table
mock_data = spark.read.jdbc(pg_url, 'mock_data', properties=pg_props)

# 1. Insert into states
states = mock_data.select('store_state').distinct() \
    .withColumnRenamed('store_state', 'state_name')
states.write.jdbc(
    url=pg_url,
    table='states',
    mode='append',
    properties=pg_props
)

# 2. Insert into product_categories
product_categories = mock_data.select('product_category').distinct() \
    .withColumnRenamed('product_category', 'category')
product_categories.write.jdbc(
    url=pg_url,
    table='product_categories',
    mode='append',
    properties=pg_props
)

# 3. Insert into pet_categories
pet_categories = mock_data.select('pet_category').distinct() \
    .withColumnRenamed('pet_category', 'category')
pet_categories.write.jdbc(
    url=pg_url,
    table='pet_categories',
    mode='append',
    properties=pg_props
)

# 4. Insert into pet_types
pet_types = mock_data.select('customer_pet_type').distinct() \
    .withColumnRenamed('customer_pet_type', 'type')
pet_types.write.jdbc(
    url=pg_url,
    table='pet_types',
    mode='append',
    properties=pg_props
)

# 5. Insert into cities
cities = mock_data.select('store_city').distinct() \
    .withColumnRenamed('store_city', 'city_name') \
    .union(mock_data.select('supplier_city').distinct() \
    .withColumnRenamed('supplier_city', 'city_name')) \
    .distinct()
cities.write.jdbc(
    url=pg_url,
    table='cities',
    mode='append',
    properties=pg_props
)

# 6. Insert into countries
countries = mock_data.select('customer_country').distinct() \
    .withColumnRenamed('customer_country', 'country_name') \
    .union(mock_data.select('seller_country').distinct() \
    .withColumnRenamed('seller_country', 'country_name')) \
    .union(mock_data.select('store_country').distinct() \
    .withColumnRenamed('store_country', 'country_name')) \
    .union(mock_data.select('supplier_country').distinct() \
    .withColumnRenamed('supplier_country', 'country_name')) \
    .distinct()
countries.write.jdbc(
    url=pg_url,
    table='countries',
    mode='append',
    properties=pg_props
)

# 7. Insert into product_colors
product_colors = mock_data.select('product_color').distinct() \
    .withColumnRenamed('product_color', 'color')
product_colors.write.jdbc(
    url=pg_url,
    table='product_colors',
    mode='append',
    properties=pg_props
)

# 8. Insert into product_brands
product_brands = mock_data.select('product_brand').distinct() \
    .withColumnRenamed('product_brand', 'brand_name')
product_brands.write.jdbc(
    url=pg_url,
    table='product_brands',
    mode='append',
    properties=pg_props
)

# Read reference tables for joins
states_df = spark.read.jdbc(pg_url, 'states', properties=pg_props)
product_categories_df = spark.read.jdbc(pg_url, 'product_categories', properties=pg_props)
pet_categories_df = spark.read.jdbc(pg_url, 'pet_categories', properties=pg_props)
pet_types_df = spark.read.jdbc(pg_url, 'pet_types', properties=pg_props)
cities_df = spark.read.jdbc(pg_url, 'cities', properties=pg_props)
countries_df = spark.read.jdbc(pg_url, 'countries', properties=pg_props)
product_colors_df = spark.read.jdbc(pg_url, 'product_colors', properties=pg_props)
product_brands_df = spark.read.jdbc(pg_url, 'product_brands', properties=pg_props)

# 9. Insert into dim_pets
dim_pets = mock_data.join(pet_categories_df, mock_data.pet_category == pet_categories_df.category) \
    .join(pet_types_df, mock_data.customer_pet_type == pet_types_df.type) \
    .select(
        'pet_category_id',
        pet_types_df.pet_type_id.alias('type_id'),
        'customer_pet_name',
        'customer_pet_breed'
    ).distinct() \
    .withColumnRenamed('customer_pet_name', 'pet_name') \
    .withColumnRenamed('customer_pet_breed', 'pet_breed')
dim_pets.write.jdbc(
    url=pg_url,
    table='dim_pets',
    mode='append',
    properties=pg_props
)

# 10. Insert into dim_suppliers
dim_suppliers = mock_data.join(cities_df, mock_data.supplier_city == cities_df.city_name) \
    .join(countries_df, mock_data.supplier_country == countries_df.country_name) \
    .select(
        'supplier_name',
        'supplier_contact',
        'supplier_email',
        'supplier_phone',
        'supplier_address',
        'city_id',
        'country_id'
    ).distinct()
dim_suppliers.write.jdbc(
    url=pg_url,
    table='dim_suppliers',
    mode='append',
    properties=pg_props
)

# 11. Insert into dim_customers
dim_customers = mock_data.join(countries_df, mock_data.customer_country == countries_df.country_name) \
    .select(
        'customer_first_name',
        'customer_last_name',
        'customer_age',
        'customer_email',
        'country_id',
        'customer_postal_code'
    ).distinct()
dim_customers.write.jdbc(
    url=pg_url,
    table='dim_customers',
    mode='append',
    properties=pg_props
)

# 12. Insert into dim_sellers
dim_sellers = mock_data.join(countries_df, mock_data.seller_country == countries_df.country_name) \
    .select(
        'seller_first_name',
        'seller_last_name',
        'seller_email',
        'country_id',
        'seller_postal_code'
    ).distinct()
dim_sellers.write.jdbc(
    url=pg_url,
    table='dim_sellers',
    mode='append',
    properties=pg_props
)

# 13. Insert into dim_stores
dim_stores = mock_data.join(cities_df, mock_data.store_city == cities_df.city_name) \
    .join(states_df, mock_data.store_state == states_df.state_name) \
    .join(countries_df, mock_data.store_country == countries_df.country_name) \
    .select(
        'store_name',
        'store_location',
        'city_id',
        'state_id',
        'country_id',
        'store_phone',
        'store_email'
    ).distinct()
dim_stores.write.jdbc(
    url=pg_url,
    table='dim_stores',
    mode='append',
    properties=pg_props
)

# 14. Insert into dim_products
dim_products = mock_data.join(product_categories_df, mock_data.product_category == product_categories_df.category) \
    .join(product_colors_df, mock_data.product_color == product_colors_df.color) \
    .join(product_brands_df, mock_data.product_brand == product_brands_df.brand_name) \
    .select(
        'product_name',
        'category_id',
        'product_price',
        'product_quantity',
        'product_weight',
        'product_color_id',
        'product_size',
        'product_brand_id',
        'product_material',
        'product_description',
        'product_rating',
        'product_reviews',
        'product_release_date',
        'product_expiry_date'
    ).distinct()
dim_products.write.jdbc(
    url=pg_url,
    table='dim_products',
    mode='append',
    properties=pg_props
)

# Read dimension tables for fact table joins
dim_customers_df = spark.read.jdbc(pg_url, 'dim_customers', properties=pg_props)
dim_sellers_df = spark.read.jdbc(pg_url, 'dim_sellers', properties=pg_props)
dim_products_df = spark.read.jdbc(pg_url, 'dim_products', properties=pg_props)
dim_stores_df = spark.read.jdbc(pg_url, 'dim_stores', properties=pg_props)
dim_suppliers_df = spark.read.jdbc(pg_url, 'dim_suppliers', properties=pg_props)
dim_pets_df = spark.read.jdbc(pg_url, 'dim_pets', properties=pg_props)

# 15. Insert into fact_sales
fact_sales = mock_data.join(dim_customers_df, mock_data.customer_email == dim_customers_df.email) \
    .join(dim_sellers_df, mock_data.seller_email == dim_sellers_df.seller_email) \
    .join(dim_products_df, (mock_data.product_name == dim_products_df.name) & 
          (mock_data.product_release_date == dim_products_df.release_date)) \
    .join(dim_stores_df, (mock_data.store_name == dim_stores_df.name) & 
          (mock_data.store_email == dim_stores_df.email)) \
    .join(dim_suppliers_df, mock_data.supplier_email == dim_suppliers_df.email) \
    .join(dim_pets_df, (mock_data.customer_pet_name == dim_pets_df.pet_name) & 
          (mock_data.customer_pet_breed == dim_pets_df.pet_breed), 'left') \
    .select(
        'customer_id',
        'seller_id',
        'product_id',
        'store_id',
        'supplier_id',
        'pet_id',
        'sale_date',
        'sale_quantity',
        'sale_total_price'
    )
fact_sales.write.jdbc(
    url=pg_url,
    table='fact_sales',
    mode='append',
    properties=pg_props
)

spark.stop()
