from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, expr, to_date
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
    ).distinct() \
    .withColumnRenamed('supplier_name', 'name') \
    .withColumnRenamed('supplier_contact', 'contact') \
    .withColumnRenamed('supplier_email', 'email') \
    .withColumnRenamed('supplier_phone', 'phone') \
    .withColumnRenamed('supplier_address', 'address')
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
    ).distinct() \
    .withColumnRenamed('customer_first_name', 'first_name') \
    .withColumnRenamed('customer_last_name', 'last_name') \
    .withColumnRenamed('customer_age', 'age') \
    .withColumnRenamed('customer_email', 'email') \
    .withColumnRenamed('customer_postal_code', 'postal_code')
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
    ).distinct() \
    .withColumnRenamed('seller_first_name', 'first_name') \
    .withColumnRenamed('seller_last_name', 'last_name') \
    .withColumnRenamed('seller_email', 'seller_email') \
    .withColumnRenamed('country_id', 'seller_country_id') \
    .withColumnRenamed('seller_postal_code', 'postal_code')
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
    ).distinct() \
    .withColumnRenamed('store_name', 'name') \
    .withColumnRenamed('store_location', 'location') \
    .withColumnRenamed('store_phone', 'phone') \
    .withColumnRenamed('store_email', 'email')
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
        product_colors_df.product_color_id.alias('color_id'),
        'product_size',
        product_brands_df.product_brand_id.alias('brand_id'),
        'product_material',
        'product_description',
        'product_rating',
        'product_reviews',
        to_date('product_release_date').alias('release_date'),
        to_date('product_expiry_date').alias('expiry_date')
    ).distinct() \
    .withColumnRenamed('product_name', 'name') \
    .withColumnRenamed('product_price', 'price') \
    .withColumnRenamed('product_quantity', 'quantity') \
    .withColumnRenamed('product_weight', 'weight') \
    .withColumnRenamed('product_size', 'size') \
    .withColumnRenamed('product_material', 'material') \
    .withColumnRenamed('product_description', 'description') \
    .withColumnRenamed('product_rating', 'rating') \
    .withColumnRenamed('product_reviews', 'reviews')
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
print("\nПроверка данных перед джоинами:")
print("Количество строк в mock_data:", mock_data.count())
print("Количество строк в dim_products_df:", dim_products_df.count())
print("Количество строк в dim_stores_df:", dim_stores_df.count())
print("Количество строк в dim_suppliers_df:", dim_suppliers_df.count())

# Проверяем уникальные значения
print("\nУникальные значения в mock_data:")
mock_data.select('product_name').distinct().show(5)
mock_data.select('product_release_date').distinct().show(5)

print("\nУникальные значения в dim_products_df:")
dim_products_df.select('name').distinct().show(5)
dim_products_df.select('release_date').distinct().show(5)

fact_sales = mock_data.join(dim_customers_df, mock_data.customer_email == dim_customers_df.email)
print(f"\nAfter customers join: {fact_sales.count()}")

fact_sales = fact_sales.join(dim_sellers_df, mock_data.seller_email == dim_sellers_df.seller_email)
print(f"After sellers join: {fact_sales.count()}")

# Преобразуем даты в один формат
fact_sales = fact_sales.withColumn('product_release_date', to_date('product_release_date'))
dim_products_df = dim_products_df.withColumn('release_date', to_date('release_date'))

# Проверяем данные перед джоином с продуктами
print("\nПроверка данных перед джоином с продуктами:")
print("Sample from fact_sales:")
fact_sales.select('product_name', 'product_release_date').show(5)
print("\nSample from dim_products_df:")
dim_products_df.select('name', 'release_date').show(5)

# Джоин с продуктами
fact_sales = fact_sales.join(
    dim_products_df,
    (fact_sales.product_name == dim_products_df.name) & 
    (fact_sales.product_release_date == dim_products_df.release_date)
)
print(f"After products join: {fact_sales.count()}")

# Проверяем данные перед джоином со stores
print("\nПроверка данных перед джоином со stores:")
print("Sample from fact_sales:")
fact_sales.select('store_name', 'store_email').show(5)
print("\nSample from dim_stores_df:")
dim_stores_df.select('name', 'email').show(5)

# Джоин со stores
fact_sales = fact_sales.join(
    dim_stores_df,
    (fact_sales.store_name == dim_stores_df.name) & 
    (fact_sales.store_email == dim_stores_df.email)
)
print(f"After stores join: {fact_sales.count()}")

# Проверяем данные перед джоином с suppliers
print("\nПроверка данных перед джоином с suppliers:")
print("Sample from fact_sales:")
fact_sales.select('supplier_email').show(5)
print("\nSample from dim_suppliers_df:")
dim_suppliers_df.select('email').show(5)

# Джоин с suppliers
fact_sales = fact_sales.join(
    dim_suppliers_df,
    fact_sales.supplier_email == dim_suppliers_df.email
)
print(f"After suppliers join: {fact_sales.count()}")

# Проверяем данные перед джоином с pets
print("\nПроверка данных перед джоином с pets:")
print("Sample from fact_sales:")
fact_sales.select('customer_pet_name', 'customer_pet_breed').show(5)
print("\nSample from dim_pets_df:")
dim_pets_df.select('pet_name', 'pet_breed').show(5)

# Джоин с pets
fact_sales = fact_sales.join(
    dim_pets_df,
    (fact_sales.customer_pet_name == dim_pets_df.pet_name) & 
    (fact_sales.customer_pet_breed == dim_pets_df.pet_breed),
    'left'
)
print(f"After pets join: {fact_sales.count()}")

# Выбираем финальные колонки
fact_sales = fact_sales.select(
    dim_customers_df.customer_id,
    dim_sellers_df.seller_id,
    dim_products_df.product_id,
    dim_stores_df.store_id,
    dim_suppliers_df.supplier_id,
    dim_pets_df.pet_id,
    to_date('sale_date').alias('sell_date'),
    'sale_quantity',
    'sale_total_price'
)
print(f"Final fact_sales count: {fact_sales.count()}")

# Записываем в базу данных
fact_sales.write.jdbc(
    url=pg_url,
    table='fact_sales',
    mode='append',
    properties=pg_props
)

# Ждем завершения всех операций
spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
spark.sparkContext._jsc.sc().getExecutorMemoryStatus()

# Останавливаем Spark только после завершения всех операций
spark.stop()
