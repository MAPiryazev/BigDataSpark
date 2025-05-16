from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, monotonically_increasing_id
import sys

print("🚀 Начало ETL процесса")

# Создаём Spark-сессию
print("🔄 Создаем SparkSession...")
spark = SparkSession.builder \
    .appName("ETLJob") \
    .getOrCreate()

print("✅ SparkSession started")
print(f"Python version: {sys.version}")
print(f"Spark version: {spark.version}")

# Чтение данных из PostgreSQL
print("🔄 Читаем данные из таблицы mock_data...")
try:
    mock_data = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "mock_data") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    print(f"✅ Прочитано {mock_data.count()} строк из mock_data")
    
    # Создаем и заполняем справочные таблицы
    print("🔄 Создаем и заполняем справочные таблицы...")
    
    # States
    states = mock_data.select("store_state").distinct() \
        .withColumnRenamed("store_state", "state_name")
    states.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "states") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("ignore") \
        .save()
    states.collect()  # Ждем завершения записи
    
    # Product Categories
    product_categories = mock_data.select("product_category").distinct() \
        .withColumnRenamed("product_category", "category")
    product_categories.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "product_categories") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("ignore") \
        .save()
    product_categories.collect()  # Ждем завершения записи
    
    # Pet Categories
    pet_categories = mock_data.select("pet_category").distinct() \
        .withColumnRenamed("pet_category", "category")
    pet_categories.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "pet_categories") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("ignore") \
        .save()
    pet_categories.collect()  # Ждем завершения записи
    
    # Pet Types
    pet_types = mock_data.select("customer_pet_type").distinct() \
        .withColumnRenamed("customer_pet_type", "type")
    pet_types.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "pet_types") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("ignore") \
        .save()
    pet_types.collect()  # Ждем завершения записи
    
    # Cities
    cities = mock_data.select("store_city").distinct() \
        .union(mock_data.select("supplier_city").distinct()) \
        .withColumnRenamed("store_city", "city_name")
    cities.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "cities") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("ignore") \
        .save()
    cities.collect()  # Ждем завершения записи
    
    # Countries
    countries = mock_data.select("customer_country").distinct() \
        .union(mock_data.select("seller_country").distinct()) \
        .union(mock_data.select("store_country").distinct()) \
        .union(mock_data.select("supplier_country").distinct()) \
        .withColumnRenamed("customer_country", "country_name")
    countries.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "countries") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("ignore") \
        .save()
    countries.collect()  # Ждем завершения записи
    
    # Product Colors
    product_colors = mock_data.select("product_color").distinct() \
        .withColumnRenamed("product_color", "color")
    product_colors.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "product_colors") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("ignore") \
        .save()
    product_colors.collect()  # Ждем завершения записи
    
    # Product Brands
    product_brands = mock_data.select("product_brand").distinct() \
        .withColumnRenamed("product_brand", "brand_name")
    product_brands.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "product_brands") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("ignore") \
        .save()
    product_brands.collect()  # Ждем завершения записи
    
    print("✅ Справочные таблицы успешно заполнены")
    
    # Теперь создаем и записываем основные таблицы
    print("🔄 Создаем и записываем основные таблицы...")
    
    # Dim Pets
    print("🔄 Создаем dim_pets...")
    
    # Читаем справочные таблицы для получения ID
    pet_categories_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "pet_categories") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    pet_types_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "pet_types") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    # Создаем dim_pets с правильными колонками
    dim_pets = mock_data.select(
        "pet_category",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed"
    ).distinct() \
    .join(pet_categories_df, mock_data.pet_category == pet_categories_df.category) \
    .join(pet_types_df, mock_data.customer_pet_type == pet_types_df.type) \
    .select(
        col("pet_category_id"),
        col("pet_type_id").alias("type_id"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed")
    )
    
    dim_pets.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_pets") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    dim_pets.collect()  # Ждем завершения записи
    
    # Dim Suppliers
    print("🔄 Создаем dim_suppliers...")
    
    # Читаем справочные таблицы
    cities_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "cities") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    countries_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "countries") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    dim_suppliers = mock_data.select(
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country"
    ).distinct() \
    .join(cities_df, mock_data.supplier_city == cities_df.city_name) \
    .join(countries_df, mock_data.supplier_country == countries_df.country_name) \
    .select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("city_id"),
        col("country_id")
    )
    
    dim_suppliers.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_suppliers") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    dim_suppliers.collect()  # Ждем завершения записи
    
    # Dim Customers
    print("🔄 Создаем dim_customers...")
    
    dim_customers = mock_data.select(
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code"
    ).distinct() \
    .join(countries_df, mock_data.customer_country == countries_df.country_name) \
    .select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("country_id"),
        col("customer_postal_code").alias("postal_code")
    )
    
    dim_customers.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_customers") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    dim_customers.collect()  # Ждем завершения записи
    
    # Dim Sellers
    print("🔄 Создаем dim_sellers...")
    
    dim_sellers = mock_data.select(
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code"
    ).distinct() \
    .join(countries_df, mock_data.seller_country == countries_df.country_name) \
    .select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email"),
        col("country_id").alias("seller_country_id"),
        col("seller_postal_code").alias("postal_code")
    )
    
    dim_sellers.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_sellers") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    dim_sellers.collect()  # Ждем завершения записи
    
    # Dim Stores
    print("🔄 Создаем dim_stores...")
    
    states_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "states") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    dim_stores = mock_data.select(
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email"
    ).distinct() \
    .join(cities_df, mock_data.store_city == cities_df.city_name) \
    .join(states_df, mock_data.store_state == states_df.state_name) \
    .join(countries_df, mock_data.store_country == countries_df.country_name) \
    .select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("city_id"),
        col("state_id"),
        col("country_id"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    )
    
    dim_stores.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_stores") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    dim_stores.collect()  # Ждем завершения записи
    
    # Dim Products
    print("🔄 Создаем dim_products...")
    
    product_categories_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "product_categories") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    product_colors_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "product_colors") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    product_brands_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "product_brands") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    dim_products = mock_data.select(
        "product_name",
        "product_category",
        "product_price",
        "product_quantity",
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date"
    ).distinct() \
    .join(product_categories_df, mock_data.product_category == product_categories_df.category) \
    .join(product_colors_df, mock_data.product_color == product_colors_df.color) \
    .join(product_brands_df, mock_data.product_brand == product_brands_df.brand_name) \
    .select(
        col("product_name").alias("name"),
        col("category_id"),
        col("product_price").alias("price"),
        col("product_quantity").alias("quantity"),
        col("product_weight").alias("weight"),
        col("product_color_id").alias("color_id"),
        col("product_size").alias("size"),
        col("product_brand_id").alias("brand_id"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date")
    )
    
    dim_products.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_products") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    dim_products.collect()  # Ждем завершения записи
    
    # Fact Sales
    print("🔄 Создаем fact_sales...")
    
    # Читаем все dimension таблицы для получения ID
    dim_customers_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_customers") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    dim_sellers_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_sellers") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    dim_products_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_products") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    dim_stores_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_stores") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    dim_suppliers_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_suppliers") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    dim_pets_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "dim_pets") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .load()
    
    fact_sales = mock_data.select(
        "customer_email",
        "seller_email",
        "product_name",
        "product_release_date",
        "store_name",
        "store_email",
        "supplier_email",
        "customer_pet_name",
        "customer_pet_breed",
        "sale_date",
        "sale_quantity",
        "sale_total_price"
    ) \
    .join(dim_customers_df, mock_data.customer_email == dim_customers_df.email) \
    .join(dim_sellers_df, mock_data.seller_email == dim_sellers_df.seller_email) \
    .join(dim_products_df, (mock_data.product_name == dim_products_df.name) & 
                          (mock_data.product_release_date == dim_products_df.release_date)) \
    .join(dim_stores_df, (mock_data.store_name == dim_stores_df.name) & 
                        (mock_data.store_email == dim_stores_df.email)) \
    .join(dim_suppliers_df, mock_data.supplier_email == dim_suppliers_df.email) \
    .join(dim_pets_df, (mock_data.customer_pet_name == dim_pets_df.pet_name) & 
                      (mock_data.customer_pet_breed == dim_pets_df.pet_breed), "left") \
    .select(
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("supplier_id"),
        col("pet_id"),
        col("sale_date").alias("sell_date"),
        col("sale_quantity"),
        col("sale_total_price")
    )
    
    fact_sales.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_database:5432/postgres_db") \
        .option("dbtable", "fact_sales") \
        .option("user", "postgres_user") \
        .option("password", "postgres_pass") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    fact_sales.collect()  # Ждем завершения записи
    
    print("✅ Основные таблицы успешно записаны")
    
except Exception as e:
    print(f"❌ Ошибка при выполнении ETL: {str(e)}")
    print(f"Детали ошибки: {type(e).__name__}")
    import traceback
    print("Traceback:")
    print(traceback.format_exc())


print("🏁 Завершение работы...")
spark.stop()
print("✅ Spark остановлен")
