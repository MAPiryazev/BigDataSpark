CREATE TABLE states(
    state_id SERIAL PRIMARY key,
    state_name TEXT
);

CREATE TABLE product_categories(
    category_id SERIAL PRIMARY key,
    category TEXT
);

CREATE TABLE pet_categories(
    pet_category_id SERIAL PRIMARY KEY,
    category TEXT UNIQUE
);

CREATE TABLE pet_types(
    pet_type_id SERIAL PRIMARY KEY,
    type TEXT UNIQUE
);

CREATE TABLE cities(
    city_id SERIAL PRIMARY KEY,
    city_name TEXT
);

CREATE TABLE countries(
    country_id SERIAL PRIMARY KEY,
    country_name TEXT UNIQUE
);

CREATE TABLE product_colors(
    product_color_id SERIAL PRIMARY KEY,
    color TEXT
);

CREATE TABLE product_brands(
    product_brand_id SERIAL PRIMARY KEY,
    brand_name TEXT
);

CREATE TABLE dim_pets(
    pet_id SERIAL PRIMARY KEY,
    pet_category_id INT REFERENCES pet_categories(pet_category_id),
    type_id INT REFERENCES pet_types(pet_type_id),
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE dim_suppliers(
    supplier_id SERIAL PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city_id INT REFERENCES cities(city_id) ON DELETE CASCADE,
    country_id INT REFERENCES countries(country_id) ON DELETE CASCADE
);

CREATE TABLE dim_customers(
    customer_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT UNIQUE,
    country_id INT REFERENCES countries(country_id) ON DELETE CASCADE,
    postal_code TEXT
);

CREATE TABLE dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    seller_email TEXT,
    seller_country_id INT REFERENCES countries(country_id) ON DELETE CASCADE,
    postal_code TEXT
);

CREATE TABLE dim_stores(
    store_id SERIAL PRIMARY key,
    name TEXT,
    location TEXT,
    city_id INT REFERENCES cities(city_id) ON DELETE CASCADE,
    country_id INT REFERENCES countries(country_id) ON DELETE CASCADE,
    phone TEXT,
    email TEXT,
    state_id INT REFERENCES states(state_id) ON DELETE CASCADE
);

CREATE TABLE dim_products(
    product_id SERIAL PRIMARY KEY,
    name TEXT,
    category_id INT REFERENCES product_categories(category_id) ON DELETE CASCADE,
    price NUMERIC(10,2),
    quantity INT,
    weight NUMERIC(10,2),
    color_id INT REFERENCES product_colors(product_color_id) ON DELETE CASCADE,
    size TEXT,
    brand_id INT REFERENCES product_brands(product_brand_id) ON DELETE CASCADE,
    material TEXT,
    description TEXT,
    rating NUMERIC(3,2),
    reviews INT,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customers(customer_id) ON DELETE CASCADE,
    seller_id INT REFERENCES dim_sellers(seller_id) ON DELETE CASCADE,
    product_id INT REFERENCES dim_products(product_id) ON DELETE CASCADE,
    store_id INT REFERENCES dim_stores(store_id) ON DELETE CASCADE,
    supplier_id INT REFERENCES dim_suppliers(supplier_id) ON DELETE CASCADE,
    pet_id INT REFERENCES dim_pets(pet_id) ON DELETE CASCADE,
    sell_date DATE,
    sale_quantity INT,
    sale_total_price NUMERIC(10,2)
);
