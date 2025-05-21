#!/usr/bin/env bash
set -e

echo "Click init started"

until clickhouse-client --query "SELECT 1" &>/dev/null; do
  echo "Waiting for ClickHouse..."
  sleep 2
done

echo "Creating analytics schema and tables..."
clickhouse-client --multiquery << 'EOSQL'
CREATE DATABASE IF NOT EXISTS analytics;

-- Витрина 1: продажи по продуктам
CREATE TABLE IF NOT EXISTS analytics.sales_by_product (
    product_id UInt64,
    product_name String,
    category String,
    total_revenue Decimal(10,2),
    total_quantity UInt64,
    avg_rating Float32,
    review_count UInt64,
    total_orders UInt64,
    avg_order_value Decimal(10,2),
    popularity_rank UInt32
) ENGINE = MergeTree()
ORDER BY (category, product_id);

-- Витрина 2: продажи по клиентам
CREATE TABLE IF NOT EXISTS analytics.sales_by_customer (
    customer_id UInt64,
    customer_name String,
    country String,
    total_spent Decimal(10,2),
    avg_order_value Decimal(10,2),
    total_orders UInt64,
    last_order_date Date,
    customer_segment String,
    top_product_category String
) ENGINE = MergeTree()
ORDER BY (country, customer_id);

-- Витрина 3: продажи по времени
CREATE TABLE IF NOT EXISTS analytics.sales_by_time (
    month UInt8,
    year UInt16,
    total_revenue Decimal(10,2),
    total_orders UInt64,
    avg_order_size Decimal(10,2),
    revenue_growth Float32,
    order_growth Float32,
    peak_sales_day Date,
    peak_sales_revenue Decimal(10,2)
) ENGINE = MergeTree()
ORDER BY (year, month);

-- Витрина 4: продажи по магазинам
CREATE TABLE IF NOT EXISTS analytics.sales_by_store (
    store_id UInt64,
    store_name String,
    city String,
    country String,
    total_revenue Decimal(10,2),
    avg_order_value Decimal(10,2),
    total_orders UInt64,
    performance_rank UInt32,
    top_product_category String,
    customer_count UInt64
) ENGINE = MergeTree()
ORDER BY (country, city, store_id);

-- Витрина 5: продажи по поставщикам
CREATE TABLE IF NOT EXISTS analytics.sales_by_supplier (
    supplier_id UInt64,
    supplier_name String,
    country String,
    total_revenue Decimal(10,2),
    avg_price Decimal(10,2),
    total_products UInt64,
    performance_rank UInt32,
    top_product_category String,
    delivery_success_rate Float32
) ENGINE = MergeTree()
ORDER BY (country, supplier_id);

-- Витрина 6: качество продукции
CREATE TABLE IF NOT EXISTS analytics.product_quality (
    product_id UInt64,
    product_name String,
    rating Float32,
    review_count UInt64,
    total_quantity UInt64,
    positive_reviews UInt64,
    negative_reviews UInt64,
    quality_rank UInt32,
    sales_correlation Float32,
    top_reviewer_rating Float32
) ENGINE = MergeTree()
ORDER BY (rating, product_id);
EOSQL

echo "ClickHouse initialized."