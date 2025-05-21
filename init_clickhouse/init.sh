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
    product_id UInt32,
    product_name String,
    category String,
    total_revenue Decimal(10,2),
    total_quantity UInt32,
    avg_rating Decimal(3,2),
    review_count UInt32,
    total_orders UInt32,
    avg_order_value Decimal(10,2),
    popularity_rank UInt32
) ENGINE = MergeTree()
ORDER BY (category, product_id);

-- Витрина 2: продажи по клиентам
CREATE TABLE IF NOT EXISTS analytics.sales_by_customer (
    customer_id UInt32,
    customer_name String,
    country String,
    total_spent Decimal(10,2),
    avg_order_value Decimal(10,2),
    total_orders UInt32,
    last_order_date Date,
    customer_segment String,
    top_product_category String
) ENGINE = MergeTree()
ORDER BY (country, customer_id);

-- Витрина 3: продажи по времени
CREATE TABLE IF NOT EXISTS analytics.sales_by_time (
    year UInt16,
    month UInt8,
    total_revenue Decimal(10,2),
    total_orders UInt32,
    avg_order_size Decimal(10,2),
    revenue_growth Decimal(10,2),
    order_growth Decimal(10,2),
    peak_sales_day Date,
    peak_sales_revenue Decimal(10,2)
) ENGINE = MergeTree()
ORDER BY (year, month);

-- Витрина 4: продажи по магазинам
CREATE TABLE IF NOT EXISTS analytics.sales_by_store (
    store_id UInt32,
    store_name String,
    city String,
    country String,
    total_revenue Decimal(10,2),
    avg_order_value Decimal(10,2),
    total_orders UInt32,
    performance_rank UInt32,
    top_product_category String,
    customer_count UInt32
) ENGINE = MergeTree()
ORDER BY (country, city, store_id);

-- Витрина 5: продажи по поставщикам
CREATE TABLE IF NOT EXISTS analytics.sales_by_supplier (
    supplier_id UInt32,
    supplier_name String,
    country String,
    total_revenue Decimal(10,2),
    avg_price Decimal(10,2),
    total_products UInt32,
    performance_rank UInt32,
    top_product_category String,
    delivery_success_rate Decimal(3,2)
) ENGINE = MergeTree()
ORDER BY (country, supplier_id);

-- Витрина 6: качество продукции
CREATE TABLE IF NOT EXISTS analytics.product_quality (
    product_id UInt32,
    product_name String,
    rating Decimal(3,2),
    review_count UInt32,
    total_quantity UInt32,
    positive_reviews UInt32,
    negative_reviews UInt32,
    quality_rank UInt32,
    sales_correlation Decimal(10,6),
    top_reviewer_rating Decimal(3,2)
) ENGINE = MergeTree()
ORDER BY (rating, product_id);
EOSQL

echo "ClickHouse initialized."