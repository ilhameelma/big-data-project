docker-compose exec trino trino << 'EOF'
-- Créer toutes les tables
CREATE TABLE IF NOT EXISTS postgresql.public.products (
    sku_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    unit_price DECIMAL(10,2),
    pack_size INTEGER,
    min_order_quantity INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS postgresql.public.suppliers (
    supplier_id VARCHAR(20),
    supplier_name VARCHAR(255),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS postgresql.public.product_supplier (
    id BIGINT,
    sku_id VARCHAR(50),
    supplier_id VARCHAR(20),
    lead_time_days INTEGER,
    is_primary BOOLEAN
);

CREATE TABLE IF NOT EXISTS postgresql.public.safety_stock (
    id BIGINT,
    sku_id VARCHAR(50),
    warehouse_id VARCHAR(20),
    safety_stock_level INTEGER,
    created_at TIMESTAMP
);

-- Vérifier
SHOW TABLES FROM postgresql.public;
EOF