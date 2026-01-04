-- Table des produits
CREATE TABLE products (
    sku_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    unit_price DECIMAL(10,2),
    pack_size INTEGER,
    min_order_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des fournisseurs
CREATE TABLE suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    supplier_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table de liaison produit-fournisseur
CREATE TABLE product_supplier (
    id SERIAL PRIMARY KEY,
    sku_id VARCHAR(50) REFERENCES products(sku_id),
    supplier_id VARCHAR(20) REFERENCES suppliers(supplier_id),
    lead_time_days INTEGER,
    is_primary BOOLEAN DEFAULT FALSE,
    UNIQUE(sku_id, supplier_id)
);

-- Table du stock de sécurité
CREATE TABLE safety_stock (
    id SERIAL PRIMARY KEY,
    sku_id VARCHAR(50) REFERENCES products(sku_id),
    warehouse_id VARCHAR(20),
    safety_stock_level INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sku_id, warehouse_id)
);