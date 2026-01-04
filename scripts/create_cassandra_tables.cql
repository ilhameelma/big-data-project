CREATE KEYSPACE IF NOT EXISTS procurement 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE procurement;

-- Table pour les commandes fournisseurs générées
CREATE TABLE IF NOT EXISTS supplier_orders (
    order_date text,
    supplier_id text,
    order_id uuid,
    sku_id text,
    quantity int,
    status text,
    generated_at timestamp,
    PRIMARY KEY ((supplier_id, order_date), sku_id)
);

-- Table pour les exceptions
CREATE TABLE IF NOT EXISTS exceptions (
    date text,
    exception_id uuid,
    exception_type text,
    sku_id text,
    supplier_id text,
    message text,
    severity text,
    created_at timestamp,
    PRIMARY KEY (date, exception_id)
);

-- Table pour l'historique des calculs
CREATE TABLE IF NOT EXISTS demand_calculations (
    calculation_date text,
    sku_id text,
    total_demand int,
    available_stock int,
    net_demand int,
    final_order_quantity int,
    calculated_at timestamp,
    PRIMARY KEY (calculation_date, sku_id)
);