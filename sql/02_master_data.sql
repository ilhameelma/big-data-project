-- Insérer des produits de base (exemple)
INSERT INTO products (sku_id, product_name, category, unit_price, pack_size, min_order_quantity) VALUES
('SKU001', 'Lait UHT 1L', 'Dairy', 0.85, 12, 24),
('SKU002', 'Pâtes Spaghetti 500g', 'Grocery', 1.20, 6, 12),
('SKU003', 'Café Moulu 250g', 'Beverages', 4.50, 1, 5),
('SKU004', 'Jus d''Orange 1L', 'Beverages', 2.10, 6, 12),
('SKU005', 'Pain de Mie 500g', 'Bakery', 1.50, 1, 10);

-- Insérer des fournisseurs
INSERT INTO suppliers (supplier_id, supplier_name, contact_email) VALUES
('SUP01', 'DairyCorp', 'orders@dairycorp.com'),
('SUP02', 'PastaWorld', 'supply@pastaworld.com'),
('SUP03', 'BeverageExperts', 'procurement@bevex.com');

-- Lier produits et fournisseurs
INSERT INTO product_supplier (sku_id, supplier_id, lead_time_days, is_primary) VALUES
('SKU001', 'SUP01', 2, TRUE),
('SKU002', 'SUP02', 3, TRUE),
('SKU003', 'SUP03', 1, TRUE),
('SKU004', 'SUP03', 1, TRUE),
('SKU005', 'SUP01', 1, TRUE);