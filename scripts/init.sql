-- Initialisation de la base de données procurement
CREATE TABLE IF NOT EXISTS produits (
    sku VARCHAR(50) PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,
    fournisseur VARCHAR(100) NOT NULL,
    pack_size INTEGER NOT NULL DEFAULT 1,
    moq INTEGER NOT NULL DEFAULT 1,
    lead_time_days INTEGER NOT NULL DEFAULT 1,
    stock_securite INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fournisseurs (
    id VARCHAR(50) PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    telephone VARCHAR(20)
);

-- Nettoyer
DELETE FROM produits;
DELETE FROM fournisseurs;

-- Données de test
INSERT INTO produits (sku, nom, fournisseur, pack_size, moq, lead_time_days, stock_securite) VALUES
('PATE500', 'Pâtes 500g', 'CARREFOUR', 10, 50, 2, 20),
('RIZ1KG', 'Riz 1kg', 'CASINO', 5, 30, 1, 15),
('LAIT1L', 'Lait 1L', 'LIDL', 20, 100, 1, 30),
('CAFE250', 'Café 250g', 'CARREFOUR', 4, 20, 3, 10),
('SUCRE1KG', 'Sucre 1kg', 'CASINO', 8, 40, 2, 25);

INSERT INTO fournisseurs (id, nom, email, telephone) VALUES
('CARREFOUR', 'Carrefour', 'commandes@carrefour.com', '01 23 45 67 89'),
('CASINO', 'Casino', 'achats@casino.fr', '02 34 56 78 90'),
('LIDL', 'Lidl', 'fournisseurs@lidl.fr', '03 45 67 89 01');

-- Vérification
SELECT '=== DONNÉES INITIALISÉES ===' as message;
SELECT 'Produits:' as table, COUNT(*) as count FROM produits
UNION ALL
SELECT 'Fournisseurs:', COUNT(*) FROM fournisseurs;
