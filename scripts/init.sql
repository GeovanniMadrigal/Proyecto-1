-- Crear esquema para datos procesados
CREATE SCHEMA IF NOT EXISTS processed;

-- Tabla de ventas
CREATE TABLE IF NOT EXISTS processed.sales (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    quantity INTEGER,
    sale_date DATE,
    customer_id VARCHAR(50),
    total_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de resumen por categoría
CREATE TABLE IF NOT EXISTS processed.sales_summary (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    total_sales DECIMAL(15,2),
    total_quantity INTEGER,
    avg_price DECIMAL(10,2),
    summary_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);