-- Consultas de ejemplo para verificar los datos

-- 1. Ventas totales por categoría
SELECT 
    category,
    ROUND(SUM(total_amount), 2) as total_ventas,
    SUM(quantity) as total_unidades
FROM processed.sales 
GROUP BY category 
ORDER BY total_ventas DESC;

-- 2. Promedio de venta por producto
SELECT 
    product_name,
    category,
    ROUND(AVG(price), 2) as precio_promedio,
    ROUND(AVG(total_amount), 2) as venta_promedio
FROM processed.sales 
GROUP BY product_name, category 
ORDER BY venta_promedio DESC;

-- 3. Resumen de ventas
SELECT 
    category,
    total_sales,
    total_quantity,
    ROUND(avg_price, 2) as avg_price
FROM processed.sales_summary 
ORDER BY total_sales DESC;