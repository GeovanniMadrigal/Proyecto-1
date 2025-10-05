import pandas as pd
import random
from datetime import datetime, timedelta
import os

def generate_sample_data(num_records=1000):
    """Genera datos de ventas de ejemplo"""
    
    products = [
        {"name": "Laptop", "category": "Electronics", "price_range": (500, 2000)},
        {"name": "Smartphone", "category": "Electronics", "price_range": (300, 1000)},
        {"name": "Headphones", "category": "Electronics", "price_range": (50, 300)},
        {"name": "Book", "category": "Education", "price_range": (10, 50)},
        {"name": "Notebook", "category": "Education", "price_range": (5, 20)},
        {"name": "T-Shirt", "category": "Clothing", "price_range": (15, 40)},
        {"name": "Jeans", "category": "Clothing", "price_range": (30, 80)},
        {"name": "Coffee", "category": "Food", "price_range": (2, 10)},
        {"name": "Sandwich", "category": "Food", "price_range": (5, 15)}
    ]
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        product = random.choice(products)
        price = round(random.uniform(*product["price_range"]), 2)
        quantity = random.randint(1, 5)
        total_amount = price * quantity
        sale_date = start_date + timedelta(days=random.randint(0, 364))
        
        record = {
            "transaction_id": f"TXN{10000 + i}",
            "product_name": product["name"],
            "category": product["category"],
            "price": price,
            "quantity": quantity,
            "sale_date": sale_date.strftime("%Y-%m-%d"),
            "customer_id": f"CUST{random.randint(1000, 9999)}",
            "total_amount": total_amount
        }
        data.append(record)
    
    df = pd.DataFrame(data)
    
    # Crear directorio si no existe
    os.makedirs('../data', exist_ok=True)
    
    # Guardar datos
    df.to_csv('../data/raw_data.csv', index=False)
    print(f"Datos generados: {num_records} registros guardados en data/raw_data.csv")

if __name__ == "__main__":
    generate_sample_data(1500)