import unittest
import pandas as pd
import sys
import os

# Agregar scripts al path
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

from etl_pipeline import ETLPipeline

class TestETLPipeline(unittest.TestCase):
    
    def setUp(self):
        """Configuración inicial para tests"""
        self.pipeline = ETLPipeline()
        
        # Datos de prueba
        self.sample_data = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002', 'TXN003'],
            'product_name': ['Laptop', 'Mouse', 'Keyboard'],
            'category': ['Electronics', 'Electronics', 'Electronics'],
            'price': [1000.0, 25.0, 75.0],
            'quantity': [1, 2, 1],
            'sale_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'total_amount': [1000.0, 50.0, 75.0]
        })
    
    def test_data_cleaning(self):
        """Test de limpieza de datos"""
        # Agregar datos duplicados
        data_with_duplicates = pd.concat([self.sample_data, self.sample_data])
        
        # Transformar debería remover duplicados
        cleaned_data = self.pipeline.transform(data_with_duplicates)
        
        self.assertEqual(len(cleaned_data), len(self.sample_data))
    
    def test_price_validation(self):
        """Test de validación de precios"""
        # Agregar precio negativo
        invalid_data = self.sample_data.copy()
        invalid_data.loc[0, 'price'] = -100
        
        cleaned_data = self.pipeline.transform(invalid_data)
        
        # Verificar que se removió el precio negativo
        self.assertTrue((cleaned_data['price'] > 0).all())
    
    def test_date_format(self):
        """Test de formateo de fechas"""
        transformed_data = self.pipeline.transform(self.sample_data)
        
        # Verificar que las fechas son datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(transformed_data['sale_date']))

if __name__ == '__main__':
    unittest.main()