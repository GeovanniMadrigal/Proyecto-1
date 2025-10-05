import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ETLPipeline:
    def __init__(self):
        self.db_connection = "postgresql://data_engineer:password123@postgres:5432/data_warehouse"
        self.engine = create_engine(self.db_connection)
    
    def extract(self, file_path):
        """Extrae datos del archivo CSV"""
        logging.info("Iniciando extracción de datos...")
        try:
            df = pd.read_csv(file_path)
            logging.info(f"Datos extraídos: {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f"Error en extracción: {e}")
            raise
    
    def transform(self, df):
        """Transforma los datos"""
        logging.info("Iniciando transformación de datos...")
        
        # Limpieza de datos
        df_clean = df.drop_duplicates()
        
        # Validar que no haya precios negativos
        df_clean = df_clean[df_clean['price'] > 0]
        
        # Asegurar que las fechas estén en formato correcto
        df_clean['sale_date'] = pd.to_datetime(df_clean['sale_date'])
        
        # Calcular métricas adicionales
        df_clean['month'] = df_clean['sale_date'].dt.month
        df_clean['year'] = df_clean['sale_date'].dt.year
        
        logging.info(f"Datos transformados: {len(df_clean)} registros válidos")
        return df_clean
    
    def load(self, df, table_name):
        """Carga datos a la base de datos"""
        logging.info(f"Iniciando carga de datos a la tabla {table_name}...")
        try:
            df.to_sql(
                table_name, 
                self.engine, 
                schema='processed', 
                if_exists='replace', 
                index=False,
                method='multi'
            )
            logging.info(f"Datos cargados exitosamente: {len(df)} registros")
        except Exception as e:
            logging.error(f"Error en carga: {e}")
            raise
    
    def create_summary(self):
        """Crea tabla de resumen"""
        logging.info("Creando resumen de ventas...")
        try:
            with self.engine.connect() as conn:
                # Resumen por categoría
                query = """
                INSERT INTO processed.sales_summary (category, total_sales, total_quantity, avg_price, summary_date)
                SELECT 
                    category,
                    SUM(total_amount) as total_sales,
                    SUM(quantity) as total_quantity,
                    AVG(price) as avg_price,
                    CURRENT_DATE as summary_date
                FROM processed.sales 
                GROUP BY category;
                """
                conn.execute(query)
                logging.info("Resumen de ventas creado exitosamente")
        except Exception as e:
            logging.error(f"Error creando resumen: {e}")
            raise
    
    def run_pipeline(self, file_path):
        """Ejecuta el pipeline ETL completo"""
        logging.info("Iniciando pipeline ETL...")
        
        # ETL
        df_raw = self.extract(file_path)
        df_transformed = self.transform(df_raw)
        self.load(df_transformed, 'sales')
        
        # Crear resumen
        self.create_summary()
        
        logging.info("Pipeline ETL completado exitosamente")

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run_pipeline('/opt/airflow/data/raw_data.csv')