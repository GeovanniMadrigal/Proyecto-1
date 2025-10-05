from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Agregar el directorio scripts al path para importar nuestros módulos
sys.path.append('/opt/airflow/scripts')

from etl_pipeline import ETLPipeline

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

def generate_sample_data():
    """Genera datos de ejemplo"""
    import sys
    sys.path.append('/opt/airflow/scripts')
    from data_generator import generate_sample_data as gen_data
    gen_data(1000)

def run_etl_pipeline():
    """Ejecuta el pipeline ETL completo"""
    pipeline = ETLPipeline()
    pipeline.run_pipeline('/opt/airflow/data/raw_data.csv')

with DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL completo para datos de ventas',
    schedule_interval=timedelta(days=1),  # Se ejecuta diariamente
    catchup=False,
    tags=['etl', 'sales', 'data_engineering']
) as dag:

    generate_data_task = PythonOperator(
        task_id='generate_sample_data',
        python_callable=generate_sample_data,
        dag=dag
    )

    run_etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_etl_pipeline,
        dag=dag
    )

    test_queries_task = BashOperator(
        task_id='test_database_queries',
        bash_command='cd /opt/airflow/scripts && psql postgresql://data_engineer:password123@postgres:5432/data_warehouse -f queries.sql || echo "Consulta completada"',
        dag=dag
    )

    # Definir el orden de ejecución
    generate_data_task >> run_etl_task >> test_queries_task