from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import time

default_args = {
    'owner': 'roma',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def run_etl_migration(table_name):
    """Запускает ETL миграцию через API"""
    print(f"🚀 Starting ETL migration for {table_name}")
    
    try:
        # Запускаем миграцию через API
        response = requests.post(
            "http://etl-service:8000/migrate/" + table_name,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ ETL for {table_name} completed: {result}")
            return result
        else:
            error_msg = f"API error: {response.status_code} - {response.text}"
            print(f"❌ {error_msg}")
            raise Exception(error_msg)
            
    except Exception as e:
        print(f"❌ ETL failed for {table_name}: {str(e)}")
        raise

# Создаём DAG для ручной миграции
manual_etl_dag = DAG(
    'manual_etl_migration',
    default_args=default_args,
    description='Manual ETL migration triggered on demand',
    schedule_interval=None,  # Только ручной запуск
    catchup=False,
    tags=['etl', 'manual'],
)

with manual_etl_dag:
    start = DummyOperator(task_id='start')
    
    migrate_customers = PythonOperator(
        task_id='migrate_customers',
        python_callable=run_etl_migration,
        op_kwargs={'table_name': 'customers'},
    )
    
    migrate_orders = PythonOperator(
        task_id='migrate_orders',
        python_callable=run_etl_migration,
        op_kwargs={'table_name': 'orders'},
    )
    
    end = DummyOperator(task_id='end')
    
    # Определяем зависимости
    start >> [migrate_customers, migrate_orders] >> end