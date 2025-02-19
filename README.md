from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine

# Configuração do banco de dados
DB_CONN = "postgresql+psycopg2://user:password@localhost:5432/meudb"
engine = create_engine(DB_CONN)

# Função de extração de dados da API
def extract_api():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    df.to_csv("/tmp/api_data.csv", index=False)

# Função de extração do PostgreSQL
def extract_db():
    query = "SELECT id, name, email FROM users"
    df = pd.read_sql(query, engine)
    df.to_csv("/tmp/db_data.csv", index=False)

# Transformação dos dados
def transform_data():
    api_df = pd.read_csv("/tmp/api_data.csv")
    db_df = pd.read_csv("/tmp/db_data.csv")
    
    # Limpeza e junção dos dados
    api_df = api_df[['id', 'title', 'body']]
    db_df = db_df.rename(columns={'id': 'user_id'})
    merged_df = api_df.merge(db_df, left_on='id', right_on='user_id', how='left')
    merged_df = merged_df.dropna()
    merged_df.to_csv("/tmp/transformed_data.csv", index=False)

# Carregamento dos dados transformados
def load_data():
    df = pd.read_csv("/tmp/transformed_data.csv")
    df.to_sql("final_table", engine, if_exists="replace", index=False)

# Definição da DAG no Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_extract_api = PythonOperator(
    task_id="extract_api",
    python_callable=extract_api,
    dag=dag
)

task_extract_db = PythonOperator(
    task_id="extract_db",
    python_callable=extract_db,
    dag=dag
)

task_transform = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag
)

task_load = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag
)

# Definição da ordem das tarefas
task_extract_api >> task_extract_db >> task_transform >> task_load
