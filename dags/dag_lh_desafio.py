from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# Diretórios do projeto e profiles dbt
DBT_PROJECT_DIR = "/home/marcelomdm/Desafio_airflow/airflow-desafio-lh/desafio-lighthouse"
DBT_PROFILE_DIR = "/home/marcelomdm/.dbt"

# Criação da DAG
with DAG(
    dag_id="dag_lh_desafio",
    default_args={
        "retries": 1,
        "execution_timeout": timedelta(minutes=30),  # Timeout de execução
    },
    start_date=datetime(2025, 1, 26),
    schedule_interval="0 6 * * *",
    catchup=False,
) as dag:

    # Comando para rodar o dbt run com --full-refresh
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/marcelomdm/Desafio_airflow/airflow-desafio-lh/venv/bin/dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILE_DIR} --full-refresh",
    )

    # Comando para rodar o dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/marcelomdm/Desafio_airflow/airflow-desafio-lh/venv/bin/dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILE_DIR}",
    )

    # Definir a sequência de execução
    dbt_run >> dbt_test
