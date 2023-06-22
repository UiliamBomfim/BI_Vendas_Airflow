from project import *
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta




default_args = {
    'start_date': datetime(2023, 6, 15)
}

with DAG(dag_id='vendas',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['currency']
         ) as dag:

    with TaskGroup('extract_tasks') as extract_tasks:
    # busca dados arquivo data

        t1 = PythonOperator(
            task_id='extract_vendas',
            python_callable=extract_vendas,
            do_xcom_push=False,
            dag=dag,
        )

        # busca dados arquivo test
        t2 = PythonOperator(
            task_id='extract_funcionario',
            python_callable=extract_func,
            do_xcom_push=False,
            dag=dag,
        )

        t3 = PythonOperator(
            task_id='extract_categoria',
            python_callable=extract_category,
            do_xcom_push=False,
            dag=dag,
        )        
        t4 = PythonOperator(
            task_id='extract_data',
            python_callable=extract_data,
            do_xcom_push=False,
            dag=dag,
        )
    with TaskGroup('truncate_tasks') as truncate_tasks:
        t5 = PostgresOperator(
        task_id='TRUNCATE_CATEGORIA',
        postgres_conn_id='postgres_default',
        sql=r"""
            TRUNCATE TABLE DM_Categoria RESTART IDENTITY cascade;
        """,
        dag=dag,
        )

        t6 = PostgresOperator(
        task_id='TRUNCATE_FUNCIONARIOS',
        postgres_conn_id='postgres_default',
        sql=r"""
            TRUNCATE TABLE DM_Funcionarios RESTART IDENTITY CASCADE;
        """,
        dag=dag,
        )
       
        t7 = PostgresOperator(
        task_id='TRUNCATE_DATA',
        postgres_conn_id='postgres_default',
        sql=r"""
            TRUNCATE TABLE DM_Data RESTART IDENTITY CASCADE;
        """,
        dag=dag,
        )

        t8 = PostgresOperator(
        task_id='TRUNCATE_VENDA',
        postgres_conn_id='postgres_default',
        sql=r"""
            TRUNCATE TABLE FT_Vendas RESTART IDENTITY;
        """,
        dag=dag,
        )


    # aplicar transformações na tabela
    t9 = PythonOperator(
        task_id='load_tables_dm',
        python_callable=load_tables_dm,
        do_xcom_push=False,
        dag=dag,
    )

    # aplicar transformações na tabela
    t10 = PythonOperator(
        task_id='load_table_fato',
        python_callable=load_table_fato,
        do_xcom_push=False,
        dag=dag,
    )


    # dependências entre as tarefas
    extract_tasks >> truncate_tasks >> t9 >> t10

