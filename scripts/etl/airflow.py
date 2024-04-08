from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.etl.etl import workflow_init, extract_data, transform_data, load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG('epa_data_pipeline', default_args=default_args, schedule_interval='@once')


def main():
    init_task = PythonOperator(
        task_id='workflow_init',
        python_callable=workflow_init,
        provide_context=True,
        dag=dag
    )

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={'max_date': "{{ ti.xcom_pull(task_ids='workflow_init', key='max_date') }}"},
        dag=dag
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'path': "{{ ti.xcom_pull(task_ids='extract_data', key='return_value')[1] }}",
                   'max_date': "{{ ti.xcom_pull(task_ids='workflow_init', key='max_date') }}"},
        dag=dag
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={
            'aqi_df': "{{ ti.xcom_pull(task_ids='transform_data', key='return_value')[0] }}",
            'element_df': "{{ ti.xcom_pull(task_ids='transform_data', key='return_value')[1] }}",
            'element': "{{ ti.xcom_pull(task_ids='extract_data', key='return_value')[0] }}"
        },
        dag=dag
    )

    init_task >> extract_data_task >> transform_data_task >> load_data_task


if __name__ == "__main__":
    main()
