from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# NOTE: Replace callbacks with your real callbacks if available (slack, pagerduty...)
def noop_callback(context):
    print("noop callback")

with DAG(
    dag_id="cars_s3_genai_pipeline_v1",
    params={
        'S3_BUCKET': 'cars-project1',
        'RAW_PREFIX': 'raw',
        'PROCESSED_PREFIX': 'processed',
        'REPORT_PREFIX': 'report',
        'SCRIPT_PATH': '/opt/airflow/scripts'  # change if you place scripts elsewhere
    },
    max_active_tasks=5,
    max_active_runs=1,
    schedule=None,
    start_date=datetime(2023, 11, 3),
    default_args={
        'retry_delay': timedelta(minutes=3),
        'owner': 'dw_etl',
        'retries': 0,
        'run_as_user': 'dw_etl'
    },
    catchup=False,
    tags=['dw_etl', 'cars'],
    on_failure_callback=[noop_callback],
    sla_miss_callback=[noop_callback],
) as dag:

    start = EmptyOperator(task_id='start')

    # Task 1: Transform raw -> processed
    transform = BashOperator(
        task_id='transform_raw_to_processed',
        bash_command=(
            "source ~/.bash_profile && "
            "python {{ params['SCRIPT_PATH'] }}/transform_data.py "
            "{{ params['S3_BUCKET'] }} {{ params['RAW_PREFIX'] }} {{ params['PROCESSED_PREFIX'] }}"
        ),
        sla=timedelta(seconds=300),
    )

    # Task 2: GenAI - read processed -> report
    genai = BashOperator(
        task_id='genai_processed_to_report',
        bash_command=(
            "source ~/.bash_profile && "
            "python {{ params['SCRIPT_PATH'] }}/genai_report.py "
            "{{ params['S3_BUCKET'] }} {{ params['PROCESSED_PREFIX'] }} {{ params['REPORT_PREFIX'] }}"
        ),
        sla=timedelta(seconds=300),
    )

    end = EmptyOperator(task_id='end', trigger_rule='none_failed')

    start >> transform >> genai >> end
