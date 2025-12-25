from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataqualify.soda import yt_elt_data_qualify

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')
core_schema = 'core'
staging_schema = 'staging'

default_args = {
    "owner": "AL",
    "depends_on_part": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "tentamvay4@gmail.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz)
}

with DAG(
    dag_id = 'produce_json',
    default_args = default_args,
    description = "DAG to producer JSON file with raw data",
    schedule = '* 14 * * *',
    catchup = False
) as dag:
    
    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_video = extract_video_data(video_ids)
    save_json = save_to_json(extract_video)
    
    trigger_update_db = TriggerDagRunOperator(
        task_id = 'trigger_update_db',
        trigger_dag_id = 'update_db'
    )
    
    # Define dependencies
    playlist_id >> video_ids >> extract_video >> save_json >> trigger_update_db
    
with DAG(
    dag_id = 'update_db',
    default_args = default_args,
    description = "DAG to process JSON file and insert data into bioth staging and core schema",
    schedule = None,
    catchup = False
) as dag:
    
    # Define tasks
    update_staging = staging_table()
    update_core = core_table()
    
    trigger_data_quality = TriggerDagRunOperator(
        task_id = "trigger_quality_data",
        trigger_dag_id = 'data_quality'
    )
    # Define dependencies
    update_staging >> update_core >> trigger_data_quality
    
with DAG(
    dag_id = 'data_quality',
    default_args = default_args,
    description = "DAG to check the data qualify on both layers in the db",
    schedule = None,
    catchup = False
) as dag:
    
    # Define tasks
    soda_validate_staging = yt_elt_data_qualify(staging_schema)
    soda_validate_core = yt_elt_data_qualify(core_schema)
    
    # Define dependencies
    soda_validate_staging >> soda_validate_core