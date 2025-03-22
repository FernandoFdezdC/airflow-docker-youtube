from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from src.extract import (youtube_channel_info_extract, youtube_videos_extract,
                    youtube_comments_extract, youtube_playlists_extract,
                    youtube_expand_videos_data, youtube_playlists_videos_extract)
from src.transform import (youtube_channel_info_transform, youtube_videos_transform, youtube_comments_transform,
                    youtube_playlists_transform, youtube_videos_to_playlists)
from src.utils.directories import DATA_DIR

default_args = {
    'owner': 'airflow',  # Propietario del DAG
    'depends_on_past': False,  # No depende de ejecuciones previas
    'email': ['ferfdezdelcerro@outlook.com'],  # Lista de correos
    'email_on_failure': False,  # Notificaciones por fallo
    'email_on_retry': True,  # Notificaciones en reintentos
    'retries': 0,  # Número de reintentos
    'retry_delay': timedelta(minutes=1),  # Intervalo entre reintentos
}

dag = DAG(
    dag_id='Kantian-Project-ETL',
    default_args=default_args,  # Argumentos por defecto
    start_date=datetime(year=2025, month=2, day=12),  # Fecha inicial
    schedule_interval='@daily',  # Intervalo de ejecución
    catchup=True,  # No ejecutar tareas atrasadas
    description="Get all 'Kantian Project' YouTube channel information, clean it and store it",
)

#---------------------------------------------------------
# EXTRACTION

channel_info_extract = PythonOperator(
    task_id='extract_channel_info',
    python_callable=youtube_channel_info_extract,
    templates_dict={
        'load_path': DATA_DIR+'logs/raw/channel_info_{{ds}}.json'
    },
    dag=dag
)

videos_extract = PythonOperator(
    task_id='extract_videos_info',
    python_callable=youtube_videos_extract,
    templates_dict={
        'channel_info': DATA_DIR+'logs/raw/channel_info_{{ds}}.json',
        'load_path': DATA_DIR+'logs/raw/videos_list_{{ds}}.json'
    },
    dag=dag
)

videos_expand_data = PythonOperator(
    task_id='youtube_expand_videos_data',
    python_callable=youtube_expand_videos_data,
    templates_dict={
        'input_path': DATA_DIR+'logs/raw/videos_list_{{ds}}.json',
        'load_path': DATA_DIR+'logs/raw/expanded_videos_data_{{ds}}.json'
    },
    dag=dag
)

comments_extract = PythonOperator(
    task_id='extract_videos_comments',
    python_callable=youtube_comments_extract,
    templates_dict={
        'videos_path': DATA_DIR+'logs/raw/videos_list_{{ds}}.json',
        'load_path': DATA_DIR+'logs/raw/videos_comments_{{ds}}.json'
    },
    dag=dag
)

playlists_extract = PythonOperator(
    task_id='extract_playlists',
    python_callable=youtube_playlists_extract,
    templates_dict={
        'channel_info': DATA_DIR+'logs/raw/channel_info_{{ds}}.json',
        'load_path': DATA_DIR+'logs/raw/channel_playlists_{{ds}}.json'
    },
    dag=dag
)

playlists_videos_extract = PythonOperator(
    task_id='extract_playlists_videos',
    python_callable=youtube_playlists_videos_extract,
    templates_dict={
        'input_path': DATA_DIR+'logs/raw/channel_playlists_{{ds}}.json',
        'load_path': DATA_DIR+'logs/raw/playlists_videos_{{ds}}.json'
    },
    dag=dag
)


notify_extraction_success = EmailOperator(
    task_id='notify_extraction_success',
    to='ferfdezdelcerro@outlook.com',
    subject='Data extraction successful',
    html_content='<h5>YouTube channel data extraction was successful. Date: {{ds}}.</h5>',
    dag=dag
)

#---------------------------------------------------------
# TRANSFORMATION

# Branching condition function:
ANALYSIS_CHANGE_DATE = dag.start_date
def _pick_analysis_type(**context):
    print(ANALYSIS_CHANGE_DATE)
    if context["execution_date"] == ANALYSIS_CHANGE_DATE:
        return "store_channel_metadata"
    elif context["execution_date"] > ANALYSIS_CHANGE_DATE:
        return "store_last_day_channel_data"
    
pick_analysis_type = BranchPythonOperator(
    task_id="pick_analysis_type",
    python_callable=_pick_analysis_type
)

store_channel_metadata = DummyOperator(
    task_id='store_channel_metadata',
    dag=dag
)

store_last_day_channel_data = DummyOperator(
    task_id='store_last_day_channel_data',
    dag=dag
)

channel_info_transform = PythonOperator(
    task_id='transform_channel_info',
    python_callable=youtube_channel_info_transform,
    templates_dict={
        'input_path': DATA_DIR+'logs/raw/channel_info_{{ds}}.json'
    },
    dag=dag
)

videos_transform = PythonOperator(
    task_id='transform_videos_info',
    python_callable=youtube_videos_transform,
    templates_dict={
        'input_path': DATA_DIR+'logs/raw/expanded_videos_data_{{ds}}.json'
    },
    dag=dag
)

comments_transform = PythonOperator(
    task_id='transform_videos_comments',
    python_callable=youtube_comments_transform,
    templates_dict={
        'input_path': DATA_DIR+'logs/raw/videos_comments_{{ds}}.json'
    },
    dag=dag
)

playlists_transform = PythonOperator(
    task_id='transform_playlists',
    python_callable=youtube_playlists_transform,
    templates_dict={
        'input_path': DATA_DIR+'logs/raw/channel_playlists_{{ds}}.json'
    },
    dag=dag
)

map_videos_to_playlists = PythonOperator(
    task_id='map_videos_to_playlists',
    python_callable=youtube_videos_to_playlists,
    templates_dict={
        'input_path': DATA_DIR+'logs/raw/playlists_videos_{{ds}}.json'
    },
    dag=dag
)

notify_transformation_success = EmailOperator(
    task_id='notify_transformation_success',
    to='ferfdezdelcerro@outlook.com',
    subject='Data extraction successful',
    html_content='<h5>YouTube channel data transformation was successful. Date: {{ds}}.</h5>',
    dag=dag
)

# analyze_entire_channel = PythonOperator(
#     task_id='analyze_entire_channel',
#     python_callable=analyze_entire_channel_with_chatGPT,
#     templates_dict={
#         'data_path': DATA_DIR+'logs/interim/channel_info_{{ds}}.json'
#     },
#     dag=dag
# )

# analyze_last_day_changes = PythonOperator(
#     task_id='compare_channel_info_with_yesterday',
#     python_callable=youtube_channel_info_compare_with_chatGPT,
#     templates_dict={
#         'data_path_yesterday': DATA_DIR+'logs/interim/channel_info_{{prev_ds}}.json',
#         'data_path_today': DATA_DIR+'logs/interim/channel_info_{{ds}}.json',
#     },
#     dag=dag
# )

channel_info_extract >> [videos_extract,  playlists_extract]
videos_extract >> [videos_expand_data, comments_extract]
playlists_extract >> playlists_videos_extract
[videos_expand_data, playlists_videos_extract, comments_extract] >> notify_extraction_success >> pick_analysis_type

pick_analysis_type >> [store_channel_metadata, store_last_day_channel_data]

store_channel_metadata >> [channel_info_transform, playlists_transform, videos_transform]
videos_transform >> comments_transform
[videos_transform, playlists_transform] >> map_videos_to_playlists
[channel_info_transform, map_videos_to_playlists, comments_transform] >> notify_transformation_success