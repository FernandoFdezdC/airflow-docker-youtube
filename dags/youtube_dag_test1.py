# from datetime import timedelta, datetime
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.email import EmailOperator
# from src.extract import (youtube_channel_info_extract, youtube_videos_extract, youtube_comments_extract,
#                     youtube_playlists_extract)
# from src.transform import (youtube_channel_info_transform, youtube_videos_transform, youtube_comments_transform,
#                     youtube_playlists_transform)
# from src.utils.directories import DATA_DIR

# default_args = {
#     'owner': 'airflow',  # Propietario del DAG
#     'depends_on_past': False,  # No depende de ejecuciones previas
#     'email': ['ferfdezdelcerr@outlook.com'],  # Lista de correos
#     'email_on_failure': True,  # Notificaciones por fallo
#     'email_on_retry': True,  # Notificaciones en reintentos
#     'retries': 1,  # Número de reintentos
#     'retry_delay': timedelta(minutes=1),  # Intervalo entre reintentos
# }

# dag = DAG(
#     dag_id='Kantian-Project-ETL_test',
#     default_args=default_args,  # Argumentos por defecto
#     start_date=datetime(year=2024, month=3, day=30),  # Fecha inicial
#     schedule_interval='@daily',  # Intervalo de ejecución
#     catchup=False,  # No ejecutar tareas atrasadas
#     description='Get all Kantian Project channel information and store it',
# )


# channel_info_extract = PythonOperator(
#     task_id='extract_channel_info',
#     python_callable=youtube_channel_info_extract,
#     templates_dict={
#         'load_path': DATA_DIR+'logs/channel_info_{{ds}}.json'
#     },
#     dag=dag
# )

# channel_info_transform = PythonOperator(
#     task_id='transform_channel_info',
#     python_callable=youtube_channel_info_transform,
#     templates_dict={
#         'input_path': DATA_DIR+'logs/channel_info_{{ds}}.json',
#         'output_path': DATA_DIR+'channel_info.json'              # Overwrites
#     },
#     dag=dag
# )

# videos_extract = PythonOperator(
#     task_id='extract_videos_info',
#     python_callable=youtube_videos_extract,
#     templates_dict={
#         'channel_info': DATA_DIR+'channel_info.json',
#         'load_path': DATA_DIR+'logs/videos_list_{{ds}}.json'
#     },
#     dag=dag
# )

# videos_transform = PythonOperator(
#     task_id='transform_videos_info',
#     python_callable=youtube_videos_transform,
#     templates_dict={
#         'input_path': DATA_DIR+'logs/videos_list_{{ds}}.json',
#         'output_path': DATA_DIR+"/videos.json"              # Overwrites
#     },
#     dag=dag
# )

# comments_extract = PythonOperator(
#     task_id='extract_video_comments',
#     python_callable=youtube_comments_extract,
#     templates_dict={
#         'videos_path': DATA_DIR+'/videos.json',
#         'load_path': DATA_DIR+'logs/videos_comments_{{ds}}.json'
#     },
#     dag=dag
# )

# comments_transform = PythonOperator(
#     task_id='transform_video_comments',
#     python_callable=youtube_comments_transform,
#     templates_dict={
#         'input_path': DATA_DIR+'logs/videos_comments_{{ds}}.json',
#         'output_path': DATA_DIR+"/comments.json"              # Overwrites
#     },
#     dag=dag
# )

# playlists_extract = PythonOperator(
#     task_id='extract_playlists',
#     python_callable=youtube_playlists_extract,
#     templates_dict={
#         'channel_info': DATA_DIR+'channel_info.json',
#         'load_path': DATA_DIR+'logs/channel_playlists_{{ds}}.json'
#     },
#     dag=dag
# )

# playlists_transform = PythonOperator(
#     task_id='transform_playlists',
#     python_callable=youtube_playlists_transform,
#     templates_dict={
#         'input_path': DATA_DIR+'logs/channel_playlists_{{ds}}.json',
#         'output_path': DATA_DIR+"/playlists.json"              # Overwrites
#     },
#     dag=dag
# )

# notify_success = EmailOperator(
#     task_id='send_email',
#     to='ferfdezdelcerro@outlook.com',
#     subject='Data extraction successful',
#     html_content='<h3>YouTube channel data extraction was successful.</h3>',
#     dag=dag
# )

# channel_info_extract >> channel_info_transform >> [videos_extract,  playlists_extract]
# videos_extract >> videos_transform
# playlists_extract >> playlists_transform
# videos_transform >> comments_extract >> comments_transform
# [playlists_transform, comments_transform] >> notify_success