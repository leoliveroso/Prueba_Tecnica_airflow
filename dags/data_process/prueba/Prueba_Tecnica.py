"""
SUMMARY.

--------
* DAG Name:
    Prueba_Tecnica
* Owner:
    Luis Oliveros
* Description:
    DAG para la prueba tecnica de Data Engineer
"""
from data_process.prueba.utils.general_utils import (download_csv_url, read_csv, clean_transform_data)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.configuration import conf
import logging
import shutil
import json
import glob
import os


# -------- dag definition ------------------------


PARAMS_DEFINITION = {
    'aws_conn_id': 'loliveros_s3',
    'folder_dir': {
        'root': os.path.join('/tmp', 'prueba_tecnica'),
        'data': os.path.join('/tmp', 'prueba_tecnica', 'data'),
        'data_raw': os.path.join('/tmp', 'prueba_tecnica', 'data', 'raw'),
        'data_stage': os.path.join('/tmp', 'prueba_tecnica', 'data', 'stage'),
    },
    'path_s3': 'archivos_varios/weather-collisions',
    's3_source_bucket': 'pruebas-loliveros',
    'website_download': 'https://opendata.arcgis.com/api/v3/datasets/34dd0a126aeb4dd5b99a397b5884e71f_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1',
    'sheet_id': '1nOEm6C883Oi5ZetmrXoI4ENseXb5NDYmu9hTk_jw8Pk',
    'default_args': {
        'owner': 'loliveros',
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
    },
    'num_workers': int(int(conf.get("core", "parallelism"))*0.2),
}

@dag(
    max_active_runs=1,
    schedule_interval='30 0 * * *',
    start_date=datetime(2023, 5, 1),
    description='Challenge Ingeniero de Datos Semi-Senior-2',
    default_args=PARAMS_DEFINITION['default_args'],
    tags=['Prueba', 'colombia'],
    catchup=False,
    concurrency=PARAMS_DEFINITION['num_workers'],
)
def prueba_tecnica():
    """DAG Definition."""
    log: logging.log = logging.getLogger("airflow")
    log.setLevel(logging.INFO)

    # Create directories
    @task(trigger_rule='all_success')
    def make_dir(dir_path):
        """Directory creation."""
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    # Delete container
    @task(trigger_rule='all_done')
    def delete_container(dir_path):
        """Delete Container."""
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

    delete_container_task = delete_container(PARAMS_DEFINITION['folder_dir']['root'])

    create_dirs = [make_dir(dir_path) for dir_path in PARAMS_DEFINITION['folder_dir'].values()]

    # Download files from url
    @task(trigger_rule='all_success')
    def download_files_url(json_params:json, folder_dir:str): 
        """Download csv from url."""
        log.info(f'json params: {json_params} and folder dir: {folder_dir}')
        filename_dest = json_params['filename_dest']
        url_csv = json_params['url_csv']
        download_csv_url(url_csv, folder_dir,
                         file_name=filename_dest,
                         format_file='csv')
        files_csv_raw = glob.glob(
            os.path.join(
                PARAMS_DEFINITION['folder_dir']['data_raw'],
                '*.csv')
        )

        log.info(f'Files downloaded: {str(files_csv_raw)}')

        log.info(f'num workers: {PARAMS_DEFINITION["num_workers"]}')
    
    list_json_args = [
        {
            'url_csv': "https://docs.google.com/spreadsheets/export?id={}&exportFormat=csv".format(
                PARAMS_DEFINITION['sheet_id']),
            'filename_dest': 'Traffic_Flow_Map_Volumes' + str(datetime.today().strftime('%Y%m%d'))
        },
        {
            'url_csv': PARAMS_DEFINITION['website_download'],
            'filename_dest': 'collisions_' + str(datetime.today().strftime('%Y%m%d'))
        }
    ]
    download_files_url_task = download_files_url.partial(
        folder_dir=PARAMS_DEFINITION['folder_dir']['data_raw']
        ).expand(
            json_params=list_json_args
        )

    @task(trigger_rule='all_success')
    def transform_data(list_file_path:list, file_path_dest:str, counter: int):
        """Transform data."""
        log.info(f'Counter: {counter} and dim list file path: {len(list_file_path)}')
        if counter < len(list_file_path):
            file_path = list_file_path[counter]

            log.info(f'File path: {file_path} and file path dest: {file_path_dest}')
            # read csv file
            data = read_csv(file_path)
            # clean and transform data
            filename_dest = file_path.split('/')[-1]
            dir_dest = os.path.join(file_path_dest, filename_dest)
            # apply clean and transform data
            if filename_dest.find('Traffic_Flow_Map_Volumes') != -1:
                json_groupby = {
                    'cols_groupby': ['STNAME', 'YEAR'],
                    'col_agg': 'AAWDT', 
                    'agg_func': 'sum'
                }
                subset_columns = ['AAWDT']
            elif filename_dest.find('collisions') != -1:
                json_groupby = {
                    'cols_groupby': ['WEATHER'],
                    'col_agg': 'WEATHER',
                    'agg_func': 'count'
                }
                subset_columns = ['WEATHER']

            clean_transform_data(df = data,subset_columns=subset_columns,
                                 json_groupby=json_groupby,path_dest = dir_dest)

            files_csv_raw = glob.glob(
                os.path.join(
                    PARAMS_DEFINITION['folder_dir']['data_stage'],
                    '*.csv')
            )

            log.info(f'Files Transformed: {str(files_csv_raw)}')

    # Get files
    files_csv_raw = glob.glob(
        os.path.join(
        PARAMS_DEFINITION['folder_dir']['data_raw'],
        '*.csv')
    )
    list_partitions = [i for i in range(PARAMS_DEFINITION['num_workers'])]

    transform_data_task = transform_data.partial(
        file_path_dest=PARAMS_DEFINITION['folder_dir']['data_stage'],
        list_file_path=files_csv_raw
        ).expand(counter=list_partitions)
    
    # upload to s3
    @task(trigger_rule='all_success')
    def upload_to_s3(path_file_orig:str):
        """Upload to s3."""
        list_files = glob.glob(os.path.join(
                PARAMS_DEFINITION['folder_dir']['data_stage'],
                '*.csv'))
        log.info(f'List files: {str(list_files)}')
        hook_s3 = S3Hook(aws_conn_id=PARAMS_DEFINITION['aws_conn_id'])
        for i, file_path in enumerate(list_files):
            path_pdf_s3 = os.path.join(
                PARAMS_DEFINITION['path_s3'], 
                'weather_per_collisions_' + str(datetime.today().strftime('%Y%m%d')) + f'_{str(i)}.csv'
                )
            hook_s3.load_file(
                filename=file_path,
                key=path_pdf_s3,
                bucket_name=PARAMS_DEFINITION['s3_source_bucket'],
                replace=True
            )
    
    upload_to_s3_task = upload_to_s3(PARAMS_DEFINITION['folder_dir']['data_stage'])
    

    delete_container_task >> create_dirs >> download_files_url_task
    download_files_url_task >> transform_data_task >> upload_to_s3_task

dag = prueba_tecnica()
