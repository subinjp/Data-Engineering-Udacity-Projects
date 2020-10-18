from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator,
                               DataQualityOperator,CreateRedshiftTablesOperator)                             
from helpers import SqlQueries
from sparkify_project_dimension_subdag import load_dimension_subdag
from airflow.operators.subdag_operator import SubDagOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
start_date = datetime.utcnow()

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2020, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables = CreateRedshiftTablesOperator(
    task_id = 'create_tables_in_redshift',
    redshift_conn_id = 'redshift',
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON",
    execution_date=start_date 
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name='sparkify_project_dag',
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.user_table_insert,
        delete_load = True,
        table_name = "users",
    ),
    task_id="Load_user_dim_table",
    dag=dag,
)


load_song_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name='sparkify_project_dag',
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.song_table_insert,
        delete_load = True,
        table_name = "songs",
    ),
    task_id="Load_song_dim_table",
    dag=dag,
)


load_artist_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name='sparkify_project_dag',
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.artist_table_insert,
        delete_load = True,
        table_name = "artists",
    ),
    task_id="Load_artist_dim_table",
    dag=dag,
)


load_time_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name='sparkify_project_dag',
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.time_table_insert,
        delete_load = True,
        table_name = "time",
    ),
    task_id="Load_time_dim_table",
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    tables=["songplay", "users", "song", "artist", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Creating Task Dependencies

start_operator >> create_tables >> [stage_songs_to_redshift,                                                                   stage_events_to_redshift]
[stage_songs_to_redshift,stage_events_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,                         load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator