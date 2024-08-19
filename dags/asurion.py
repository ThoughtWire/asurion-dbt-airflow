from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag

from datetime import datetime 
from datetime import timedelta
from pathlib import Path


from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from dbt_airflow.core.config import DbtAirflowConfig
from dbt_airflow.core.config import DbtProfileConfig
from dbt_airflow.core.config import DbtProjectConfig
from dbt_airflow.core.task_group import DbtTaskGroup
from dbt_airflow.operators.execution import ExecutionOperator

import os
from datetime import datetime

profile_config_prod = DbtProfileConfig(
               profiles_path=Path('/opt/airflow/dbt/asurion_piplines'),
            target='prod',
    )


profile_config_uat = DbtProfileConfig(
               profiles_path=Path('/opt/airflow/dbt/asurion_piplines'),
            target='uat',
    )



@dag( schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="asurion_alerton",
    default_args={"retries": 2}, tags=['asurion'])

def asurion_alerton():

    task = DbtTaskGroup(
    # dbt/cosmos-specific parameters
        group_id='asurion_alerton',
        dbt_project_config = DbtProjectConfig(
            project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
            manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
        ),
        dbt_airflow_config = DbtAirflowConfig(
            execution_operator=ExecutionOperator.BASH,
                    include_tags = ['finance'],
        ),
        dbt_profile_config = profile_config_prod,


)

    
@dag(schedule_interval="@daily", 
     start_date=datetime(2023, 1, 1), 
     catchup=False, dag_id="asurion_eaton", 
     default_args={"retries": 2}, 
     tags=['asurion'])

def asurion_eaton():
    task = DbtTaskGroup(
        group_id='asurion_eaton',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
            manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
        ),

        dbt_airflow_config=DbtAirflowConfig(
            execution_operator=ExecutionOperator.BASH,
                    include_tags=['eaton'],
        ),
        dbt_profile_config=profile_config_prod,

    )

@dag(schedule_interval="@daily", 
     start_date=datetime(2023, 1, 1), 
     catchup=False, dag_id="asurion_lutron", 
     default_args={"retries": 2}, 
     tags=['asurion'])

def asurion_lutron():
    task = DbtTaskGroup(
        group_id='asurion_lutron',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
            manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
        ),
        dbt_airflow_config=DbtAirflowConfig(
            execution_operator=ExecutionOperator.BASH,
                 include_tags=['lutron'],
        ),
        dbt_profile_config=profile_config_prod,
   
    )

@dag(schedule_interval="@daily", 
     start_date=datetime(2023, 1, 1), 
     catchup=False, dag_id="asurion_workorder", 
     default_args={"retries": 2}, 
     tags=['asurion'])

def asurion_workorder():
    task = DbtTaskGroup(
        group_id='asurion_workorder',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
            manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
        ),
        dbt_airflow_config=DbtAirflowConfig(
            execution_operator=ExecutionOperator.BASH,
                 include_tags=['myfacilities'],
        ),
        dbt_profile_config=profile_config_prod,
   
    )

@dag(schedule_interval="@daily", 
     start_date=datetime(2023, 1, 1), 
     catchup=False, dag_id="asurion_splan", 
     default_args={"retries": 2}, 
     tags=['asurion'])

def asurion_splan():
    task = DbtTaskGroup(
        group_id='asurion_splan',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
            manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
        ),
        dbt_airflow_config=DbtAirflowConfig(
            execution_operator=ExecutionOperator.BASH,
                 include_tags=['splan'],
        ),
        dbt_profile_config=profile_config_prod,
   
    )

@dag(schedule_interval="@daily", 
     start_date=datetime(2023, 1, 1), 
     catchup=False, 
     dag_id="asurion_chargepoint", 
     default_args={"retries": 2}, 
     tags=['asurion'])

def asurion_chargepoint():
    task = DbtTaskGroup(
        group_id='asurion_chargepoint',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
            manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
        ),
        dbt_airflow_config = DbtAirflowConfig(
            execution_operator = ExecutionOperator.BASH,
                    include_tags=['chargepoint'],
        ),
        dbt_profile_config = profile_config_prod,

    )

@dag(schedule_interval="@daily", 
     start_date=datetime(2023, 1, 1),
       catchup=False, 
       dag_id="asurion_alerts",
         default_args={"retries": 2},
           tags=['asurion'])

def asurion_alerts():
    task = DbtTaskGroup(
        group_id='asurion_alerts',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
            manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
        ),
        dbt_airflow_config = DbtAirflowConfig(
            execution_operator=ExecutionOperator.BASH,
                    include_tags=['alerts'],
        ),
        dbt_profile_config=profile_config_uat,

    )

@dag(schedule_interval="@daily", 
     start_date=datetime(2023, 1, 1), 
     catchup=False, 
     dag_id="asurion_msgraph", 
     default_args={"retries": 2},
       tags=['asurion'])

def asurion_msgraph():
    task = DbtTaskGroup(
        group_id='asurion_msgraph',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
            manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
        ),
        dbt_airflow_config = DbtAirflowConfig(
            execution_operator=ExecutionOperator.BASH,
                include_tags=['msgraph'],
        ),
        dbt_profile_config=profile_config_uat,
   
    )

asurion_alerton()
asurion_chargepoint()
asurion_alerts()
asurion_lutron()
asurion_msgraph()
asurion_eaton()