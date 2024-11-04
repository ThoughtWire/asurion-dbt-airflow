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


def create_asurion_dag(dag_id, include_tags, profile_config, schedule_interval:str = "@daily"):
    @dag(
        schedule_interval=schedule_interval,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id=dag_id,
        default_args={"retries": 2, "execution_timeout": timedelta(hours=3)},
        tags=['asurion']
    )
    def asurion_dag():
        task = DbtTaskGroup(
            group_id=dag_id,
            dbt_project_config=DbtProjectConfig(
                project_path=Path('/opt/airflow/dbt/asurion_piplines/'),
                manifest_path=Path('/opt/airflow/dbt/asurion_piplines/target/manifest.json'),
            ),
            dbt_airflow_config=DbtAirflowConfig(
                execution_operator=ExecutionOperator.BASH,
                include_tags=include_tags,
            ),
            dbt_profile_config=profile_config,
        )
    
    return asurion_dag()


# Define all DAGs
asurion_alerton = create_asurion_dag("asurion_alerton", ['alerton', 'zones'], profile_config_prod, "0 0 * * *")
asurion_eaton = create_asurion_dag("asurion_eaton", ['eaton', 'zones'], profile_config_prod, "0 1 * * *")
asurion_lutron = create_asurion_dag("asurion_lutron", ['lutron', 'zones'], profile_config_prod, "0 0 * * *")
asurion_workorder = create_asurion_dag("asurion_workorder", ['myfacilities'], profile_config_prod, "0 0 * * *")
asurion_splan = create_asurion_dag("asurion_splan", ['splan'], profile_config_prod, "0 0 * * *")
asurion_chargepoint = create_asurion_dag("asurion_chargepoint", ['chargepoint'], profile_config_prod, "0 0 * * *")
asurion_alerts = create_asurion_dag("asurion_alerts", ['alerts', 'zones'], profile_config_uat, "0 0 * * *")
asurion_msgraph = create_asurion_dag("asurion_msgraph", ['msgraph'], profile_config_uat, "0 0 * * *")
asurion_vergesense = create_asurion_dag("asurion_vergesense", ['vergesense', 'zones'], profile_config_prod, "0 2 * * *")
asurion_lockers = create_asurion_dag("asurion_lockers", ['deliveries', 'luxerone'], profile_config_prod, "0 0 * * *")
asurion_indect = create_asurion_dag("asurion_indect", ['indect'], profile_config_prod, "0 0 * * *")
asurion_lenel = create_asurion_dag("asurion_lenel", ['lenel_global', 'lenel_gulch', 'desk_assignment'], profile_config_prod, "0 4 * * *")
