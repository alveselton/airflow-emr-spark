from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

#Configuracao
BUCKET_NAME = "my-codehead"
local_script = "/home/elton/airflow/dags/EMR_CLUSTER/scripts/spark/emr_spark_step_hello_world.py"
s3_script = "scripts/spark/emr_spark_step_hello_world.py"

SPARK_STEPS = [
    {
        "Name": "emr_spark_step_hello_world",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://my-codehead/scripts/spark/emr_spark_step_hello_world.py",
            ],
        },        
    }
]

JOB_FLOW_OVERRIDES_LARGE = {
    "Name": "Create EMR",
    "ReleaseLabel": "emr-6.3.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

JOB_FLOW_OVERRIDES_SMALL = {
    "Name": "Movie review classifier",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "LogUri": "s3://my-codehead/my-emr-log-bucket/",
    
    # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }, 
                },

                # Utilizar essa configuracao para que aplicativos externos tenham acesso 
                # {
                #    "Classification": "hive-site",
                #    "Properties": {
                #        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                #    },
                # },
                # {
                #    "Classification": "spark-hive-site",
                #    "Properties": {
                #        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                #    },
                # }                

            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "Applications":[
        {"Name": "Spark"},
        # {"Name": "Hive"},
        # {"Name": "Hadoop"}
    ],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2020, 11, 16),
    "email": ["alveselton@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "submit_cluster_to_AWS",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    tags=['Pyspark']
) as dag:

    last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch

    # Copiar o script do spark para o S3
    def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
        s3 = S3Hook()
        s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


    script_to_s3 = PythonOperator(
        task_id="Script_to_s3",
        python_callable=_local_to_s3,
        op_kwargs={"filename": local_script, "key":s3_script}
    )
    # Fim Copiar o script do spark para o S3


    start_data_pipeline = DummyOperator(task_id="start_data_pipeline_submit_cluster", dag=dag)

    # Cria o Cluster EMR
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides = JOB_FLOW_OVERRIDES_SMALL,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default"
    )

    cluster_job_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}"

    # Adicionas as etapas para o EMR cluster
    step_adder = EmrAddStepsOperator(
        task_id ="add_steps",
        #job_flow_id = create_emr_cluster.output,
        job_flow_id=cluster_job_id,
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={ 
            # these params are used to fill the paramterized values in SPARK_STEPS json
            "BUCKET_NAME": BUCKET_NAME,
            "s3_script": s3_script
        }
    )


    step_checker = EmrStepSensor(
        task_id="watch_step",
        #job_flow_id=create_emr_cluster.output,
        job_flow_id=cluster_job_id,
        step_id="{{task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    # Finaliza o Cluster EMR
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster", 
        job_flow_id=cluster_job_id,
        aws_conn_id="aws_default",
    )

    end_data_pipeline = DummyOperator(task_id="end_data_pipeline")

    start_data_pipeline >> script_to_s3 >> create_emr_cluster 
    create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster >> end_data_pipeline