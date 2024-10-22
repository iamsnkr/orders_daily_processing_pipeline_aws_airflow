import boto3
from time import sleep
import botocore
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.param import Param

# Replace 'your_connection_id' with the actual connection ID
connection_id = 'aws_pipeline'
conn = BaseHook.get_connection(connection_id)

default_args = {
    'owner': "aws_orders_pipeline",
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['your-email@yopmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id="AWS_SPARK_ORDERS_PIPELINE",
          description='An example aws_airflow_dag',
          schedule=timedelta(days=1),
          catchup=False,
          start_date=datetime(year=2024, month=10, day=18),
          default_args=default_args,
          # Passing Date Parameter to Dag and Also adding s3 bucket name to Dag level so that we can access it
          params={
              'execution_date': Param(default="NA", type='string', description='Execution date in yyyymmdd format'),
              's3_bucket_name': 'aws-spark-jobs-bucket',
          })

# Connecting AWS S3 with Boto3
s3_client = boto3.client('s3', region_name='us-east-1',
                         aws_access_key_id=f"{conn.login}",
                         aws_secret_access_key=f"{conn.password}")


# Uploading Code to S3 bucket
def upload_code(bucket_name):
    # Get Path Details
    code_path = Variable.get("code_path")
    s3_code_path = Variable.get("s3_code_path")
    # Upload files to S3 Bucket
    s3_client.upload_file(code_path, bucket_name, s3_code_path)


# Uploading Data to S3 bucket
def upload_data(bucket_name, **kwargs):
    execution_date = kwargs['execution_date']
    # Get Path Details
    data_path = Variable.get("data_path")
    s3_data_path = Variable.get("s3_data_path")
    # Upload files to S3 Bucket
    s3_client.upload_file(f"{data_path}orders_{execution_date}.csv",
                          bucket_name,
                          f"{s3_data_path}orders_{execution_date}.csv")


upload_data_tk = PythonOperator(
    task_id='upload_data_to_s3_bucket',
    python_callable=upload_data,
    op_args=["{{params.s3_bucket_name}}"],
    op_kwargs={"execution_date": "{{params.execution_date}}"},
    dag=dag
)

upload_spark_code_tk = PythonOperator(
    task_id='upload_spark_code_to_s3_task',
    python_callable=upload_code,
    op_args=["{{params.s3_bucket_name}}"],
    dag=dag
)


# Check file exists or not in the S3 bucket
def file_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"Key: '{key}' found!")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"Key: '{key}' does not exist!")
            raise
        else:
            print("Something else went wrong")
            raise


file_exists_tk = PythonOperator(
    task_id='file_sensor_task',
    python_callable=file_exists,
    op_args=["{{params.s3_bucket_name}}", f"{Variable.get('s3_code_path')}"],
    dag=dag
)

# Connecting AWS with Boto3
client = boto3.client('emr', region_name='us-east-1',
                      aws_access_key_id=f"{conn.login}",
                      aws_secret_access_key=f"{conn.password}")


# Create EMR Cluster
def create_emr_cluster():
    cluster_info = client.run_job_flow(
        Name="transient_demo_testing",
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 1,
                }
            ],
            'Ec2KeyName': 'EC2_KEY_PAIR',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-008dec633a322d87d',
        },
        LogUri="s3://aws-spark-jobs-bucket/emrfolder/",
        ReleaseLabel='emr-7.1.0',
        BootstrapActions=[],
        VisibleToAllUsers=True,
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        Applications=[{'Name': 'Spark'}, {'Name': 'Hive'}]
    )
    cluster_id = cluster_info['JobFlowId']
    print(f"The cluster started with cluster id: {cluster_id}")
    return cluster_id


# Get EMR Cluster Status
def get_status_of_cluster(cluster_id):
    response = client.describe_cluster(ClusterId=cluster_id)
    status = response['Cluster']['Status']['State']
    print(f"Cluster Status is {status}")
    return status


# Wait for Cluster Status to be 'WAITING'
def wait_for_cluster_status(cluster_id):
    while True:
        status = get_status_of_cluster(cluster_id)
        if status == 'WAITING':
            break
        print(f"The step is {status}")
        sleep(40)


# Submit Spark Job
def add_spark_submit(job_flow_id, **kwargs):
    execution_date = kwargs['execution_date']
    response = client.add_job_flow_steps(
        JobFlowId=job_flow_id,
        Steps=[{
            'Name': 'Submit_Spark_Job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    '--master', 'yarn',
                    "--deploy-mode", "cluster",
                    "--date", execution_date,
                    "s3://aws-spark-jobs-bucket/code/orders_data_process.py"
                ],
            },
        }]
    )
    return response['StepIds'][0]


# Get Status of Spark Job Step
def get_status_of_step(cluster_id, step_id):
    response = client.describe_step(ClusterId=cluster_id, StepId=step_id)
    return response['Step']['Status']['State']


# Wait for Spark Job Step to Complete
def wait_for_step_to_complete(cluster_id, step_id):
    while True:
        status = get_status_of_step(cluster_id, step_id)
        if status == 'COMPLETED':
            break
        print(f"The step is {status}")
        sleep(40)


# Terminate EMR Cluster
def terminate_cluster(cluster_id):
    client.terminate_job_flows(JobFlowIds=[cluster_id])


# Define Airflow Tasks
start_tk = BashOperator(
    task_id="start_aws_pipeline",
    bash_command="echo Start_AWS_Pipeline",
    dag=dag,
    trigger_rule="one_success"
)

create_emr_cluster_tk = PythonOperator(
    task_id='create_emr_cluster',
    python_callable=create_emr_cluster,
    dag=dag,
    trigger_rule="all_success"
)

get_cluster_status_tk = PythonOperator(
    task_id='get_cluster_status',
    python_callable=wait_for_cluster_status,
    op_args=["{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}"],
    dag=dag
)

spark_submit_tk = PythonOperator(
    task_id='spark_submit_task',
    python_callable=add_spark_submit,
    op_args=["{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}"],
    op_kwargs={"execution_date": "{{params.execution_date}}"},
    dag=dag
)

wait_for_step_to_complete_tk = PythonOperator(
    task_id='wait_for_step_to_complete',
    python_callable=wait_for_step_to_complete,
    op_args=[
        "{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        "{{ task_instance.xcom_pull(task_ids='spark_submit_task') }}"
    ],
    dag=dag
)

terminate_cluster_tk = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_cluster,
    op_args=["{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}"],
    dag=dag
)

# Setting task dependencies
start_tk >> [upload_data_tk,
             upload_spark_code_tk] >> file_exists_tk >> create_emr_cluster_tk >> get_cluster_status_tk >> spark_submit_tk >> wait_for_step_to_complete_tk >> terminate_cluster_tk
