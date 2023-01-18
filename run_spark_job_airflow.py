from __future__ import annotations
# datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

import logging
import json

# Some Required Operators
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, ClusterGenerator, \
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

###################
# CLASS IMPORTS
###################

import time
from typing import TYPE_CHECKING, Sequence

from google.api_core.exceptions import ServerError
from google.cloud.dataproc_v1.types import Batch, JobStatus

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DataprocJobSensor(BaseSensorOperator):
    """
    Check for the state of a previously submitted Dataproc job.
    :param dataproc_job_id: The Dataproc job ID to poll. (templated)
    :param region: Required. The Cloud Dataproc region in which to handle the request. (templated)
    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :param wait_timeout: How many seconds wait for job to be ready.
    """

    template_fields: Sequence[str] = ("project_id", "region", "dataproc_job_id")
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        dataproc_job_id: str,
        region: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        wait_timeout: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataproc_job_id = dataproc_job_id
        self.region = region
        self.wait_timeout = wait_timeout
        self.start_sensor_time: float | None = None
        self.response_obj = {}

    def execute(self, context: Context) -> None:
        self.log.info(" exe 1")
        self.start_sensor_time = time.monotonic()
        super().execute(context)
        self.log.info(" exe 1 fomosh")
        return str(self.response_obj)

    def _duration(self):
        return time.monotonic() - self.start_sensor_time

    def poke(self, context: Context) -> bool:
        self.log.info("in func")
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        if self.wait_timeout:
            try:
                job = hook.get_job(
                    job_id=self.dataproc_job_id, region=self.region, project_id=self.project_id
                )
            except ServerError as err:
                duration = self._duration()
                self.log.info("DURATION RUN: %f", duration)
                if duration > self.wait_timeout:
                    raise AirflowException(
                        f"Timeout: dataproc job {self.dataproc_job_id} "
                        f"is not ready after {self.wait_timeout}s"
                    )
                self.log.info("Retrying. Dataproc API returned server error when waiting for job: %s", err)
                return False
        else:
            job = hook.get_job(job_id=self.dataproc_job_id, region=self.region, project_id=self.project_id)

        state = job.status.state
        if state == JobStatus.State.ERROR:
            # raise AirflowException(f"Job failed:\n{job}")
            self.response_obj["status"] = str(job.status.state)
            self.response_obj["logs"] = job.driver_output_resource_uri

            return True
        elif state in {
            JobStatus.State.CANCELLED,
            JobStatus.State.CANCEL_PENDING,
            JobStatus.State.CANCEL_STARTED,
        }:
            # raise AirflowException(f"Job was cancelled:\n{job}")
            self.response_obj["status"] = str(job.status.state)
            self.response_obj["logs"] = job.driver_output_resource_uri
            return True

        elif JobStatus.State.DONE == state:
            self.log.debug("Job %s completed successfully.", self.dataproc_job_id)
            self.response_obj["status"] = str(job.status.state)
            self.response_obj["logs"] = job.driver_output_resource_uri
            return True
        elif JobStatus.State.ATTEMPT_FAILURE == state:
            self.log.debug("Job %s attempt has failed.", self.dataproc_job_id)

        self.log.info("Waiting for job %s to complete.", self.dataproc_job_id)
        return False



PYSPARK_FILE = "spark_files/sample.py"
BUCKET_NAME = "spark-scripts-31122022"
CLUSTER_NAME = "cluster-dbe5"
PROJECT_ID = "PROJECT_ID_GCP"
REGION = "REGION"
conn_id = "google_cloud_default"
job_id = "custom_job"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID, "job_id": job_id},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/{PYSPARK_FILE}",
        "args": [job_id],
    }
}

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone="us-central1-c",
    master_machine_type="n1-standard-4",
    # worker_machine_type="n1-standard-2",
    num_workers=0,
    master_disk_size=30,
    storage_bucket=BUCKET_NAME,
).make()


def print_logs_v1(**kwargs):
    logging.info(type(kwargs["key"]))
    spark_logs = json.loads(kwargs["key"].replace("'", '"'))
    logging.info(f"Logs Info :: {spark_logs}")

    if spark_logs["status"] == "State.DONE":
        logging.info("Job has been successfully executed ... ")
    else:
        logging.info("Something went wrong with the job please check the logs")

    gcs_hook = GCSHook(gcp_conn_id=conn_id)
    bucket_name = spark_logs["logs"].split("/")[2]
    prefix_object_name = spark_logs["logs"].split("/", 3)[3] + ".000000000"

    file_content = gcs_hook.download(bucket_name=bucket_name,
                                     object_name=prefix_object_name,
                                     num_max_attempts=1)
    logging.info(f"Spark cluster Logs :: {file_content}")


dag = DAG(
    dag_id='spark-error-reporting-dag-re',
    description='Checks the job status by polling the job',
    default_args=default_args)

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
    do_xcom_push=True
)

pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task",
    job=PYSPARK_JOB,
    region=REGION,
    gcp_conn_id=conn_id,
    project_id=PROJECT_ID,
    # polling_interval_seconds=5,
    # timeout=100,
    asynchronous=True,
    do_xcom_push=True,
    dag=dag
)

spark_task_async_sensor = DataprocJobSensor(
    task_id='spark_task_async_sensor_task',
    region=REGION,
    project_id=PROJECT_ID,
    dataproc_job_id=pyspark_task.output,
    dag=dag,
    do_xcom_push=True
)


print_logs_data = PythonOperator(
    task_id="print_logs",
    python_callable=print_logs_v1,
    op_kwargs={'key': spark_task_async_sensor.output},
    dag=dag,

)


delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
    trigger_rule="all_done"
)


# Create the cluster then run the spark job after that delete the cluster
create_cluster >> pyspark_task >> spark_task_async_sensor >> print_logs_data >> delete_cluster


