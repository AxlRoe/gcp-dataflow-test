#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG for Google Cloud Dataflow service
"""
import os
from datetime import datetime
from urllib.parse import urlparse

from airflow import models
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator


START_DATE = datetime(2021, 1, 1)

GCS_TMP = os.environ.get('GCP_DATAFLOW_GCS_TMP', 'gs://dump-bucket-4/temp/')
GCS_STAGING = os.environ.get('GCP_DATAFLOW_GCS_STAGING', 'gs://dump-bucket-4/staging/')

default_args = {
    'dataflow_default_options': {
        'tempLocation': GCS_TMP,
        'stagingLocation': GCS_STAGING,
    }
}
with models.DAG(
    "prepare_pipeline",
    default_args=default_args,
    start_date=START_DATE,
    catchup=False,
    schedule_interval='@once',  # Override to match your needs
    tags=['example'],
) as dag_native_python:

    # [START howto_operator_start_python_job]
    p1_job = BeamRunPythonPipelineOperator(
        task_id="p1_job",
        py_file='gs://dump-bucket-4/pipeline/p1_test.py',
        py_options=[],
        # pipeline_options={
        #     'output': GCS_OUTPUT,
        # },
        py_requirements=['apache-beam[gcp]==2.39.0','numpy==1.22.4','jsonpickle==2.1.0'],
        py_interpreter='python3',
        py_system_site_packages=True,
        dataflow_config={'location': 'europe-west1'},
    )

    p2_job = BeamRunPythonPipelineOperator(
        task_id="p2_job",
        py_file='gs://dump-bucket-4/pipeline/p2_test.py',
        py_options=[],
        # pipeline_options={
        #     'output': GCS_OUTPUT,
        # },
        py_requirements=['apache-beam[gcp]==2.39.0', 'numpy==1.22.4', 'jsonpickle==2.1.0'],
        py_interpreter='python3',
        py_system_site_packages=True,
        dataflow_config={'location': 'europe-west1'},
    )

    p1_job >> p2_job

