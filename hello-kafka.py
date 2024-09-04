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
from __future__ import annotations

import functools
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG

# This is just for setting up connections in the demo - you should use standard
# methods for setting these connections in production
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

consumer_logger = logging.getLogger("airflow")

def consumer_function_batch(messages, prefix=None):
    for message in messages:
        #key = json.loads(message.key())
        value = json.loads(message.value())
        consumer_logger.info("%s %s @ %s; %s", prefix, message.topic(), message.offset(), value)
    context = get_current_context()
    ti = context["ti"]
    ti.xcom_push(key="consume_from_topic_2_b", value=messages)
    return



def uploadtomongo(ti, **context):
    data = context["result"]
    if not data:
        return

    try:
        hook = MongoHook(mongo_conn_id='atlas-mongo-db')
        client = hook.get_conn()
        db = client.personal
        transactions=db.transactions
        print(f"Connected to MongoDB - {client.server_info()}")
        transactions.insert_one(json.loads(data))
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")
        raise

with DAG(
    "kafka-example",
    default_args=default_args,
    description="Examples of Kafka Operators",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    t1 = ConsumeFromTopicOperator(
        kafka_config_id="k8s-kafka",
        task_id="consume_from_topic_2_b",
        topics=["transactions"],
        apply_function_batch=functools.partial(consumer_function_batch, prefix="consumed:::"),
        commit_cadence="end_of_batch",
        max_messages=200,
        max_batch_size=50,
        do_xcom_push=True,
    )

    t1.doc_md = "Does the same thing as the t2 task, but passes the callable directly"
    "instead of using the string notation."

    t2 = PythonOperator(
        task_id='upload-mongodb',
        python_callable=uploadtomongo,
        op_kwargs={"result": t1.output},
        dag=dag
        )

    t1 >> t2
