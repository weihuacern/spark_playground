"""The Python implementation of Apache Airflow DAG for compute engine."""

from datetime import datetime, timedelta
import inspect
import logging
import os
import subprocess

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

LOG_DIR = f"{os.environ['HOME']}/log"
LOG_FNAME = "compute-engine.log"
logging.basicConfig(filename=f"{LOG_DIR}/{LOG_FNAME}", filemode='a', level=logging.INFO)

def log_args(func):
    """
    log_args: logging arguements before func get excuted
    """
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        func_args_str = ', '.join('{} = {!r}'.format(*item) for item in func_args.items())
        logging.info(f'{func.__module__}.{func.__qualname__} ( {func_args_str} )') # pylint: disable=logging-format-interpolation
        return func(*args, **kwargs)
    return wrapper

def log_string(string):
    """
    log_string: logging string into log file
    """
    logging.info(string)


DAG_ID = "compute_engine_application_scheduler"
APACHE_SPARK_HOME = "/usr/local/spark/bin"

OPS_NAME_STATICDATAPROCESSOR = "StaticDataProcessor"
OPS_NAME_DYNAMICDATAPROCESSOR = "DynamicDataProcessor"
OPS_NAME_COREDATAGENERATOR = "CoreDataGenerator"

class ComputeEngineApplicationScheduler():
    """
    ComputeEngineApplicationScheduler: A scheduler for compute engine's applications
    """
    def __init__(self):
        self.dag = None

    def __airflow_dag_init(self):
        dag_default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime.today(),
            'email': ['weihua19900704@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        self.dag = DAG(
            DAG_ID,
            default_args=dag_default_args,
            description='Compute Engine Application DAG',
            schedule_interval=timedelta(days=1),
        )

    def create_dag(self):
        """
        create_dag: create airflow dag for excution
        """
        self.__airflow_dag_init()
        ops_static_data_processor = self.create_operator(OPS_NAME_STATICDATAPROCESSOR)
        ops_dynamic_data_processor = self.create_operator(OPS_NAME_DYNAMICDATAPROCESSOR)
        ops_core_data_generator = self.create_operator(OPS_NAME_COREDATAGENERATOR)
        [ops_static_data_processor, ops_dynamic_data_processor] >> ops_core_data_generator # pylint: disable=pointless-statement
        self.dag.sync_to_db()
        return self.dag

    @staticmethod
    def __apache_spark_submit(**kwargs):
        opt_name = kwargs['opt_name']
        jar_dir = f"{os.environ['HOME']}/spark_playground/template/scala/target/scala-2.11"
        jar_name = "computeapplicationentrypoint_2.11-1.0.jar"
        try:
            submit_pipe = [
                f'{APACHE_SPARK_HOME}/spark-submit',
                '--class', 'com.huawei.compute.ComputeApplicationEntrypoint',
                '--master', 'local[*]',
                #'--driver-memory', f'4g',
                #'--executor-memory', f'4g',
                #'--executor-cores', 8,
                f'{jar_dir}/{jar_name}',
                f'{opt_name}',
            ]
            log_string(f"Submitting application with entrypoint: {opt_name}")
            log_string(f"Cmd parameters: {submit_pipe}")
            subprocess.call(submit_pipe)
        except Exception as err: # pylint: disable=broad-except
            log_string(f"Error when submitting application: {err}")
        log_string(f"Complete application submission with entrypoint: {opt_name}")

    def create_operator(self, opt_name):
        """
        create_operator: create airflow operator that attached to dag
        """
        opt = PythonOperator(
            task_id=f'{opt_name}',
            provide_context=True,
            python_callable=self.__apache_spark_submit,
            op_kwargs={'opt_name': opt_name},
            dag=self.dag)
        return opt

scheduler_cls = ComputeEngineApplicationScheduler() # pylint: disable=invalid-name
globals()[DAG_ID] = scheduler_cls.create_dag()
