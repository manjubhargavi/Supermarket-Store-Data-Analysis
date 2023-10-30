from datetime import datetime, timedelta

from airflow.models import DAG, BaseOperator

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.utils.edgemodifier import Label

with DAG(
    dag_id="store_adf_run_pipeline",
    start_date=datetime(2023, 10, 26),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "myairflow", #This is a connection created on Airflow UI
        "factory_name": "ganeshdatafactory123",  # This can also be specified in the ADF connection.
        "resource_group_name": "ganesh",  # This can also be specified in the ADF connection.
    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    customer_ingestion: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="customer_ingestion_task",
        pipeline_name="customer_ingestion", 
        wait_for_termination=True,
    )
    orders_ingestion: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="orders_ingestion_task",
        pipeline_name="orders_ingestion", 
        wait_for_termination=True,
    )
    ships_ingestion: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="ships_ingestion_task",
        pipeline_name="ships_ingestion", 
        wait_for_termination=True,
    )
    sales_ingestion: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="sales_ingestion_task",
        pipeline_name="sales_ingestion", 
        wait_for_termination=True,
    )
    Product_ingestion: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="Product_ingestion_task",
        pipeline_name="Product_ingestion", 
        wait_for_termination=True,
    )
    
    storedatabricks: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="storedatabricks_task",
        pipeline_name="storedatabricks", 
        wait_for_termination=True,
    )
    load_data_to_final: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="load_to_final_layer_task",
        pipeline_name="load_to_final_layer", 
        wait_for_termination=True,
    )
	
    begin>>[customer_ingestion,orders_ingestion,ships_ingestion,sales_ingestion,Product_ingestion]>>storedatabricks>>load_data_to_final>>end