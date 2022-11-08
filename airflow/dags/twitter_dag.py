from airflow.models import DAG
from os.path import join
from datetime import datetime
from operators.twitter_operator import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

with DAG(dag_id="twitter_dag", start_date=datetime.now()) as dag:
	twitter_operator = TwitterOperator(
		task_id="twitter_aluraonline",
		query="AluraOnline",
		file_path=join(
			"/home/renato/Documentos/datapipeline/datalake/bronze",
			"twitter_aluraonline",
			"extract_date={{ ds }}",
			"AluraOnline_{{ ds_nodash }}.json"
		)
	)

	twitter_transform = SparkSubmitOperator(
		task_id="transform_twitter_aluraonline",
		application=(
			"/home/renato/Documentos/datapipeline/"
			"spark/transformation.py"
		),
		name="twitter_transformation",
		application_args=[
			"--src",
			"/home/renato/Documentos/datapipeline/"
			"datalake/bronze/twitter_aluraonline/extract_date=2022-11-08",
			"--dest",
			"/home/renato/Documentos/"
			"datapipeline/datalake/silver/twitter_aluraonline",
			"--process-date",
			"{{ ds }}"			
		]

	)

	twitter_operator >> twitter_transform