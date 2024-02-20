from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, date
from modules import cepik

default_args = {
	'owner': 'Mikolaj',
	'start_date': datetime(2023, 10, 13)
}

dag = DAG(
	'Pobranie_wartosci_z_wojewodztwa',
	default_args=default_args,
	schedule='@daily',
	catchup=False
)

def insert_date():
	mysql_hook = MySqlHook(mysql_conn_id='aws_mysql')

	data = date.today()

	sql_query = f"call dodajCodziennaDate('{data}')"
	mysql_hook.run(sql_query)

def scrap_day():
	pass

insert_daily_date = PythonOperator(
	task_id='insert_date',
	python_callable=insert_date,
	dag=dag
)

mysql_hook = MySqlHook(mysql_conn_id='aws_mysql')
sql = f"select kod_iso from wojewodztwo;"

wojewodztwa = mysql_hook.get_many(sql)

for wojewodztwo in wojewodztwa:
    task_id = f'przetwarzanie_wojewodztwa_{wojewodztwo}'
    processing = PythonOperator(
        task_id=task_id,
        python_callable=check_count,
        op_args=[wojewodztwo],
        dag=dag,
	)