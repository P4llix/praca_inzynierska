from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
from modules.cepik import cepik_operator as cpk
import os
import boto3
from botocore.exceptions import ClientError
import logging

dag = DAG(
	'Dodawanie_przeszlych_rekordow',
	description='Pobieranie historycznych rekordów',
	schedule='*/1 * * * * *',
	start_date=datetime(2023, 11, 22),
	catchup=False
)

def get_wojewodztwa():
	mysql_hook = MySqlHook(mysql_conn_id='aws_mysql')
	conn = mysql_hook.get_conn()

	curs = conn.cursor()
	curs.execute("select kod_iso, nazwa from wojewodztwo order by kod_iso")

	result = curs.fetchall()
	if result:
		return result
	return None

def get_newest_dates(**kwargs):
	mysql_hook = MySqlHook(mysql_conn_id='aws_mysql')
	conn = mysql_hook.get_conn()

	curs = conn.cursor()
	curs.execute("select data_od, data_do, rowid from cepik_synchronizacja order by rowid desc limit 1")
	# curs.execute("select data_od, data_do, rowid from cepik_synchronizacja where status is null order by rowid asc limit 1")

	result = curs.fetchone()
	if result:
		start_date = result[0]
		end_date = result[1]
		rowid = result[2]
		
		data_to_push = {
			'start': start_date.strftime('%Y%m%d'),
			'end': end_date.strftime('%Y%m%d'),
			'rowid': rowid
		}
		print(data_to_push)
		
		kwargs['ti'].xcom_push(key='dates', value=data_to_push)

		return 1
	return 0

def scrap_data_to_local_file(**kwargs):
	ti = kwargs['ti']
	nazwa = kwargs['nazwa']
	iso = kwargs['iso']
	pulled_value = ti.xcom_pull(task_ids='Get_newest_dates', key='dates')
	start_date = pulled_value['start']
	end_date = pulled_value['end']
	
	client = cpk()
	client.set_wojewodztwo(iso)
	client.set_dates(start_date, end_date)

	folder = f"{start_date}_{end_date}"

	try:
		os.makedirs(f"~/airflow/cepik/{folder}")
	except FileExistsError:
		pass

	path = f"~/airflow/cepik/{folder}/{iso}.json"
	with open(path, "w", encoding='utf-8') as file:
		json.dump(client.get_entity_array(), file,  ensure_ascii=False, indent=4)

	
	print(path)
	kwargs['ti'].xcom_push(key='path', value=path)
	return 1


def send_local_file_to_bucket(**kwargs):
	ti = kwargs['ti']
	nazwa = kwargs['nazwa']
	iso = kwargs['iso']
	pulled_value = ti.xcom_pull(task_ids='Get_newest_dates', key='dates')
	start_date = pulled_value['start']
	end_date = pulled_value['end']
	local_file_path = ti.xcom_pull(task_ids=f'Scrap_data_local_{iso}', key='path')

	print(local_file_path)

	s3 = boto3.client(
		service_name='s3',
		region_name='eu-central-1',
		aws_access_key_id='xxx',
		aws_secret_access_key='xxx'
	)

	try:
		s3.upload_file(local_file_path, 'pracainzairflow', f"{start_date}-{end_date}/{iso}-{nazwa}.json")
	except ClientError as e:
		logging.error(e)
		return 0
	return 1

def get_value(value):
	if value and value != 'None':
		value = value.replace('\'', '\'\'')
		return chr(39) + value + chr(39)
	else:
		return 'null'

def parse_bucket_data(**kwargs):
	ti = kwargs['ti']
	nazwa = kwargs['nazwa']
	iso = kwargs['iso']
	pulled_value = ti.xcom_pull(task_ids='Get_newest_dates', key='dates')
	local_file_path = ti.xcom_pull(task_ids=f'Scrap_data_local_{iso}', key='path')
	start_date = pulled_value['start']
	end_date = pulled_value['end']

	s3 = boto3.client(
		service_name='s3',
		region_name='eu-central-1',
		aws_access_key_id='xxx',
		aws_secret_access_key='xxx'
	)

	try:
		response = s3.get_object(Bucket='pracainzairflow', Key=f"{start_date}-{end_date}/{iso}-{nazwa}.json")
		s3_content = response['Body'].read().decode('utf-8')

		json_content = json.loads(s3_content)
		sql = ''
		for obj in json_content:
			row = ''
			row += 'call insertPojazd('
			row += get_value(str(obj['marka'])) + ','
			row += get_value(str(obj['model'])) + ','
			row += get_value(str(obj['kategoria_pojazdu'])) + ','
			row += get_value(str(obj['typ'])) + ','
			row += get_value(str(obj['wariant'])) + ','
			row += get_value(str(obj['wersja'])) + ','
			row += get_value(str(obj['rodzaj_pojazdu'])) + ','
			row += get_value(str(obj['podrodzaj_pojazdu'])) + ','
			row += get_value(str(obj['przeznaczenie_pojazdu'])) + ','
			row += get_value(str(obj['pochodzenie'])) + ','
			row += get_value(str(obj['tabliczka_znamienowa'])) + ','
			row += get_value(str(obj['sposob_produckji'])) + ','
			row += get_value(str(obj['rok_produkcji'])) + ','
			row += get_value(str(obj['rejestracja_w_kraju_pierwsza'])) + ','
			row += get_value(str(obj['rejestracja_w_kraju_ostatnia'])) + ','
			row += get_value(str(obj['rejestracja_za_granica'])) + ','
			row += get_value(str(obj['pojemnosc_skokowa'])) + ','
			row += get_value(str(obj['moc_netto_silnika'])) + ','
			row += get_value(str(obj['moc_netto_silnika_hybrydowego'])) + ','
			row += get_value(str(obj['masa_wlasna'])) + ','
			row += get_value(str(obj['max_masa_calkowita'])) + ','
			row += get_value(str(obj['dopuszczalna_masa'])) + ','
			row += get_value(str(obj['dopuszczalna_ladownosc'])) + ','
			row += get_value(str(obj['max_ladownosc'])) + ','
			row += get_value(str(obj['liczba_osi'])) + ','
			row += get_value(str(obj['miejsca_ogolem'])) + ','
			row += get_value(str(obj['miejsca_siedzace'])) + ','
			row += get_value(str(obj['miejsca_stojace'])) + ','
			row += get_value(str(obj['rodzaj_paliwa'])) + ','
			row += get_value(str(obj['rodzaj_paliwa_alternatywnego_pierwszego'])) + ','
			row += get_value(str(obj['rodzaj_paliwa_alternatywnego_drugiego'])) + ','
			row += get_value(str(obj['srednie_zuzycie'])) + ','
			row += get_value(str(obj['poziom_emisji_co2'])) + ','
			row += get_value(str(obj['rodzaj_zawieszenia'])) + ','
			row += get_value(str(obj['wersja_wyposazenia'])) + ','
			row += get_value(str(obj['hak'])) + ','
			row += get_value(str(obj['katalizator'])) + ','
			row += get_value(str(obj['kierownica_po_prawej_stronie'])) + ','
			row += get_value(str(obj['kierownica_po_prawej_stronie_pierwotnie'])) + ','
			row += get_value(str(obj['kod_instytutu_transaportu_samochodowego'])) + ','
			row += get_value(str(obj['nazwa_producenta'])) + ','
			row += get_value(str(obj['pierwsza_rejestracja'])) + ','
			row += get_value(str(obj['wyrejestrowanie'])) + ','
			row += get_value(str(obj['przyczyna_wyrejestrowania_pojazdu'])) + ','
			row += get_value(str(obj['rejestracja_wojewodztwo'])) + ','
			row += get_value(str(obj['rejestracja_gmina'])) + ','
			row += get_value(str(obj['rejestracja_powiat'])) + ','
			row += get_value(str(obj['wlasciciel_wojewodztwo'])) + ','
			row += get_value(str(obj['wlasciciel_powiat'])) + ','
			row += get_value(str(obj['wlasciciel_gmina'])) + ','
			row += get_value(str(obj['wlasciciel_wojewodztwo_kod'])) + ','
			row += get_value(str(obj['poziom_emisji_co2_paliwo_alternatywne'])) + ','
			row += get_value(str(obj['wojewodztwo_kod'])) + ','
			row += get_value(str(obj['cepik_id']))
			row += ');'
			sql += row
		mysql_hook = MySqlHook(mysql_conn_id='aws_mysql')

		mysql_hook.run(sql)
		return 1

	except Exception as e:
		print(f"Error: {e}")
		return 0


def update_date_status(**kwargs):
	ti = kwargs['ti']
	pulled_value = ti.xcom_pull(task_ids='Get_newest_dates', key='dates')
	start_date = pulled_value['start']
	end_date = pulled_value['end']
	rowid = pulled_value['rowid']
	
	print(f'rowid: {rowid}, {start_date} - {end_date}')

	mysql_hook = MySqlHook(mysql_conn_id='aws_mysql')
	mysql_hook.run("update cepik_synchronizacja set ostatnie_sprawdzenie = now(), c_status = 1 where rowid=" + str(rowid))

	return 1


get_dates = PythonOperator(
	execution_timeout = timedelta(seconds=60),
	task_id='Get_newest_dates',
	python_callable=get_newest_dates,
	dag=dag
)

update_status = BranchPythonOperator(
	task_id=f'update_date_status',
	python_callable=update_date_status,
	dag=dag
)

wojewodztwa = get_wojewodztwa()
for kod_iso, nazwa in wojewodztwa:

	scrap_data_task = PythonOperator(
		task_id=f'Scrap_data_local_{kod_iso}',
		python_callable=scrap_data_to_local_file,
		op_kwargs={'nazwa': nazwa, 'iso': kod_iso},
		dag=dag
	)

	send_data_task = PythonOperator(
		task_id=f'send_data_{kod_iso}',
		python_callable=send_local_file_to_bucket,
		op_kwargs={'nazwa': nazwa, 'iso': kod_iso},
		dag=dag
	)

	parse_date = PythonOperator(
		task_id=f'parse_date_{kod_iso}',
		python_callable=parse_bucket_data,
		op_kwargs={'nazwa': nazwa, 'iso': kod_iso},
		dag=dag
	)

	get_dates >> scrap_data_task >> send_data_task >> parse_date >> update_status