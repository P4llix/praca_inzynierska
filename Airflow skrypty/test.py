import json
import requests, time
import datetime
import random

end = datetime.date(2023, 11, 16)
start = datetime.date(1990, 1, 4)
current = start

sql = ''

while current < end:
    sql = sql + "insert into cepik_synchronizacja(data_od,data_do) values ('" + current.strftime('%Y-%m-%d') + "','" + (current + datetime.timedelta(days=6)).strftime('%Y-%m-%d') + "');\n"
    current = current + datetime.timedelta(days=7)

with open('sql.sql', "w", encoding='utf-8') as file:
    file.write(sql)
