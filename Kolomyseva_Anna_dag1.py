# Инжиниринг данных. Итоговый проект.
# Отработка навыков работы с Airflow

# Коломыцева Анна


import logging
import pendulum
import pandas as pd
import os
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from transform_script import transfrom

# Set name and owner
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
owner = "KolomytsevaAnna"

# Set all arguments for DAG
default_args = {
   # Базовая секция аргументов
   "dag_name": DAG_ID,
   "owner": owner,
   "owner_links": {owner: "https://airflow.apache.org"},
   "start_date": pendulum.datetime(2023, 3, 5, tz=pendulum.timezone("Europe/Moscow")),
   "schedule_interval": '0 0 5 * *',
   "description": f"{owner}: расчет витрины активности клиентов по сумме и количеству их транзакций",
   "catchup": False,
   "tags": ["SF", "homework2"],
   "max_active_runs": 1,
   "template_searchpath": "/usr/local/airflow/include",
   # "on_failure_callback": alert_failure_function2
   "retries": 1,
   "retry_delay": timedelta(seconds=60)
}

def Extract(ti):
   # считываем данные по ссылке
   url = 'https://drive.usercontent.google.com/download?id=1hkkOIxnYQTa7WD1oSIDUFgEoBoWfjxK2&export=download&authuser=0&confirm=t&uuid=af8f933c-070d-4ea5-857b-2c31f2bad050&at=APZUnTVuHs3BtcrjY_dbuHsDceYr:1716219233729'  # URL для загрузки данных
   output_path = os.path.join('/opt/airflow/dags', 'profit_table.csv')  # путь для сохранения файла

   response = requests.get(url)  # загружаем данные по указанному URL
   response.raise_for_status()  # проверка успешности запроса
   logging.info("Extracting data...")
   with open(output_path, 'wb') as file:  
      file.write(response.content)  # записываем содержимое ответа в файл

   # Логирование успешного скачивания данных
   ti.xcom_push(key='download_success', value=True) # Сохраняем значение True в XCom
   
def Transform(date):
   profit_table = pd.read_csv('/opt/airflow/dags/profit_table.csv')
   product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']  # список продуктов
   all_flags = []

   for product in product_list:
      transformed_df = transfrom(profit_table, date)  # трансформируем данные
      all_flags.append(transformed_df)
      
   # объединяем в один DataFrame
   flags_activity = pd.concat(all_flags, axis=1).T.drop_duplicates().T  
   logging.info("Transforming data...")
   Load(flags_activity)

def Load(flags_activity):
   logging.info("Loading data...")
   if os.path.exists('/opt/airflow/dags/flags_activity.csv'):
      existing_df = pd.read_csv('/opt/airflow/dags/flags_activity.csv')  # читаем существующий файл
      # объединяем существующие данные с новыми
      updated_df = pd.concat([existing_df, flags_activity], ignore_index=True)  
      # сохраняем объединенные данные в файл
      updated_df.to_csv('/opt/airflow/dags/flags_activity.csv', index=False) 
   else:
      flags_activity.to_csv('/opt/airflow/dags/flags_activity.csv', index=False)


with DAG(
       dag_id=default_args.get("dag_name"),
       default_args=default_args,
       owner_links=default_args.get("owner_links"),
       description=default_args.get("description"),
       start_date=default_args.get("start_date"),
       schedule_interval=default_args.get("schedule_interval"),
       catchup=default_args.get("catchup"),
       tags=default_args.get("tags"),
       template_searchpath=default_args.get("template_searchpath"),
       max_active_runs=default_args.get("max_active_runs")
) as dag:
   date = '2024-03-01'
   Extract = PythonOperator(
      task_id="extract",
      python_callable=Extract
   )

   Transform = PythonOperator(
      task_id=f"transform",
      python_callable=Transform,
      op_kwargs={'date': '{{ ds }}'},  # передаем аргумент 'date' в функцию, который будет заменен на дату запуска DAG (Execution Date)
      provide_context=True
   )
   
   
Extract >> Transform