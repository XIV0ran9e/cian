
import io
import json
import pickle, ast
import logging
import numpy as np
import pandas as pd
import pickle
import cianparser

import glob
import os

from datetime import datetime, timedelta, date
from sklearn.model_selection import train_test_split
from sklearn.metrics import explained_variance_score, mean_absolute_error, r2_score
from typing import Any, Dict, Literal
from catboost import CatBoostRegressor

#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    'owner' : 'Airflow Admin',
    'email' : ' airflowadmin@example.com',
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retry' : 3,
    'retry_delay' : timedelta(minutes=1)
}

#dag = DAG(#TO-DO: прописать аргументы)
dag = DAG(
    dag_id = 'pipeline_dag',
    schedule_interval = '0 12 * * *',
    start_date = days_ago(2),
    catchup = False,
    tags = ['mlops'],
    default_args = DEFAULT_ARGS
)


_LOG = logging.getLogger()
_LOG.addHandler(logging.StreamHandler())

def init() -> None:
    _LOG.info("Pipeline started.")
    
# def parse_data_to_csv(): -> None:
#     start_page = 1
#     for i in range(6):
#         data = cianparser.parse(
#             deal_type="sale",
#             accommodation_type="flat",
#             location="Москва",
#             rooms="all",
#             start_page=start_page,
#             end_page=start_page + 10,
#             is_saving_csv=False,
#             is_express_mode=True
#         )
#         start_page += 10
#     return None

    
def parse_data_to_csv() -> None:
    start_page = 1
    for i in range(2):
        data = cianparser.parse(
            deal_type="sale",
            accommodation_type="flat",
            location="Москва",
            rooms="all",
            start_page=start_page,
            end_page=start_page + 10,
            is_saving_csv=False,
            is_express_mode=True
        )
        start_page += 1
    return None


def prepare_data() -> None:
    # Последний .csv файл в директории
    latest_file = max(glob.glob('cian*.csv'), key=os.path.getctime)
    
    # Загрузка данных
    raw = pd.read_csv(latest_file, sep=';') 
    
    # Предобработка
    # даление дубликатов, пропусков и некорректных данных, формирование итогового датасета
    raw = raw.dropna()
    raw = raw.drop_duplicates (subset=['price', 'residential_complex', 'street', 'house_number'])
    
    # Датасет с необходимыми для обучения модели колонками
    df = raw.drop(['house_number', 'residential_complex', 'link', 'author', 'street'], axis=1)
    df = df[df['rooms_count'] != -1]
    df = df.drop_duplicates()
    
    # Удаление данных с выбросами по ценам при помощи IQR:
    # любое значение данных, которое находится на расстоянии более (1,5 * IQR) 
    # от квартилей Q1 и Q3, считается выбросом.
    Q1 = df['price'].quantile(0.25)
    Q3 = df['price'].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5*IQR
    upper = Q3 + 1.5*IQR
    df = df.query('price <= @upper & price >= @lower')
    
    # Добавим дату формирования выборки
    df['date'] = pd.to_datetime(datetime.now().date())
    df['date']
    
    # Сохраним в .csv
    df.to_csv('clean_' + datetime.now().strftime("%d-%m-%Y_%H-%M-%S") + '.csv', index=False)
    
    return None

def insert_data() -> None:
    # Использовать созданный ранее PG connection
    #pg_hook = PostgresHook('pg_connection')
    #con = pg_hook.get_conn()
    
    # Получим чистые данные
    data = pd.read_csv(max(glob.glob('clean*.csv'), key=os.path.getctime))
    
    # connection
    postgres_sql_upload = PostgresHook(postgres_conn_id='pg_connection' #, 
                                       #schema='airflow_db'
                                      ) 
    postgres_sql_upload.insert_rows('flats_cleaned', rows=data)

    # Прочитать все данные из таблицы california_housing
    # data = pd.read_sql_query('select * from california_housing', con)

    

def train_model() -> None:
   #TO-DO: Заполнить все шаги
    
    # Использовать созданный ранее PG connection
    pg_hook = PostgresHook('pg_connection')
    con = pg_hook.get_conn()
    # Прочитать все данные из таблицы airflow_db
    df = pd.read_sql_query('select * from airflow_db', con)
    
    # X, y
    X_c = df.drop(['price', 'price_per_m2', 'date'], axis=1)
    y_c = df['price']
    
    # Обучающая и тестовая выборка
    X_train, X_test, y_train, y_test = train_test_split(X_c, y_c, test_size=0.2, random_state=0)
    
    # Объявим модель и обучим её, посчитаем метрики
    cat = CatBoostRegressor(cat_features=['author_type', 'city', 'deal_type', 
                                      'accommodation_type', 'district', 'underground'], 
                        depth=4, l2_leaf_reg=1, learning_rate=0.1)
    cat.fit(X_train, y_train)
    y_pred_cat = cat.predict(X_test)
    _LOG.info("-"*10)
    _LOG.info("METRICS:")
    _LOG.info(f"R^2: {r2_score(y_test, y_pred_cat)}")
    _LOG.info(F'explained_variance_score: {explained_variance_score(y_test, y_pred)}')
    _LOG.info(F'MAE: {mean_absolute_error(y_test, y_pred_cat)}')
    _LOG.info("-"*10)
    
    cat.save_model('model_' + datetime.now().strftime("%d-%m-%Y_%H-%M-%S") + '.json')
    
    # connection
    #postgres_sql_upload = PostgresHook(postgres_conn_id='pg_connection' #, 
    #                                   #schema='airflow_db'
    #                                  ) 
    #postgres_sql_upload.insert_rows('flats_cleaned', data)
    _LOG.info("Success.")
    


task_init = PythonOperator(task_id='init', python_callable=init, dag=dag) 

task_parse_data_to_csv = PythonOperator(task_id='parse_data_to_csv', python_callable=parse_data_to_csv, dag=dag) 

task_prepare_data = PythonOperator(task_id='prepare_data', python_callable=prepare_data, dag=dag)

task_insert_data = PythonOperator(task_id='insert_data', python_callable=insert_data, dag=dag)

task_train_model = PythonOperator(task_id='train_model', python_callable=train_model, dag=dag)

#TO-DO: Архитектура DAG'а
task_init >> task_parse_data_to_csv >> task_prepare_data >> task_insert_data >> task_train_model


