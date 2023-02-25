import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_domains():
    def split(x):
        return x.split('.')[1]
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_zone'] = df['domain'].apply(split)
    top_10_domains_zone = df['domain_zone'].value_counts().head(10).to_frame().reset_index()
    with open('top_10_domains_zone.csv', 'w') as f:
        f.write(top_10_domains_zone.to_csv(index=False, header=False))

def get_the_longiest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['len'] = df['domain'].str.len()
    the_longiest = df.sort_values(['len', 'domain'], ascending=[False, True])[['domain', 'len']].head(1)
    with open('the_longiest.csv', 'w') as f:
        f.write(the_longiest.to_csv(index=False, header=False))

def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if 'airflow.com' in np.array(df['domain']):
        df = df.set_index('domain')
        rank = df.loc['airflow.com', 'rank']
        output = f'Domain airflow.com place is {rank}'
    else:
        output = 'Domain airflow.com is absent'
    with open('rank_airflow.txt', 'w') as f:
        f.write(output)

        
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domains_zone.csv', 'r') as f:
        all_data_top = f.read()
    with open('the_longiest.csv', 'r') as f:
        all_data_len = f.read()
    with open('rank_airflow.txt', 'r') as f:
        all_data_rank = f.read()
    date = ds

    print(f'Top domain zones on {date}')
    print(all_data_top)

    print(f'The longiest domain on {date}')
    print(all_data_len)
    
    print(f'About airflow.com place on {date}')
    print(all_data_rank)

default_args = {
    'owner': 'd.dontsov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('about_domains_d_dontsov', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domains',
                    python_callable=get_top_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_the_longiest_domain',
                        python_callable=get_the_longiest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5
