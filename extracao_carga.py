from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pendulum
from datetime import datetime, timedelta
import finnhub
import pandas as pd

TIMESTAMP_FORMAT = '%Y-%m-%d'
end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
start_time = (datetime.now() + timedelta(-1)).strftime(TIMESTAMP_FORMAT)


def extract_amzn_news():
    API_KEY = 'secret'

    COMPANY = 'AMZN'

    fields = ['category', 'datetime', 'headline', 'image', 'related', 'source',
              'summary', 'url']

    finnhub_client = finnhub.Client(API_KEY)

    res = finnhub_client.company_news(COMPANY, _from=start_time, to=end_time)

    return pd.DataFrame(res)[fields].to_excel(
        f'/home/bruno/Documents/dataengineer_projects/amzn_news_api_extraction/dia={start_time}to{end_time}/amzn_news.xlsx')


TIMESTAMP_FORMAT = '%Y-%m-%d'

with DAG(
    'extract_amzn_news',
    start_date=pendulum.datetime(2023, 4, 5),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='cria_pasta',
        bash_command=f'mkdir -p "/home/bruno/Documents/dataengineer_projects/amzn_news_api_extraction/dia={start_time}to{end_time}"'

    )

    task2 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_amzn_news
    )

    task1 >> task2
