import os
import re
import redis
import requests
import itertools
import pandas as pd
from dotenv import load_dotenv

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import timedelta

# from redis.commands.search.field import TextField
# from redis.commands.search.field import TagField

# from redis.conn import redis_conn
# from redis.load_stock_ticker_data_to_redis import load_stock_ticker_data

# load env variables
load_dotenv('../.env')


def fetch_files_from_repo(repo_owner, repo_name, no_of_companies):
    base_url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/contents/MAEC_Dataset'

    # Fetch the contents of the repository
    response = requests.get(base_url)
    response.raise_for_status()
    files = response.json()

    # Filter files based on the pattern
    selected_tickers = []

    for file in files:
        company_ticker = file['name'].split("_")[1]

        if len(selected_tickers) < no_of_companies:
            if company_ticker not in selected_tickers:
                selected_tickers.append(company_ticker)
        else:
            break

    return files, selected_tickers


def fetch_companies_data(company_tickers):
    ticker_data = pd.read_csv(str(os.getenv("TICKER_DATA_FILE")))
    filtered_companies = ticker_data[ticker_data['Symbol'].isin(company_tickers)]

    return filtered_companies


def fetch_earnings_call_data(file_name):
    repo_owner = os.getenv("REPO_OWNER", 'Earnings-Call-Dataset')
    repo_name = os.getenv("REPO_NAME", 'MAEC-A-Multimodal-Aligned-Earnings-Conference-Call-Dataset-for-Financial-Risk-Prediction')

    base_url = f'https://raw.githubusercontent.com/{repo_owner}/{repo_name}/master/MAEC_Dataset/{file_name}/text.txt'
    page = requests.get(base_url)

    return page.text


#  Redis functions
def redis_conn(db_host, db_port, db_username="", db_password="", decode_responses=True):
    # TODO: validation & try except
    return redis.Redis(host=db_host, port=db_port, username=db_username, password= db_password, decode_responses = decode_responses)


def delete_data(client: redis.Redis):
    client.flushall()


# def create_index(client):
#     client.ft().create_index([
#         TagField("company"),
#         TextField("date"),
#     ])


def chunk(it, size):
    it = iter(it)
    while True:
        p = dict(itertools.islice(it, size))
        if not p:
            break
        yield p


def extract_data_from_github(**context):
    repo_owner = os.getenv("REPO_OWNER")
    repo_name = os.getenv("REPO_NAME")

    no_of_companies = context['params']['no_of_companies']

    files, selected_tickers = fetch_files_from_repo(repo_owner, repo_name, no_of_companies)
    selected_companies = fetch_companies_data(selected_tickers)

    context['ti'].xcom_push(key='files', value=files)
    context['ti'].xcom_push(key='selected_tickers', value=selected_tickers)
    context['ti'].xcom_push(key='selected_companies', value=selected_companies)


def load_data_to_redis(**context):
    db_host = os.getenv("REDIS_DB_HOST")
    db_port = os.getenv("REDIS_DB_PORT")

    files = context['ti'].xcom_pull(key='files')
    selected_tickers = context['ti'].xcom_pull(key='selected_tickers')
    selected_companies = context['ti'].xcom_pull(key='selected_companies')

    client = redis_conn(db_host, db_port)

    # index
    client.execute_command('FT.CREATE', 'idx:companies', 'ON', 'hash', 'PREFIX', '1', 'companies:', 'SCHEMA', 'date', 'TEXT', 'SORTABLE', 'company', 'TEXT', 'SORTABLE')

    pattern = r".*(?:{})$".format("".join(list(map(lambda x: "_" + x + "|", selected_tickers))).strip("|"))

    # Pipeline the 300 articles in one go
    p = client.pipeline(transaction=False)

    for file in files:
        if re.match(pattern, file["name"]):
            # hash key
            key = "companies:" + file['name']

            # hash fields
            # Fetch a specific column based on a condition
            date, ticker = file["name"].split("_")

            condition = selected_companies['Symbol'] == ticker
            company = selected_companies.loc[condition, "Company"].to_string(index=False)
            earnings_data = fetch_earnings_call_data(file["name"])

            p.hset(key, mapping={"company": company, "date": date, "ticker": ticker, "data": earnings_data})

    p.execute()

    client.close()

    print("Data loaded to redis")


#  Create DAG to load data
user_input = {
    "no_of_companies": Param(default=5, type='number'),
}

dag = DAG(
    dag_id="load_stock_ticker_data_to_redis",
    schedule="0 0 * * *",   # https://crontab.guru/
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["damg7245", "assignment01"],
    params=user_input,
)

with dag:
    # load_data_from_github_to_redis = PythonOperator(
    #     task_id='load_data_from_github_to_redis',
    #     python_callable=load_data,
    #     provide_context=True,
    #     dag=dag,
    # )

    extract_data = PythonOperator(
        task_id='extract_data_from_github',
        python_callable=extract_data_from_github,
        provide_context=True,
        dag=dag
    )

    load_data = PythonOperator(
        task_id='index_and_load_data_to_redis',
        python_callable=load_data_to_redis,
        provide_context=True,
        dag=dag
    )

    # Flow
    extract_data >> load_data
