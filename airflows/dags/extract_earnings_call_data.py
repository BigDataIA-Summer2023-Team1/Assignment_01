import os
import re
import redis
import requests
import itertools
import pandas as pd
from dotenv import load_dotenv

from sentence_transformers import SentenceTransformer
import openai
import numpy as np
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import timedelta


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

def divide_text_into_chunks(text, chunk_size=500):
    words = text.split()  # Split the text into words
    chunked_data = [words[i:i + chunk_size] for i in range(0, len(words), chunk_size)]

    return chunked_data


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

    pattern = r".*(?:{})$".format("".join(list(map(lambda x: "_" + x + "|", selected_tickers))).strip("|"))

    data_to_load = []

    for file in files:
        if re.match(pattern, file["name"]):
            date, ticker = file["name"].split("_")

            condition = selected_companies['Symbol'] == ticker
            company = selected_companies.loc[condition, "Company"].to_string(index=False)

            earnings_data = fetch_earnings_call_data(file["name"])
            chunks = divide_text_into_chunks(earnings_data)

            for index, chunk in enumerate(chunks):
                data_to_load.append({"company": company, "ticker": ticker, "date": date, "part": index+1, "text": " ".join(chunk)})

  
    context['ti'].xcom_push(key='chunk_data', value=data_to_load)
   

def transform_data_from_github(**context):
    # Load the pre-trained models for OpenAI and SBERT embeddings
    openai_model = context['params']['openai_model']
    sbert_model = context['params']['sbert_model']


    if os.getenv("OPENAI_API_KEY") is not None:
        openai.api_key = os.getenv("OPENAI_API_KEY")
    else:
        print("OpenAI API Key is Missing")
        return

    sbert_model = SentenceTransformer(sbert_model)
    
    data_to_load = context['ti'].xcom_pull(key='chunk_data')
    
    for data in data_to_load:
        text = data['text']

        # Get OpenAI embeddings
        openai_embeddings = openai.Embedding.create(input=data["text"],
                                                            model=openai_model,
                                                            )["data"][0]['embedding']
        # Get SBERT embeddings
        sbert_embeddings = sbert_model.encode([text])
        
        # Convert the embeddings to numpy arrays
        openai_embeddings_byte_vector = np.array(openai_embeddings, dtype=np.float32).tobytes()
        sbert_embeddings_byte_vector = np.array(sbert_embeddings, dtype=np.float32).tobytes()

        data["sbert_embeddings"]=sbert_embeddings_byte_vector
        data["openai_embeddings"]=openai_embeddings_byte_vector

    # Store the embeddings in the context for downstream tasks
    context['ti'].xcom_push(key='transformed_data', value=data_to_load)

    
def create_flat_index(
        redis_flat_db_client: redis.Redis,
        openai_embedding_vector_dim: int,  # length of the openai embedding vectors
        sbert_embedding_vector_dim: int,   # length of the sbert embedding vectors
        initial_no_of_vectors: int,        # initial number of vectors
        index_name: str,                   # name of the search index
        distance_metric="COSINE",          # prefix for the document keys
        key_prefix="tickers:"):  # distance metric for the vectors (ex. COSINE, IP, L2)

    create_command = ["FT.CREATE", index_name, 'ON', 'hash', 'PREFIX', '1', key_prefix, "SCHEMA", "company", "TEXT", "date", "TEXT"]
    create_command += ["sbert_embedding", "VECTOR", "FLAT", "8",
                       "TYPE", "FLOAT32",
                       "DIM", sbert_embedding_vector_dim,
                       "DISTANCE_METRIC", str(distance_metric),
                       "INITIAL_CAP", initial_no_of_vectors]

    create_command += ["openai_embedding", "VECTOR", "FLAT", "8",
                       "TYPE", "FLOAT32",
                       "DIM", openai_embedding_vector_dim,
                       "DISTANCE_METRIC", str(distance_metric),
                       "INITIAL_CAP", initial_no_of_vectors]

    redis_flat_db_client.execute_command(*create_command)

def create_hnsw_index(
        redis_hnsw_db_client: redis.Redis,
        openai_embedding_vector_dim: int,  # length of the openai embedding vectors
        sbert_embedding_vector_dim: int,   # length of the sbert embedding vectors
        initial_no_of_vectors: int,        # initial number of vectors
        index_name: str,                   # name of the search index
        distance_metric="COSINE",          # prefix for the document keys
        key_prefix="tickers:"):  # distance metric for the vectors (ex. COSINE, IP, L2)

    create_command = ["FT.CREATE", index_name, 'ON', 'hash', 'PREFIX', '1', key_prefix, "SCHEMA", "company", "TEXT", "date", "TEXT"]
    create_command += ["sbert_embedding", "VECTOR", "HNSW", "12",
                       "TYPE", "FLOAT32",
                       "DIM", sbert_embedding_vector_dim,
                       "DISTANCE_METRIC", str(distance_metric),
                       "INITIAL_CAP", initial_no_of_vectors,
                       "M", 16,
                       "EF_CONSTRUCTION", 10]

    create_command += ["openai_embedding", "VECTOR", "HNSW", "12",
                       "TYPE", "FLOAT32",
                       "DIM", openai_embedding_vector_dim,
                       "DISTANCE_METRIC", str(distance_metric),
                       "INITIAL_CAP", initial_no_of_vectors,
                       "M", 16,
                       "EF_CONSTRUCTION", 10]

    redis_hnsw_db_client.execute_command(*create_command)

def load_flatindex_data_to_redis(**context):
    distance_metric = context['params']['distance_metric']

    db_host = os.getenv("REDIS_FLAT_DB_HOST")
    db_port = os.getenv("REDIS_FLAT_DB_PORT")

    client = redis_conn(db_host, db_port)

    # Pipeline the 300 articles in one go
    p = client.pipeline(transaction=False)

    # Load the data and perform necessary transformations
    data_to_load = context['ti'].xcom_pull(key='transformed_data')

    # Call the create_flat_index function
    index_name = "idx:embedding_index"
    key_prefix = "tickers:"
    
    create_flat_index(
        redis_flat_db_client=client,
        openai_embedding_vector_dim=len(data_to_load[0]["openai_embeddings"]),
        sbert_embedding_vector_dim=len(data_to_load[0]["sbert_embeddings"]),
        initial_no_of_vectors=len(data_to_load),
        index_name=index_name,
        distance_metric=distance_metric,
        key_prefix=key_prefix
    )

    # Load the transformed data into Redis
    for data in data_to_load:
        # Generate a unique key for each document
        doc_key = key_prefix + data["date"] + "_" + data["company"] + "_" + str(data["part"])

        # Store the data in Redis
        client.hset(doc_key, mapping=data)
        # client.execute_command("FT.ADD", index_name, doc_key, 1.0, "FIELDS",
        #                        "sbert_embedding", sbert_embeddings,
        #                        "openai_embedding", openai_embeddings)

    p.execute()
    client.close()

    print("Data loaded to Redis with flat index")


def load_hnswindex_data_to_redis(**context):
    distance_metric = context['params']['distance_metric']
    
    db_host = os.getenv("REDIS_HNSW_DB_HOST")
    db_port = os.getenv("REDIS_HNSW_DB_PORT")

    client = redis_conn(db_host, db_port)

    # Pipeline the 300 articles in one go
    p = client.pipeline(transaction=False)

    # Load the data and perform necessary transformations
    data_to_load = context['ti'].xcom_pull(key='transformed_data')

    # Call the create_flat_index function
    index_name = "idx:embedding_index"
    key_prefix = "tickers:"
    create_flat_index(
        redis_flat_db_client=client,
        openai_embedding_vector_dim=len(data_to_load[0]["openai_embeddings"]),
        sbert_embedding_vector_dim=len(data_to_load[0]["sbert_embeddings"]),
        initial_no_of_vectors=len(data_to_load),
        index_name=index_name,
        distance_metric=distance_metric,
        key_prefix=key_prefix
    )

    # Load the transformed data into Redis
    for data in data_to_load:
        # Generate a unique key for each document
        doc_key = key_prefix + data["date"] + "_" + data["company"] + "_" + str(data["part"])

        # Store the data in Redis
        client.hset(doc_key, mapping=data)
        # client.execute_command("FT.ADD", index_name, doc_key, 1.0, "FIELDS",
        #                        "sbert_embedding", sbert_embeddings,
        #                        "openai_embedding", openai_embeddings)

    p.execute()
    client.close()

    print("Data loaded to Redis with hnsw index")

    

#  Create DAG to load data
user_input = {
    "no_of_companies": Param(default=5, type='number'),
    "distance_metric": Param(default="COSINE", type='string'),
    "openai_model": Param(default="text-embedding-ada-002", type='string'),
    "sbert_model": Param(default="all-MiniLM-L6-v2", type='string')
}

dag = DAG(
    dag_id="load_company_call_earnings_data_to_redis",
    schedule="0 0 * * *",   # https://crontab.guru/
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["damg7245", "assignment01"],
    params=user_input,
)

with dag:
    extract_data = PythonOperator(
        task_id='extract_data_from_github',
        python_callable=extract_data_from_github,
        provide_context=True,
        dag=dag
    )

    transform_data = PythonOperator(
        task_id='transform_data_from_github',
        python_callable=transform_data_from_github,
        provide_context=True,
        dag=dag
    )

    load_flatdata = PythonOperator(
        task_id='index_and_load_flatdata_to_redis',
        python_callable=load_flatindex_data_to_redis,
        provide_context=True,
        dag=dag
    )

    load_hnswdata = PythonOperator(
        task_id='index_and_load_hnswdata_to_redis',
        python_callable=load_hnswindex_data_to_redis,
        provide_context=True,
        dag=dag
    )

    # Flow
    extract_data >> transform_data >> [load_flatdata, load_hnswdata]
