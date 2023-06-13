import os
import re
import numpy as np
from datetime import timedelta
from dotenv import load_dotenv

from airflow.models import DAG
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

import openai
from sentence_transformers import SentenceTransformer

import redis
from redis.commands.search.field import TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

from extract_stock_ticker_data import fetch_files_from_repo, fetch_companies_data, fetch_earnings_call_data, redis_conn

# load env variables
load_dotenv('../.env')


def extract_data_from_github(**context):
    repo_owner = os.getenv("REPO_OWNER")
    repo_name = os.getenv("REPO_NAME")

    no_of_companies = context['params']['no_of_companies']

    files, selected_tickers = fetch_files_from_repo(repo_owner, repo_name, no_of_companies)
    selected_companies = fetch_companies_data(selected_tickers)

    context['ti'].xcom_push(key='files', value=files)
    context['ti'].xcom_push(key='selected_tickers', value=selected_tickers)
    context['ti'].xcom_push(key='selected_companies', value=selected_companies)


def split_text_into_chunks(text, chunk_size=500):
    words = text.split()
    chunks = [words[i:i + chunk_size] for i in range(0, len(words), chunk_size)]

    return chunks


def transform_data_to_chunks(**context):
    files = context['ti'].xcom_pull(key='files')
    selected_tickers = context['ti'].xcom_pull(key='selected_tickers')
    selected_companies = context['ti'].xcom_pull(key='selected_companies')

    openai_model = context['params']['openai_model']
    sbert_model = context['params']['sbert_model']

    print("================================================================")
    print(openai_model, sbert_model)
    print("================================================================")

    if os.getenv("OPENAI_API_KEY") is not None:
        openai.api_key = os.getenv("OPENAI_API_KEY")
    else:
        print("OpenAI API Key is Missing")
        return

    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Change the length to 500
    model.max_seq_length = 500

    # repo_owner = os.getenv("REPO_OWNER")
    # repo_name = os.getenv("REPO_NAME")
    # no_of_companies = 5
    #
    # files, selected_tickers = fetch_files_from_repo(repo_owner, repo_name, no_of_companies)
    # selected_companies = fetch_companies_data(selected_tickers)

    pattern = r".*(?:{})$".format("".join(list(map(lambda x: "_" + x + "|", selected_tickers))).strip("|"))

    transformed_data = []

    for file in files:
        if re.match(pattern, file["name"]):
            # Fetch a specific column based on a condition
            date, ticker = file["name"].split("_")

            condition = selected_companies['Symbol'] == ticker
            company = selected_companies.loc[condition, "Company"].to_string(index=False)

            earnings_data = fetch_earnings_call_data(file["name"])
            earnings_data_chunks = split_text_into_chunks(earnings_data, 500)

            for index, chunk in enumerate(earnings_data_chunks):
                openai_embeddings = openai.Embedding.create(input=" ".join(chunk),
                                                            model="text-embedding-ada-002",
                                                            )["data"][0]['embedding']
                sbert_embeddings = model.encode(" ".join(chunk))

                # create byte vectors for openai_embeddings and sbert_embeddings
                openai_embeddings_byte_vector = np.array(openai_embeddings, dtype=np.float32).tobytes()
                sbert_embeddings_byte_vector = np.array(sbert_embeddings, dtype=np.float32).tobytes()

                transformed_data.append(
                    {"date": date, "ticker": ticker, "company": company, "part": index + 1, "data": " ".join(chunk),
                     "sbert_embeddings": sbert_embeddings_byte_vector, "openai_embeddings": openai_embeddings_byte_vector})

    context['ti'].xcom_push(key='transformed_data', value=transformed_data)


def create_flat_index(
        redis_flat_db_client: redis.Redis,
        openai_embedding_vector_dim: int,  # length of the openai embedding vectors
        sbert_embedding_vector_dim: int,   # length of the sbert embedding vectors
        initial_no_of_vectors: int,        # initial number of vectors
        index_name: str,                   # name of the search index
        distance_metric="COSINE",          # prefix for the document keys
        key_prefix="tickers:"):  # distance metric for the vectors (ex. COSINE, IP, L2)

    # Define RediSearch fields for each of the columns in the dataset
    company = TextField(name="company")
    date = TextField(name="date")
    sbert_embedding = VectorField("sbert_embedding",
                                  "FLAT", {
                                      "TYPE": "FLOAT32",
                                      "DIM": sbert_embedding_vector_dim,
                                      "DISTANCE_METRIC": distance_metric,
                                      "INITIAL_CAP": initial_no_of_vectors,
                                  })
    openai_embedding = VectorField("openai_embedding",
                                   "FLAT", {
                                       "TYPE": "FLOAT32",
                                       "DIM": openai_embedding_vector_dim,
                                       "DISTANCE_METRIC": distance_metric,
                                       "INITIAL_CAP": initial_no_of_vectors,
                                   })

    fields = [company, date, sbert_embedding, openai_embedding]

    # Check if index exists
    try:
        redis_flat_db_client.ft(index_name).info()
        print("Index already exists")
    except:
        # Create RediSearch Index
        redis_flat_db_client.ft(index_name).create_index(
            fields=fields,
            definition=IndexDefinition(prefix=[key_prefix], index_type=IndexType.HASH)
        )


def create_hnsw_index(
        redis_hnsw_db_client: redis.Redis,
        openai_embedding_vector_dim: int,  # length of the openai embedding vectors
        sbert_embedding_vector_dim: int,   # length of the sbert embedding vectors
        initial_no_of_vectors: int,        # initial number of vectors
        index_name: str,                   # name of the search index
        distance_metric="COSINE",          # prefix for the document keys
        key_prefix="tickers:"):  # distance metric for the vectors (ex. COSINE, IP, L2)

    # Define RediSearch fields for each of the columns in the dataset
    company = TextField(name="company")
    date = TextField(name="date")
    sbert_embedding = VectorField("sbert_embedding",
                                  "HNSW", {
                                      "TYPE": "FLOAT32",
                                      "DIM": sbert_embedding_vector_dim,
                                      "DISTANCE_METRIC": distance_metric,
                                      "INITIAL_CAP": initial_no_of_vectors,
                                      "EF_RUNTIME": 5,
                                      "EPSILON": 0.3
                                  })
    openai_embedding = VectorField("openai_embedding",
                                   "HNSW", {
                                       "TYPE": "FLOAT32",
                                       "DIM": openai_embedding_vector_dim,
                                       "DISTANCE_METRIC": distance_metric,
                                       "INITIAL_CAP": initial_no_of_vectors,
                                       "EF_RUNTIME": 5,
                                       "EPSILON": 0.3
                                   })

    fields = [company, date, sbert_embedding, openai_embedding]

    # Check if index exists
    try:
        redis_hnsw_db_client.ft(index_name).info()
        print("Index already exists")
    except:
        # Create RediSearch Index
        redis_hnsw_db_client.ft(index_name).create_index(
            fields=fields,
            definition=IndexDefinition(prefix=[key_prefix], index_type=IndexType.HASH)
        )


def load_data_to_redis(redis_db_client, transformed_data, key_prefix):
    p = redis_db_client.pipeline(transaction=False)
    for data in transformed_data:
        key = key_prefix + data["date"] + "_" + data["company"] + "_" + data["part"]

        p.hset(key, mapping=data)

    p.execute()
    redis_db_client.close()

    print("Successfully loaded data to redis node.")


def load_flat_indexed_data_to_redis(**context):
    redis_flat_db_host = os.getenv("REDIS_FLAT_DB_HOST")
    redis_flat_db_port = os.getenv("REDIS_FLAT_DB_PORT")

    redis_flat_db_client = redis_conn(redis_flat_db_host, redis_flat_db_port)

    distance_metric = context['params']['distance_metric']
    transformed_data = context['ti'].xcom_pull(key='transformed_data')

    # Create a FLAT Index on fields: company, date, sbert embeddings, openai embeddings
    key_prefix = "tickers:"
    index_name = "idx:embedding_index"

    create_flat_index(redis_flat_db_client, len(transformed_data[0]["openai_embeddings"]),
                      len(transformed_data[0]["sbert_embeddings"]), len(transformed_data),
                      index_name, distance_metric, key_prefix)

    # load FLAT indexed data to redis node
    load_data_to_redis(redis_flat_db_client, transformed_data, key_prefix)


def load_hnsw_indexed_data_to_redis(**context):
    redis_hnsw_db_host = os.getenv("REDIS_HNSW_DB_HOST")
    redis_hnsw_db_port = os.getenv("REDIS_HNSW_DB_PORT")

    redis_hnsw_db_client = redis_conn(redis_hnsw_db_host, redis_hnsw_db_port)

    distance_metric = context['params']['distance_metric']
    transformed_data = context['ti'].xcom_pull(key='transformed_data')

    # Create a HNSW Index on fields: company, date, sbert embeddings, openai embeddings
    key_prefix = "tickers:"
    index_name = "idx:embedding_index"

    create_hnsw_index(redis_hnsw_db_client, len(transformed_data[0]["openai_embeddings"]),
                      len(transformed_data[0]["sbert_embeddings"]), len(transformed_data),
                      index_name, distance_metric, key_prefix)

    # load HNSW indexed data to redis node
    load_data_to_redis(redis_hnsw_db_client, transformed_data, key_prefix)


#  Create DAG to load data
user_input = {
    "no_of_companies": Param(default=5, type='number'),
    "openai_model": Param(default="text-embedding-ada-002", type='string'),
    "sbert_model": Param(default="all-MiniLM-L6-v2", type='string'),
    "distance_metric": Param(default="COSINE", type='string'),
}

company_earning_call_data_dag = DAG(
    dag_id="load_company_earnings_call_data_to_redis",
    schedule="0 0 * * *",  # https://crontab.guru/
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["DAMG7245", "Assignment01"],
    params=user_input,
)

with company_earning_call_data_dag:
    extract_data = PythonOperator(
        task_id='extract_data_from_github',
        python_callable=extract_data_from_github,
        provide_context=True,
        dag=company_earning_call_data_dag
    )

    transform_data = PythonOperator(
        task_id='transform_data_and_add_embeddings',
        python_callable=transform_data_to_chunks,
        provide_context=True,
        dag=company_earning_call_data_dag
    )

    load_flat_indexed_data = PythonOperator(
        task_id='load_flat_indexed_data_to_redis',
        python_callable=load_flat_indexed_data_to_redis,
        provide_context=True,
        dag=company_earning_call_data_dag
    )

    load_hnsw_indexed_data = PythonOperator(
        task_id='load_hnsw_indexed_data_to_redis',
        python_callable=load_hnsw_indexed_data_to_redis,
        provide_context=True,
        dag=company_earning_call_data_dag
    )

    # Flow
    extract_data >> transform_data >> [load_flat_indexed_data, load_hnsw_indexed_data]
