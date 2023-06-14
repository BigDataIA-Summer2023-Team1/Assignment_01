## Live application Links :octopus:

- Please use this application responsibly, as we have limited free credits remaining.

[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)](http://34.23.210.12:30006/)

[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=FastAPI&logoColor=white)](http://34.23.210.12:30005/docs)

[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-007A88?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](http://34.23.210.12:8080)

[![Redis FLAT Index Node](https://img.shields.io/badge/redis-%23DD0031.svg?style=for-the-badge&logo=redis&logoColor=white)](http://34.23.210.12:30002/)

[![Redis HNSW Index Node](https://img.shields.io/badge/redis-%23DD0031.svg?style=for-the-badge&logo=redis&logoColor=white)](http://34.23.210.12:30004/)

[![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=1DyfWnAj3sUVshCtt96xWAaFhrE6ifEfT13ZruDm3dMc)

## Problem Statement :memo:
Intelligence Co, a financial research firm, wants to explore the potential of large language models by building a contextual search application for their investment analysts. The application should allow analysts to search through earnings call transcripts using vector similarity search, traditional filtering, and hybrid search techniques. The company has approached our team to develop this application, which will involve data exploration, search functionality, and deployment using various tools such as Airflow, Redis, Streamlit, FastAPI, and Docker. The primary goal is to create an efficient and user-friendly platform that enables analysts to filter transcripts based on company and year, ask specific questions, and retrieve relevant responses. The application should also incorporate a workflow to process the dataset, compute embeddings using SBERT and OpenAI models, and implement a Q/A system using VSS with RAG. Security measures, such as authentication with API keys, should be implemented to safeguard sensitive information. The final deliverables include a hosted Streamlit application, a documented codebase on GitHub, and comprehensive test cases and results analysis.

## Project Goals :dart:
Task -1:
1. Extract ticker data from given github repo and combine with assosiated companies earnings call data from given github repo
2. Build airflow pipelines that does [1] and index that data and load it to redis.
3. Design an Stremlit screen where user can query a company and year, and need to fetch assosiated data using redis full text search.
4. Build REST API using FastAPI to do redis full text search for a given qurey.

Task -2:
1. Extract companies earnings call data from given github repo and chunk it each 500 words and attach a chunck number (which part this chunk blog to) and append company name, date, ticker to the chunk which is extracted from ticker github repo.
2. To each chuck of the data add 2 columns each for openai embeddings and sbert embeddings respectively.
3. Index the above data using FLAt and HNSW algorithms and load it to 2 different redis nodes.
4. Build airflow pipeline that trigger the steps [1, 2, 3].
5. Design Stremlit screen where logged in user choose type of embeddings[openai/sbert], API key, Index Type[FLAT/HNSW] and Query.
6. Develop a REST API using FastAPI where it takes user inputs from step 5 and fetch top 5 similar chunks using vector similarity search using redisearch queries.

Task -3
7. Build github actions that trigger unit tests.
8. Dockerize entire application and  Deploy on GCP using terraform and make it public.


## Technologies Used :computer:
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)](https://streamlit.io/)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)
[![FastAPI](https://img.shields.io/badge/fastapi-109989?style=for-the-badge&logo=FASTAPI&logoColor=white)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
![Redis](https://img.shields.io/badge/redis-%23DD0031.svg?style=for-the-badge&logo=redis&logoColor=white)
[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![GitHub Actions](https://img.shields.io/badge/Github%20Actions-282a2e?style=for-the-badge&logo=githubactions&logoColor=367cfe)](https://github.com/features/actions)

## Data Source :flashlight:
1. Companies earnings call dataset: https://github.com/Earnings-Call-Dataset/MAEC-A-Multimodal-Aligned-Earnings-Conference-Call-Dataset-for-Financial-Risk-Prediction/tree/master/MAEC_Dataset
2. Ticker data: https://github.com/plotly/dash-stock-tickers-demo-app/blob/master/tickers.csv


## Requirements
```
redis
openai
bcrypt
fastapi
uvicorn
passlib
requests
streamlit
python-jose
sentence_transformers
```

## Project Structure
```
📦 Assignment_01
├─ .gitignore
├─ Makefile
├─ README.md
├─ airflows
│  ├─ .gitignore
│  ├─ Dockerfile
│  ├─ dags
│  │  ├─ extract_earnings_call_data.py
│  │  └─ extract_stock_ticker_data.py
│  ├─ redis
│  │  ├─ conn.py
│  │  └─ load_stock_ticker_data_to_redis.py
│  └─ requirements.txt
├─ api
│  ├─ .gitignore
│  ├─ Dockerfile
│  ├─ main.py
│  ├─ requirements.txt
│  └─ utils.py
├─ docker-compose-cloud.yml
├─ docker-compose-local.yml
├─ frontend
│  ├─ .gitignore
│  ├─ Dockerfile
│  ├─ main.py
│  ├─ pages
│  │  ├─ 1_Login.py
│  │  ├─ 2_part_1.py
│  │  └─ 3_search.py
│  └─ requirements.txt
└─ terraform
   ├─ .gitignore
   ├─ Makefile
   ├─ install.sh
   ├─ main.tf
   ├─ output.tf
   └─ variables.tf
```
©generated by [Project Tree Generator](https://woochanleee.github.io/project-tree-generator)

## How to run Application locally
To run the application locally, follow these steps:
1. Clone the repository to get all the source code on your machine.

2. Install docker desktop on your system

3. Create a .env file in the root directory with the following variables:

    ``` 
      # Github Variables
      TICKER_DATA_FILE=https://raw.githubusercontent.com/plotly/dash-stock-tickers-demo-app/master/tickers.csv 
      STOCK_TICKER_DATA_FILE=https://raw.githubusercontent.com/plotly/dash-stock-tickers-demo-app/master/stock-ticker.csv

      REPO_OWNER='Earnings-Call-Dataset'
      REPO_NAME='MAEC-A-Multimodal-Aligned-Earnings-Conference-Call-Dataset-for-Financial-Risk-Prediction'

      # API URL
      API_URL=http://fastapi:30004/api/v1

      # FastAPI variables
      FASTAPI_HOST='fastapi'
      FASTAPI_PORT=8095

      # Airflow variables
      AIRFLOW_UID=501
      AIRFLOW_PROJ_DIR=./airflows

      # Redis variables
      REDIS_DB_HOST='redis-stack-flat'
      REDIS_DB_PORT=6379

      REDIS_FLAT_DB_HOST='redis-stack-flat'
      REDIS_FLAT_DB_PORT=6379

      REDIS_HNSW_DB_HOST='redis-stack-hnsw'
      REDIS_HNSW_DB_PORT=6379

      # JWT variables
      JWT_SECRET_KEY=""                  # JWT secret to hash
      JWT_ALGORITHM="HS256"              # JWT algorithm to hash 
      JWT_ACCESS_TOKEN_EXPIRE_MINUTES=30 # JWT toekn expiry time

      # Embedding variables
      OPENAI_API_KEY="".                 # OPENAI API KEY for embeddings

    ```

4. Once you have set up your environment variables, Start the application by executing 
  ``` 
    Make build-up
  ```

5. Once all the docker containers spin up, Access the application at following links
    ``` 
     1. Stremlit UI: http://localhost:30006/
     2. FAST API   : http://localhost:30005/docs
     
     # Airflow Credentials - username: airflow; password: airflow
     3. Airflow    : http://localhost:8080/  
     4. Redis(FLAT): http://localhost:30002/
     5. Redis(HNSW): http://localhost:30004/ 
    ```
6. Hard code user crendiatls on RedisInsight query pannel: accessable on http://localhost:30002/  
   Note: Here providing REST API and interface for user registration is not the goal so hardcoded user credentials.
    ``` 
    # user credentials: email: test@gmail.com  password: Abc123
    HSET "users:test@gmail.com" password "$2b$12$3SaTuLnEg1nAu7QITTSJreFIi9KeVNymSfEzWXrM5FUuYQmfLlTpS"
    ```    
7. To delete all active docker containers execute 
     ``` 
     Make down
     ``` 
     
## References
1. SBERT Embeddings: https://www.sbert.net/
2. OpenAI Embeddings: https://platform.openai.com/docs/guides/embeddings
3. Redis VSS: https://redis.com/blog/rediscover-redis-for-vector-similarity-search/
4. FLAT & HNSW indexing: https://github.com/RedisAI/financial-news/blob/main/GettingStarted.ipynb
5. Redis Quering: https://github.com/openai/openai-cookbook/blob/main/examples/vector_databases/redis/getting-started-with-redis-and-openai.ipynb
     
## Learning Outcomes
1. Built airflow pipelines to extract and load data to redis.
2. How to use REDIS as vector database and query from redis.
3. How to model data as a service. 
4. How to deploy DaaS on cloud and make it accissable.

## Team Information and Contribution 

Name | Contribution %| Contributions 
--- |--- | --- |
Sanjana Karra | 33.3%.| Built air flow pipeline to extract data from github, add embeddings, index and load to redis, Designed UI in Stremlit Part 1
Nikhil Reddy Polepally | 33.3% | Designed UI screen in streamlit for part 2, Developed REST API for part 1, Added test cases and github actions
Shiva Sai Charan Ruthala | 33.3% | Designed airflow pipeline to extract data from ticker data from github, index and load to redis, Buit UI and REST API for login page, Deployed application on GCP
