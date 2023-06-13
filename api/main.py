import os
import numpy as np
import fastapi
import uvicorn
import redis
import openai
import cohere
from redis.exceptions import ConnectionError

from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse
from pydantic import BaseModel

from redis.commands.search.query import Query
from sentence_transformers import SentenceTransformer
from utils import is_valid_email, is_valid_password, hash_password, verify_password, create_access_token, \
    validate_jwt_token, redis_conn

# load env variables
load_dotenv('./.env')


class UserInput(BaseModel):
    company: str
    startYr: int
    endYr: int


class User(BaseModel):
    email: str
    password: str


class Querying(BaseModel):
    embeddingType: str
    indexType: str
    showData: bool
    query: str


app = FastAPI(title = "DAMG 7245")

@app.post("/api/v1/login")

async def login(user: User) -> JSONResponse:
    # user input validations
    response = {}

    if not is_valid_email(user.email):
        response["msg"] = "Invalid Field: email"
        response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        raise HTTPException(status_code=400, detail=response)

    if not is_valid_password(user.password):
        response["msg"] = "Invalid Field: password"
        response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        raise HTTPException(status_code=400, detail=response)

    try:
        redis_host = os.getenv('REDIS_DB_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_DB_PORT', 30001))

        client = redis_conn(redis_host, redis_port)

        # check if user with provided email exists
        if client.exists('users:{}'.format(user.email)) == 1:
            # fetch the hashed password form redis for the provided email
            hashed_password = client.hget('users:{}'.format(user.email), 'password')

            # validate the password provided for the existing email with its stored hash
            is_password_valid = verify_password(user.password, hashed_password)

            if is_password_valid:
                # if given a valid password, generate a JWT token valid for 30min for this email
                token = create_access_token({"email": user.email})

                response["email"] = user.email
                response['token'] = token

                return JSONResponse(status_code=200, content=response)
            else:
                response["msg"] = "Invalid Field: password"
                response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                raise HTTPException(status_code=400, detail=response)
        else:
            response["msg"] = "Invalid Field: email, user with this email doesnt exist"
            response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            raise HTTPException(status_code=400, detail=response)

    except ConnectionError as e:
        response["msg"] = "Server is down"
        response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        raise HTTPException(status_code=500, detail=response)


@app.post("/api/v1/search")
async def querying(req: Request, query: Querying) -> JSONResponse:
    response = {}

    # Access the headers
    headers = req.headers

    # Get the value of a specific header
    authorizationToken = headers.get("Authorization")
    if authorizationToken and authorizationToken.startswith("Bearer "):
        token = authorizationToken.removeprefix("Bearer ")
        if not validate_jwt_token(token):
            response["msg"] = "Invalid Token"
            response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            raise HTTPException(status_code=401, detail=response)

    else:
        response["msg"] = "Forbidden Access"
        response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        raise HTTPException(status_code=401, detail=response)

    # user validations
    if query.embeddingType not in ["openapi", "sbert"]:
        raise HTTPException(status_code=400, detail='Invalid Field: embeddingType.')

    if query.indexType not in ["flat", "hnsw"]:
        raise HTTPException(status_code=400, detail='Invalid Field: indexType.')

    if os.getenv("OPENAI_API_KEY") is not None:
        openai.api_key = os.getenv("OPENAI_API_KEY")
    else:
        response["msg"] = "OpenAI API key missing"
        response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        raise HTTPException(status_code=400, detail=response)

    # if os.getenv("COHERE_API_KEY") is not None:
    #     co = cohere.Client("YOUR_API_KEY")
    # else:
    #     response["msg"] = "Cohere API key missing"
    #     response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #
    #     raise HTTPException(status_code=400, detail=response)

    try:
        redis_flat_db_host = os.getenv("REDIS_FLAT_DB_HOST", 'localhost')
        redis_flat_db_port = os.getenv("REDIS_FLAT_DB_PORT", 30001)

        redis_hnsw_db_host = os.getenv("REDIS_HNSW_DB_HOST", 'localhost')
        redis_hnsw_db_port = os.getenv("REDIS_HNSW_DB_PORT", 30003)

        redis_flat_db_client = redis_conn(redis_flat_db_host, redis_flat_db_port)
        redis_hnsw_db_client = redis_conn(redis_hnsw_db_host, redis_hnsw_db_port)

        embeddings_mapping = {"openapi": "openai_embedding", "sbert": "sbert_embedding"}

        if query.embeddingType == "openapi":
            # TODO: embed question using openapi
            # Creates embedding vector from user query
            embedded_query = openai.Embedding.create(input=query.query,
                                                     model="text-embedding-ada-002",
                                                     )["data"][0]['embedding']
            params_dict = {"vector_param": np.array(embedded_query).astype(dtype=np.float32).tobytes()}

            if query.indexType == "flat":
                #     TODO:  query flast index redis node
                print("here 1")

                print(search_redis(redis_flat_db_client, params_dict, embeddings_mapping[query.embeddingType]))
            elif query.indexType == "hnsw":
                #    TODO:  query hnsw index redis node
                print("here 2")
                # print(search_redis(redis_hnsw_db_client, params_dict, embeddings_mapping[query.embeddingType]))

        elif query.embeddingType == "sbert":
            # TODO: embed question using cohere
            model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
            model.max_seq_length = 500

            embedded_query = model.encode(query.query).astype(np.float32).tobytes()
            params_dict = {"vector_param": embedded_query}

            if query.indexType == "flat":
                #     TODO:  query flast index redis node
                print("here 3")
                print(search_redis(redis_flat_db_client, params_dict, embeddings_mapping[query.embeddingType]))
            elif query.indexType == "hnsw":
                #    TODO:  query hnsw index redis node
                print("here 4")
                print(search_redis(redis_hnsw_db_client, params_dict, embeddings_mapping[query.embeddingType]))

    except ConnectionError as e:
        response["msg"] = "Redis server is down"
        response["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        raise HTTPException(status_code=500, detail=response)


def search_redis(
        redis_client: redis.Redis,
        params_dict: dict,
        vector_field: str,
        index_name: str = "idx:embedding_index",
        return_fields: list = ["company", "date", "text", "ticker", "part", "vector_score"],
        hybrid_fields="*",
        k: int = 5,
        print_results: bool = True,
):
    return_fields.append(vector_field)

    # Prepare the Query
    # base_query = f'{hybrid_fields}=>[KNN {k} @{vector_field} $vector_param AS vector_score]'
    base_query = f'*=>[KNN {k} @{vector_field} $vector_param AS vector_score]'

    # query = Query(base_query).return_fields(*return_fields).paging(0, k).dialect(2)
    query = Query(base_query).return_fields(*return_fields).sort_by('vector_score').paging(0, k).dialect(2)
    # perform vector search
    results = redis_client.ft(index_name).search(query, query_params=params_dict)

    return results


@app.get("/api/v1/fetch-companies-data")
async def fetchCompaniesData(data: UserInput) -> dict:
    #user input validations
    if len(data.company) < 3:
        response={"msg": "Company name should be of atleast 3 characters", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        raise HTTPException(status_code=400, detail=response)

    if data.startYr > data.endYr:
        response={"msg": "Start year cannot be greater than end year", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        raise HTTPException(status_code=400, detail=response)

    # if data.startYr > datetime.now().year and data.endYr > datetime.now().year:
    #     response={"msg": "Start year, End year cannot be greater than current year", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    #     raise HTTPException(status_code=400, detail=response)

    try:
        redis_host = os.getenv('REDIS_DB_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_DB_PORT', 30001))

        client = redis.Redis(host=redis_host, port=redis_port, username="", password="", decode_responses=True)

        date_range = ""
        for year in range(data.startYr, data.endYr+1):
            date_range += "^" + str(year) + "*|"

        date_range = date_range[:-1]

        query="@company:{} @date:({})".format(data.company, date_range)

        result = client.execute_command("FT.SEARCH", "idx:companies", query)

        even_elements = result[2::2]

        response = [{elements[i]: elements[i + 1] for i in range(0, len(elements), 2)} for elements in even_elements]

        return JSONResponse(status_code=200, content=response)

    except ConnectionError as e:
        response={"msg": "Server is down", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        raise HTTPException(status_code=500, detail=response)

host = os.getenv("FASTAPI_HOST", "localhost")
port = os.getenv("FASTAPI_PORT", 8000)

uvicorn.run(app, host=host, port=int(port))
