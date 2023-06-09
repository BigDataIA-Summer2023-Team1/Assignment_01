import os
import redis


def redis_conn(db_host, db_port, db_username="", db_password="", decode_responses=True):
    # TODO: validation & try except
    return redis.Redis(host=db_host, port=db_port, username=db_username, password= db_password, decode_responses = decode_responses)


def delete_data(client: redis.Redis):
    client.flushall()
