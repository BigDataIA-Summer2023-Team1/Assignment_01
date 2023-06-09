import os
import re

import redis
import requests
import itertools

from redis.commands.search.field import VectorField
from redis.commands.search.field import TextField
from redis.commands.search.field import TagField
from redis.commands.search.field import NumericField
from redis.commands.search.query import Query
from redis.commands.search.result import Result

from airflow.dags.extract_stock_ticker_data import fetch_earnings_call_data


def create_index(client):
    client.ft().create_index([
        TagField("company"),
        TextField("date"),
    ])


def chunk(it, size):
    it = iter(it)
    while True:
        p = dict(itertools.islice(it, size))
        if not p:
            break
        yield p


def load_stock_ticker_data(client: redis.Redis, selected_tickers, selected_companies, data):
    pattern = r".*(?:{})$".format("".join(list(map(lambda x: "_" + x + "|", selected_tickers))).strip("|"))

    # Pipeline the 300 articles in one go
    p = client.pipeline(transaction=False)

    for file in data:
        if re.Match(pattern, file["name"]):
            # hash key
            key = file['name']

            # hash fields
            # Fetch a specific column based on a condition
            date, ticker = file["name"].split("_")

            condition = selected_companies['Symbol'] = ticker
            company = selected_companies.loc[condition, "Company"]
            earnings_data = fetch_earnings_call_data(file["name"])

            p.hset(key, mapping={"company": company, "date": date, "ticker": ticker, "data": earnings_data})

    p.execute()

    print("Data loaded to redis")
