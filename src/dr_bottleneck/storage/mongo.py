from __future__ import annotations

import os

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.uri_parser import parse_uri

DEFAULT_BOTTLENECK_MONGODB_URL = "mongodb://localhost:27017/dr_bottleneck"


def bottleneck_mongodb_url() -> str:
    return os.environ.get(
        "BOTTLENECK_MONGODB_URL",
        DEFAULT_BOTTLENECK_MONGODB_URL,
    )


def _database_name(url: str) -> str:
    parsed = parse_uri(url)
    database = parsed.get("database")
    if database:
        return database
    return "dr_bottleneck"


def get_bottleneck_collection(collection_name: str) -> Collection:
    url = bottleneck_mongodb_url()
    client = MongoClient(url)
    database = client.get_database(_database_name(url))
    return database[collection_name]


def ensure_bottleneck_indexes(collection: Collection, *, unique_run_id: bool) -> None:
    if unique_run_id:
        collection.create_index([("run_id", ASCENDING)], unique=True)
    else:
        collection.create_index([("run_id", ASCENDING), ("timestamp", ASCENDING)])
        collection.create_index([("timestamp", ASCENDING)])
