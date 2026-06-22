from __future__ import annotations

import os
from typing import Any

from bson import BSON
from bson.errors import InvalidDocument
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from pymongo.uri_parser import parse_uri

DEFAULT_BOTTLENECK_MONGODB_URL = "mongodb://localhost:27017/dr_bottleneck"


class MongoDocumentError(ValueError):
    """Raised when a document cannot be stored in MongoDB."""


class MongoPersistError(RuntimeError):
    """Raised when MongoDB persistence fails after document preparation."""


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


def ensure_bottleneck_indexes(
    collection: Collection, *, unique_run_id: bool
) -> None:
    if unique_run_id:
        collection.create_index([("run_id", ASCENDING)], unique=True)
    else:
        collection.create_index(
            [("run_id", ASCENDING), ("timestamp", ASCENDING)]
        )
        collection.create_index([("timestamp", ASCENDING)])


def non_string_key_paths(value: Any, *, prefix: str = "") -> list[str]:
    bad_paths: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            segment = f"{prefix}.{key}" if prefix else str(key)
            if not isinstance(key, str):
                bad_paths.append(segment)
            bad_paths.extend(
                non_string_key_paths(nested, prefix=segment),
            )
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            bad_paths.extend(
                non_string_key_paths(
                    nested,
                    prefix=f"{prefix}[{index}]",
                ),
            )
    return bad_paths


def assert_mongo_safe_document(document: Any) -> None:
    bad_paths = non_string_key_paths(document)
    if bad_paths:
        joined = ", ".join(bad_paths)
        msg = f"MongoDB document has non-string keys at: {joined}"
        raise MongoDocumentError(msg)
    try:
        BSON.encode(document)
    except InvalidDocument as exc:
        msg = f"MongoDB document is not BSON-encodable: {exc}"
        raise MongoDocumentError(msg) from exc


def coerce_mongo_document(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): coerce_mongo_document(nested)
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [coerce_mongo_document(item) for item in value]
    return value


def prepare_for_mongo(document: dict[str, Any]) -> dict[str, Any]:
    prepared = coerce_mongo_document(document)
    if not isinstance(prepared, dict):
        msg = "MongoDB documents must be top-level mappings."
        raise MongoDocumentError(msg)
    assert_mongo_safe_document(prepared)
    return prepared


def insert_prepared_document(
    collection: Collection,
    document: dict[str, Any],
) -> None:
    prepared = prepare_for_mongo(document)
    try:
        collection.insert_one(prepared)
    except PyMongoError as exc:
        msg = f"Failed to insert document into {collection.name}: {exc}"
        raise MongoPersistError(msg) from exc


def replace_prepared_document(
    collection: Collection,
    *,
    filter_doc: dict[str, Any],
    document: dict[str, Any],
    upsert: bool = False,
) -> None:
    prepared = prepare_for_mongo(document)
    try:
        collection.replace_one(filter_doc, prepared, upsert=upsert)
    except PyMongoError as exc:
        msg = f"Failed to replace document in {collection.name}: {exc}"
        raise MongoPersistError(msg) from exc
