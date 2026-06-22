from __future__ import annotations

import re
from unittest.mock import MagicMock

import pytest
from pymongo.errors import ConnectionFailure

from dr_bottleneck.storage.mongo import (
    MongoDocumentError,
    MongoPersistError,
    assert_mongo_safe_document,
    coerce_mongo_document,
    insert_prepared_document,
    non_string_key_paths,
    prepare_for_mongo,
    replace_prepared_document,
)


def test_non_string_key_paths_finds_integer_key() -> None:
    document = {"summary": {"by_budget": {128: {"total": 1}}}}
    assert non_string_key_paths(document) == ["summary.by_budget.128"]


def test_assert_mongo_safe_document_rejects_integer_keys() -> None:
    with pytest.raises(
        MongoDocumentError,
        match=re.escape("summary.by_budget.128"),
    ):
        assert_mongo_safe_document({"summary": {"by_budget": {128: {}}}})


def test_coerce_mongo_document_stringifies_keys() -> None:
    document = {"summary": {"by_budget": {128: {"total": 1}}}}
    prepared = prepare_for_mongo(document)
    assert prepared["summary"]["by_budget"]["128"]["total"] == 1
    assert_mongo_safe_document(prepared)


def test_nested_string_key_document_is_mongo_safe() -> None:
    summary = {"by_budget": {"128": {"total": 1}}}
    assert_mongo_safe_document({"summary": summary})


def test_insert_prepared_document_coerces_before_insert() -> None:
    collection = MagicMock()
    insert_prepared_document(
        collection,
        {"summary": {"by_budget": {128: {"total": 1}}}},
    )
    inserted = collection.insert_one.call_args.args[0]
    assert "128" in inserted["summary"]["by_budget"]


def test_replace_prepared_document_wraps_pymongo_errors() -> None:
    collection = MagicMock()
    collection.replace_one.side_effect = ConnectionFailure("connection lost")
    with pytest.raises(MongoPersistError, match="Failed to replace document"):
        replace_prepared_document(
            collection,
            filter_doc={"run_id": "run-1"},
            document={"run_id": "run-1", "summary": {}},
            upsert=True,
        )


def test_coerce_mongo_document_leaves_scalar_values() -> None:
    assert coerce_mongo_document(128) == 128
    assert coerce_mongo_document([{"a": 1}]) == [{"a": 1}]
