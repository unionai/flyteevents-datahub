import pytest
import pandas as pd
import numpy as np


def make_ds(name="foo", **kwargs):
    from lineage.dataset import DatasetSchema

    return DatasetSchema(name, **kwargs)


def test_np_1d():
    ds = make_ds()
    a = np.arange(0, 10, dtype=np.int64)
    schema = ds.infer(pd.DataFrame(a))
    assert len(schema) == 1
    assert str(schema[0].type) == "int64"
    assert str(schema[0].name) == "0"
    assert ds.shape == (10, 1)
    assert ds.nbytes == 80


def test_np_matrix():
    ds = make_ds()
    a = np.array([[5, "hello", True], [2, "goodbye", False]])
    schema = ds.infer(pd.DataFrame(a))
    assert len(schema) == 3
    assert ds.shape == (2, 3)


def test_np_matrix_cols():
    ds = make_ds()
    a = np.array([[5, "hello", True], [2, "goodbye", False]])
    schema = ds.infer(pd.DataFrame(a, columns=list("ABC")))
    assert len(schema) == 3
    assert ds.shape == (2, 3)
    assert schema.names == ["A", "B", "C"]
    import pyarrow as pa

    assert str(schema[0].type) == "string"
    assert str(schema[1].type) == "string"
    assert str(schema[2].type) == "string"


def test_json_complex():
    ds = make_ds()
    json_data = """{
    "uri": "https://www.bbc.co.uk/news/business-60087798",
    "meta_lang": "en",
    "text": "The five groups - the British Chambers of Commerce, the Confederation of British Industry, the Federation of Small Businesses, the Institute of Directors and Make UK - said businesses were likely to be faced with further costs as existing fixed tariff contracts come to an end.",
    "title": "Firms call for urgent help with energy bills",
    "keywords": [
        "tariff",
        "small",
        "uk",
        "firms",
        "groups",
        "urgent",
        "industry",
        "british",
        "fixed",
        "businesses",
        "likely",
        "institute",
        "help",
        "energy",
        "bills"
    ],
    "tags": [],
    "authors": [],
    "article_fetched_time": "2022/01/21 20:23:18",
    "rss_publication_time": "2022/01/21T17:52:43",
    "rss_feed_url": "http://feeds.bbci.co.uk/news/business/rss.xml",
    "rss_feed_title": "BBC News - Business",
    "article_publication_time": null,
    "summary": "The five groups - the British Chambers of Commerce, the Confederation of British Industry, the Federation of Small Businesses, the Institute of Directors and Make UK - said businesses were likely to be faced with further costs as existing fixed tariff contracts come to an end."
}"""
    from pyarrow import json
    import io

    f = io.BytesIO(json_data.encode())
    df = json.read_json(f)
    schema = ds.infer(df)
    from lineage.datahub import DataHubSchemaConverter

    converter = DataHubSchemaConverter()
    target_schema = converter.convert(schema)
    assert len(target_schema) == 13
