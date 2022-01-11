import pytest
import pandas as pd
import numpy as np


def make_ds(name="foo", **kwargs):
    from lineage.dataset import DatasetSchema

    return DatasetSchema(name, **kwargs)


def test_dataset_np_1d():
    ds = make_ds()
    a = np.arange(0, 10)
    schema = ds.infer(pd.DataFrame(a))
    assert len(schema) == 1
    assert str(schema[0].type) == "int32"
    assert str(schema[0].name) == "0"
    assert ds.shape == (10, 1)
    assert ds.nbytes == 40


def test_dataset_np_matrix():
    ds = make_ds()
    a = np.array([[5, "hello", True], [2, "goodbye", False]])
    schema = ds.infer(pd.DataFrame(a))
    assert len(schema) == 3
    assert ds.shape == (2, 3)


def test_dataset_np_matrix_cols():
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
