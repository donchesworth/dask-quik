from pathlib import Path
import pytest
import pandas as pd
import json
from argparse import Namespace
# from dask_quik.cartesian import sparse_cudf_matrix

DATA = Path.cwd().joinpath("dask_quik", "tests", "sample_data.json")

@pytest.fixture
def cols_dict(scope="module"):
    """sample column names"""
    cols = {
        "user": "user_number",
        "item": "item_id",
        "product": "product_id"
    }
    return cols


@pytest.fixture
def sample_data(scope="module"):
    """sample user/item dataset"""
    with open(DATA) as f:
        df = pd.DataFrame(json.load(f))
    return df


@pytest.fixture
def counts_dict(scope="module"):
    """unique counts for sample_data"""
    counts = {
        "user_num": len(sample_data.iloc[0].unique()),
        "item_num": len(sample_data.iloc[1].unique())
    }
    return counts


@pytest.fixture
def args(gpus):
    """sample args namespace"""    
    args = Namespace()
    args.gpus = gpus
    args.partitions = 2
    return args


def test_arg_gpus(gpus):
    """check gpus options selected"""
    if gpus == 0:
        print("gpus is zero")
    elif gpus > 0:
        print("gpus is greater than 0")


def test_dask_df(sample_data, args):
    """create a dask df"""
    import dask.dataframe as dd
    ddf = dd.from_pandas(sample_data, npartitions=args.partitions)
    assert isinstance(ddf, dd.DataFrame)


def test_cudf_df(sample_data, args):
    """create a cudf df"""
    if bool(args.gpus):
        import cudf
        cdf = cudf.from_pandas(sample_data)
        assert isinstance(cdf, cudf.DataFrame)
        print("cudf created!")
    else:
        with pytest.raises(Exception) as e_info:
            import cudf
        print(e_info)


def test_dask_cudf(sample_data, args):
    """create a dask_cudf df"""
    if bool(args.gpus):
        import cudf
        import dask_cudf
        cdf = cudf.from_pandas(sample_data)
        dcdf = dask_cudf.from_cudf(cdf, npartitions=args.partitions)
        assert isinstance(dcdf, dask_cudf.DataFrame)
        print("dask_cudf created")
    else:
        with pytest.raises(Exception) as e_info:
            import dask_cudf
        print(e_info)


def test_cartesian_ddf(sample_data, args):
    """create a dask cartesian df. If gpus, 
    output should be a dask_cudf df, else dask df"""
    import dask.dataframe as dd
    ddf = dd.from_pandas(sample_data, npartitions=2)
    from dask_quik.cartesian import dask_cudf_cartesian
    sm = dask_cudf_cartesian(ddf, ["user", "item"], args)
    if bool(args.gpus):
        import dask_cudf
        assert isinstance(sm, dask_cudf.DataFrame)
        print("made a dask_cudf df")
    else:
        assert isinstance(ddf, dd.DataFrame)
        print("made a dask df")
    print(sm.compute())