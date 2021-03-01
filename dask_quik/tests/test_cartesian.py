from pathlib import Path
import pytest
import pandas as pd
import json
from argparse import Namespace
import dask.dataframe as dd
import dask_quik.cartesian as dqcart

SAMPLE = Path.cwd().joinpath("dask_quik", "tests", "sample_data.json")
FINAL = Path.cwd().joinpath("dask_quik", "tests", "final_data.json")


@pytest.fixture
def sample_data(scope="module"):
    """sample user/item dataset"""
    with open(SAMPLE) as f:
        df = pd.DataFrame(json.load(f))
    return df


@pytest.fixture
def final_data(scope="module"):
    """final user/item dataset"""
    with open(FINAL) as f:
        df = pd.DataFrame(json.load(f))
    df.index.names = ['ui_index']
    return df


@pytest.fixture
def cols_dict(scope="module"):
    """sample column names"""
    cols = {
        "user": "user_number",
        "item": "item_id"
    }
    return cols


@pytest.fixture
def colk(cols_dict, scope="module"):
    """column keys"""
    colk = list(cols_dict.keys())
    return colk


@pytest.fixture
def colv(cols_dict, scope="module"):
    """column values"""
    colv = list(cols_dict.values())
    return colv


@pytest.fixture
def tcol(cols_dict, scope="module"):
    """two column value"""
    tcol = "_".join(cols_dict.keys())
    return tcol

@pytest.fixture
def counts_dict(sample_data, scope="module"):
    """unique counts for sample_data"""
    counts = {
        "user_number": len(sample_data.iloc[:,0].unique()),
        "item_id": len(sample_data.iloc[:,1].unique())
    }
    return counts

@pytest.fixture
def args(gpus):
    """sample args namespace"""    
    args = Namespace()
    args.gpus = gpus
    args.has_gpu = has_gpu
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
    ddf = dd.from_pandas(sample_data, npartitions=args.partitions)
    assert isinstance(ddf, dd.DataFrame)


def test_cudf_df(sample_data, args):
    """create a cudf df"""
    if bool(args.gpus) and args.has_gpu:
        import cudf
        cdf = cudf.from_pandas(sample_data)
        assert isinstance(cdf, cudf.DataFrame)
    elif bool(args.gpus):
        with pytest.raises(Exception) as e_info:
            import cudf
        print(e_info)
    else:
        pass


def test_dask_cudf(sample_data, args):
    """create a dask_cudf df"""
    if bool(args.gpus) and args.has_gpu:
        import cudf
        import dask_cudf
        cdf = cudf.from_pandas(sample_data)
        dcdf = dask_cudf.from_cudf(cdf, npartitions=args.partitions)
        assert isinstance(dcdf, dask_cudf.DataFrame)
    elif bool(args.gpus):
        with pytest.raises(Exception) as e_info:
            import dask_cudf
        print(e_info)
    else:
        pass


def test_cartesian_df(sample_data, colv, args):
    """create a dask cartesian df. If gpus, 
    output should be a dask_cudf df, else dask df"""
    if bool(args.gpus) and args.has_gpu:
        import cudf
        import dask_cudf
        cdf = cudf.from_pandas(sample_data)
        dcdf = dask_cudf.from_cudf(cdf, npartitions=args.partitions)
        sm = dqcart.dask_cudf_cartesian(dcdf, colv, args)
        assert isinstance(sm, dask_cudf.DataFrame)
    elif bool(args.gpus):
        with pytest.raises(Exception) as e_info:
            import dask_cudf
        print(e_info)        
    else:
        ddf = dd.from_pandas(sample_data, npartitions=args.partitions)
        sm = dqcart.dask_cudf_cartesian(ddf, colv, args)
        assert isinstance(sm, dd.DataFrame)

def test_indexized_df(sample_data, counts_dict, colv, args):
    """create an indexed df. If gpus, 
    output should be a dask_cudf df, else dask df"""
    ddf = dd.from_pandas(sample_data, npartitions=args.partitions)
    ddf = dqcart.indexize(ddf, counts_dict, colv)


def test_indexized_cartesian(sample_data, counts_dict, colv, args):
    """create an indexed cartesian df. If gpus, 
    output should be a dask_cudf df, else dask df"""
    ddf = dd.from_pandas(sample_data, npartitions=args.partitions)
    sm = dqcart.dask_cudf_cartesian(ddf, colv, args)
    sm = dqcart.indexize(sm, counts_dict, colv)


def test_sparse_matrix(final_data, sample_data, cols_dict, counts_dict, colk, args):
    """create an indexed cartesian df. If gpus, 
    output should be a dask_cudf df, else dask df"""
    if bool(args.gpus) and args.has_gpu:
        import cudf
        import dask_cudf
        cdf = cudf.from_pandas(sample_data)
        gdf = dask_cudf.from_cudf(cdf, npartitions=args.partitions)
    elif bool(args.gpus):
        with pytest.raises(Exception) as e_info:
            import dask_cudf
        print(e_info)
        return
    else:
        gdf = dd.from_pandas(sample_data, npartitions=args.partitions)
    sm = dqcart.sparse_cudf_matrix(gdf, cols_dict, counts_dict, colk, args)
    if bool(args.gpus): 
        sm = sm.sort_index().to_pandas()
    assert(sm.equals(final_data))
    print(sm.equals(final_data))