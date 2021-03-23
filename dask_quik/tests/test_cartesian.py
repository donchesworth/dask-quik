import pytest
import pandas as pd
import dask.dataframe as dd
import dask_quik as dq


@pytest.fixture(scope="module")
def colk(ui_cols):
    """column keys"""
    colk = list(ui_cols.keys())
    return colk


@pytest.fixture(scope="module")
def colv(ui_cols):
    """column values"""
    colv = list(ui_cols.values())
    return colv


@pytest.fixture(scope="module")
def tcol(ui_cols):
    """two column value"""
    tcol = "_".join(ui_cols.keys())
    return tcol


@pytest.fixture(scope="module")
def counts_dict(sample_data):
    """unique counts for sample_data"""
    counts = {
        "user_number": len(sample_data.iloc[:, 0].unique()),
        "item_id": len(sample_data.iloc[:, 1].unique()),
    }
    return counts


def is_dc_dd(dc_ddf, dc_ddt):
    if bool(dc_ddt):
        import dask_cudf

        assert isinstance(dc_ddf, dask_cudf.DataFrame)
    else:
        assert isinstance(dc_ddf, dd.DataFrame)


def test_cartesian_dc_dd(sample_data, colv, args):
    """create a dask cartesian df. If gpus,
    output should be a dask_cudf df, else dask df"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    dc_ddf = dq.transform.scatter_and_gpu(sample_data, args)
    is_dc_dd(dc_ddf, args.gpus)
    sm = dq.cartesian.dask_cudf_cartesian(dc_ddf, colv, args)
    is_dc_dd(dc_ddf, args.gpus)


def test_indexized_ddf(sample_data, counts_dict, colv, args):
    """create an indexed df. If gpus,
    output should be a dask_cudf df, else dask df"""
    ddf = dd.from_pandas(sample_data, npartitions=args.partitions)
    ddf = dq.cartesian.indexize(ddf, counts_dict, colv)
    assert isinstance(ddf, dd.DataFrame)


def test_indexized_dc_dd(sample_data, counts_dict, colv, args):
    """create an indexed df. If gpus,
    output should be a dask_cudf df, else dask df"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    dc_ddf = dq.transform.scatter_and_gpu(sample_data, args)
    dc_ddf = dq.cartesian.indexize(dc_ddf, counts_dict, colv)
    is_dc_dd(dc_ddf, args.gpus)


def test_indexized_cartesian(sample_data, counts_dict, colv, args):
    """create an indexed cartesian df. If gpus,
    output should be a dask_cudf df, else dask df"""
    if bool(args.gpus) and not args.has_gpu:
        print("unable to test dask_cudf portion")
        pytest.skip()
    else:
        dc_ddf = dq.transform.scatter_and_gpu(sample_data, args)
        sm = dq.cartesian.dask_cudf_cartesian(dc_ddf, colv, args)
        sm = dq.cartesian.indexize(sm, counts_dict, colv)


def test_sparse_matrix(
    final_data, sample_data, cols_dict, counts_dict, colk, args
):
    """create an indexed cartesian df. If gpus,
    output should be a dask_cudf df, else dask df"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    dc_ddf = dq.transform.scatter_and_gpu(sample_data, args)
    sm = dq.cartesian.sparse_cudf_matrix(
        dc_ddf, cols_dict, counts_dict, colk, args
    )
    is_dc_dd(sm, args.gpus)
    if bool(args.gpus):
        sm = dq.transform.dc_sort_index(sm).compute().to_pandas()
    else:
        sm = sm.compute()
    assert sm.equals(final_data)
