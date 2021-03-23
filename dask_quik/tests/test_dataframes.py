import pytest
import pandas as pd
import dask.dataframe as dd
import dask_quik as dq
import dask_quik.dummy as dc


def t_dc_dd(dc_ddf, dc_ddt):
    if bool(dc_ddt):
        import dask_cudf

        assert isinstance(dc_ddf, dask_cudf.DataFrame)
    else:
        assert isinstance(dc_ddf, dd.DataFrame)


def test_dummy_df(args):
    dum_df = dc.DataFrame()
    # dum_call = dum_df.__call__
    # dum_attr = dum_df.__getattr__


def test_dask_df(sample_data, args):
    """create a dask df"""
    ddf = dd.from_pandas(sample_data, npartitions=args.partitions)
    assert isinstance(ddf, dd.DataFrame)


def test_cudf_df(sample_data, args):
    """create a cudf df"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    elif bool(args.gpus):
        import cudf

        cdf = cudf.from_pandas(sample_data)
        assert isinstance(cdf, cudf.DataFrame)
    else:
        dum_df = dc.DataFrame()
        assert isinstance(dum_df, dc.DataFrame)


def test_dask_cudf(sample_data, args):
    """create a dask_cudf df"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import dask_cudf
        return
    elif bool(args.gpus):
        import cudf
        import dask_cudf

        cdf = cudf.from_pandas(sample_data)
        dcdf = dask_cudf.from_cudf(cdf, npartitions=args.partitions)
        assert isinstance(dcdf, dask_cudf.DataFrame)
    else:
        dum_df = dc.DataFrame()
        assert isinstance(dum_df, dc.DataFrame)
