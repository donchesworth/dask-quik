import pytest
import dask_quik as dq
import dask.dataframe as dd
import pandas as pd


def is_dc_dd(dc_ddf, dc_ddt):
    if bool(dc_ddt):
        import dask_cudf

        assert isinstance(dc_ddf, dask_cudf.DataFrame)
    else:
        assert isinstance(dc_ddf, dd.DataFrame)


def test_merge(sample_data, prod_data, cols_dict, args):
    """create two dask or dask_cudf DataFrames
    and then combine them"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    smp_df = dq.transform.scatter_and_gpu(sample_data, args)
    is_dc_dd(smp_df, args.gpus)
    prd_df = dq.transform.scatter_and_gpu(prod_data, args)
    is_dc_dd(prd_df, args.gpus)
    sm = dq.combine.left_merge_cols(smp_df, prd_df, cols_dict, ["item"])
    is_dc_dd(sm, args.gpus)
    print(sm)


def test_prune(real_data, cols_dict, args):
    dc = dq.transform.scatter_and_gpu(real_data, args)
    is_dc_dd(dc, dq.utils.gpus())
    dcp = dq.combine.prune_rows(
        dc,
        cols_dict,
        grby_cols=["user", "item", "prod"],
        p=args.partitions,
        max=["recent"],
        min=["late"],
    )
    is_dc_dd(dcp, dq.utils.gpus())
    slist = [45671158, 42599686, 85, -6, 443]
    assert list(dcp.sum().compute()) == slist
    rlist = [411070, 6038, 1, 0, 45]
    testrow = dcp[(dcp.user_number == 411070) & (dcp.product_id == 1)]
    assert list(testrow.compute().T.iloc[:, 0]) == rlist
