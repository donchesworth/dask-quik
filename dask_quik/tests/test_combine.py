import pytest
import dask_quik as dq
import dask.dataframe as dd

def is_dc_dd(dc_ddf, dc_ddt):
    if bool(dc_ddt):
        import dask_cudf
        assert isinstance(dc_ddf, dask_cudf.DataFrame)
    else:
        assert isinstance(dc_ddf, dd.DataFrame)


def test_combine(sample_data, prod_data, cols_dict, args):
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
    sm = dq.combine.left_merge_cols(smp_df, prd_df, cols_dict, ['item'])
    is_dc_dd(sm, args.gpus)
    print(sm)