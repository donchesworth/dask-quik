import pytest
import dask.dataframe as dd
import dask_quik as dq

def is_dc_dd(dc_ddf, dc_ddt):
    if bool(dc_ddt):
        import dask_cudf
        assert isinstance(dc_ddf, dask_cudf.DataFrame)
    else: 
        assert isinstance(dc_ddf, dd.DataFrame)

def test_scatter_gpu(sample_data, args):
    """create a dc_dd using scatter and gpu"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    dc_ddf = dq.transform.scatter_and_gpu(sample_data, args)
    is_dc_dd(dc_ddf, args.gpus)


def gpu_sort_cpu(sample_data, args):
    """create a dc_dd using scatter and gpu"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    elif bool(args.gpus):
        dc_ddf = dq.transform.scatter_and_gpu(sample_data, args)
        dc_ddf = dq.transform.gpu_sort_cpu(dc_ddf, "user_number")
        is_dc_dd(dc_ddf, args.gpus)
    else:
        pytest.skip()
