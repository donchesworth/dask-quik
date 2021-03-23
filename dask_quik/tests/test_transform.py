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


def test_gpu_sort_cpu(sample_data, args):
    """create a dc_dd using scatter_and_gpu.
    If gpus=1, scatter_and_gpu will create a
    dask_cudf.DataFrame, then it will be sorted
    and sent back to the CPU. If gpus=0, we'll
    do the same but all on CPU."""
    df = sample_data.copy()
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    dc_ddf = dq.transform.scatter_and_gpu(df, args)
    is_dc_dd(dc_ddf, args.gpus)
    df = df.set_index("item_id").sort_index()
    if bool(args.gpus):
        ddf = dq.transform.gpu_sort_cpu(dc_ddf, "item_id")
        print(ddf.compute())
    else:
        ddf = dc_ddf.set_index("item_id")
        print(ddf.compute())
    assert ddf.compute().equals(df)


def test_dc_sort_index(sample_data, args):
    """create a dc_dd using scatter and gpu"""
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(ImportError):
            import cudf
        return
    df = sample_data.copy()
    df.index.names = ["ui_index"]
    dc_ddf = dq.transform.scatter_and_gpu(df, args)
    is_dc_dd(dc_ddf, args.gpus)
    if bool(args.gpus):
        dc_ddf = dc_ddf.sort_values("item_id")
        dc_ddf = dq.transform.dc_sort_index(dc_ddf)
        dc_ddf = dc_ddf.compute().to_pandas()
    else:
        dc_ddf = dc_ddf.reset_index().set_index("item_id")
        dc_ddf = dc_ddf.reset_index().set_index("ui_index")
        dc_ddf = dc_ddf[["user_number", "item_id"]].compute()
    assert dc_ddf.equals(df.sort_index())
