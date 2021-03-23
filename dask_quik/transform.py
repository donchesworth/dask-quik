import pandas as pd
import dask.dataframe as dd
from typing import Union
from argparse import Namespace
import warnings
import time
import dask_quik.utils as du

try:
    import dask_cudf as dc
    import cudf
except ImportError:
    warnings.warn(
        "dask_quik.transform unable to import GPU libraries, \
        importing a dummy dc.DataFrame"
    )
    import dask_quik.dummy as dc

# allow the ability of a dask_cudf.DataFrame or a dd.DataFrame
dc_dd = Union[dc.DataFrame, dd.DataFrame]


def scatter_and_gpu(cdf: pd.DataFrame, args: Namespace) -> dc_dd:
    """First convert a pandas dataframe to a dask dataframe (scatter)
    and then if there's a gpu, move it to the gpu.

    Args:
        cdf (pd.DataFrame): initial df
        args (Namespace): arguments

    Returns:
        cdf dc_dd: the final df
    """
    cdf = dd.from_pandas(cdf, npartitions=args.partitions)
    if bool(du.gpus()):
        cdf = cdf.map_partitions(cudf.DataFrame.from_pandas)
    return cdf


def gpu_sort_cpu(gdf: dc.DataFrame, idx_col: str) -> dd.DataFrame:
    """First sort the dask_cudf DataFrame by an index column,
    then push the partitions to the CPU as a dask DataFrame

    Args:
        gdf (dask_cudf.DataFrame): The unsorted dask_cudf DataFrame on GPU
        idx_col (str): The index column to be sorted

    Returns:
        dd.DataFrame: The sorted, partitioned dask DataFrame on CPU
    """
    st = time.time()
    gdf = gdf.set_index(idx_col)
    cdf = gdf.map_partitions(lambda df: df.to_pandas())
    print("sorted and moved to cpu in " + du.sec_str(st))
    return cdf


def dc_sort_index(dcdf: dc.DataFrame) -> dc.DataFrame:
    """emulate pandas sort_index for a dask_cudf DataFrame.
    This pulls the index into a column, then uses set_index.

    Args:
        dcdf (dc.DataFrame): original dask_cudf DataFrame

    Returns:
        dc.DataFrame: newly sorted index for dask_cudf DataFrame
    """
    idx = dcdf.index.name
    dcdf = dcdf.reset_index().rename(columns={"index": idx})
    dcdf = dcdf.set_index(idx)
    return dcdf
