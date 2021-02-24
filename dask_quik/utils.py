import os
import time
import dask.dataframe as dd
from dask_cuda import LocalCUDACluster
from dask.distributed import Client
from argparse import Namespace
from typing import Union, Optional, Dict, Any
import warnings
try:
    import dask_cudf as dc
    import cudf
except ImportError:
    warnings.warn(
        "dask_quik.utils unable to import GPU libraries, \
        using dask instead of dask_cudf"
    )
    import dask_quik.dummy as dc

dc_dd = Union[dc.DataFrame, dd.DataFrame]

def setup_cluster(worker_space: Optional[str] = None) -> Client:   
    """ Setup a dask distributed cuda cluster on GPUs. Sometimes
    if this has been done before, the legacy data needs to be 
    removed.

    Args:
        worker_space (Optional[str], optional): The location of the 
        dask-worker-space director. Defaults to None.

    Returns:
        Client: A distributed cluster to contain dask or dask_cudf objects.
    """    
    if worker_space is not None and os.path.exists(worker_space):
        os.system(f"rm -r {worker_space}/*")
    cluster = LocalCUDACluster(local_directory=worker_space)
    return Client(cluster)


def subdict(cols: Dict[str, Any], subkeys: list) -> Dict[str, Any]:
    """Take a dictionary and subset it based on a list of keys.

    Args:
        full_dict (Dict[str, Any]): The full dictionary to be subsetted
        subkeys (list): list of keys to be contained in the subset

    Returns:
        Dict[str, Any]: A subsetted dictionary
    """    
    return {key: cols[key] for key in subkeys}


def sec_str(start_time: float) -> str:
    """[summary]

    Args:
        start_time (float): output from time.time() earlier in the program

    Returns:
        str: A formatted string with the seconds that a code segment took to run
    """
    return str(round(time.time() - start_time, 2)) + " seconds"


def row_str(dflen: int) -> str:
    """[summary]

    Args:
        dflen (int): [description]

    Returns:
        str: A formatted string with the number of rows (in millions).
    """    
    return str(round(dflen/1000000, 1)) + "M rows"


def shrink_dtypes(df: dc_dd, df_dtypes: dict) -> dc_dd:
    """reduces the data type of a set of series.

    Args:
        df (dc_dd): data frame with series to reduce dtypes
        df_dtypes (dict): key, value pairs of column names and their new data types

    Returns:
        dc_dd: dask_cudf or dd with reduced dtypes
    """    
    for colname, newdtype in df_dtypes.items():
        df[colname] = df[colname].astype(newdtype)
    return df