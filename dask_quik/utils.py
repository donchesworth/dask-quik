import os
import time
import dask.dataframe as dd
from dask.distributed import Client
from subprocess import check_output, STDOUT
from typing import Union, Optional, Dict, Any
import warnings
import logging
import sys

try:
    import dask_cudf as dc
    import cudf
    from dask_cuda import LocalCUDACluster
except ImportError:
    warnings.warn(
        "dask_quik.utils unable to import GPU libraries, \
        using dask instead of dask_cudf"
    )
    import dask_quik.dummy as dc

dc_dd = Union[dc.DataFrame, dd.DataFrame]
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def setup_cluster(use_gpus: Optional[int] = None, worker_space: Optional[str] = None) -> Client:
    """Setup a dask distributed cuda cluster on GPUs. Sometimes
    if this has been done before, the legacy data needs to be
    removed.

    Args:
        use_gpus (int, optional): Number of gpus to use
        worker_space (str, optional): The location of the
        dask-worker-space director. Defaults to None.

    Returns:
        Client: A distributed cluster to contain dask or dask_cudf objects.
    """
    if worker_space is not None and os.path.exists(worker_space):
        os.system(f"rm -r {worker_space}/*")
    if use_gpus is None:
        use_gpus = gpus()
    if bool(use_gpus):
        cluster = LocalCUDACluster(n_workers=use_gpus,
                                   local_directory=worker_space)
        logger.info("GPU cluster has been established")
    else:
        cluster = None
        logger.info("No GPUs found - running cluster on CPU.")
    return Client(address=cluster)


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
        str: A formatted string with the seconds for execution time.
    """
    return str(round(time.time() - start_time, 2)) + " seconds"


def row_str(dflen: int) -> str:
    """[summary]

    Args:
        dflen (int): [description]

    Returns:
        str: A formatted string with the number of rows (in millions).
    """
    return str(round(dflen / 1000000, 1)) + "M rows"


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


def gpus() -> int:
    """Determines if there are GPUs on the system, and
    how many

    Returns:
        int: number of GPUs on the system
    """
    gpu_cmd = "nvidia-smi -L | wc -l"
    gpus = check_output(gpu_cmd, stderr=STDOUT, shell=True)
    gpus = int(gpus.splitlines()[-1])
    logger.info(f"{gpus} gpus found by polling nvidia-smi")
    return None if gpus == 0 else gpus