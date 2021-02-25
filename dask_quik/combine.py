import pandas as pd
import numpy as np
import dask.dataframe as dd
import dask.array as da
from typing import Union, Tuple, Optional, Dict, Any, List
from argparse import Namespace
import warnings
import dask_quik.utils as du

try:
    import dask_cudf as dc
    import cudf
except ImportError:
    warnings.warn(
        "dask_quik.cartesian unable to import GPU libraries, \
        importing a dummy dc.DataFrame"
    )
    import dask_quik.dummy as dc

# allow the ability of a dask_cudf.DataFrame or a dd.DataFrame
dc_dd = Union[dc.DataFrame, dd.DataFrame]


def left_merge_cols(
    left: dc_dd, right: dc_dd, cols: Dict[str, Any], col_list: List[str]
) -> dc_dd:
    """Simple left merge.

    Args:
        left (dc_dd): left DataFrame to merge
        right (dc_dd): right DataFrame to merge
        cols (Dict[str, Any]): dictionary of all column types and column names
        col_list (List[str]): names of columns to merge on

    Returns:
        dc_dd: final merged dataframe
    """
    on_cols = list(du.subdict(cols, col_list).values())
    left = left.merge(right, how="left", on=on_cols)
    return left
