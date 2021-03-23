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


def prune_rows(
    dc: dc_dd,
    cols: Dict[str, Any],
    grby_cols: List[str],
    p: int,
    **agg_groups: Optional[List[str]]
) -> dc_dd:
    """When it's necessary to remove slight duplicates, this
    function will group by certain columms, then take the max of some,
    the min of some, and the average of others.

    Args:
        dc (dc_dd): original dask or dask_cudf DataFrame
        cols (Dict[str, Any]): a dictionary of column keys and values
        grby_cols (List[str]): column keys to be grouped by
        p (int): [description]
        **agg_groups: column key value pairs, where the key is the agg
        type (avg, min, max) and the value is the column key to be
        aggregated

    Returns:
        dc_dd: A final pruned dask or dask_cudf DataFrame
    """
    gr = list(du.subdict(cols, grby_cols).values())
    # First pull the column values, then spread them across the aggs, then
    # combine them into a flat dictionary
    aggs = {
        k: list(du.subdict(cols, v).values()) for k, v in agg_groups.items()
    }
    aggs = [dict.fromkeys(v, k) for k, v in aggs.items()]
    aggs = {k: ad[k] for ad in aggs for k in ad}
    dc = dc.groupby(gr).agg(aggs)[list(aggs.keys())].reset_index()
    return dc


def pr(dc, cols, grby_cols, p: int, **kwargs):
    print(kwargs)
