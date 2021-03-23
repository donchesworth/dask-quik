import pandas as pd
import numpy as np
import dask.dataframe as dd
import dask.array as da
from typing import Union, Tuple, Optional, Dict, Any, List
from argparse import Namespace
import warnings
import dask_quik.utils as du
import dask_quik.transform as dt
import time

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


def add_cartesian_dummy(odf: dc_dd) -> dc_dd:
    """create a dummy dataframe with a column "cartesian" with the value
    0 to force a cartesian join using merge.

    Args:
        odf (dc_dd): original dask_cudf or dd

    Returns:
        dc_dd: dummy dask_cudf or dd
    """
    odf["cartesian"] = 0
    odf = du.shrink_dtypes(odf, {"cartesian": "int8"})
    return odf


def dask_cudf_cartesian(odf: dc_dd, colv: List[str], args: Namespace) -> dc_dd:
    """Create an cartesian product of two columns using a dummy inner join.
    This was the best option for dask and cudf. The function:
    - adds a dummy column with value 0
    - creates a distinct list of each column (with dummy)
    - does an inner join of the two lists on the dummy column

    Args:
        odf (dc_dd): dask_cudf or dd to cross-join
        colv (Dict[str, Any]): dictionary of column type and column name
        p (int): number of partitions

    Returns:
        dc_dd: dask_cudf or dd cartesian product of the original dataframe
    """
    odf = add_cartesian_dummy(odf)
    if bool(args.gpus):
        gv_list = [
            odf[[col, "cartesian"]]
            .sort_values(col)
            .drop_duplicates(split_out=args.partitions)
            .reset_index(drop=True)
            for col in colv
        ]
    else:
        gv_list = [
            odf[[col, "cartesian"]]
            .set_index(col)
            .persist()
            .reset_index()
            .drop_duplicates(split_out=args.partitions)
            .reset_index(drop=True)
            for col in colv
        ]
    gv_list[1] = gv_list[1].compute()
    odf = gv_list[0].merge(gv_list[1], how="inner", on="cartesian")
    odf = odf.drop("cartesian", axis=1)
    return odf


def indexize(
    dc: dc_dd, cnts: Dict[str, int], colv: List[str], rmcols: Optional[bool] = False
) -> dc_dd:
    """create an index for the permutation of all values. Usually two columns,
    this takes the max of each, and indexes from 0 to max all the possible permutations.

    Args:
        dc (dc_dd): the dask_cudf or dask df to be indexized
        cnts (Dict[str, int]): the counts of each column for permutations
        colv (List[str]): the columns to be indexized
        rmcols (Optional[bool], optional): whether the original columns should be
        removed. Defaults to False.

    Returns:
        dc_dd: The final indexized dask_cudf or dask dataframe
    """
    col2max = cnts[colv[1]]
    idx = colv[0][:1] + colv[1][:1] + "_index"
    dc[idx] = (dc[colv[0]] - 1) * col2max + dc[colv[1]] - 1
    dc = dc.persist()
    dc = dc.set_index(idx)
    if rmcols:
        dc = dc.drop(colv, axis=1)
    return dc


def sparse_cudf_matrix(
    gdf: dc_dd,
    cols: Dict[str, Any],
    cnts: Dict[str, int],
    colk: List[str],
    args: Namespace,
) -> dc_dd:
    """Create an sparse matrix of true customer data within the total
    universe of customer data (cartesian product).
    - determines column names
    - creates the true set
    - creates the sparse matrix (cudf_cartesian)
    - notes which in the sparse matrix are true, false

    Args:
        gdf (dc_dd): dataframe to create sparse matrix
        cols (Dict[str, Any]): dictionary of column type and column name
        cnts (Dict[str, int]): [description]
        colk (List[str]): [description]
        args (Namespace): [description]

    Returns:
        dc_dd: dask_cudf or dd final sparse matrix
    """
    # column name stuff
    st = time.time()
    tcol = "_".join(colk)
    colv = [cols[k] for k in colk]
    # create the cartesian product
    gdf = gdf[colv].drop_duplicates(split_out=args.partitions)
    sm = dask_cudf_cartesian(gdf.copy(), colv, args)
    print("created the " + tcol + " cartesian product in " + du.sec_str(st), flush=True)
    sm = indexize(sm, cnts, colv)
    # idx = sm.index.name
    gdf = indexize(gdf, cnts, colv)
    gdf[tcol] = True
    if not bool(args.gpus):
        colv = None
        gdf = gdf[[tcol]]
    sm = sm.merge(
        gdf,
        how="left",
        on=colv,
        npartitions=args.partitions,
        left_index=True,
        right_index=True,
    )
    print("indexed and merged in " + du.sec_str(st), flush=True)
    sm = dt.dc_sort_index(sm)
    del gdf
    sm[tcol] = sm[tcol].fillna(False)
    return sm
