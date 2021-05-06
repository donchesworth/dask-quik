import pandas as pd
import dask
import dask.dataframe as dd
import dask_ml
from typing import Union
from argparse import Namespace
import warnings
import time
import dask_quik.utils as du
from argparse import Namespace
from typing import Dict, Any

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


def tt_leave_one_out(
    ddf: dd.DataFrame, cols: Dict[str, Any], args: Namespace
) -> Dict[str, pd.DataFrame]:
    """Using the leave one out method, "leaving out" the most recent
    record for test, and creating a training set with all the remaining
    records. This is common for recommender systems.

    Args:
        ddf (dd.DataFrame): The original dataframe
        cols (Dict[str, Any]): A dictionary of column keys and values
        args (Namespace): argumnets for train, (valid), test, and seed.

    Returns:
        Dict[str, pd.DataFrame]: A dict of pandas dataframes for train, (valid),
        and test.
    """
    te_ddf = ddf[(ddf.latest == 1) | (ddf.rating == -1)]
    tr_ddf = ddf[~((ddf.latest == 1) | (ddf.rating == -1))]
    tr_ddf, vl_ddf = dask_ml.model_selection.train_test_split(
        tr_ddf,
        train_size=args.train_size,
        test_size=args.test_size,
        random_state=args.seed,
    )
    tt_dict = {"train": tr_ddf, "test": te_ddf}
    if args.valid_size > 0:
        tt_dict['valid'] = vl_ddf
    colk = ["user", "item", "late", "rate"]
    t_cols = list(du.subdict(cols, colk).values())
    tt_dict = {kt: dask.compute(ddf[t_cols]) for kt, ddf in tt_dict.items()}
    return tt_dict
