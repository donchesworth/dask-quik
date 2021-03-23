import pytest
import dask_quik as dq
from pathlib import Path
from time import time, sleep
from pynvml.nvml import NVMLError


def test_setup_cluster(args):
    """setup a dask cluster"""
    print(args)
    worker_path = Path.cwd().joinpath("dask-worker-space")
    worker_path.mkdir(parents=True, exist_ok=True)
    worker_path.joinpath("tmp").touch(exist_ok=True)
    if bool(args.gpus) and not args.has_gpu:
        with pytest.raises(
            (KeyError, OSError, NVMLError, AttributeError, NameError)
        ):
            client = dq.utils.setup_cluster(worker_space=worker_path)
        return
    elif bool(args.gpus):
        client = dq.utils.setup_cluster(worker_space=worker_path)
    else:
        pytest.skip()


def test_subdict(cols_dict, args):
    """create a subdict"""
    tsd = dq.utils.subdict(cols_dict, ["item"])
    fd = {"item": "item_id"}
    assert tsd == fd


def test_sec_str(args):
    """print a sec_str"""
    start_time = time()
    sleep(2)
    print(dq.utils.sec_str(start_time))


def test_row_str(sample_data, args):
    """print a row_str"""
    print(dq.utils.row_str(sample_data.shape[0]))
