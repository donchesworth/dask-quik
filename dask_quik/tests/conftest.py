from pathlib import Path
import pytest
import pandas as pd
import json
from argparse import Namespace
from os import system
import warnings
import dask_quik as dq

TESTDIR = Path(__file__).parent
SAMPLE = TESTDIR.joinpath("sample_data.json")
PROD = TESTDIR.joinpath("prod_data.json")
FINAL = TESTDIR.joinpath("final_data.json")
REAL = TESTDIR.joinpath("real_data.json")


def pytest_addoption(parser):
    # parser.addoption("--gpus", type=int, default=0,
    # help="number of gpus per node")
    parser.addoption(
        "--has_gpu",
        action="store_true",
        default=bool(dq.utils.gpus()),
        dest="has_cpu",
        help="testing on a node with a GPU",
    )


def pytest_generate_tests(metafunc):
    metafunc.parametrize("gpus", [0, 1])


@pytest.fixture
def has_gpu(request):
    """argument whether there actually is a gpu"""
    return request.config.getoption("--has_gpu")


@pytest.fixture
def args(gpus):
    """sample args namespace"""
    args = Namespace()
    args.gpus = gpus
    args.has_gpu = system("nvidia-smi -L") == 0
    if not args.has_gpu:
        warnings.warn(
            "GPU not found, setting has_gpu to False. \
            Some tests will be skipped"
        )
    args.partitions = 2
    return args


@pytest.fixture(scope="module")
def cols_dict():
    """sample column names"""
    cols = {
        "user": "user_number",
        "item": "item_id",
        "prod": "product_id",
        "recent": "recency",
        "late": "latest",
    }
    return cols


@pytest.fixture(scope="module")
def ui_cols(cols_dict):
    """sample column names"""
    uicols = dq.utils.subdict(cols_dict, ["user", "item"])
    return uicols


@pytest.fixture(scope="session")
def sample_data():
    """sample user/item dataset"""
    with open(SAMPLE) as f:
        df = pd.DataFrame(json.load(f))
    return df


@pytest.fixture(scope="session")
def prod_data():
    """sample user/item dataset"""
    with open(PROD) as f:
        df = pd.DataFrame(json.load(f))
    return df


@pytest.fixture(scope="session")
def real_data():
    """sample user/item dataset"""
    with open(REAL) as f:
        df = pd.DataFrame(json.load(f))
    return df


@pytest.fixture(scope="session")
def final_data():
    """final user/item dataset"""
    with open(FINAL) as f:
        df = pd.DataFrame(json.load(f))
    df.index.names = ["ui_index"]
    return df
