import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--gpus", type=int, default=0, help="number of gpus per node"
    )


def pytest_addoption(parser):
    parser.addoption(
        "--has_gpu", action="store_true", help="testing on a node with a GPU"
    )


def pytest_generate_tests(metafunc):
    metafunc.parametrize("gpus", [0,1])


@pytest.fixture
def has_gpu(request):
    return request.config.getoption("--has_gpu")
