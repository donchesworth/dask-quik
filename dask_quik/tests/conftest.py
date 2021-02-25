import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--gpus", type=int, default=0, help="number of gpus per node"
    )


@pytest.fixture
def gpus(request):
    return request.config.getoption("--gpus")
