import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--gpus", type=int, default=0, help="number of gpus per node"
    )

def pytest_generate_tests(metafunc):
    metafunc.parametrize("gpus", [0,1])

# @pytest.fixture
# def gpus(request):
#     return request.config.getoption("--gpus")
