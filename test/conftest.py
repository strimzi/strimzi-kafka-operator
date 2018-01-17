import pytest

def pytest_addoption(parser):
    parser.addoption("--bootstrap-cluster",
                     action="store",
                     #choices=["oc", "minikube"],
                     #metavar="type",
                     help="Start a cluster of the given type, fail if a cluster is already running.")
    parser.addoption("--leave-cluster-up",
                     action="store_true",
                     default=False,
                     help="With --bootstrap-cluster: don't stop the cluster at the end of the tests. Without --bootstrap-cluster: noop.")
    parser.addoption("--no-clean",
                     action="store_true",
                     default=False,
                     help="Don't clean up between each test. Only really useful when running a single (failing) test.")