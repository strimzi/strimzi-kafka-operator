
import pytest
from exec_ import *
from session import *

class OpenShiftCluster(object):
    """
    Manages a local OpenShift cloud using `oc cluster up` etc.
    """

    name = "openshift"

    @staticmethod
    def start():
        exec_status(["oc", "cluster", "up"])

    @staticmethod
    def stop():
        exec_status(["oc", "cluster", "down"])

    @staticmethod
    def is_running():
        try:
            return exec_status(["oc", "cluster", "status"]) == 0
        except OSError:
            return False

    @staticmethod
    def session():
        return OpenShiftSession()

class MinikubeCluster(object):
    """
    Manages a local kubernetes cluster using `minikube start`, etc.
    """

    name = "minikube"

    @staticmethod
    def start():
        exec_status([MinikubeCluster.name, "start"])

    @staticmethod
    def stop():
        exec_status([MinikubeCluster.name, "stop"])

    @staticmethod
    def is_running():
        try:
            output = exec_output([MinikubeCluster.name, "status"], raise_on_error=False)
            return "cluster: Running" in output.splitlines()
        except OSError:
            return False

    @staticmethod
    def session():
        return K8SSession()

class MinishiftCluster(object):
    """
    Manages a local OpenShift cluster using `minishift start`, etc.
    """

    name = "minishift"

    @staticmethod
    def start():
        exec_status([MinishiftCluster.name, "start"])

    @staticmethod
    def stop():
        exec_status([MinishiftCluster.name, "stop"])

    @staticmethod
    def is_running():
        try:
            output = exec_output([MinishiftCluster.name, "status"], raise_on_error=False)
            return "cluster: Running" in output.splitlines()
        except OSError:
            return False

    @staticmethod
    def session():
        return OpenShiftSession()

CLUSTER_TYPES = {c.name: c for c in [OpenShiftCluster, MinikubeCluster, MinishiftCluster]}

@pytest.fixture(scope="session")
def cluster(request):
    """
    Fixture to use the already running cluster, or start a local cluster, according to the given
    ``--bootstrap-cluster`` command line option.
    :param request:
    :return: The cluster
    """
    cluster_type = request.config.getoption("--bootstrap-cluster")
    if cluster_type:
        cluster = CLUSTER_TYPES[cluster_type]
        if not cluster:
            raise Exception(cluster_type + " is not supported")
        if cluster.is_running():
            raise Exception(cluster.name + " is already running")
        cluster.start()
        if request.config.getoption("--leave-cluster-up"):
            # TODO oc delete --all statefulsets,svc,po
            request.addfinalizer(cluster.stop)
        return cluster
    else:
        for cluster in CLUSTER_TYPES.itervalues():
            if cluster.is_running():
                return cluster
        raise Exception("no running cluster detected")