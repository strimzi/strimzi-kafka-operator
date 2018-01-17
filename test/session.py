
import pytest
import os.path
import time
import logging
from exec_ import *

LOGGER = logging.getLogger("session")

class SessionTimeoutException(Exception):
    pass

class BaseSession(object):
    """Baseclass for managing a cluster"""

    def __init__(self, cmd):
        self.cmd = cmd
        self.finalizers = []

    def _exec(self, cmd_args):
        try:
            exec_output(cmd_args)
            return 0
        except subprocess.CalledProcessError as e:
            LOGGER.warn("output: %s" % (e.output))
            return e.returncode

    def apply(self, *files):
        "oc/kubectl apply -f files"
        for f in files:
            if not os.path.exists(f):
                raise Exception("%s does not exist" % f)
        exec_ok([self.cmd, "apply"] + [item for sublist in (["-f", f] for f in files) for item in sublist])

    def create(self, *files):
        "oc/kubectl create -f files"
        for f in files:
            if not os.path.exists(f):
                raise Exception("%s does not exist" % (f))
        exec_ok([self.cmd, "create"] + [item for sublist in (["-f", f] for f in files) for item in sublist])

    def delete(self, *options):
        exec_ok([self.cmd, "delete"] + list(options))

    def get(self, *what):
        "oc/kubectl get what"
        return exec_output([self.cmd, "get"] + list(what)).splitlines()[1:]

    def await_endpoint(self, endpoint_name, required_number, timeout=300):
        """
        Wait for the number of endpoints with the given name to reach the given number,
        throwing a :py:class:`SessionTimeoutException` if it takes more than the given timeout.
        """
        wait = True
        start = time.time()
        LOGGER.info("Waiting for up to %ss for %s endpoints called %s", timeout, required_number, endpoint_name)
        while wait:
            if (time.time() - start > timeout):
                raise SessionTimeoutException("timeout after %ss waiting for %s endpoints with name %s" %
                                       (timeout, required_number, endpoint_name))
            time.sleep(5)
            output = self.get("endpoints", endpoint_name)
            d = dict([y.split()[:2] for y in output])
            num_endpoints = len(d[endpoint_name].split(","))
            LOGGER.debug("%s of %s endpoints, remaining timeout=%s", num_endpoints, required_number, start + timeout - time.time())
            wait = num_endpoints != required_number

    def exec_(self, pod, cmd, container=None, cmd_timeout_secs=30.0):
        """
        oc/kubectl exec pod -c container -i -t -- CMD
        """
        c = [self.cmd, "exec", pod, "-i", "-t"]
        if container:
            c += ["-c", container]
        c += ["--"]
        return exec_output(c + [str(a) for a in cmd], timeout_secs=cmd_timeout_secs)

    def addfinalizer(self, finalizer):
        """
        Add a nullary function to clean-up state when the session closes.
        :param finalizer: The finalizer function. This function should take no arguments.
        """
        self.finalizers.append(finalizer)

    def close(self):
        LOGGER.info("Closing session")
        for finalizer in self.finalizers:
            finalizer()
        self.delete("statefulset", "--all")
        self.delete("svc", "--all")


class OpenShiftSession(BaseSession):
    """A running openshift session, with deployments managed using the `oc` tool."""

    def __init__(self):
        super(OpenShiftSession, self).__init__("oc")

    def new_app(self, name):
        self._exec([self.cmd, "new-app", name])

    @property
    def is_openshift(self):
        return True

    def __str__(self):
        return "oc session"


class K8SSession(BaseSession):
    """A running kubernetes session, with deployments managed using the ``kubectl`` tool."""

    def __init__(self):
        super(K8SSession, self).__init__("kubectl")

    @property
    def is_openshift(self):
        return False

    def __str__(self):
        return "kubectl session"


@pytest.fixture
def session(request, cluster):
    sess = cluster.session()
    if not request.config.getoption("--no-clean"):
        request.addfinalizer(sess.close)
    return sess

