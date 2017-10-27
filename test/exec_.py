
import subprocess
import threading
import logging

#logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger("exec")

class ExecTimeoutException(Exception):
    """
    Exception thrown to indicate a subprocess took too long to complete.
    """
    def __init__(self, exit_code, cmd, output, timeout_secs):
        self.exit_code = exit_code
        self.cmd = cmd
        self.output = output
        self.timeout_secs = timeout_secs

    def __str__(self):
        return "%s timed-out after %s seconds with output %s" % (" ".join(self.cmd), self.timeout_secs, self.output)

def exec_status(cmd, timeout_secs=None):
    """
    Execute cmd in a subprocess, wait for it to complete, and return the exit code.

    :param cmd: The command to execute.
    :param timeout_secs: raise a :py:class:`ExecTimeoutException` if the subprocess doesn't finish within that timeout.
    :return: The status code of the process.
    """
    timer = None
    timedout = threading.Event()
    process = subprocess.Popen(cmd)
    LOGGER.info("exec pid=%s timeout=%s cmd=%s", process.pid, timeout_secs, " ".join(cmd))
    if timeout_secs:
        def on_timeout():
            if not process.poll():
                LOGGER.info("timeout pid=%s", process.pid)
                timedout.set()
                process.terminate()
        timer = threading.Timer(float(timeout_secs), on_timeout)
        timer.start()
    exit_code = process.wait()
    LOGGER.info("exit pid=%s code=%s", process.pid, exit_code)
    if timer:
        timer.cancel()
    if timedout.is_set():
        raise ExecTimeoutException(exit_code, cmd, None, timeout_secs)
    return exit_code

def exec_ok(cmd, timeout_secs=None):
    """
    Execute cmd in a subprocess, wait for it to complete, and raise Exception if the exit status was non-zero.

    :param cmd: The command to execute.
    :param timeout_secs: raise a :py:class:`ExecTimeoutException` if the subprocess doesn't finish within that timeout.
    """
    sc = exec_status(cmd, timeout_secs)
    if sc != 0:
        raise Exception("Expected zero status code from %s, but got %s" %(" ".join(cmd), sc))

def exec_output(cmd, raise_on_error=True, timeout_secs=None):
    """
    Execute cmd in a subprocess, wait for it to complete and return its output.

    :param cmd: The command to execute.
    :param raise_on_error: If true and the process exits with non-zero status, raise subprocess.CalledProcessError.
    :param timeout_secs: raise a :py:class:`ExecTimeoutException` if the subprocess doesn't finish within that timeout.
    :return: The output of the command (what it wrote to standard output).
    """
    timer = None
    timedout = threading.Event()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    LOGGER.info("exec pid=%s timeout=%s cmd=%s", process.pid, timeout_secs, " ".join(cmd))
    if timeout_secs:
        def on_timeout():
            if not process.poll():
                LOGGER.info("timeout pid=%s", process.pid)
                process.terminate()
                timedout.set()
        timer = threading.Timer(float(timeout_secs), on_timeout)
        timer.start()
    output, unused_err = process.communicate()
    exit_code = process.poll()
    LOGGER.info("exit pid=%s code=%s", process.pid, exit_code)
    if timer:
        timer.cancel()
    if timedout.is_set():
        raise ExecTimeoutException(exit_code, cmd, output, timeout_secs)
    if exit_code and raise_on_error:
        LOGGER.info("pid=%s output=%s", process.pid, output)
        raise subprocess.CalledProcessError(exit_code, cmd, output=output)
    return output

