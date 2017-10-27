
# System integration tests

These tests require a `python` and `pytest` installation. 
`python` is usually installed on linux by default.
You can install `pytest` via `pip` like this:

    # install pip, if you need to
    sudo dnf install python2-pip
    # use pip to install pytest
    sudo pip install pytest
    
You can then run these tests, either from python directly, like this: 

    python test_deployments.py
    
or more conveniently using `pytest`, like this:

    # run all the test_* functions and Test* classes in found test_*.py files
    pytest
    
## Bootstrapping a cluster
    
These assume you have a cluster (`oc`, `minikube`, `minishift`) running. If not, the test
can bootstrap a cluster for you:

    pytest --bootstrap=minikube
    
If it bootstraps a cluster then the cluster will be stopped at the end of the tests. 
Otherwise, if the tests uses a running cluster, the cluster will be left up.
    
## Standard output
    
By default `pytest` will capture the standard output and error streams from your test and display them if the test fails. 
When working interactively, this can make it harder to understand what a test is doing. 
Use `pytest -s` to print the standard streams in real time:

    # as above, but don't capture standard error and output for display after failed tests
    pytest -s

## Exiting immediately

If a test is failing it can be useful to 1) exit immediately and 2) leave the cluster in the 
failing state, rather than cleaning up after the test.

    pytest -x --no-clean

The tests themselves require a local installation of the `oc` tools, `minikube` and `kubectl`.