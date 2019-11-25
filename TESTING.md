# Testing Strimzi

This document gives a detailed breakdown of the testing processes and testing options for Strimzi within system tests. 
For more information about build process see [Hacking document](https://github.com/strimzi/strimzi-kafka-operator/blob/master/HACKING.md).

<!-- TOC depthFrom:2 -->

- [Pre-requisites](#pre-requisites)
- [Package Structure](#package-structure)
- [Test Phases](#test-phases)
- [Available Test Groups](#available-test-groups)
- [Environment Variables](#environment-variables)
- [Use Remote Cluster](#use-remote-cluster)
- [Helper Script](#helper-script)
- [Running single test class](#running-single-test-class)
- [Skip Teardown](#skip-teardown)

<!-- /TOC -->

## Pre-requisites 

To run any system tests you need a Kubernetes or Openshift cluster available in your active kubernetes context. 
You can use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/), [minishift](https://www.okd.io/minishift/), [oc cluster up](https://github.com/openshift/origin) or [CodeReady Contaners](https://github.com/code-ready/crc) to have access to a cluster on your local machine. You can also access a remote cluster on any machine you want, but make sure you active kubernetes context points to it. 
For more information about remote cluster see [remote cluster](#use-remote-cluster) section.

Next requirement is to have build systemtest package dependencies, which are:

* test
* crd-annotations
* crd-generator
* api

You can achieve that with `mvn clean install -DskipTests` or `mvn clean install -am -pl systemtest -DskipTests` commands. 
The dependencies are needed because we use methods from `test` package and strimzi model from `api` package. 

## Package Structure 

Systemtest package is divided into `main` and `test` as usually. 
In `main` you can find all support classes, which are used in the tests. 
Modules worth for mention are:

* **annotations** — we have our own `@OpenShiftOnly` annotation, which check if current cluster is Openshift or not. All other annotations should be stored here.
* **clients** — clients implementations used in tests.
* **matchers** — it contains our matcher implementation for checking cluster operator logs. For more info see [matcher details](#matcher).
* **utils** — a lot of actions are the same for most of the tests, and we share them through utils class ad static methods. You can find here most of the useful methods.

And classes: 

* **Environment** — singleton class, which load environment variables, which are used in the tests.
* **Constants** — simple interface for store all constants used in the tests.
* **Resources** — heart of the systemtest package. In this class, you can find all methods needed for deploy Strimzi, Kafka, Kafka Connect, Kafka Bridge, Kafka Mirror Maker and all other useful resources. Current mechanism will ensure, that all resources created within these class will be deleted after tests.   

## Test Phases

In general, we have classic test phases: setup, exercise, test, teardown.

#### Setup

In this phase we do the following things:

* Create namespace
* Deploy Strimzi Cluster operator
* Deploy Kafka cluster or other components (optional)

The reason why last point is optional is because we have some test cases, where you want to have different kafka configuration for each test scenario and creation of Kafka cluster and other resources is done in test phase.

We create resources in Kubernetes cluster via `Resources` instance, which allow you to deploy all components and change them from their default configuration if needed via builder. 
Currently, we have two instances of `Resources` class — one for whole test class and one for test method.

Example:
```
    @BeforeAll
    void createClassResources() {
        prepareEnvForOperator(NAMESPACE);                          <--- Create namespaces
        createTestClassResources();                                <--- Create Resources instance for class
        applyRoleBindings(NAMESPACE);                              <--- Apply Cluster Operator bindings
        testClassResources().clusterOperator(NAMESPACE).done();    <--- Deploy Cluster Operator
        ...
    }
```

##### Exercise
In that phase, you can specify all steps, which you need to cover some functionality. 
In case you didn't create Kafka cluster, you should do it at the begging of test via test method resources instance of `Resources` available in `AbstractST`.

##### Test

When your environment code is on place from a previous phase, you can add code for some checks, msg exchange, etc.

#### Teardown

Because we have two instances of `Resources`, cluster resources deletion can be easily done in `@AfterEach` or `@AfterAll` methods. Our implementation will ensure, that all resources tied to specific instance will be deleted in correct order.
Teardown is triggered in `@AfterAll` of `AbstractST`:
```
    @AfterAll
    void teardownEnvironmentClass() {
        if (Environment.SKIP_TEARDOWN == null) {
            tearDownEnvironmentAfterAll();
            teardownEnvForOperator();
        }
    }
```

so if you want change teardown from your `@AfterAll`, you must override method `tearDownEnvironmentAfterAll()` like this:
```
    @Override
    protected void tearDownEnvironmentAfterAll() {
        doSomethingYouNeed();
        super.tearDownEnvironmentAfterAll();
    }
```

For delete all resources from specific `Resources` instance you can do it like:
```
    testMethodResources().deleteResources();
    testClassResources().deleteResources();
```


Another important thing is environment recreation in case of failure. There is method in `AbstractST` called `recreateTestEnv()` which is called in case exception during test execution. 
This is useful for these tests, which can break cluster operator for next test cases.

Example of skip recreate environment in case of failures. You must override method from `AbstractST` in your test class:
```
    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests!");
    }
```

#### Cluster Operator log check

After each test, there a check for cluster operator logs, which is looking for unexpected errors or unexpected exceptions.
You can see the code of matcher based on Hamcrest in systemtest [matchers module](https://github.com/strimzi/strimzi-kafka-operator/blob/master/systemtest/src/main/java/io/strimzi/systemtest/matchers/LogHasNoUnexpectedErrors.java).
There is a whitelist for expected errors, which happen from time to time.
Expected errors doesn't have any problematic impact on cluster behavior and required action is usually executed during next reconciliation.


## Available Test groups

To execute an expected group of system tests need to add system property `groups` for example with the following values:

`-Dgroups=integration` — to execute one test group
`-Dgroups=acceptance,regression` — to execute many test groups
`-Dgroups=all` — to execute all test groups

If `-Dgroups` system property isn't defined, all tests without an explicitly declared test group will be executed. 
The following table shows currently used tags:

| Name            | Description                                                                        |
| :-------------: | :--------------------------------------------------------------------------------: |
| travis          | Marks tests executed on Travis                                                     |
| acceptance      | Acceptance tests, which guarantee, that basic functionality of Strimzi is working. |
| regression      | Regression tests, which contains all non-flaky tests.                              |
| upgrade         | Upgrade tests for specific versions of the Strimzi.                                |
| flaky           | Execute all flaky tests (tests, which are failing from time to time)               |
| nodeport        | Execute tests which use external lister of type nodeport                           |
| loadbalancer    | Execute tests which use external lister of type loadbalancer                       |
| bridge          | Execute tests which use Kafka Bridge                                               |
| specific        | Specific tests, which cannot be easily added to other categories                   |
| networkpolicies | Execute tests which use Kafka with Network Policies                                |
| tracing         | Execute tests for Tracing                                                          |
| prometheus      | Execute tests for Kafka with Prometheus                                            |
| oauth           | Execute tests which use OAuth                                                      |
| helm            | Execute tests which use Helm for deploy cluster operator                           |

For some reasons, your Kubernetes cluster doesn't support for example Network Policies or NodePort services. 
In that case, you can easily skip these tests with `-DexcludeGroups=networkpolicies,nodeport` property.

There is also a mvn profile for most of the groups, but we suggest to use profile with id `all` (default) and then include or exclude specific groups.

All available test groups are listed in [Constants](https://github.com/strimzi/strimzi-kafka-operator/blob/master/systemtest/src/main/java/io/strimzi/systemtest/Constants.java) class.

## Environment variables

We can configure our system tests with several environment variables, which are loaded before test execution. 
All environment variables can be seen in [Environment](https://github.com/strimzi/strimzi-kafka-operator/blob/master/systemtest/src/main/java/io/strimzi/systemtest/Environment.java) class:

| Name                      | Description                                                                          | Default                                          |
| :-----------------------: | :----------------------------------------------------------------------------------: | :----------------------------------------------: |
| DOCKER_ORG                | Specify organization which owns image used in system tests                           | strimzi                                          |
| DOCKER_TAG                | Specify image tags used in system tests                                              | latest                                           |
| DOCKER_REGISTRY           | Specify docker registry used in system tests                                         | docker.io                                        |
| TEST_CLIENT_IMAGE         | Specify test client image used in system tests                                       | docker.io/strimzi/test-client:latest-kafka-2.3.0 |
| BRIDGE_IMAGE              | Specify kafka bridge image used in system tests                                      | docker.io/strimzi/kafka-bridge:latest            |
| TEST_LOG_DIR              | Directory for store logs collected during the tests                                  | ../systemtest/target/logs/                       |
| ST_KAFKA_VERSION          | Kafka version used in images during the system tests                                 | 2.3.0                                            |
| STRIMZI_DEFAULT_LOG_LEVEL | Log level for cluster operator                                                       | DEBUG                                            |
| KUBERNETES_DOMAIN         | Cluster domain                                                                       | .nip.io                                          |
| TEST_CLUSTER_CONTEXT      | Context which will be used to reach the cluster*                                     | currently active kubernetes context              |
| TEST_CLUSTER_USER         | Default user which will be used for command line admin operations                    | developer                                        |
| SKIP_TEARDOWN             | Variable for skip teardown phase for more debug if needed                            | false                                            |
| IMAGE_PULL_POLICY         | Image Pull Policy                                                                    | IfNotPresent                                     |

If you want to use your own images with different tag or from a different repository, you can use `DOCKER_REGISTRY`, `DOCKER_ORG` and `DOCKER_TAG` environment variables.

`KUBERNETES_DOMAIN` should be specified only in case you are using specific configuration in your kubernetes cluster.

##### Specific Kafka version

To set custom Kafka version in system tests you need to set environment variable `ST_KAFKA_VERSION` to one of the values in [kafka-versions](https://github.com/strimzi/strimzi-kafka-operator/blob/master/kafka-versions).

##### Cluster Operator Log level

To set the log level of Strimzi for system tests you need to set environment variable `STRIMZI_DEFAULT_LOG_LEVEL` with one of the following values: `ERROR`, `WARNING`, `INFO`, `DEBUG`, `TRACE`.

## Use Remote Cluster

The integration and system tests are run against a cluster specified in the environment variable `TEST_CLUSTER_CONTEXT`. 
If this variable is not set, kubernetes client will use currently active context. 
Otherwise, will use context from kubeconfig with a name specified by `TEST_CLUSTER_CONTEXT` variable.

For example command `TEST_CLUSTER_CONTEXT=remote-cluster ./systemtest/scripts/run_tests.sh` will execute tests with cluster context `remote-cluster`. 
However, since system tests use command line `Executor` for some actions, make sure that you are using context from `TEST_CLUSTER_CONTEXT`.

System tests uses admin user for some actions. 
You can specify admin user via variable `TEST_CLUSTER_ADMIN` (by default it use `developer` because `system:admin` cannot be used over remote connections).

## Helper script

The `./systemtest/scripts/run_tests.sh` script can be used to run the `systemtests` using the same configuration as used in the travis build. 
You can use this script to easily run the `systemtests` project.

Pass additional parameters to `mvn` by populating the `EXTRA_ARGS` env var.

    EXTRA_ARGS="-Dfoo=bar" ./systemtest/scripts/run_tests.sh
    
## Running single test class

Use the `verify` build goal and provide a `-Dit.test=TestClassName[#testMethodName]` system property. 

    mvn verify -pl systemtest -Dit.test=KafkaST#testKafkaAndZookeeperScaleUpScaleDown

## Skip Teardown

We already introduced this environment variable, but we didn't describe full potential. 
This env variable is every useful in case you debug some types of test case. 
When this variable is set, teardown phase will be skipped when test finish and if you keep it set, setup phase will be much quicker, because all components are already deployed. 
Unfortunately, this approach is not friendly for tests where component configuration change.  