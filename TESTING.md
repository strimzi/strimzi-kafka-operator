# Testing Strimzi

This document gives a detailed breakdown of the testing processes and testing options for Strimzi within system tests. 
For more information about build process see [Hacking document](HACKING.md).

<!-- TOC depthFrom:2 -->

- [Pre-requisites](#pre-requisites)
- [Package Structure](#package-structure)
- [Test Phases](#test-phases)
- [Cluster Operator log check](#cluster-operator-log-check)
- [Available Test Groups](#available-test-groups)
- [Environment Variables](#environment-variables)
- [Use Remote Cluster](#use-remote-cluster)
- [Helper Script](#helper-script)
- [Running single test class](#running-single-test-class)
- [Skip Teardown](#skip-teardown)

<!-- /TOC -->

## Pre-requisites 

To run any system tests you need a Kubernetes or Openshift cluster available in your active kubernetes context. 
You can use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/), [minishift](https://www.okd.io/minishift/), [oc cluster up](https://github.com/openshift/origin) or [CodeReady Contaners](https://github.com/code-ready/crc) to have access to a cluster on your local machine. 
You can also access a remote cluster on any machine you want, but make sure your active kubernetes context points to it. 
For more information about a remote cluster see the [remote cluster](#use-remote-cluster) section.

The next requirement is to have built the `systemtest` package dependencies, which are:

* test
* crd-annotations
* crd-generator
* api

You can achieve that with `mvn clean install -DskipTests` or `mvn clean install -am -pl systemtest -DskipTests` commands. 
The dependencies are needed because we use methods from the `test` package and strimzi model from `api` package. 

## Package Structure 

Systemtest package is divided into `main` and `test` as usuall. 
In `main` you can find all support classes, which are used in the tests. 
Modules worth mentioning are:

* **annotations** — we have our own `@OpenShiftOnly` annotation, which checks if the current cluster is Openshift or not. Any other annotations should be stored here.
* **clients** — client implementations used in tests.
* **matchers** — contains our matcher implementation for checking cluster operator logs. For more info see [Cluster Operator log check](#cluster-operator-log-check).
* **utils** — a lot of actions are the same for most of the tests, and we share them through utils class and static methods. You can find here most of the useful methods.
* **resources** — heart of the systemtest package. In classes from this package, you can find all methods needed for deploy Strimzi, Kafka, Kafka Connect, Kafka Bridge, Kafka Mirror Maker and all other useful resources. 
The current mechanism will ensure that all resources created within these classesurce deleto will be deleted after tests.

And classes: 

* **Environment** — singleton class, which loads the test environment variables (see following section), which are used in the tests.
* **Constants** — simple interface holding all constants used in the tests. 
* **resources/ResourceManager** - singleton class which store info about deployed resources and take care about proper resource deletion.  

## Test Phases

In general, we have classic test phases: setup, exercise, test, teardown.

#### Setup

In this phase we do the following things:

* Create namespace(s)
* Deploy the Strimzi Cluster operator
* Deploy a Kafka cluster or other components (optional)

The reason why the last point is optional is because we have some test cases where you want to have a different kafka configuration for each test scenario so creation of the Kafka cluster and other resources is done in the test phase.

We create resources in Kubernetes cluster via classes in `resources` package, which allows you to deploy all components and, if needed, change them from their default configuration using a builder. 
Currently, we have two stacks, which are stored in `ResourceManager` singleton instance — one for whole test class resources and one for test method resources.
You can create resources anywhere you want, and our implementation will take care about put resource on top of one stack and delete them at the end of test method/class.


`ResourceManager` store info, which stack is active (class or method) in pointer stack.
You can change between class and method resources stack with method `ResourceManager.setMethodResources()` or `ResourceManager.setClassResources()`. 
Note that pointer stack is set automatically in base `@BeforeAll` or `@BeforeEach`.


Cluster Operator setup example:
```
    @BeforeAll
    void createClassResources() {
        prepareEnvForOperator(NAMESPACE);                          <--- Create namespaces
        createTestClassResources();                                <--- Create Resources instance for class
        applyRoleBindings(NAMESPACE);                              <--- Apply Cluster Operator bindings
        KubernetesResource.clusterOperator(NAMESPACE).done();      <--- Deploy Cluster Operator
        ...
    }
```

##### Exercise
In this phase you specify all the steps which you need to cover some functionality. 
If you didn't create the Kafka cluster you should do so at the begging of test using the test method resources instance of `Resources` inherited from `BaseST`.

##### Test

When your environment is in place from the previous phase, you can add code for some checks, msg exchange, etc.

#### Teardown

Because we have two stacks for store resources info, cluster resources deletion can be easily done in `@AfterEach` or `@AfterAll` methods. 
Our implementation will ensure that all resources tied to a specific stack will be deleted in the correct order.
Teardown is triggered in `@AfterAll` of `BaseST`:
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
    ResourceManager.deleteMethodResources();
    ResourceManager.deleteClassResources();
```


Another important thing is environment recreation in the case of failure. The `recreateTestEnv()` method in `BaseST`  is called in the case of an exception during test execution. 
This is useful for these tests, which can break the cluster operator for subsequent test cases.

Example of skip recreate environment in the case of failures. You must override the method from `BaseST` in your test class:
```
    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests!");
    }
```

#### Cluster Operator log check

After each test there is a check for cluster operator logs which looks for unexpected errors or unexpected exceptions.
You can see the code of the Hamcrest-based matcher in the systemtest [matchers module](systemtest/src/main/java/io/strimzi/systemtest/matchers/LogHasNoUnexpectedErrors.java).
There is a whitelist for expected errors, which happen from time to time.
Expected errors doesn't have any problematic impact on cluster behavior and required action is usually executed during next reconciliation.


## Available Test groups

You need to use the `groups` system property in order to execute a group of system tests. For example with the following values:

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
| olm             | Execute tests which use OLM for deploy cluster operator                            |

If your Kubernetes cluster doesn't support, for example, Network Policies or NodePort services, you can easily skip those tests with `-DexcludeGroups=networkpolicies,nodeport`.

There is also a mvn profile for most of the groups, but we suggest to use profile with id `all` (default) and then include or exclude specific groups.

All available test groups are listed in [Constants](systemtest/src/main/java/io/strimzi/systemtest/Constants.java) class.

## Environment variables

We can configure our system tests with several environment variables, which are loaded before test execution. 
All environment variables can be seen in [Environment](systemtest/src/main/java/io/strimzi/systemtest/Environment.java) class:

| Name                      | Description                                                                          | Default                                          |
| :-----------------------: | :----------------------------------------------------------------------------------: | :----------------------------------------------: |
| DOCKER_ORG                | Specify the organization/repo containing the image used in system tests                           | strimzi                                          |
| DOCKER_TAG                | Specify the image tags used in system tests                                              | latest                                           |
| DOCKER_REGISTRY           | Specify the docker registry used in system tests                                         | docker.io                                        |
| TEST_CLIENT_IMAGE         | Specify the test client image used in system tests                                       | docker.io/strimzi/test-client:latest-kafka-2.3.0 |
| BRIDGE_IMAGE              | Specify the kafka bridge image used in system tests                                      | docker.io/strimzi/kafka-bridge:latest            |
| TEST_LOG_DIR              | Directory for storing logs collected during the tests                                  | ../systemtest/target/logs/                       |
| ST_KAFKA_VERSION          | Kafka version used in images during the system tests                                 | 2.3.0                                            |
| STRIMZI_DEFAULT_LOG_LEVEL | Log level for the cluster operator                                                       | DEBUG                                            |
| KUBERNETES_DOMAIN         | Cluster domain                                                                       | .nip.io                                          |
| TEST_CLUSTER_CONTEXT      | Context which will be used to reach the cluster*                                     | currently active kubernetes context              |
| TEST_CLUSTER_USER         | Default user which will be used for command line admin operations                    | developer                                        |
| SKIP_TEARDOWN             | Variable for skip teardown phase for more debug if needed                            | false                                            |
| OPERATOR_IMAGE_PULL_POLICY   | Image Pull Policy for Operator image                                              | Always                                           |
| COMPONENTS_IMAGE_PULL_POLICY | Image Pull Policy for Kafka, Bridge, etc.                                         | IfNotPresent                                     |
| STRIMZI_TEST_LOG_LEVEL    | Log level for system tests                                                           | INFO                                             |
| OLM_OPERATOR_NAME         | Operator name in manifests CSV                                                       | strimzi                                             |
| OLM_APP_BUNDLE_PREFIX     | CSV bundle name                                                                      | strimzi                                             |
| OLM_OPERATOR_VERSION      | Version of the operator which will be installed                                      | v0.16.2                                             |
| DEFAULT_TO_DENY_NETWORK_POLICIES | Determines how will be network policies set - to deny-all (true) or allow-all (false)    | true                                            |

If you want to use your own images with a different tag or from a different repository, you can use `DOCKER_REGISTRY`, `DOCKER_ORG` and `DOCKER_TAG` environment variables.

`KUBERNETES_DOMAIN` should be specified only in case you are using specific configuration in your kubernetes cluster.

##### Specific Kafka version

To set custom Kafka version in system tests you need to set environment variable `ST_KAFKA_VERSION` to one of the values in [kafka-versions](kafka-versions.yaml).

##### Cluster Operator Log level

To set the log level of Strimzi for system tests you need to set the environment variable `STRIMZI_DEFAULT_LOG_LEVEL` with one of the following values: `ERROR`, `WARNING`, `INFO`, `DEBUG`, `TRACE`.

## Use Remote Cluster

The integration and system tests are run against a cluster specified in the environment variable `TEST_CLUSTER_CONTEXT`. 
If this variable is not set, the kubernetes client will use the currently active context. 
Otherwise, it will use the context from kubeconfig with a name specified by the `TEST_CLUSTER_CONTEXT` variable.

For example command `TEST_CLUSTER_CONTEXT=remote-cluster ./systemtest/scripts/run_tests.sh` will execute tests with cluster context `remote-cluster`. 
However, since system tests use command line `Executor` for some actions, make sure that you are using context from `TEST_CLUSTER_CONTEXT`.

System tests uses admin user for some actions. 
You can specify the admin user using variable `TEST_CLUSTER_ADMIN` (by default it uses `developer` because `system:admin` cannot be used over remote connections).

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
This env variable is every useful in debugging some types of test case. 
When this variable is set the teardown phase will be skipped when the test finishes and if you keep it set, setup phase will be much quicker, because all components are already deployed. 
Unfortunately, this approach is not friendly for tests where component configuration change.  

## Testing Cluster Operator deployment via OLM

Strimzi also supports deploy Cluster Operator through OperatorHub. 
For these, we need to have manifests updated for each version and test them. 
We created a simple [OLM test suite](systemtest/src/test/java/io/strimzi/systemtest/olm), which deploys Cluster Operator and all other components via OLM and examples from manifests.

To run these tests, you have to build an image with manifests and create a new CatalogSource resource, which points to the built image.
Docker could look like the following one:
```
   FROM quay.io/openshift/origin-operator-registry:latest
   
   COPY /manifests /manifests
   
   RUN /usr/bin/initializer -m /manifests -o bundles.db
   ENTRYPOINT ["/usr/bin/registry-server"]
   CMD ["-d", "bundles.db", "-t", "termination.log"]
```

When you build a new image, you should create new `CatalogSource` like this:

```
apiVersion: operators.coreos.com/v1alpha1
kind: CatalogSource
metadata: 
  name: strimzi-source
  namespace: openshift-marketplace
  labels:
    app: Strimzi
spec:
  displayName: Strimzi Operator Source
  image: quay.io/myorg/myimage:latest
  publisher: Strimzi
  sourceType: grpc
```

Now you can easily run our OLM tests.

