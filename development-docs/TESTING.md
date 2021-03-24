# Testing Strimzi

This document gives a detailed breakdown of the testing processes and testing options for Strimzi within system tests.
For more information about build process see [Dev guide document](DEV_GUIDE.md).

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
You can use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/), [minishift](https://www.okd.io/minishift/), [oc cluster up](https://github.com/openshift/origin) or [CodeReady Containers](https://github.com/code-ready/crc) to have access to a cluster on your local machine.
You can also access a remote cluster on any machine you want, but make sure your active kubernetes context points to it.
For more information about a remote cluster see the [remote cluster](#use-remote-cluster) section.

The next requirement is to have built the `systemtest` package dependencies, which are:

* test
* crd-annotations
* crd-generator
* api

You can achieve that with `mvn clean install -DskipTests` or `mvn clean install -am -pl systemtest -DskipTests` commands.
These dependencies are needed because we use methods from the `test` package and strimzi model from `api` package.

## Package Structure

Systemtest package is divided into `main` and `test` as usual.
In `main` you can find all support classes, which are used in the tests.

Notable modules:

* **annotations** — we have our own `@OpenShiftOnly` annotation, which checks if the current cluster is Openshift or not. Any other annotations should be stored here.
* **clients** — client implementations used in tests.
* **matchers** — contains our matcher implementation for checking cluster operator logs. For more info see [Cluster Operator log check](#cluster-operator-log-check).
* **utils** — a lot of actions are the same for most of the tests, and we share them through utils class and static methods. You can find here most of the useful methods.
* **resources** —  you can find here all methods needed for deploying and managing lifecycle of Strimzi, Kafka, Kafka Connect, Kafka Bridge, Kafka Mirror Maker and other resources using CRUD methods.
* **templates** - predefined templates that are used on a broader basis. When creating a resource it is necessary to provide a template for instance  `resource.createResource(extensionContext, template.build())`.

Notable classes:

* **Environment** — singleton class, which loads the test environment variables (see following section), which are used in tests.
* **Constants** — simple interface holding all constants used in tests.
* **resources/ResourceManager** - singleton class which stores data about deployed resources and takes care of proper resource deletion.

## Test Phases

In general, we use classic test phases: `setup`, `exercise`, `test` and `teardown`.

### Setup

In this phase we perform:

* Create namespace(s)
* Deploy the Strimzi Cluster operator
* Deploy the Kafka cluster and/or other components (optional)

The reason why the last point is optional, is because we have some test cases where you want to have a different kafka configuration for each test scenario, so creation of the Kafka cluster and other resources is done in the test phase.

We create resources in Kubernetes cluster via classes in `resources` package, which allows you to deploy all components and, if needed, change them from their default configuration using a builder.
Currently, we have two stacks, which are stored in `ResourceManager` singleton instance — one for all test class resources and one for test method resources.
You can create resources anywhere you want. Our resource lifecycle implementation will handle insertion of the resource on top of stack and deletion at the end of the test method/class.


`ResourceManager` has Map<String, Stack<Runnable>>, which means that for each test case we have brand-new stack which 
stores all resources needed for specific test. An important aspect is also the `ExtensionContext.class` with which
we are able to uniquely know which stack is associated with which test.

Cluster Operator setup example from user point of view:
```
    @BeforeAll
    void createClassResources(ExtensionContext extensionContext) {          <--- extension context used primarily to obtain the name of the test
        installClusterOperator(extensionContext, NAMESPACE);                <--- this method encapsulate loading ENV variables, binding and lastly creating resource
    }
```

### Exercise
In this phase you specify all steps which you need to execute to cover some specific functionality.

### Test

When your environment is in place from the previous phase, you can add code for some checks, msg exchange, etc.

### Teardown

Resource lifecycle implementation will ensure that all resources tied to a specific stack will be deleted in the correct order.
Teardown is triggered in `@AfterAll` of `AbstractST`:
```
    @AfterAll
    void tearDownTestSuite(ExtensionContext extensionContext) throws Exception {
        afterAllMayOverride(extensionContext);
    }
```

so if you want to change teardown from your `@AfterAll`, you must override method `afterAllMayOverride()`:
```
    @Override
    protected void tearDownTestSuite() {
        doSomethingYouNeed();
        super.afterAllMayOverride();
    }
    
    // default implementation for top AbstractST.class
    protected void afterAllMayOverride(ExtensionContext extensionContext) throws Exception {
        if (!Environment.SKIP_TEARDOWN) {
            teardownEnvForOperator();
            ResourceManager.getInstance().deleteResources(extensionContext);
        }
    }
```

In order to delete all resources from specific test case you have to provide `ExtensionContext.class`, by which the 
resource manager will know which resources he can delete. 
For instance `extensionContext.getDisplayName()` is `myNewTestName` then resource manager will delete all resources 
related to `myNewTestName` test.
```
    ResourceManager.getInstance().deleteResources(extensionContext);
```

## Parallel execution of tests

In case you want to run system tests locally in parallel, you need to take a few additional steps. You have to modify
two junit properties which are located `systemtests/src/test/resources/junit-platform.properties`:
- junit.jupiter.execution.parallel.enabled=true
- junit.jupiter.execution.parallel.config.fixed.parallelism=5 <- specify any number, you just have to be sure that your cluster can take it

On the other hand you can also override it in mvn command using additional parameters:
- -Djunit.jupiter.execution.parallel.enabled=true
- -Djunit.jupiter.execution.parallel.config.fixed.parallelism=5

## Cluster Operator log check

After each test, there is a check for cluster operator logs, which searches for unexpected errors or unexpected exceptions.
You can see the code of the Hamcrest-based matcher in the systemtest [matchers module](systemtest/src/main/java/io/strimzi/systemtest/matchers/LogHasNoUnexpectedErrors.java).
There is a whitelist for expected errors, which occasionally happen.
Expected errors don't have any problematic impact on cluster behavior and required action is usually executed during next reconciliation.

## Available Test groups

You need to use the `groups` system property in order to execute a group of system tests. For example with the following values:

`-Dgroups=integration` — to execute one test group
`-Dgroups=acceptance,regression` — to execute many test groups
`-Dgroups=all` — to execute all test groups

If `-Dgroups` system property isn't defined, all tests without an explicitly declared test group will be executed.
The following table shows currently used tags:

| Name               | Description                                                                        |
| :----------------: | :--------------------------------------------------------------------------------: |
| acceptance         | Acceptance tests, which guarantee, that basic functionality of Strimzi is working. |
| regression         | Regression tests, which contains all non-flaky tests.                              |
| upgrade            | Upgrade tests for specific versions of the Strimzi.                                |
| smoke              | Execute all smoke tests                                                            |
| flaky              | Execute all flaky tests (tests, which are failing from time to time)               |
| scalability        | Execute scalability tests                                                          |
| specific           | Specific tests, which cannot be easily added to other categories                   |
| nodeport           | Execute tests which use external lister of type nodeport                           |
| loadbalancer       | Execute tests which use external lister of type loadbalancer                       |
| networkpolicies    | Execute tests which use Kafka with Network Policies                                |
| prometheus         | Execute tests for Kafka with Prometheus                                            |
| tracing            | Execute tests for Tracing                                                          |
| helm               | Execute tests which use Helm for deploy cluster operator                           |
| oauth              | Execute tests which use OAuth                                                      |
| recovery           | Execute recovery tests                                                             |
| connectoroperator  | Execute tests which deploy KafkaConnector resource                                 |
| connect            | Execute tests which deploy KafkaConnect resource                                   |
| connects2i         | Execute tests which deploy KafkaConnectS2I resource                                |
| mirrormaker        | Execute tests which deploy KafkaMirrorMaker resource                               |
| mirrormaker2       | Execute tests which deploy KafkaMirrorMaker2 resource                              |
| conneccomponents   | Execute tests which deploy KafkaConnect, KafkaConnectS2I, KafkaMirrorMaker2, KafkaConnector resources |
| bridge             | Execute tests which use Kafka Bridge                                               |
| internalclients    | Execute tests which use internal (from pod) kafka clients in tests                 |
| externalclients    | Execute tests which use external (from code) kafka clients in tests                |
| olm                | Execute tests which test examples from Strimzi manifests                          |
| metrics            | Execute tests where metrics are used                                               |
| cruisecontrol      | Execute tests which deploy CruiseControl resource                                  |
| rollingupdate      | Execute tests where is rolling update triggered                                    |

If your Kubernetes cluster doesn't support for example, Network Policies or NodePort services, you can easily skip those tests with `-DexcludeGroups=networkpolicies,nodeport`.

There is also a mvn profile for the main groups - `acceptance`, `regression`, `smoke`, `bridge` and `all`, but we suggest to use profile with id `all` (default) and then include or exclude specific groups.
If you want specify the profile, use the `-P` flag - for example `-Psmoke`.

All available test groups are listed in [Constants](systemtest/src/main/java/io/strimzi/systemtest/Constants.java) class.

## Environment variables

System tests can be configured by several environment variables, which are loaded before test execution.

Variables can be defined via environmental variables or a configuration file, this file can be located anywhere on the file system as long as a path is provided to this file.
The path is defined by environmental variable `ST_CONFIG_PATH`, if the `ST_CONFIG_PATH` environmental variable is not defined, the default config file location is used `systemtest/config.json`.
Loading of system configuration has the following priority order:
1. Environment variable
2. Variable defined in configuration file
3. Default value

All environment variables are defined in [Environment](systemtest/src/main/java/io/strimzi/systemtest/Environment.java) class:

| Name                                   | Description                                                                              | Default                                          |
| :------------------------------------: | :--------------------------------------------------------------------------------------: | :----------------------------------------------: |
| DOCKER_ORG                             | Specify the organization/repo containing the image used in system tests                  | strimzi                                          |
| DOCKER_TAG                             | Specify the image tags used in system tests                                              | latest                                           |
| DOCKER_REGISTRY                        | Specify the docker registry used in system tests                                         | quay.io                                        |
| TEST_CLIENT_IMAGE                      | Specify the test client image used in system tests                                       | quay.io/strimzi/test-client:latest-kafka-2.3.0 |
| BRIDGE_IMAGE                           | Specify the kafka bridge image used in system tests                                      | quay.io/strimzi/kafka-bridge:latest            |
| TEST_LOG_DIR                           | Directory for storing logs collected during the tests                                    | ../systemtest/target/logs/                       |
| ST_KAFKA_VERSION                       | Kafka version used in images during the system tests                                     | 2.3.0                                            |
| STRIMZI_LOG_LEVEL                      | Log level for the cluster operator                                                       | DEBUG                                            |
| STRIMZI_COMPONENTS_LOG_LEVEL           | Log level for the components                                                             | INFO                                            |
| KUBERNETES_DOMAIN                      | Cluster domain                                                                           | .nip.io                                          |
| TEST_CLUSTER_CONTEXT                   | Context which will be used to reach the cluster*                                         | currently active kubernetes context              |
| TEST_CLUSTER_USER                      | Default user which will be used for command line admin operations                        | developer                                        |
| SKIP_TEARDOWN                          | Variable for skip teardown phase for more debug if needed                                | false                                            |
| OPERATOR_IMAGE_PULL_POLICY             | Image Pull Policy for Operator image                                                     | Always                                           |
| COMPONENTS_IMAGE_PULL_POLICY           | Image Pull Policy for Kafka, Bridge, etc.                                                | IfNotPresent                                     |
| STRIMZI_TEST_LOG_LEVEL                 | Log level for system tests                                                               | INFO                                             |
| STRIMZI_RBAC_SCOPE                     | Set to 'CLUSTER' or 'NAMESPACE' to deploy the operator with ClusterRole or Roles respectively | cluster                                     |
| OLM_OPERATOR_NAME                      | Operator name in manifests CSV                                                           | strimzi                                          |
| OLM_SOURCE_NAME                        | CatalogSource name which contains desired operator                                       | strimzi-source                                   |
| OLM_APP_BUNDLE_PREFIX                  | CSV bundle name                                                                          | strimzi                                          |
| OLM_OPERATOR_VERSION                   | Version of the operator which will be installed                                          | v0.16.2                                          |
| DEFAULT_TO_DENY_NETWORK_POLICIES       | Determines how will be network policies set - to deny-all (true) or allow-all (false)    | true                                             |
| STRIMZI_EXEC_MAX_LOG_OUTPUT_CHARACTERS | Set maximum count of characters printed from [Executor](test/src/main/java/io/strimzi/test/executor/Exec.java) stdout and stderr | 20000    |
| CLUSTER_OPERATOR_INSTALL_TYPE          | Specify how the CO will be deployed. `OLM` option will install operator via OLM, you just need to set other `OLM` env variables. | bundle   |

If you want to use your own images with a different tag or from a different repository, you can use `DOCKER_REGISTRY`, `DOCKER_ORG` and `DOCKER_TAG` environment variables.

`KUBERNETES_DOMAIN` should be specified only in case, when you are using specific configuration in your kubernetes cluster.

##### Specific Kafka version

To set custom Kafka version in system tests you need to set the environment variable `ST_KAFKA_VERSION` to one of the values in [kafka-versions](kafka-versions.yaml).

#### Using private registries

If you want use private registries, before executing the tests you have to create secret and then specify name of the created secret in env variable called
`SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET` with the container registry credentials to be able pull images. Note that secret has to be created in `default` namespace.

##### Cluster Operator Log level

To set the log level of Strimzi for system tests you need to set the environment variable `STRIMZI_DEFAULT_LOG_LEVEL` with one of the following values: `ERROR`, `WARNING`, `INFO`, `DEBUG`, `TRACE`.

## Use Remote Cluster

The integration and system tests are ran against a cluster specified in the environment variable `TEST_CLUSTER_CONTEXT`.
If this variable is unset, the kubernetes client will use the currently active context.
Otherwise, it will use the context from kubeconfig with a name specified by the `TEST_CLUSTER_CONTEXT` variable.

For example, command `TEST_CLUSTER_CONTEXT=remote-cluster ./systemtest/scripts/run_tests.sh` will execute tests with cluster context `remote-cluster`.
However, since system tests use command line `Executor` for some actions, make sure that you are using context from `TEST_CLUSTER_CONTEXT`.

## Helper script

The `./systemtest/scripts/run_tests.sh` script can be used to run the `systemtests` using the same configuration as used in the Azure build.
You can use this script to easily run the `systemtests` project.

Pass additional parameters to `mvn` by populating the `EXTRA_ARGS` env var.

    EXTRA_ARGS="-Dfoo=bar" ./systemtest/scripts/run_tests.sh

## Running single test class

Use the `verify` build goal and provide `-Dit.test=TestClassName[#testMethodName]` system property.

    mvn verify -pl systemtest -Dit.test=KafkaST#testCustomAndUpdatedValues

## Skip Teardown

When debugging some types of test cases, `SKIP_TEARDOWN` env variable can be very helpful.
Once this variable is set, the teardown phase will be skipped after test finishes. If you keep it set,
subsequent setup phase will be much faster, due to all components being already deployed.
Unfortunately, this approach is not applicable for tests where component configuration changes.

## Skip surefire tests

There are several tests, which are executed via Maven Surefire plugin. 
Those tests are some kind of unit tests for internal systemtest package tooling.
You can skip them by adding `-Dskip.surefire.tests` option to mvn command.

## Testing Cluster Operator deployment via OLM

Strimzi also supports deployment of Cluster Operator through OperatorHub, which needs to have updated manifest for each version and be tested.
We created a simple [OLM test suite](systemtest/src/test/java/io/strimzi/systemtest/olm), which deploys Cluster Operator and other needed components via OLM and examples from manifests.

To run these tests, you have to build an image with manifests and create a new `CatalogSource` resource, which points to the built image.
Dockerfile can have following structure:
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

Now you can easily run OLM tests.

