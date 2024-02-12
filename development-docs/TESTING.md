# Testing Strimzi

This document gives a detailed breakdown of the testing processes and testing options for Strimzi within system tests.
For more information about the build process, see [Dev guide document](DEV_GUIDE.md).

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
- [Testing-farm](../systemtest/tmt/README.md)

<!-- /TOC -->

## Pre-requisites

You need a Kubernetes or Openshift cluster available in your active Kubernetes context to run any system tests.
You can use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) to have access to a cluster on your local machine.
You can also access a remote cluster on any machine you want, but make sure your active Kubernetes context points to it.
See the [remote cluster](#use-remote-cluster) section for more information about a remote cluster.

The following requirement is to have built the `systemtest` package dependencies, which are:

* test
* crd-annotations
* crd-generator
* api
* config-model
* operator-common
* kafka-oauth-client

You can achieve that with `mvn clean install -DskipTests` or `mvn clean install -am -pl systemtest -DskipTests` commands.
These dependencies are needed because we use methods from the `test` package and the strimzi model from the `api` package.

## Package Structure

The `systemtest` package is divided into `main` and `test` as usual.
In `main`, you can find all support classes, which are used in the tests.

Notable modules:

* **kafkaclients** — client implementations used in tests.
* **matchers** — contains our matcher implementation for checking cluster operator logs. For more info see [Cluster Operator log check](#cluster-operator-log-check).
* **utils** — many actions are the same for most of the tests, and we share them through utils class and static methods. You can find here most of the useful methods.
* **resources** —  you can find here all methods needed for deploying and managing the lifecycle of Strimzi, Kafka, Kafka Connect, Kafka Bridge, Kafka Mirror Maker and other resources using CRUD methods.
* **templates** - predefined templates that are used on a broader basis. When creating a resource it is necessary to provide a template for instance  `resource.createResource(extensionContext, template.build())`.

Notable classes:

* **Environment** — singleton class, which loads the test environment variables (see following section) used in tests.
* **Constants** — simple interface holding all constants used in tests.
* **resources/ResourceManager** - singleton class which stores data about deployed resources and takes care of proper resource deletion.
* **resources/operator/SetupClusterOperator** - encapsulates the whole installation process of Cluster Operator (i.e., `RoleBinding`, `ClusterRoleBinding`,
* `ConfigMap`, `Deployment`, `CustomResourceDefinition`, preparation of the Namespace). The Environment class
  values decide how Cluster Operator should be installed (i.e., Olm, Helm, Bundle). Moreover, it provides `rollbackToDefaultConfiguration()`
  method, which basically re-install Cluster Operator to the default values. In case user wants to edit specific installation,
  one can use `defaultInstallation()`, which returns `SetupClusterOperatorBuilder`.
* **storage/TestStorage** - generate and stores values in the specific `ExtensionContext`. This ensures that if one want to retrieve data from
  `TestStorage` it can be done via `ExtensionContext` (with help of ConcurrentHashMap) inside `AbstractST`.
* **parallel/TestSuiteNamespaceManager** - This class contains additional namespaces will are needed for test suite before the execution of `@BeforeAll`. 
  By this we can easily prepare the namespace in the `AbstractST` class and not in the children.

## Test Phases

We generally use classic test phases: `setup`, `exercise`, `test` and `teardown`.

### Setup

In this phase, we perform:

* Setup Cluster Operator 
* Deploy the shared Kafka cluster and other components (optional)

The second point is optional because we have some test cases where you want to have a different Kafka configuration for 
each test scenario, so creating the Kafka cluster and other resources is done in the test phase.
Here is an example how to create an instance of Cluster Operator:
```java
@BeforeAll
void setup(ExtensionContext extensionContext){
    // deploy Cluster Operator using default configuration
    clusterOperator = clusterOperator.defaultInstallation(extensionContext)
        .createInstallation()
        .runInstallation();
}
```

We create resources in the Kubernetes cluster via classes in the `resources` package, which allows you to deploy all 
components and, if needed, change them from their default configuration using a builder.
You can create resources anywhere you want. Our resource lifecycle implementation will handle insertion of the resource 
on top of the stack and deletion at the end of the test method/class.
Moreover, you should always use `Templates` classes for pre-defined resources. 
For instance, when one want deploy Kafka cluster with tree nodes it can be simply done by following code:
```java
final int numberOfKafkaBrokers = 3;
final int numberOfZooKeeperNodes = 1;

resourceManager.createResourceWithWait(extensionContext, 
    // using KafkaTemplate class for pre-defined values
    KafkaTemplates.kafkaEphemeral(
        clusterName,
        numberOfKafkaBrokers, 
        numberOfZooKeeperNodes).build()
    );
```

`ResourceManager` has Map<String, Stack<ResourceItem>>, which means that for each test case, we have a brand-new stack 
that stores all resources needed for specific tests. An important aspect is also the `ExtensionContext.class` with which
we can know which stack is associated with which test uniquely.

Example of setup shared resources in scope of the test suite:
```java
@BeforeAll
void setUp(ExtensionContext extensionContext) {
    // create resources without wait to deploy them simultaneously
    resourceManager.createResourceWithoutWait(extensionContext, // we do not wait for readiness and therefore deploy all resources asynchronously
    // kafka with cruise control and metrics
    KafkaTemplates.kafkaWithMetricsAndCruiseControlWithMetrics(...).build(),
    KafkaTemplates.kafkaWithMetrics(...).build(),
    KafkaClientsTemplates.kafkaClients(...).build(),
    KafkaClientsTemplates.kafkaClients(...).build(),
    KafkaMirrorMaker2Templates.kafkaMirrorMaker2WithMetrics(...).build(),
    KafkaBridgeTemplates.kafkaBridgeWithMetrics(...).build()
    );
    
    // sync resources (barier)
    resourceManager.synchronizeResources(extensionContext);
}
```

### Exercise
In this phase, you specify all steps which you need to execute to cover some specific functionality.

### Test

When your environment is in place from the previous phase, you can add code for checks, msg exchange, etc.

### Teardown

Resource lifecycle implementation will ensure that all resources tied to a specific stack will be deleted in the correct order.
The teardown is triggered in `@AfterAll` of `AbstractST`:
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
        install.unInstall(); <- install is instance of SetupClusterOperator class
    }
```

To delete all resources from a specific test case, you have to provide `ExtensionContext.class`, by which the
the resource manager will know which resources he can delete.
For instance `extensionContext.getDisplayName()` is `myNewTestName` then resource manager will delete all resources
related to `myNewTestName` test.
```
    ResourceManager.getInstance().deleteResources(extensionContext);
```

## Adding brand-new test suite 

When you need to create a new test suite, firstly, make sure that it has the suffix `ST` (i.e., KafkaST, ConnectBuilderST).

## Parallel execution of tests

If you want to run system tests locally in parallel, you need to take a few additional steps. You have to modify
two JUnit properties and there are two approaches how to do it:
1) Using IDE (InteliJ)
   1. on the left side between build project and run buttons you click on edit configuration
   2. then only one thing is to modify VM options and add these two:
      - `-Djunit.jupiter.execution.parallel.enabled=true`
      - `-Djunit.jupiter.execution.parallel.config.fixed.parallelism=4` 

2. Using Maven
- override it in mvn command using additional parameters:
  - `-Djunit.jupiter.execution.parallel.enabled=true`
  - `-Djunit.jupiter.execution.parallel.config.fixed.parallelism=4`
- for instance 
```
// to run 4 test cases in parallel in ListenersST test class
mvn verify -pl systemtest -P all -Dit.test=ListenersST -Djunit.jupiter.execution.parallel.enabled=true -Djunit.jupiter.execution.parallel.config.fixed.parallelism=4
```

### Parallelism architecture

Key aspects:
1. [JUnit5 parallelism](https://junit.org/junit5/docs/snapshot/user-guide/#writing-tests-parallel-execution)
2. **annotations** = overrides parallelism configuration in runtime phase (i.e., @IsolatedTest, @ParallelTest, @ParallelNamespaceTest).
3. **auxiliary classes** (i.e., `SuiteThreadController`, `TestSuiteNamespaceManager`)

#### 1. JUnit5 parallelism

Provides [ForkJoinPool](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/ForkJoinPool.html)
class spawns as many threads as we specify in the `JUnit-platform.properties`.

#### 2. Annotations

- **@IsolatedTest** = using `@ResourceLock` (prohibit read-write mode), which ensures this test case is executed in isolation.
- **@ParallelTest** = overrides parallelism configuration by `@Execution(ExecutionMode.CONCURRENT)` and thus test case will
  run simultaneously with other parallel tests
- **@ParallelNamespaceTest** = same as @ParallelTest but with additional change. This type of test automatically creates
  its namespace where all resources will be deployed (f.e., needed where we deploy `Kafka` or `KafkaMirrorMaker`).

#### 3. Auxiliary classes

-- **TestSuiteNamespaceManager** = provides complete management of namespaces for specific test suites.
    1. **@ParallelSuite** - such a test suite creates its namespace (f.e., for TracingST creates `tracing-st` namespace).
       This is needed because we must ensure that each parallel suite runs in a separate namespace and thus runs in isolation.
    2. **@ParallelNamespaceTest** = responsible for creating and deleting auxiliary namespaces for such test cases.
- **SuiteThreadController** = provides synchronization between test cases (i.e., @ParallelTest, @ParallelNamespaceTest and @IsolatedTest). 
- ForkJoinPool spawns additional (unnecessary) threads (because of Work-Stealing algorithm), exceeding our configured parallelism limit:
    - synchronization provide by `waitUntilAllowedNumberTestCasesParallel()` and `notifyParallelTestToAllowExecution()` method
    - synchronize point where test suite end it in a situation when `ForkJoinPool` spawn additional threads,
      which can exceed the limit specified. Thus many threads can start executing test suites which could potentially
      destroy clusters. This mechanism will put all other threads (i.e., not needed) into the waiting room.
      After one of the `ParallelTest` is done with execution we release `notifyParallelTestToAllowExecution()`one test case
      setting `isParallelTestReleased` flag. This ensures that only one test case will continue execution, and others
      will still wait.

## Cluster Operator log check

After each test, there is a check for cluster operator logs, which searches for unexpected errors or exceptions.
You can see the code of the Hamcrest-based matcher in the `systemtest` [matchers module](systemtest/src/main/java/io/strimzi/systemtest/matchers/LogHasNoUnexpectedErrors.java).
There is a list of standard errors, which occasionally happen.
Standard errors don't have any problematic impact on cluster behaviour, and required action is usually executed during the subsequent reconciliation.

## Available Test groups

You need to use the `groups` system property to execute a group of system tests. For example, with the following values:

`-Dgroups=integration` — to execute one test group
`-Dgroups=acceptance,regression` — to execute many test groups
`-Dgroups=all` — to run all test groups

If `-Dgroups` system property isn't defined, all tests without an explicitly declared test group will be executed.
The following table shows currently used tags:

| Name               |                                      Description                                      |
| :----------------: |:-------------------------------------------------------------------------------------:|
| acceptance         | Acceptance tests, which guarantee that the basic functionality of Strimzi is working. |
| regression         |                 Regression tests, which contains all non-flaky tests.                 |
| upgrade            |                  Upgrade tests for specific versions of the Strimzi.                  |
| smoke              |                                Execute all smoke tests                                |
| flaky              |         Execute all flaky tests (tests, which are failing from time to time)          |
| scalability        |                               Execute scalability tests                               |
| componentscaling   |                            Execute component scaling tests                            |
| specific           |           Specific tests, which cannot be easily added to other categories            |
| nodeport           |               Execute tests which use external lister of type nodeport                |
| loadbalancer       |             Execute tests which use external lister of type loadbalancer              |
| networkpolicies    |                  Execute tests that use Kafka with Network Policies                   |
| prometheus         |                        Execute tests for Kafka with Prometheus                        |
| tracing            |                               Execute tests for Tracing                               |
| helm               |                Execute tests that use Helm for deploy cluster operator                |
| oauth              |                             Execute tests that use OAuth                              |
| recovery           |                                Execute recovery tests                                 |
| connectoroperator  |                   Execute tests that deploy KafkaConnector resource                   |
| connect            |                    Execute tests that deploy KafkaConnect resource                    |
| mirrormaker        |                  Execute tests that deploy KafkaMirrorMaker resource                  |
| mirrormaker2       |                 Execute tests that deploy KafkaMirrorMaker2 resource                  |
| conneccomponents   |  Execute tests that deploy KafkaConnect, KafkaMirrorMaker2, KafkaConnector resources  |
| bridge             |                          Execute tests that use Kafka Bridge                          |
| internalclients    |         Execute tests that use internal (from the Pod) Kafka clients in tests         |
| externalclients    |          Execute tests that use external (from code) Kafka clients in tests           |
| olm                |                Execute tests that test examples from Strimzi manifests                |
| metrics            |                         Execute tests where metrics are used                          |
| cruisecontrol      |                   Execute tests that deploy CruiseControl resource                    |
| rollingupdate      |                    Execute tests where is rolling update triggered                    |

If your Kubernetes cluster doesn't support Network Policies or NodePort services, you can easily skip those tests with `-DexcludeGroups=networkpolicies,nodeport`.

There is also a mvn profile for the main groups - `acceptance`, `regression`, `smoke`, `bridge`, `operators`, `components` and `all`. Still, we suggest using a profile with id `all` (default) and then including or excluding specific groups.
If you want to specify the profile, use the `-P` flag - for example `-Psmoke`.

All available test groups are listed in [Constants](systemtest/src/main/java/io/strimzi/systemtest/Constants.java) class.

## Environment variables

System tests can be configured by several environment variables, which are loaded before test execution.

Variables can be defined via environmental variables or a configuration file; this file can be located anywhere on the file system as long as a path is provided to this file.
The path is defined by the environmental variable `ST_CONFIG_PATH`; if the `ST_CONFIG_PATH` environmental variable is not defined, the default config file location is used `systemtest/config.yaml`.
Loading of system configuration has the following priority order:
1. Environment variable
2. Variable defined in the configuration file
3. Default value

After every systemtest test run all env variables are automatically stored into $TEST_LOG_DIR/test-run.../config.yaml so each test run can be easily reproduced just by running
```commandline
ST_CONFIG_PATH="path/to/config/file/config.yaml" mvn verify ...
```

All environment variables are defined in [Environment](systemtest/src/main/java/io/strimzi/systemtest/Environment.java) class:

| Name                                   | Description                                                                                                                                           | Default                                                     |
|:---------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------|
| DOCKER_ORG                             | Specify the organization/repo containing the image used in system tests                                                                               | strimzi                                                     |
| DOCKER_TAG                             | Specify the image tags used in system tests                                                                                                           | latest                                                      |
| DOCKER_REGISTRY                        | Specify the docker registry used in system tests                                                                                                      | quay.io                                                     |
| BRIDGE_IMAGE                           | Specify the Kafka bridge image used in system tests                                                                                                   | quay.io/strimzi/kafka-bridge:latest                         |
| TEST_LOG_DIR                           | Directory for storing logs collected during the tests                                                                                                 | ../systemtest/target/logs/                                  |
| ST_KAFKA_VERSION                       | Kafka version used in images during the system tests                                                                                                  | 3.6.0                                                       |
| ST_CONFIG_PATH                         | Path to file where environment variables are defined in yaml format                                                                                   | ../systemtest/config.yaml                                   |
| STRIMZI_FEATURE_GATES                  | Strimzi feature gates                                                                                                                                 | empty                                                       |
| STRIMZI_LOG_LEVEL                      | Log level for the cluster operator                                                                                                                    | DEBUG                                                       |
| STRIMZI_COMPONENTS_LOG_LEVEL           | Log level for the components                                                                                                                          | INFO                                                        |
| STRIMZI_USE_KRAFT_IN_TESTS             | Enable kraft mode                                                                                                                                     | false                                                       |
| KUBERNETES_DOMAIN                      | Cluster domain                                                                                                                                        | .nip.io                                                     |
| TEST_CLUSTER_CONTEXT                   | context which will be used to reach the cluster*                                                                                                      | currently active Kubernetes context                         |
| TEST_CLUSTER_USER                      | Default user which will be used for command-line admin operations                                                                                     | developer                                                   |
| TEST_CLIENTS_IMAGE                     | Image with strimzi test clients                                                                                                                       | quay.io/strimzi-test-clients/test-clients:0.6.0-kafka-3.6.0 |
| TEST_CLIENTS_VERSION                   | Version of strimzi test clients                                                                                                                       | 0.6.0                                                       |
| SCRAPER_IMAGE_ENV                      | Scraper image                                                                                                                                         | quay.io/strimzi/kafka:latest-kafka-3.6.1                    |
| SKIP_TEARDOWN                          | Variable for skip teardown phase for more debug if needed                                                                                             | false                                                       |
| OPERATOR_IMAGE_PULL_POLICY             | Image Pull Policy for Operator image                                                                                                                  | Always                                                      |
| COMPONENTS_IMAGE_PULL_POLICY           | Image Pull Policy for Kafka, Bridge, etc.                                                                                                             | IfNotPresent                                                |
| SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET  | Image pull secret                                                                                                                                     | ""                                                          |
| STRIMZI_TEST_LOG_LEVEL                 | Log level for system tests                                                                                                                            | INFO                                                        |
| STRIMZI_TEST_ROOT_LOG_LEVEL            | Root Log level for system tests                                                                                                                       | DEBUG                                                       |
| STRIMZI_RBAC_SCOPE                     | Set to 'CLUSTER' or 'NAMESPACE' to deploy the operator with ClusterRole or Roles, respectively                                                        | cluster                                                     |
| OLM_OPERATOR_NAME                      | Operator name in manifests CSV                                                                                                                        | strimzi                                                     |
| OLM_SOURCE_NAME                        | CatalogSource name which contains desired operator                                                                                                    | strimzi-source                                              |
| OLM_APP_BUNDLE_PREFIX                  | CSV bundle name                                                                                                                                       | strimzi                                                     |
| OLM_OPERATOR_VERSION                   | Version of the operator which will be installed                                                                                                       | v0.16.2                                                     |
| DEFAULT_TO_DENY_NETWORK_POLICIES       | Determines how will be network policies set - to deny-all (true) or allow-all (false)                                                                 | true                                                        |
| STRIMZI_EXEC_MAX_LOG_OUTPUT_CHARACTERS | Set maximum count of characters printed from [Executor](test/src/main/java/io/strimzi/test/executor/Exec.java) stdout and stderr                      | 20000                                                       |
| RESOURCE_ALLOCATION_STRATEGY           | Set memory allocation for strimzi pod set                                                                                                             | SHARE_MEMORY_FOR_ALL_COMPONENTS                             |
| CLUSTER_OPERATOR_INSTALL_TYPE          | Specify how the CO will be deployed. `OLM` option will install operator via OLM, and you just need to set other `OLM` env variables.                  | bundle                                                      |
| CONNECT_BUILD_IMAGE_PATH               | Specify the registry together with org used by KafkaConnect build. For example `quay.io/strimzi/custom-connect-build`                                 | empty                                                       |
| CONNECT_BUILD_REGISTRY_SECRET          | Specify the registry secret used by KafkaConnect build. It has to be k8s secret deployed in `default` namespace and systemtests will handle the rest. | empty                                                       |
| CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN    | Connect image                                                                                                                                         | empty                                                       |

If you want to use your images with a different tag or from a separate repository, you can use `DOCKER_REGISTRY`, `DOCKER_ORG` and `DOCKER_TAG` environment variables.

`KUBERNETES_DOMAIN` should be specified only in case when you are using a specific configuration in your Kubernetes cluster.

##### Specific Kafka version

To set the custom Kafka version in system tests, you need to set the Environment variable `ST_KAFKA_VERSION` to one of the values in [kafka-versions](kafka-versions.yaml).

#### Using private registries

If you want to use private registries, before executing the tests, you have to create a secret and then specify the name of the created secret in the env variable called
`SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET` with the container registry credentials to be able to pull images. Note that a secret has to be created in the `default` namespace.

For tests that use `KafkaConnect` build feature, users can use their own registry to where the result image will be pushed.
The config is specified by two env variables: `CONNECT_BUILD_IMAGE_PATH` and `CONNECT_BUILD_REGISTRY_SECRET`.
`CONNECT_BUILD_IMAGE_PATH` has to be in format `<REGISTRY>/<ORGANIZATION>/<IMAGE_NAME>`.
The image format has to be without floating tag.
This is needed because our tests can be executed in parallel so each connect build needs unique tag.
The tag is generated and appended by tests during the execution.

In case the registry needs authorization, users can specify push-secret by `CONNECT_BUILD_REGISTRY_SECRET`.
Secret name should reference to k8s secret in `default` namespace.
In case any of the env vars are not specified, default Minikube or OpenShift registries will be used.

##### Cluster Operator Log level

To set the log level of Strimzi for system tests, you need to set the environment variable `STRIMZI_DEFAULT_LOG_LEVEL` with one of the following values: `ERROR`, `WARNING`, `INFO`, `DEBUG`, `TRACE`.

## Use Remote Cluster

The integration and system tests are run against a cluster specified in the environment variable `TEST_CLUSTER_CONTEXT`.
If this variable is unset, the Kubernetes client will use the currently active context.
Otherwise, it will use the context from KubeConfig with a name specified by the `TEST_CLUSTER_CONTEXT` variable.

For example, command `TEST_CLUSTER_CONTEXT=remote-cluster ./systemtest/scripts/run_tests.sh` will execute tests with cluster context `remote-cluster`.
However, since system tests use the command line `Executor` for some actions, make sure you use context from `TEST_CLUSTER_CONTEXT`.

## Helper script

The `./systemtest/scripts/run_tests.sh` script can be used to run the `systemtests` using the same configuration as used in the Azure build.
You can use this script to run the `systemtests` project efficiently.

Pass additional parameters to `mvn` by populating the `EXTRA_ARGS` env var.

    EXTRA_ARGS="-Dfoo=bar" ./systemtest/scripts/run_tests.sh

## Running single test class

Use the `verify` build goal and provide `-Dit.test=TestClassName[#testMethodName]` system property.

    mvn verify -pl systemtest -P all -Dit.test=KafkaST#testCustomAndUpdatedValues

You can also run a test with a particular feature gate enabled via the feature gate environment variable.

    STRIMZI_FEATURE_GATES="+FeatureName" mvn verify -pl systemtest -P all -Dit.test=KafkaST#testCustomAndUpdatedValues

## Skip Teardown

When debugging some types of test cases, the `SKIP_TEARDOWN` env variable can be beneficial.
Once this variable is set, the teardown phase will be skipped after the test finishes. In addition, if you keep it fixed,
the subsequent setup phase will be much faster because all components are already deployed.
Unfortunately, this approach is not applicable for tests where component configuration changes.

## Skip surefire tests

There are several tests, which are executed via Maven Surefire plugin.
Those tests are some unit tests for internal systemtest package tooling.
You can skip them by adding the `-Dskip.surefire.tests` option to the mvn command.

## Testing Cluster Operator deployment via OLM

Strimzi also supports the deployment of Cluster Operator through OperatorHub, which needs to have an updated manifest for each version and be tested.
We created a simple [OLM test suite](systemtest/src/test/java/io/strimzi/systemtest/olm), which deploys Cluster Operator and other needed components via OLM and examples from manifests.

You have to build an image with manifests and create a new `CatalogSource` resource to run these tests, pointing to the constructed image.
Dockerfile can have the following structure:
```
   FROM quay.io/openshift/origin-operator-registry:latest
   
   COPY /manifests /manifests
   
   RUN /usr/bin/initializer -m /manifests -o bundles.db
   ENTRYPOINT ["/usr/bin/registry-server"]
   CMD ["-d", "bundles.db", "-t", "termination.log"]
```

When you build a new image, you should create a new `CatalogSource` like this:

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

Now you can efficiently run OLM tests.
