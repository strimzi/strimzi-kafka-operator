/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.skodjob.testframe.LogCollector;
import io.skodjob.testframe.LogCollectorBuilder;
import io.skodjob.testframe.clients.KubeClient;
import io.skodjob.testframe.clients.cmdClient.Kubectl;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.utils.StUtils;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Class for encapsulating Test-Frame's {@link LogCollector}.
 * It provides methods for collecting logs for the test-cases -> collects Namespaces that are created for
 * particular test-class and test-case and builds the whole path to the logs.
 * The structure of the logs then looks like this:
 *
 * systemtest/target/logs/2024-10-09-18-45-39
 * └── io.strimzi.systemtest.bridge.HttpBridgeST
 *     ├── testReceiveSimpleMessage
 *     │          └── 1
 *     │              ├── co-namespace
 *     │              │          ├── configmap
 *     │              │          │          ├── kube-root-ca.crt.yaml
 *     │              │          │          ├── openshift-service-ca.crt.yaml
 *     │              │          │          └── strimzi-cluster-operator.yaml
 *     │              │          ├── deployment
 *     │              │          │          └── strimzi-cluster-operator.yaml
 *     │              │          ├── events.log
 *     │              │          ├── pod
 *     │              │          │          ├── describe-pod-strimzi-cluster-operator-57d455495c-98t8f.log
 *     │              │          │          └── logs-pod-strimzi-cluster-operator-57d455495c-98t8f-container-strimzi-cluster-operator.log
 *     │              │          └── secret
 *     │              │              ├── builder-dockercfg-j99st.yaml
 *     │              │              ├── default-dockercfg-pb9z6.yaml
 *     │              │              ├── deployer-dockercfg-ltvsj.yaml
 *     │              │              └── strimzi-cluster-operator-dockercfg-64p64.yaml
 *     │              └── test-suite-namespace
 *     │                  ├── configmap
 *     │                  │          ├── cluster-2acba704-b-6eac3ce0-0.yaml
 *     │                  │          ├── cluster-2acba704-bridge-config.yaml
 *     │                  │          ├── cluster-2acba704-c-6eac3ce0-1.yaml
 *     │                  │          ├── cluster-2acba704-entity-topic-operator-config.yaml
 *     │                  │          ├── cluster-2acba704-entity-user-operator-config.yaml
 *     │                  │          ├── kube-root-ca.crt.yaml
 *     │                  │          └── openshift-service-ca.crt.yaml
 *     │                  ├── deployment
 *     │                  │          ├── cluster-2acba704-bridge.yaml
 *     │                  │          └── cluster-2acba704-entity-operator.yaml
 *     │                  ├── events.log
 *     │                  ├── kafka
 *     │                  │          └── cluster-2acba704.yaml
 *     │                  ├── kafkabridge
 *     │                  │          └── cluster-2acba704.yaml
 *     │                  ├── kafkanodepool
 *     │                  │          ├── b-6eac3ce0.yaml
 *     │                  │          └── c-6eac3ce0.yaml
 *     │                  ├── kafkatopic
 *     │                  │          └── my-topic-446554728-1349472388.yaml
 *     │                  ├── pod
 *     │                  │          ├── describe-pod-cluster-2acba704-b-6eac3ce0-0.log
 *     │                  │          ├── describe-pod-cluster-2acba704-bridge-5dd6db4bdd-454jg.log
 *     │                  │          ├── describe-pod-cluster-2acba704-c-6eac3ce0-1.log
 *     │                  │          ├── describe-pod-cluster-2acba704-entity-operator-7db664df48-bxxrx.log
 *     │                  │          ├── logs-pod-cluster-2acba704-b-6eac3ce0-0-container-kafka.log
 *     │                  │          ├── logs-pod-cluster-2acba704-bridge-5dd6db4bdd-454jg-container-cluster-2acba704-bridge.log
 *     │                  │          ├── logs-pod-cluster-2acba704-c-6eac3ce0-1-container-kafka.log
 *     │                  │          ├── logs-pod-cluster-2acba704-entity-operator-7db664df48-bxxrx-container-topic-operator.log
 *     │                  │          └── logs-pod-cluster-2acba704-entity-operator-7db664df48-bxxrx-container-user-operator.log
 *     │                  └── secret
 *     │                      ├── builder-dockercfg-n9jrk.yaml
 *     │                      ├── cluster-2acba704-bridge-dockercfg-nqhfv.yaml
 *     │                      ├── cluster-2acba704-clients-ca-cert.yaml
 *     │                      ├── cluster-2acba704-clients-ca.yaml
 *     │                      ├── cluster-2acba704-cluster-ca-cert.yaml
 *     │                      ├── cluster-2acba704-cluster-ca.yaml
 *     │                      ├── cluster-2acba704-cluster-operator-certs.yaml
 *     │                      ├── cluster-2acba704-entity-operator-dockercfg-9dwh7.yaml
 *     │                      ├── cluster-2acba704-entity-topic-operator-certs.yaml
 *     │                      ├── cluster-2acba704-entity-user-operator-certs.yaml
 *     │                      ├── cluster-2acba704-kafka-brokers.yaml
 *     │                      ├── cluster-2acba704-kafka-dockercfg-8xq8s.yaml
 *     │                      ├── default-dockercfg-2mzxb.yaml
 *     │                      └── deployer-dockercfg-lg5z9.yaml
 * ...
 */
public class TestLogCollector {
    private static final String CURRENT_DATE;
    private final LogCollector logCollector;

    static {
        // Get current date to create a unique folder
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.of("GMT"));
        CURRENT_DATE = dateTimeFormatter.format(LocalDateTime.now());
    }

    /**
     * TestLogCollector's constructor
     */
    public TestLogCollector() {
        this.logCollector = defaultLogCollector();
    }

    /**
     * Method for creating default configuration of the {@link LogCollector}.
     * It provides default list of resources into the builder and configures the required {@link KubeClient} and
     * {@link Kubectl}
     *
     * @return  {@link LogCollector} configured with default configuration for the tests
     */
    private LogCollector defaultLogCollector() {
        List<String> resources = new ArrayList<>(List.of(
            TestConstants.SECRET.toLowerCase(Locale.ROOT),
            TestConstants.DEPLOYMENT.toLowerCase(Locale.ROOT),
            TestConstants.CONFIG_MAP.toLowerCase(Locale.ROOT),
            TestConstants.SERVICE.toLowerCase(Locale.ROOT),
            Kafka.RESOURCE_SINGULAR,
            KafkaNodePool.RESOURCE_SINGULAR,
            KafkaConnect.RESOURCE_SINGULAR,
            KafkaConnector.RESOURCE_SINGULAR,
            KafkaBridge.RESOURCE_SINGULAR,
            KafkaMirrorMaker2.RESOURCE_SINGULAR,
            KafkaRebalance.RESOURCE_SINGULAR,
            KafkaTopic.RESOURCE_SINGULAR,
            KafkaUser.RESOURCE_SINGULAR
        ));

        if (Environment.isOlmInstall()) {
            resources.addAll(List.of(
                TestConstants.OPERATOR_GROUP.toLowerCase(Locale.ROOT),
                TestConstants.SUBSCRIPTION.toLowerCase(Locale.ROOT),
                TestConstants.INSTALL_PLAN.toLowerCase(Locale.ROOT),
                TestConstants.CLUSTER_SERVICE_VERSION.toLowerCase(Locale.ROOT)
            ));
        }

        return new LogCollectorBuilder()
            .withKubeClient(new KubeClient())
            .withKubeCmdClient(new Kubectl())
            .withRootFolderPath(Environment.TEST_LOG_DIR)
            .withNamespacedResources(resources.toArray(new String[0]))
            .build();
    }

    /**
     * Method that checks existence of the folder on specified path.
     * From there, if there are no sub-dirs created - for each of the test-case run/re-run - the method returns the
     * full path containing the specified path and index (1).
     * Otherwise, it lists all the directories, filtering all the folders that are indexes, takes the last one, and returns
     * the full path containing specified path and index increased by one.
     *
     * @param rootPathToLogsForTestCase     complete path for test-class/test-class and test-case logs
     *
     * @return  full path to logs directory built from specified root path and index
     */
    private Path checkPathAndReturnFullRootPathWithIndexFolder(Path rootPathToLogsForTestCase) {
        File logsForTestCase = rootPathToLogsForTestCase.toFile();
        int index = 1;

        if (logsForTestCase.exists()) {
            String[] filesInLogsDir = logsForTestCase.list();

            if (filesInLogsDir != null && filesInLogsDir.length > 0) {
                index = Integer.parseInt(
                    Arrays
                        .stream(filesInLogsDir)
                        .filter(file -> {
                            try {
                                Integer.parseInt(file);
                                return true;
                            } catch (NumberFormatException e) {
                                return false;
                            }
                        })
                        .sorted()
                        .toList()
                        .get(filesInLogsDir.length - 1)
                ) + 1;
            }
        }

        return rootPathToLogsForTestCase.resolve(String.valueOf(index));
    }

    /**
     * Method for building the full path to logs for specified test-class and test-case.
     *
     * @param testClass     name of the test-class
     * @param testCase      name of the test-case
     *
     * @return full path to the logs for test-class and test-case, together with index
     */
    private Path buildFullPathToLogs(String testClass, String testCase) {
        Path rootPathToLogsForTestCase = Path.of(Environment.TEST_LOG_DIR, CURRENT_DATE, testClass);

        if (testCase != null) {
            rootPathToLogsForTestCase = rootPathToLogsForTestCase.resolve(testCase);
        }

        return checkPathAndReturnFullRootPathWithIndexFolder(rootPathToLogsForTestCase);
    }

    /**
     * Method that encapsulates {@link #collectLogs(String, String)}, taking the test-class and test-case names from
     * {@link ResourceManager#getTestContext()}
     */
    public void collectLogs() {
        collectLogs(
            KubeResourceManager.get().getTestContext().getRequiredTestClass().getName(),
            KubeResourceManager.get().getTestContext().getRequiredTestMethod().getName()
        );
    }

    /**
     * Method that encapsulates {@link #collectLogs(String, String)}, where test-class is passed as a parameter and the
     * test-case name is `null` -> that's used when the test fail in `@BeforeAll` or `@AfterAll` phases.
     *
     * @param testClass     name of the test-class, for which the logs should be collected
     */
    public void collectLogs(String testClass) {
        collectLogs(testClass, null);
    }

    /**
     * Method that uses {@link LogCollector#collectFromNamespaces(String...)} method for collecting logs from Namespaces
     * for the particular combination of test-class and test-case.
     *
     * @param testClass     name of the test-class, for which the logs should be collected
     * @param testCase      name of the test-case, for which the logs should be collected
     */
    public void collectLogs(String testClass, String testCase) {
        String testClassShortName = StUtils.removePackageName(testClass);
        Path rootPathToLogsForTestCase = buildFullPathToLogs(testClass, testCase);

        final LogCollector testCaseCollector = new LogCollectorBuilder(logCollector)
            .withRootFolderPath(rootPathToLogsForTestCase.toString())
            .build();

        List<String> namespaces = new ArrayList<>();

        // Old way of keeping the list of Namespaces - delete this once we are done with the integration
        namespaces.addAll(NamespaceManager.getInstance().getListOfNamespacesForTestClassAndTestCase(testClass, testCase));
        // New way using labels on the Namespaces
        namespaces.addAll(getListOfNamespaces(testClassShortName, testCase));

        namespaces = namespaces.stream().distinct().toList();

        testCaseCollector.collectFromNamespaces(namespaces.toArray(new String[0]));
    }

    /**
     * For {@param testClass} and {@param testCase} returns list of Namespaces based on the LabelSelector.
     * In case that {@param testCase} is `null` it returns just the Namespaces that are labeled with the test class.
     *
     * @param testClass     name of the test class for which we should collect logs
     * @param testCase      name of the test case for which we should collect logs
     *
     * @return  list of Namespaces from which we should collect logs
     */
    private List<String> getListOfNamespaces(String testClass, String testCase) {
        List<String> namespaces = new ArrayList<>(KubeResourceManager.get().kubeClient().getClient()
            .namespaces()
            .withLabelSelector(getTestClassLabelSelector(testClass))
            .list()
            .getItems()
            .stream()
            .map(namespace -> namespace.getMetadata().getName())
            .toList());

        if (testCase != null) {
            namespaces.addAll(
                KubeResourceManager.get().kubeClient().getClient()
                    .namespaces()
                    .withLabelSelector(getTestCaseLabelSelector(testClass, testCase))
                    .list()
                    .getItems()
                    .stream()
                    .map(namespace -> namespace.getMetadata().getName())
                    .toList()
            );
        }

        return namespaces;
    }

    /**
     * Returns LabelSelector for the {@param testClass}.
     *
     * @param testClass     name of the test class for which we should collect logs
     *
     * @return  LabelSelector for the test class
     */
    private LabelSelector getTestClassLabelSelector(String testClass) {
        return new LabelSelectorBuilder()
            .withMatchLabels(Map.of(TestConstants.TEST_SUITE_NAME_LABEL, testClass))
            .build();
    }

    /**
     * Returns LabelSelector for the {@param testClass} and {@param testCase}.
     *
     * @param testClass     name of the test class for which we should collect logs
     * @param testCase      name of the test case for which we should collect logs
     *
     * @return  LabelSelector for the test class and test case
     */
    private LabelSelector getTestCaseLabelSelector(String testClass, String testCase) {
        return new LabelSelectorBuilder()
            .withMatchLabels(
                Map.of(
                    TestConstants.TEST_SUITE_NAME_LABEL, testClass,
                    TestConstants.TEST_CASE_NAME_LABEL, StUtils.trimTestCaseBaseOnItsLength(testCase)
                )
            )
            .build();
    }
}
