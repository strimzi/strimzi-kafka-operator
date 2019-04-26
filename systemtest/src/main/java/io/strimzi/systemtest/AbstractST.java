/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.jayway.jsonpath.JsonPath;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.clients.lib.KafkaClient;
import io.strimzi.systemtest.interfaces.TestSeparator;
import io.strimzi.systemtest.utils.TestExecutionWatcher;
import io.strimzi.test.BaseITST;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.HelmClient;
import io.strimzi.test.k8s.KubeClusterException;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.systemtest.matchers.Matchers.logHasNoUnexpectedErrors;
import static io.strimzi.test.TestUtils.entry;
import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.TestUtils.toYamlString;
import static io.strimzi.test.TestUtils.waitFor;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
@ExtendWith(TestExecutionWatcher.class)
public abstract class AbstractST extends BaseITST implements TestSeparator {

    static {
        Crds.registerCustomKinds();
    }

    protected static final Environment ENVIRONMENT = Environment.getInstance();

    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);
    protected static final String CLUSTER_NAME = "my-cluster";
    protected static final String ZK_IMAGE = "STRIMZI_DEFAULT_ZOOKEEPER_IMAGE";
    protected static final String KAFKA_IMAGE_MAP = "STRIMZI_KAFKA_IMAGES";
    protected static final String KAFKA_CONNECT_IMAGE_MAP = "STRIMZI_KAFKA_CONNECT_IMAGES";
    protected static final String TO_IMAGE = "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE";
    protected static final String UO_IMAGE = "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE";
    protected static final String KAFKA_INIT_IMAGE = "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE";
    protected static final String TLS_SIDECAR_ZOOKEEPER_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ZOOKEEPER_IMAGE";
    protected static final String TLS_SIDECAR_KAFKA_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE";
    protected static final String TLS_SIDECAR_EO_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE";
    protected static final String TEST_TOPIC_NAME = "test-topic";
    private static final String CLUSTER_OPERATOR_PREFIX = "strimzi";

    public static final String TOPIC_CM = "../examples/topic/kafka-topic.yaml";
    public static final String HELM_CHART = "../helm-charts/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";
    public static final String REQUESTS_MEMORY = "512Mi";
    public static final String REQUESTS_CPU = "200m";
    public static final String LIMITS_MEMORY = "512Mi";
    public static final String LIMITS_CPU = "1000m";

    public static final String TEST_LOG_DIR = ENVIRONMENT.getTestLogDir();

    Resources testMethodResources;
    static Resources testClassResources;
    static String operationID;
    Random rng = new Random();

    protected HelmClient helmClient() {
        return CLUSTER.helmClient();
    }

    static String kafkaClusterName(String clusterName) {
        return KafkaResources.kafkaStatefulSetName(clusterName);
    }

    static String kafkaConnectName(String clusterName) {
        return clusterName + "-connect";
    }

    static String kafkaMirrorMakerName(String clusterName) {
        return clusterName + "-mirror-maker";
    }

    static String kafkaPodName(String clusterName, int podId) {
        return KafkaResources.kafkaPodName(clusterName, podId);
    }

    static String kafkaServiceName(String clusterName) {
        return KafkaResources.bootstrapServiceName(clusterName);
    }

    static String kafkaHeadlessServiceName(String clusterName) {
        return KafkaResources.brokersServiceName(clusterName);
    }

    static String kafkaMetricsConfigName(String clusterName) {
        return KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName);
    }

    static String zookeeperClusterName(String clusterName) {
        return KafkaResources.zookeeperStatefulSetName(clusterName);
    }

    static String zookeeperPodName(String clusterName, int podId) {
        return KafkaResources.zookeeperPodName(clusterName, podId);
    }

    static String zookeeperServiceName(String clusterName) {
        return zookeeperClusterName(clusterName) + "-client";
    }

    static String zookeeperHeadlessServiceName(String clusterName) {
        return zookeeperClusterName(clusterName) + "-nodes";
    }

    static String zookeeperMetricsConfigName(String clusterName) {
        return KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName);
    }

    static String zookeeperPVCName(String clusterName, int podId) {
        return "data-" + zookeeperClusterName(clusterName) + "-" + podId;
    }

    static String entityOperatorDeploymentName(String clusterName) {
        return KafkaResources.entityOperatorDeploymentName(clusterName);
    }

    private <T extends CustomResource, L extends CustomResourceList<T>, D extends Doneable<T>>
        void replaceCrdResource(Class<T> crdClass, Class<L> listClass, Class<D> doneableClass, String resourceName, Consumer<T> editor) {
        Resource<T, D> namedResource = Crds.operation(kubeClient().getClient(), crdClass, listClass, doneableClass).inNamespace(kubeClient().getNamespace()).withName(resourceName);
        T resource = namedResource.get();
        editor.accept(resource);
        namedResource.replace(resource);
    }

    void replaceKafkaResource(String resourceName, Consumer<Kafka> editor) {
        replaceCrdResource(Kafka.class, KafkaList.class, DoneableKafka.class, resourceName, editor);
    }

    void replaceKafkaConnectResource(String resourceName, Consumer<KafkaConnect> editor) {
        replaceCrdResource(KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class, resourceName, editor);
    }

    void replaceTopicResource(String resourceName, Consumer<KafkaTopic> editor) {
        replaceCrdResource(KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class, resourceName, editor);
    }

    void replaceMirrorMakerResource(String resourceName, Consumer<KafkaMirrorMaker> editor) {
        replaceCrdResource(KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class, resourceName, editor);
    }

    String getBrokerApiVersions(String podName) {
        AtomicReference<String> versions = new AtomicReference<>();
        waitFor("kafka-broker-api-versions.sh success", Constants.GET_BROKER_API_INTERVAL, Constants.GET_BROKER_API_TIMEOUT, () -> {
            try {
                String output = kubeClient().execInPod(podName, "kafka",
                        "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092");
                versions.set(output);
                return true;
            } catch (KubeClusterException e) {
                LOGGER.trace("/opt/kafka/bin/kafka-broker-api-versions.sh: {}", e.getMessage());
                return false;
            }
        });
        return versions.get();
    }

    void waitForZkMntr(Pattern pattern, int... podIndexes) {
        long timeoutMs = 120_000L;
        long pollMs = 1_000L;

        for (int podIndex : podIndexes) {
            String zookeeperPod = zookeeperPodName(CLUSTER_NAME, podIndex);
            String zookeeperPort = String.valueOf(2181 * 10 + podIndex);
            waitFor("mntr", pollMs, timeoutMs, () -> {
                try {
                    String output = kubeClient().execInPod(zookeeperPod, "zookeeper",
                        "/bin/bash", "-c", "echo mntr | nc localhost " + zookeeperPort);

                    if (pattern.matcher(output).find()) {
                        return true;
                    }
                } catch (KubeClusterException e) {
                    LOGGER.trace("Exception while waiting for ZK to become leader/follower, ignoring", e);
                }
                return false;
                },
                () -> LOGGER.info("zookeeper `mntr` output at the point of timeout does not match {}:{}{}",
                    pattern.pattern(),
                    System.lineSeparator(),
                    indent(kubeClient().execInPod(zookeeperPod, "zookeeper", "/bin/bash", "-c", "echo mntr | nc localhost " + zookeeperPort)))
            );
        }
    }

    /**
     * Translate key/value pairs fromatted like properties into a Map
     * @param keyValuePairs Pairs in key=value format; pairs are separated by newlines
     * @return THe map of key/values
     */
    static Map<String, String> loadProperties(String keyValuePairs) {
        try {
            Properties actual = new Properties();
            actual.load(new StringReader(keyValuePairs));
            return (Map) actual;
        } catch (IOException e) {
            throw new AssertionError("Invalid Properties definiton", e);
        }
    }

    /**
     * Get a Map of properties from an environment variable in json.
     * @param json The json from which to extract properties
     * @param envVar The environment variable name
     * @return The properties which the variable contains
     */
    static Map<String, String> getPropertiesFromJson(String json, String envVar) {
        List<String> array = JsonPath.parse(json).read(globalVariableJsonPathBuilder(envVar));
        return loadProperties(array.get(0));
    }

    /**
     * Get a jsonPath which can be used to extract envariable variables from a spec
     * @param envVar The environment variable name
     * @return The json path
     */
    static String globalVariableJsonPathBuilder(String envVar) {
        return "$.spec.containers[*].env[?(@.name=='" + envVar + "')].value";
    }

    List<Event> getEvents(String resourceUid) {
        return CLIENT.events().inNamespace(KUBE_CLIENT.namespace()).list().getItems().stream()
                .filter(event -> event.getInvolvedObject().getUid().equals(resourceUid))
                .collect(Collectors.toList());
    }

    public void sendMessages(String podName, String clusterName, String topic, int messagesCount) {
        LOGGER.info("Sending messages");
        String command = "sh bin/kafka-verifiable-producer.sh --broker-list " +
                KafkaResources.plainBootstrapAddress(clusterName) + " --topic " + topic + " --max-messages " + messagesCount + "";

        LOGGER.info("Command for kafka-verifiable-producer.sh {}", command);

        kubeClient().execInPod(podName, containerName, "/bin/bash", "-c", command);
    }

    protected void assertResources(String namespace, String podName, String containerName, String memoryLimit, String cpuLimit, String memoryRequest, String cpuRequest) {
        Pod po = kubeClient().getPod(podName);
        assertNotNull(po, "Not found an expected pod  " + podName + " in namespace " + namespace + " but found " +
                kubeClient().listPods().stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()));

        Optional optional = po.getSpec().getContainers().stream().filter(c -> c.getName().equals(containerName)).findFirst();
        assertTrue(optional.isPresent(), "Not found an expected container " + containerName);

        Container container = (Container) optional.get();
        Map<String, Quantity> limits = container.getResources().getLimits();
        assertEquals(memoryLimit, limits.get("memory").getAmount());
        assertEquals(cpuLimit, limits.get("cpu").getAmount());
        Map<String, Quantity> requests = container.getResources().getRequests();
        assertEquals(memoryRequest, requests.get("memory").getAmount());
        assertEquals(cpuRequest, requests.get("cpu").getAmount());
    }

    protected void assertExpectedJavaOpts(String podName, String containerName, String expectedXmx, String expectedXms, String expectedServer, String expectedXx) {
        List<List<String>> cmdLines = commandLines(podName, containerName, "java");
        assertEquals(1, cmdLines.size(), "Expected exactly 1 java process to be running");
        List<String> cmd = cmdLines.get(0);
        int toIndex = cmd.indexOf("-jar");
        if (toIndex != -1) {
            // Just consider arguments to the JVM, not the application running in it
            cmd = cmd.subList(0, toIndex);
            // We should do something similar if the class not -jar was given, but that's
            // hard to do properly.
        }
        assertCmdOption(cmd, expectedXmx);
        assertCmdOption(cmd, expectedXms);
        assertCmdOption(cmd, expectedServer);
        assertCmdOption(cmd, expectedXx);
    }

    private void assertCmdOption(List<String> cmd, String expectedXmx) {
        if (!cmd.contains(expectedXmx)) {
            fail("Failed to find argument matching " + expectedXmx + " in java command line " +
                    cmd.stream().collect(Collectors.joining("\n")));
        }
    }

    private List<List<String>> commandLines(String podName, String containerName, String cmd) {
        List<List<String>> result = new ArrayList<>();
        String output = kubeClient().execInPod(podName, containerName, "/bin/bash", "-c",
                "for pid in $(ps -C java -o pid h); do cat /proc/$pid/cmdline; done"
        );
        for (String cmdLine : output.split("\n")) {
            result.add(asList(cmdLine.split("\0")));
        }
        return result;
    }

    void assertNoCoErrorsLogged(long sinceSeconds) {
        LOGGER.info("Search in strimzi-cluster-operator log for errors in last {} seconds", sinceSeconds);
        String clusterOperatorLog = cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", sinceSeconds, "Exception", "Error", "Throwable");
        assertThat(clusterOperatorLog, logHasNoUnexpectedErrors());
    }

    public List<String> listTopicsUsingPodCLI(String clusterName, int zkPodId) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return Arrays.asList(kubeClient().execInPod(podName, "zookeeper", "/bin/bash", "-c",
                "bin/kafka-topics.sh --list --zookeeper localhost:" + port).split("\\s+"));
    }

    public String createTopicUsingPodCLI(String clusterName, int zkPodId, String topic, int replicationFactor, int partitions) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return kubeClient().execInPod(podName, "zookeeper", "/bin/bash", "-c",
                "bin/kafka-topics.sh --zookeeper localhost:" + port + " --create " + " --topic " + topic +
                        " --replication-factor " + replicationFactor + " --partitions " + partitions);
    }

    public String deleteTopicUsingPodCLI(String clusterName, int zkPodId, String topic) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return kubeClient().execInPod(podName, "zookeeper", "/bin/bash", "-c",
                "bin/kafka-topics.sh --zookeeper localhost:" + port + " --delete --topic " + topic);
    }

    public List<String>  describeTopicUsingPodCLI(String clusterName, int zkPodId, String topic) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return Arrays.asList(kubeClient().execInPod(podName, "zookeeper", "/bin/bash", "-c",
                "bin/kafka-topics.sh --zookeeper localhost:" + port + " --describe --topic " + topic).split("\\s+"));
    }

    public String updateTopicPartitionsCountUsingPodCLI(String clusterName, int zkPodId, String topic, int partitions) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return kubeClient().execInPod(podName, "zookeeper", "/bin/bash", "-c",
                "bin/kafka-topics.sh --zookeeper localhost:" + port + " --alter --topic " + topic + " --partitions " + partitions);
    }

    public Map<String, String> getImagesFromConfig() {
        Map<String, String> images = new HashMap<>();
        for (Container c : kubeClient().getDeployment("strimzi-cluster-operator").getSpec().getTemplate().getSpec().getContainers()) {
            for (EnvVar envVar : c.getEnv()) {
                images.put(envVar.getName(), envVar.getValue());
            }
        }
        return images;
    }

    public String getContainerImageNameFromPod(String podName) {
        return kubeClient().getPod(podName).getSpec().getContainers().get(0).getImage();
    }

    public String getContainerImageNameFromPod(String podName, String containerName) {
        return kubeClient().getPod(podName).getSpec().getContainers().stream()
                .filter(c -> c.getName().equals(containerName))
                .findFirst().get().getImage();
    }

    public String  getInitContainerImageName(String podName) {
        return kubeClient().getPod(podName).getSpec().getInitContainers().stream()
                .findFirst().get().getImage();
    }

    protected void createTestMethodResources() {
        LOGGER.info("Creating resources before the test");
        testMethodResources = new Resources(kubeClient());
    }

    protected static void createTestClassResources() {
        LOGGER.info("Creating test class resources");
        testClassResources = new Resources(kubeClient());
    }

    protected void deleteTestMethodResources() throws Exception {
        if (testMethodResources != null) {
            testMethodResources.deleteResources();
            testMethodResources = null;
        }
    }

    Resources testMethodResources() {
        return testMethodResources;
    }

    String startTimeMeasuring(Operation operation) {
        TimeMeasuringSystem.setTestName(testClass, testName);
        return TimeMeasuringSystem.startOperation(operation);
    }

    String clusterCaCertSecretName(String cluster) {
        return cluster + "-cluster-ca-cert";
    }

    void waitTillSecretExists(String secretName) {
        waitFor("secret " + secretName + " exists", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> kubeClient().getSecret(secretName) != null);
        try {
            Thread.sleep(60000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void waitForPodDeletion(String namespace, String podName) {
        LOGGER.info("Waiting when Pod {} will be deleted", podName);

        TestUtils.waitFor("statefulset " + podName, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> CLIENT.pods().inNamespace(namespace).withName(podName).get() == null);
    }

    /**
     * Wait till all pods in specific namespace being deleted and recreate testing environment in case of some pods cannot be deleted.
     * @param time timeout in miliseconds
     * @throws Exception exception
     */
    void waitForDeletion(long time, String namespace) throws Exception {
        List<Pod> pods = CLIENT.pods().inNamespace(namespace).list().getItems().stream().filter(
            p -> !p.getMetadata().getName().startsWith(CLUSTER_OPERATOR_PREFIX)).collect(Collectors.toList());
        // Delete pods in case of kubernetes keep them up
        pods.forEach(p -> CLIENT.pods().inNamespace(namespace).delete(p));

        LOGGER.info("Wait for {} ms after cleanup to make sure everything is deleted", time);
        Thread.sleep(time);

        // Collect pods again after proper removal
        pods = kubeClient().listPods().stream().filter(
                p -> !p.getMetadata().getName().startsWith(CLUSTER_OPERATOR_PREFIX)).collect(Collectors.toList());
        long podCount = pods.size();

        StringBuilder nonTerminated = new StringBuilder();
        if (podCount > 0) {
            List<Pod> pods = kubeClient().listPods().stream().filter(
                p -> !p.getMetadata().getName().startsWith(CLUSTER_OPERATOR_PREFIX)).collect(Collectors.toList());
            pods.forEach(
                p -> nonTerminated.append("\n").append(p.getMetadata().getName()).append(" - ").append(p.getStatus().getPhase())
            );
            throw new Exception("There are some unexpected pods! Cleanup is not finished properly!" + nonTerminated);
        }
    }

    /**
     * Recreate namespace and CO after test failure
     * @param coNamespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to.
     */
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        testClassResources.deleteResources();

        deleteClusterOperatorInstallFiles();
        deleteNamespaces();

        createNamespaces(coNamespace, bindingsNamespaces);
        applyClusterOperatorInstallFiles();

        testClassResources = new Resources(kubeClient());

        applyRoleBindings(coNamespace, bindingsNamespaces);
        // 050-Deployment
        testClassResources.clusterOperator(coNamespace).done();
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces list of namespaces where Bindings should be deployed to
     */
    private static void applyRoleBindings(String namespace, List<String> bindingsNamespaces) {
        for (String bindingsNamespace : bindingsNamespaces) {
            // 020-RoleBinding
            testClassResources.kubernetesRoleBinding("../install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 021-ClusterRoleBinding
            testClassResources.kubernetesClusterRoleBinding("../install/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 030-ClusterRoleBinding
            testClassResources.kubernetesClusterRoleBinding("../install/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace, bindingsNamespace);
            // 031-RoleBinding
            testClassResources.kubernetesRoleBinding("../install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", namespace, bindingsNamespace);
            // 032-RoleBinding
            testClassResources.kubernetesRoleBinding("../install/cluster-operator/032-RoleBinding-strimzi-cluster-operator-topic-operator-delegation.yaml", namespace, bindingsNamespace);
        }
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     */
    static void applyRoleBindings(String namespace) {
        applyRoleBindings(namespace, Collections.singletonList(namespace));
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to
     */
    static void applyRoleBindings(String namespace, String... bindingsNamespaces) {
        applyRoleBindings(namespace, Arrays.asList(bindingsNamespaces));
    }

    /**
     * Deploy CO via helm chart. Using config file stored in test resources.
     */
    void deployClusterOperatorViaHelmChart() {
        String dockerOrg = ENVIRONMENT.getStrimziOrg();
        String dockerTag = ENVIRONMENT.getStrimziTag();

        Map<String, String> values = Collections.unmodifiableMap(Stream.of(
                entry("imageRepositoryOverride", dockerOrg),
                entry("imageTagOverride", dockerTag),
                entry("image.pullPolicy", Constants.IMAGE_PULL_POLICY),
                entry("resources.requests.memory", REQUESTS_MEMORY),
                entry("resources.requests.cpu", REQUESTS_CPU),
                entry("resources.limits.memory", LIMITS_MEMORY),
                entry("resources.limits.cpu", LIMITS_CPU),
                entry("logLevel", ENVIRONMENT.getStrimziLogLevel()))
                .collect(TestUtils.entriesToMap()));

        LOGGER.info("Creating cluster operator with Helm Chart before test class {}", testClass);
        Path pathToChart = new File(HELM_CHART).toPath();
        String oldNamespace = setNamespace("kube-system");
        InputStream helmAccountAsStream = getClass().getClassLoader().getResourceAsStream("helm/helm-service-account.yaml");
        String helmServiceAccount = TestUtils.readResource(helmAccountAsStream);
        cmdKubeClient().applyContent(helmServiceAccount);
        helmClient().init();
        setNamespace(oldNamespace);
        helmClient().install(pathToChart, HELM_RELEASE_NAME, values);
    }

    /**
     * Delete CO deployed via helm chart.
     */
    void deleteClusterOperatorViaHelmChart() {
        LOGGER.info("Deleting cluster operator with Helm Chart after test class {}", testClass);
        helmClient().delete(HELM_RELEASE_NAME);
    }

    /**
     * Wait for cluster availability, check availability of external routes with TLS
     * @param userName user name
     * @param namespace cluster namespace
     * @throws Exception
     */
    void waitForClusterAvailabilityTls(String userName, String namespace) throws Exception {
        int messageCount = 50;
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);

        KafkaClient testClient = new KafkaClient();
        try {
            Future producer = testClient.sendMessagesTls(topicName, namespace, CLUSTER_NAME, userName, messageCount);
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, CLUSTER_NAME, userName, messageCount);

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
            e.printStackTrace();
            throw e;
        } finally {
            testClient.close();
        }
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @throws Exception
     */
    void waitForClusterAvailability(String namespace) throws Exception {
        int messageCount = 50;
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);

        KafkaClient testClient = new KafkaClient();
        try {
            Future producer = testClient.sendMessages(topicName, namespace, CLUSTER_NAME, messageCount);
            Future consumer = testClient.receiveMessages(topicName, namespace, CLUSTER_NAME, messageCount);

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
            e.printStackTrace();
            throw e;
        } finally {
            testClient.close();
        }
    }

    void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
    }

    void tearDownEnvironmentAfterAll() {
        testClassResources.deleteResources();
    }

    @AfterEach
    void teardownEnvironmentMethod(ExtensionContext context) throws Exception {
        if (ENVIRONMENT.getNoteardown() == null) {
            if (context.getExecutionException().isPresent()) {
                LOGGER.info("Test execution contains exception, going to recreate test environment");
                recreateTestEnv(clusterOperatorNamespace, bindingsNamespaces);
                LOGGER.info("Env recreated.");
            }
            tearDownEnvironmentAfterEach();
        }
    }

    @AfterAll
    void teardownEnvironmentClass() {
        if (ENVIRONMENT.getNoteardown() == null) {
            tearDownEnvironmentAfterAll();
            teardownEnvForOperator();
        }
    }
}