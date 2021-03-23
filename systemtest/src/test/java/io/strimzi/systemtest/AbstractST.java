/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.interfaces.IndicativeSentences;
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.resources.specific.HelmResource;
import io.strimzi.systemtest.resources.specific.OlmResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.Minishift;
import io.strimzi.test.k8s.cluster.OpenShift;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.matchers.Matchers.logHasNoUnexpectedErrors;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestExecutionWatcher.class)
@DisplayNameGeneration(IndicativeSentences.class)
public abstract class AbstractST implements TestSeparator {

    static {
        Crds.registerCustomKinds();
    }

    protected final ResourceManager resourceManager = ResourceManager.getInstance();
    protected final HelmResource helmResource = new HelmResource();
    protected OlmResource olmResource;
    protected KubeClusterResource cluster;
    protected static TimeMeasuringSystem timeMeasuringSystem = TimeMeasuringSystem.getInstance();
    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);
    private final Object lock = new Object();
    private final Object lockForTimeMeasuringSystem = new Object();

    // maps for local variables {thread safe}
    protected static Map<String, String> mapWithClusterNames = new HashMap<>();
    protected static Map<String, String> mapWithTestTopics = new HashMap<>();
    protected static Map<String, String> mapWithTestUsers = new HashMap<>();
    protected static Map<String, String> mapWithKafkaClientNames = new HashMap<>();

    protected static final String CLUSTER_NAME_PREFIX = "my-cluster-";
    protected static final String KAFKA_IMAGE_MAP = "STRIMZI_KAFKA_IMAGES";
    protected static final String KAFKA_CONNECT_IMAGE_MAP = "STRIMZI_KAFKA_CONNECT_IMAGES";
    protected static final String KAFKA_MIRROR_MAKER_2_IMAGE_MAP = "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES";
    protected static final String TO_IMAGE = "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE";
    protected static final String UO_IMAGE = "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE";
    protected static final String KAFKA_INIT_IMAGE = "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE";
    protected static final String TLS_SIDECAR_EO_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE";
    protected static final String TEST_TOPIC_NAME = "test-topic";

    private Stack<String> clusterOperatorConfigs = new Stack<>();
    public static final String CO_INSTALL_DIR = TestUtils.USER_PATH + "/../packaging/install/cluster-operator";

    public static Random rng = new Random();

    public static final int MESSAGE_COUNT = 100;
    public static final String USER_NAME = KafkaUserUtils.generateRandomNameOfKafkaUser();
    public static final String TOPIC_NAME = KafkaTopicUtils.generateRandomNameOfTopic();

    // Constants for host aliases tests
    protected final String aliasIp = "34.89.152.196";
    protected final String aliasHostname = "strimzi";
    protected final String etcHostsData = "# Entries added by HostAliases.\n" + aliasIp + "\t" + aliasHostname;

    /**
     * This method install Strimzi Cluster Operator based on environment variable configuration.
     * It can install operator by classic way (apply bundle yamls) or use OLM. For OLM you need to set all other OLM env variables.
     * Don't use this method in tests, where specific configuration of CO is needed.
     * @param namespace namespace where CO should be installed into
     */
    protected void installClusterOperator(ExtensionContext extensionContext, String clusterOperatorName, String namespace, List<String> bindingsNamespaces, long operationTimeout, long reconciliationInterval) {
        if (Environment.isOlmInstall()) {
            LOGGER.info("Going to install ClusterOperator via OLM");
            cluster.setNamespace(namespace);
            cluster.createNamespace(namespace);
            olmResource = new OlmResource(namespace);
            olmResource.create(namespace, operationTimeout, reconciliationInterval);
        } else if (Environment.isHelmInstall()) {
            LOGGER.info("Going to install ClusterOperator via Helm");
            cluster.setNamespace(namespace);
            cluster.createNamespace(namespace);
            helmResource.create(operationTimeout, reconciliationInterval);
        } else {
            LOGGER.info("Going to install ClusterOperator via Yaml bundle");
            prepareEnvForOperator(extensionContext,  namespace, bindingsNamespaces);
            if (Environment.isNamespaceRbacScope()) {
                // if roles only, only deploy the rolebindings
                applyRoleBindings(extensionContext, namespace, namespace);
            } else {
                applyBindings(extensionContext, namespace, bindingsNamespaces);
            }
            // 060-Deployment
            ResourceManager.setCoDeploymentName(clusterOperatorName);
            ResourceManager.getInstance().createResource(extensionContext, BundleResource.clusterOperator(clusterOperatorName, namespace, operationTimeout, reconciliationInterval).build());
        }
    }

    protected void installClusterOperator(ExtensionContext extensionContext, String clusterOperatorName, String namespace, long operationTimeout, long reconciliationInterval) {
        installClusterOperator(extensionContext, clusterOperatorName, namespace, Collections.singletonList(namespace), operationTimeout, reconciliationInterval);
    }

    protected void installClusterOperator(ExtensionContext extensionContext, String namespace, long operationTimeout, long reconciliationInterval) {
        installClusterOperator(extensionContext, Constants.STRIMZI_DEPLOYMENT_NAME, namespace, operationTimeout, reconciliationInterval);
    }

    protected void installClusterOperator(ExtensionContext extensionContext, String name, String namespace) {
        installClusterOperator(extensionContext, name, namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    protected void installClusterOperator(ExtensionContext extensionContext, String namespace, long operationTimeout) {
        installClusterOperator(extensionContext, Constants.STRIMZI_DEPLOYMENT_NAME, namespace, operationTimeout, Constants.RECONCILIATION_INTERVAL);
    }

    protected void installClusterOperator(ExtensionContext extensionContext, String namespace) {
        installClusterOperator(extensionContext, Constants.STRIMZI_DEPLOYMENT_NAME, namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    /**
     * Perform application of ServiceAccount, Roles and CRDs needed for proper cluster operator deployment.
     * Configuration files are loaded from packaging/install/cluster-operator directory.
     */
    public void applyClusterOperatorInstallFiles(String namespace) {
        clusterOperatorConfigs.clear();
        List<File> operatorFiles = Arrays.stream(new File(CO_INSTALL_DIR).listFiles()).sorted()
                .filter(File::isFile)
                .filter(file ->
                        !file.getName().matches(".*(Binding|Deployment)-.*"))
                .collect(Collectors.toList());

        for (File operatorFile : operatorFiles) {
            File createFile = operatorFile;
            if (operatorFile.getName().contains("ClusterRole-")) {
                createFile = switchClusterRolesToRolesIfNeeded(createFile);
            }

            LOGGER.info("Creating configuration file: {}", createFile.getAbsolutePath());
            cmdKubeClient().namespace(namespace).createOrReplace(createFile);
            clusterOperatorConfigs.push(createFile.getPath());
        }
    }

    /**
     * Replace all references to ClusterRole to Role.
     * This includes ClusterRoles themselves as well as RoleBindings that reference them.
     */
    public static File switchClusterRolesToRolesIfNeeded(File oldFile) {
        if (Environment.isNamespaceRbacScope()) {
            try {
                File tmpFile = File.createTempFile("rbac-" + oldFile.getName().replace(".yaml", ""), ".yaml");
                TestUtils.writeFile(tmpFile.getAbsolutePath(), TestUtils.readFile(oldFile).replace("ClusterRole", "Role"));
                LOGGER.info("Replaced ClusterRole for Role in {}", oldFile.getAbsolutePath());

                return tmpFile;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return oldFile;
        }
    }

    /**
     * Delete ServiceAccount, Roles and CRDs from kubernetes cluster.
     */
    public void deleteClusterOperatorInstallFiles() {
        while (!clusterOperatorConfigs.empty()) {
            String clusterOperatorConfig = clusterOperatorConfigs.pop();
            LOGGER.info("Deleting configuration file: {}", clusterOperatorConfig);
            cmdKubeClient().delete(clusterOperatorConfig);
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param namespaces list of namespaces which will be created
     * @param resources list of path to yaml files with resources specifications
     */
    protected void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace, List<String> namespaces, String... resources) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());
        cluster.createNamespaces(clientNamespace, namespaces);
        cluster.createCustomResources(resources);
        applyClusterOperatorInstallFiles(clientNamespace);
        NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, namespaces);

        if (cluster.cluster() instanceof Minishift || cluster.cluster() instanceof OpenShift) {
            // This is needed in case you are using internal kubernetes registry and you want to pull images from there
            if (kubeClient().getNamespace(Environment.STRIMZI_ORG) != null) {
                for (String namespace : namespaces) {
                    LOGGER.debug("Setting group policy for Openshift registry in namespace: " + namespace);
                    Exec.exec(null, Arrays.asList("oc", "policy", "add-role-to-group", "system:image-puller", "system:serviceaccounts:" + namespace, "-n", Environment.STRIMZI_ORG), 0, false, false);
                }
            }
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param resources list of path to yaml files with resources specifications
     */
    protected void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace, String... resources) {
        prepareEnvForOperator(extensionContext, clientNamespace, Collections.singletonList(clientNamespace), resources);
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     */
    protected void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace) {
        prepareEnvForOperator(extensionContext, clientNamespace, Collections.singletonList(clientNamespace));
    }

    /**
     * Clear cluster from all created namespaces and configurations files for cluster operator.
     */
    protected void teardownEnvForOperator() {
        deleteClusterOperatorInstallFiles();
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }

    /**
     * Method to apply Strimzi cluster operator specific RoleBindings and ClusterRoleBindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces list of namespaces where Bindings should be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace, List<String> bindingsNamespaces) {
        for (String bindingsNamespace : bindingsNamespaces) {
            applyClusterRoleBindings(extensionContext, namespace);
            applyRoleBindings(extensionContext, namespace, bindingsNamespace);
        }
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace) {
        applyBindings(extensionContext, namespace, Collections.singletonList(namespace));
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace, String... bindingsNamespaces) {
        applyBindings(extensionContext, namespace, Arrays.asList(bindingsNamespaces));
    }

    private static void applyClusterRoleBindings(ExtensionContext extensionContext, String namespace) {
        // 021-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace);
        // 030-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace);
        // 033-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRoleBinding-strimzi-cluster-operator-kafka-client-delegation.yaml", namespace);
    }

    protected static void applyRoleBindings(ExtensionContext extensionContext, String namespace, String bindingsNamespace) {
        // 020-RoleBinding
        RoleBindingResource.roleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
        // 031-RoleBinding
        RoleBindingResource.roleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", namespace, bindingsNamespace);
        // 032-RoleBinding
        RoleBindingResource.roleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/032-RoleBinding-strimzi-cluster-operator-topic-operator-delegation.yaml", namespace, bindingsNamespace);
    }

    protected void assertResources(String namespace, String podName, String containerName, String memoryLimit, String cpuLimit, String memoryRequest, String cpuRequest) {
        Pod po = kubeClient().getPod(podName);
        assertThat("Not found an expected pod  " + podName + " in namespace " + namespace + " but found " +
            kubeClient().listPods().stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()), po, is(notNullValue()));

        Optional optional = po.getSpec().getContainers().stream().filter(c -> c.getName().equals(containerName)).findFirst();
        assertThat("Not found an expected container " + containerName, optional.isPresent(), is(true));

        Container container = (Container) optional.get();
        Map<String, Quantity> limits = container.getResources().getLimits();
        assertThat(limits.get("memory"), is(new Quantity(memoryLimit)));
        assertThat(limits.get("cpu"), is(new Quantity(cpuLimit)));
        Map<String, Quantity> requests = container.getResources().getRequests();
        assertThat(requests.get("memory"), is(new Quantity(memoryRequest)));
        assertThat(requests.get("cpu"), is(new Quantity(cpuRequest)));
    }

    private void assertCmdOption(List<String> cmd, String expectedXmx) {
        if (!cmd.contains(expectedXmx)) {
            fail("Failed to find argument matching " + expectedXmx + " in java command line " +
                cmd.stream().collect(Collectors.joining("\n")));
        }
    }

    private List<List<String>> commandLines(String podName, String containerName, String cmd) {
        List<List<String>> result = new ArrayList<>();
        String output = cmdKubeClient().execInPodContainer(podName, containerName, "/bin/bash", "-c",
            "for pid in $(ps -C java -o pid h); do cat /proc/$pid/cmdline; done"
        ).out();
        for (String cmdLine : output.split("\n")) {
            result.add(asList(cmdLine.split("\0")));
        }
        return result;
    }

    protected void assertExpectedJavaOpts(String podName, String containerName, String expectedXmx, String expectedXms, String expectedXx) {
        List<List<String>> cmdLines = commandLines(podName, containerName, "java");
        assertThat("Expected exactly 1 java process to be running", cmdLines.size(), is(1));
        List<String> cmd = cmdLines.get(0);
        int toIndex = cmd.indexOf("-jar");
        if (toIndex != -1) {
            // Just consider arguments to the JVM, not the application running in it
            cmd = cmd.subList(0, toIndex);
            // We should do something similar if the class not -jar was given, but that's
            // hard to do properly.
        }
        if (expectedXmx != null)
            assertCmdOption(cmd, expectedXmx);
        if (expectedXms != null)
            assertCmdOption(cmd, expectedXms);
        if (expectedXx != null)
            assertCmdOption(cmd, expectedXx);
    }

    public Map<String, String> getImagesFromConfig() {
        Map<String, String> images = new HashMap<>();
        LOGGER.info(ResourceManager.getCoDeploymentName());
        for (Container c : kubeClient().getDeployment(ResourceManager.getCoDeploymentName()).getSpec().getTemplate().getSpec().getContainers()) {
            for (EnvVar envVar : c.getEnv()) {
                images.put(envVar.getName(), envVar.getValue());
            }
        }
        return images;
    }

    /**
     * Verifies container configuration for specific component (kafka/zookeeper/bridge/mm) by environment key.
     * @param podNamePrefix Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param configKey Expected configuration key
     * @param config Expected component configuration
     */
    protected void checkComponentConfiguration(String podNamePrefix, String containerName, String configKey, Map<String, Object> config) {
        LOGGER.info("Getting pods by prefix in name {}", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing configuration for container {}", containerName);

            Map<String, Object> actual = pods.stream()
                .flatMap(p -> p.getSpec().getContainers().stream()) // get containers
                .filter(c -> c.getName().equals(containerName))
                .flatMap(c -> c.getEnv().stream().filter(envVar -> envVar.getName().equals(configKey)))
                .map(envVar -> StUtils.loadProperties(envVar.getValue()))
                .collect(Collectors.toList()).get(0);

            assertThat(actual.entrySet().containsAll(config.entrySet()), is(true));
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    /**
     * Verifies container environment variables passed as a map.
     * @param podNamePrefix Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param config Expected environment variables with values
     */
    protected void checkSpecificVariablesInContainer(String podNamePrefix, String containerName, Map<String, String> config) {
        LOGGER.info("Getting pods by prefix in name {}", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing EnvVars configuration for container {}", containerName);

            Map<String, Object> actual = pods.stream()
                .flatMap(p -> p.getSpec().getContainers().stream()) // get containers
                .filter(c -> c.getName().equals(containerName))
                .flatMap(c -> c.getEnv().stream().filter(envVar -> config.containsKey(envVar.getName())))
                .collect(Collectors.toMap(EnvVar::getName, EnvVar::getValue, (item, duplicatedItem) -> item));
            assertThat(actual, is(config));
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    /**
     * Verifies readinessProbe and livenessProbe properties in expected container
     * @param podNamePrefix Prefix of pod name where container is located
     * @param containerName The container where verifying is expected
     * @param initialDelaySeconds expected value for property initialDelaySeconds
     * @param timeoutSeconds expected value for property timeoutSeconds
     * @param periodSeconds expected value for property periodSeconds
     * @param successThreshold expected value for property successThreshold
     * @param failureThreshold expected value for property failureThreshold
     */
    protected void checkReadinessLivenessProbe(String podNamePrefix, String containerName, int initialDelaySeconds, int timeoutSeconds,
                                               int periodSeconds, int successThreshold, int failureThreshold) {
        LOGGER.info("Getting pods by prefix {} in pod name", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing Readiness and Liveness configuration for container {}", containerName);

            List<Container> containerList = pods.stream()
                .flatMap(p -> p.getSpec().getContainers().stream())
                .filter(c -> c.getName().equals(containerName))
                .collect(Collectors.toList());

            containerList.forEach(container -> {
                assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(initialDelaySeconds));
                assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(initialDelaySeconds));
                assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(timeoutSeconds));
                assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(timeoutSeconds));
                assertThat(container.getLivenessProbe().getPeriodSeconds(), is(periodSeconds));
                assertThat(container.getReadinessProbe().getPeriodSeconds(), is(periodSeconds));
                assertThat(container.getLivenessProbe().getSuccessThreshold(), is(successThreshold));
                assertThat(container.getReadinessProbe().getSuccessThreshold(), is(successThreshold));
                assertThat(container.getLivenessProbe().getFailureThreshold(), is(failureThreshold));
                assertThat(container.getReadinessProbe().getFailureThreshold(), is(failureThreshold));
            });
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    protected void verifyLabelsForKafkaCluster(String clusterName, String appName) {
        verifyLabelsOnPods(clusterName, "zookeeper", appName, Kafka.RESOURCE_KIND);
        verifyLabelsOnPods(clusterName, "kafka", appName, Kafka.RESOURCE_KIND);
        verifyLabelsOnCOPod();
        verifyLabelsOnPods(clusterName, "entity-operator", appName, Kafka.RESOURCE_KIND);
        verifyLabelsForCRDs();
        verifyLabelsForKafkaAndZKServices(clusterName, appName);
        verifyLabelsForSecrets(clusterName, appName);
        verifyLabelsForConfigMaps(clusterName, appName, "");
        verifyLabelsForRoleBindings(clusterName, appName);
        verifyLabelsForServiceAccounts(clusterName, appName);
    }

    void verifyLabelsOnCOPod() {
        LOGGER.info("Verifying labels for cluster-operator pod");

        Map<String, String> coLabels = kubeClient().listPods("name", ResourceManager.getCoDeploymentName()).get(0).getMetadata().getLabels();
        assertThat(coLabels.get("name"), is(ResourceManager.getCoDeploymentName()));
        assertThat(coLabels.get(Labels.STRIMZI_KIND_LABEL), is("cluster-operator"));
    }

    protected void verifyLabelsOnPods(String clusterName, String podType, String appName, String kind) {
        LOGGER.info("Verifying labels on pod type {}", podType);
        kubeClient().listPods().stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName.concat("-" + podType)))
            .forEach(pod -> {
                LOGGER.info("Verifying labels for pod: " + pod.getMetadata().getName());
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(kind));
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(clusterName.concat("-" + podType)));
            });
    }

    void verifyLabelsForCRDs() {
        LOGGER.info("Verifying labels for CRDs");
        String crds = cmdKubeClient().exec("get", "crds", "--selector=app=strimzi", "-o", "jsonpath='{.items[*].metadata.name}'").out();
        crds = crds.replace(" ", "\n").trim();
        assertThat(crds.split("\n").length, is(Crds.getNumCrds()));

    }

    void verifyLabelsForKafkaAndZKServices(String clusterName, String appName) {
        LOGGER.info("Verifying labels for Services");
        String kafkaServiceName = clusterName + "-kafka";
        String zookeeperServiceName = clusterName + "-zookeeper";

        Map<String, String> servicesMap = new HashMap<>();
        servicesMap.put(kafkaServiceName + "-bootstrap", kafkaServiceName);
        servicesMap.put(kafkaServiceName + "-brokers", kafkaServiceName);

        servicesMap.put(zookeeperServiceName + "-nodes", zookeeperServiceName);
        servicesMap.put(zookeeperServiceName + "-client", zookeeperServiceName + "-client");

        for (String serviceName : servicesMap.keySet()) {
            kubeClient().listServices().stream()
                .filter(service -> service.getMetadata().getName().equals(serviceName))
                .forEach(service -> {
                    LOGGER.info("Verifying labels for service {}", serviceName);
                    assertThat(service.getMetadata().getLabels().get("app"), is(appName));
                    assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                    assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("Kafka"));
                    assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(servicesMap.get(serviceName)));
                });
        }
    }

    protected void verifyLabelsForService(String clusterName, String serviceToTest, String kind) {
        LOGGER.info("Verifying labels for Kafka Connect Services");

        String serviceName = clusterName.concat("-").concat(serviceToTest);
        kubeClient().listServices().stream()
            .filter(service -> service.getMetadata().getName().equals(serviceName))
            .forEach(service -> {
                LOGGER.info("Verifying labels for service {}", service.getMetadata().getName());
                assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(kind));
                assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(serviceName));
            }
        );
    }

    void verifyLabelsForSecrets(String clusterName, String appName) {
        LOGGER.info("Verifying labels for secrets");
        kubeClient().listSecrets().stream()
            .filter(p -> p.getMetadata().getName().matches("(" + clusterName + ")-(clients|cluster|(entity))(-operator)?(-ca)?(-certs?)?"))
            .forEach(p -> {
                LOGGER.info("Verifying secret {}", p.getMetadata().getName());
                assertThat(p.getMetadata().getLabels().get("app"), is(appName));
                assertThat(p.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("Kafka"));
                assertThat(p.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
            }
        );
    }

    protected void verifyLabelsForConfigMaps(String clusterName, String appName, String additionalClusterName) {
        LOGGER.info("Verifying labels for Config maps");

        kubeClient().listConfigMaps()
            .forEach(cm -> {
                LOGGER.info("Verifying labels for CM {}", cm.getMetadata().getName());
                if (cm.getMetadata().getName().equals(clusterName.concat("-connect-config"))) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("KafkaConnect"));
                } else if (cm.getMetadata().getName().contains("-mirror-maker-config")) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("KafkaMirrorMaker"));
                } else if (cm.getMetadata().getName().contains("-mirrormaker2-config")) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("KafkaMirrorMaker2"));
                } else if (cm.getMetadata().getName().equals(clusterName.concat("-kafka-config"))) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(appName));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("Kafka"));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                } else if (cm.getMetadata().getName().equals(additionalClusterName.concat("-kafka-config"))) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(appName));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("Kafka"));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(additionalClusterName));
                } else {
                    LOGGER.info("CM {} is not related to current test", cm.getMetadata().getName());
                }
            }
        );
    }

    protected void verifyLabelsForServiceAccounts(String clusterName, String appName) {
        LOGGER.info("Verifying labels for Service Accounts");

        kubeClient().listServiceAccounts().stream()
            .filter(sa -> sa.getMetadata().getName().equals("strimzi-cluster-operator"))
            .forEach(sa -> {
                LOGGER.info("Verifying labels for service account {}", sa.getMetadata().getName());
                assertThat(sa.getMetadata().getLabels().get("app"), is("strimzi"));
            }
        );

        kubeClient().listServiceAccounts().stream()
            .filter(sa -> sa.getMetadata().getName().startsWith(clusterName))
            .forEach(sa -> {
                LOGGER.info("Verifying labels for service account {}", sa.getMetadata().getName());
                if (sa.getMetadata().getName().equals(clusterName.concat("-connect"))) {
                    assertThat(sa.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("KafkaConnect"));
                } else if (sa.getMetadata().getName().equals(clusterName.concat("-mirror-maker"))) {
                    assertThat(sa.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("KafkaMirrorMaker"));
                } else {
                    assertThat(sa.getMetadata().getLabels().get("app"), is(appName));
                    assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("Kafka"));
                }
                assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
            }
        );
    }

    void verifyLabelsForRoleBindings(String clusterName, String appName) {
        LOGGER.info("Verifying labels for Cluster Role bindings");
        kubeClient().listRoleBindings().stream()
            .filter(rb -> rb.getMetadata().getName().startsWith("strimzi-cluster-operator"))
            .forEach(rb -> {
                LOGGER.info("Verifying labels for cluster role {}", rb.getMetadata().getName());
                assertThat(rb.getMetadata().getLabels().get("app"), is("strimzi"));
            });

        kubeClient().listRoleBindings().stream()
            .filter(rb -> rb.getMetadata().getName().startsWith("strimzi-".concat(clusterName)))
            .forEach(rb -> {
                LOGGER.info("Verifying labels for cluster role {}", rb.getMetadata().getName());
                assertThat(rb.getMetadata().getLabels().get("app"), is(appName));
                assertThat(rb.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                assertThat(rb.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("Kafka"));
            }
        );
    }

    protected void verifyCRStatusCondition(Condition condition, String status, Enum<?> type) {
        verifyCRStatusCondition(condition, null, null, status, type);
    }

    protected void verifyCRStatusCondition(Condition condition, String message, String reason, String status, Enum<?> type) {
        assertThat(condition.getStatus(), is(status));
        assertThat(condition.getType(), is(type.toString()));

        if (condition.getMessage() != null && condition.getReason() != null) {
            assertThat(condition.getMessage(), containsString(message));
            assertThat(condition.getReason(), is(reason));
        }
    }

    protected void assertNoCoErrorsLogged(long sinceSeconds) {
        LOGGER.info("Search in strimzi-cluster-operator log for errors in last {} seconds", sinceSeconds);
        String clusterOperatorLog = cmdKubeClient().searchInLog("deploy", ResourceManager.getCoDeploymentName(), sinceSeconds, "Exception", "Error", "Throwable");
        assertThat(clusterOperatorLog, logHasNoUnexpectedErrors());
    }

    protected void testDockerImagesForKafkaCluster(String clusterName, String namespace, int kafkaPods, int zkPods, boolean rackAwareEnabled) {
        LOGGER.info("Verifying docker image names");
        //Verifying docker image for cluster-operator

        Map<String, String> imgFromDeplConf = getImagesFromConfig();

        String kafkaVersion = Crds.kafkaOperation(kubeClient().getClient()).inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getVersion();
        if (kafkaVersion == null) {
            kafkaVersion = Environment.ST_KAFKA_VERSION;
        }

        //Verifying docker image for zookeeper pods
        for (int i = 0; i < zkPods; i++) {
            String imgFromPod = PodUtils.getContainerImageNameFromPod(KafkaResources.zookeeperPodName(clusterName, i), "zookeeper");
            assertThat("Zookeeper pod " + i + " uses wrong image", imgFromPod, containsString(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion)));
        }

        //Verifying docker image for kafka pods
        for (int i = 0; i < kafkaPods; i++) {
            String imgFromPod = PodUtils.getContainerImageNameFromPod(KafkaResources.kafkaPodName(clusterName, i), "kafka");
            assertThat("Kafka pod " + i + " uses wrong image", imgFromPod, containsString(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion)));
            if (rackAwareEnabled) {
                String initContainerImage = PodUtils.getInitContainerImageName(KafkaResources.kafkaPodName(clusterName, i));
                assertThat(initContainerImage, is(imgFromDeplConf.get(KAFKA_INIT_IMAGE)));
            }
        }

        //Verifying docker image for entity-operator
        String entityOperatorPodName = cmdKubeClient().listResourcesByLabel("pod",
                Labels.STRIMZI_NAME_LABEL + "=" + clusterName + "-entity-operator").get(0);
        String imgFromPod = PodUtils.getContainerImageNameFromPod(entityOperatorPodName, "topic-operator");
        assertThat(imgFromPod, containsString(imgFromDeplConf.get(TO_IMAGE)));
        imgFromPod = PodUtils.getContainerImageNameFromPod(entityOperatorPodName, "user-operator");
        assertThat(imgFromPod, containsString(imgFromDeplConf.get(UO_IMAGE)));
        imgFromPod = PodUtils.getContainerImageNameFromPod(entityOperatorPodName, "tls-sidecar");
        assertThat(imgFromPod, containsString(imgFromDeplConf.get(TLS_SIDECAR_EO_IMAGE)));

        LOGGER.info("Docker images verified");
    }
    protected void afterEachMayOverride(ExtensionContext testContext) throws Exception {
        if (!Environment.SKIP_TEARDOWN) {
            ResourceManager.getInstance().deleteResources(testContext);
        }
    }

    protected void afterAllMayOverride(ExtensionContext testContext) throws Exception {
        if (!Environment.SKIP_TEARDOWN) {
            teardownEnvForOperator();
            ResourceManager.getInstance().deleteResources(testContext);
        }
    }

    /**
     * BeforeEachMayOverride, is a method, which gives you option to override @BeforeAll in sub-classes and
     * ensure that this is also executed if you call it with super.beforeEachMayOverride(). You can also skip it and
     * you your implementation in sub-class as you want.
     * @param extensionContext
     */
    protected void beforeEachMayOverride(ExtensionContext extensionContext) {
        // this is because we need to have different clusterName and kafkaClientsName in each test case without
        // synchronization it can produce `data-race`
        String testName = null;

        synchronized (lock) {
            if (extensionContext.getTestMethod().isPresent()) {
                testName = extensionContext.getTestMethod().get().getName();
            }

            LOGGER.info("Not first test we are gonna generate cluster name");
            String clusterName = CLUSTER_NAME_PREFIX + new Random().nextInt(Integer.MAX_VALUE);

            mapWithClusterNames.put(testName, clusterName);
            mapWithTestTopics.put(testName, KafkaTopicUtils.generateRandomNameOfTopic());
            mapWithTestUsers.put(testName, KafkaUserUtils.generateRandomNameOfKafkaUser());
            mapWithKafkaClientNames.put(testName, clusterName + "-" + Constants.KAFKA_CLIENTS);

            LOGGER.debug("CLUSTER_NAMES_MAP: \n{}", mapWithClusterNames);
            LOGGER.debug("USERS_NAME_MAP: \n{}", mapWithTestUsers);
            LOGGER.debug("TOPIC_NAMES_MAP: \n{}", mapWithTestTopics);
            LOGGER.debug("============THIS IS CLIENTS MAP:\n{}", mapWithKafkaClientNames);
        }
    }

    /**
     * BeforeAllMayOverride, is a method, which gives you option to override @BeforeAll in sub-classes and
     * ensure that this is also executed if you call it with super.beforeAllMayOverride(). You can also skip it and
     * you your implementation in sub-class as you want.
     * @param extensionContext
     */
    protected void beforeAllMayOverride(ExtensionContext extensionContext) {
        cluster = KubeClusterResource.getInstance();
        String testClass = null;

        if (extensionContext.getTestClass().isPresent()) {
            testClass = extensionContext.getTestClass().get().getName();
        }
    }

    @BeforeEach
    void setUpTestCase(ExtensionContext testContext) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("{} - [BEFORE EACH] has been called", this.getClass().getName());
        beforeEachMayOverride(testContext);
    }

    @BeforeAll
    void setUpTestSuite(ExtensionContext testContext) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("{} - [BEFORE ALL] has been called", this.getClass().getName());
        beforeAllMayOverride(testContext);
    }

    @AfterEach
    void tearDownTestCase(ExtensionContext testContext) throws Exception {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("{} - [AFTER EACH] has been called", this.getClass().getName());
        afterEachMayOverride(testContext);
    }

    @AfterAll
    void tearDownTestSuite(ExtensionContext testContext) throws Exception {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("{} - [AFTER ALL] has been called", this.getClass().getName());
        afterAllMayOverride(testContext);
    }
}
