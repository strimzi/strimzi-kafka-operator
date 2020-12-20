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
import io.strimzi.systemtest.interfaces.TestSeparator;
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.resources.operator.HelmResource;
import io.strimzi.systemtest.resources.operator.OlmResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.Minishift;
import io.strimzi.test.k8s.cluster.OpenShift;
import io.strimzi.test.timemeasuring.Operation;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
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

    protected KubeClusterResource cluster;
    protected static TimeMeasuringSystem timeMeasuringSystem = TimeMeasuringSystem.getInstance();
    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);

    protected static final String CLUSTER_NAME = "my-cluster";
    protected static final String KAFKA_CLIENTS_NAME = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS;
    protected static final String KAFKA_IMAGE_MAP = "STRIMZI_KAFKA_IMAGES";
    protected static final String KAFKA_CONNECT_IMAGE_MAP = "STRIMZI_KAFKA_CONNECT_IMAGES";
    protected static final String KAFKA_MIRROR_MAKER_2_IMAGE_MAP = "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES";
    protected static final String TO_IMAGE = "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE";
    protected static final String UO_IMAGE = "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE";
    protected static final String KAFKA_INIT_IMAGE = "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE";
    protected static final String TLS_SIDECAR_EO_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE";
    protected static final String TEST_TOPIC_NAME = "test-topic";
    public static final String CRUISE_CONTROL_POD_PREFIX = CLUSTER_NAME + "-cruise-control-";

    protected String testClass;
    protected String testName;

    public static Random rng = new Random();

    public static final int MESSAGE_COUNT = 100;

    public static final String EXAMPLE_TOPIC_NAME = "my-topic";
    public static final String AVAILABILITY_TOPIC_SOURCE_NAME = "availability-topic-source-" + rng.nextInt(Integer.MAX_VALUE);
    public static final String AVAILABILITY_TOPIC_TARGET_NAME = "availability-topic-target-" + rng.nextInt(Integer.MAX_VALUE);

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
    protected void installClusterOperator(String namespace, List<String> bindingsNamespaces, long operationTimeout, long reconciliationInterval) {
        if (Environment.isOlmInstall()) {
            LOGGER.info("Going to install ClusterOperator via OLM");
            cluster.setNamespace(namespace);
            cluster.createNamespace(namespace);
            OlmResource.clusterOperator(namespace, operationTimeout, reconciliationInterval);
        } else if (Environment.isHelmInstall()) {
            LOGGER.info("Going to install ClusterOperator via Helm");
            cluster.setNamespace(namespace);
            cluster.createNamespace(namespace);
            HelmResource.clusterOperator(operationTimeout, reconciliationInterval);
        } else {
            LOGGER.info("Going to install ClusterOperator via Yaml bundle");
            prepareEnvForOperator(namespace, bindingsNamespaces);
            applyRoleBindings(namespace, bindingsNamespaces);
            // 060-Deployment
            BundleResource.clusterOperator(namespace, operationTimeout, reconciliationInterval).done();
        }
    }

    protected void installClusterOperator(String namespace, long operationTimeout, long reconciliationInterval) {
        installClusterOperator(namespace, Collections.singletonList(namespace), operationTimeout, reconciliationInterval);
    }

    protected void installClusterOperator(String namespace, long operationTimeout) {
        installClusterOperator(namespace, operationTimeout, Constants.RECONCILIATION_INTERVAL);
    }

    protected void installClusterOperator(String namespace) {
        installClusterOperator(namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param namespaces list of namespaces which will be created
     * @param resources list of path to yaml files with resources specifications
     */
    protected void prepareEnvForOperator(String clientNamespace, List<String> namespaces, String... resources) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());
        cluster.createNamespaces(clientNamespace, namespaces);
        cluster.createCustomResources(resources);
        cluster.applyClusterOperatorInstallFiles();
        KubernetesResource.applyDefaultNetworkPolicySettings(namespaces);

        if (cluster.cluster() instanceof Minishift || cluster.cluster() instanceof OpenShift) {
            // This is needed in case you are using internal kubernetes registry and you want to pull images from there
            for (String namespace : namespaces) {
                LOGGER.debug("Setting group policy for Openshift registry in namespace: " + namespace);
                Exec.exec(null, Arrays.asList("oc", "policy", "add-role-to-group", "system:image-puller", "system:serviceaccounts:" + namespace, "-n", Environment.STRIMZI_ORG), 0, false, false);
            }
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param resources list of path to yaml files with resources specifications
     */
    protected void prepareEnvForOperator(String clientNamespace, String... resources) {
        prepareEnvForOperator(clientNamespace, Collections.singletonList(clientNamespace), resources);
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     */
    protected void prepareEnvForOperator(String clientNamespace) {
        prepareEnvForOperator(clientNamespace, Collections.singletonList(clientNamespace));
    }

    /**
     * Clear cluster from all created namespaces and configurations files for cluster operator.
     */
    protected void teardownEnvForOperator() {
        cluster.deleteClusterOperatorInstallFiles();
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces list of namespaces where Bindings should be deployed to
     */
    public static void applyRoleBindings(String namespace, List<String> bindingsNamespaces) {
        for (String bindingsNamespace : bindingsNamespaces) {
            // 020-RoleBinding
            KubernetesResource.roleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 021-ClusterRoleBinding
            KubernetesResource.clusterRoleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 030-ClusterRoleBinding
            KubernetesResource.clusterRoleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace, bindingsNamespace);
            // 031-RoleBinding
            KubernetesResource.roleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", namespace, bindingsNamespace);
            // 032-RoleBinding
            KubernetesResource.roleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/032-RoleBinding-strimzi-cluster-operator-topic-operator-delegation.yaml", namespace, bindingsNamespace);
            // 033-ClusterRoleBinding
            KubernetesResource.clusterRoleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/033-ClusterRoleBinding-strimzi-cluster-operator-kafka-client-delegation.yaml", namespace, bindingsNamespace);
        }
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     */
    public static void applyRoleBindings(String namespace) {
        applyRoleBindings(namespace, Collections.singletonList(namespace));
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to
     */
    public static void applyRoleBindings(String namespace, String... bindingsNamespaces) {
        applyRoleBindings(namespace, Arrays.asList(bindingsNamespaces));
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

    protected void tearDownEnvironmentAfterEach() throws Exception {
        ResourceManager.deleteMethodResources();
    }

    protected void tearDownEnvironmentAfterAll() {
        ResourceManager.deleteClassResources();
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

    @BeforeEach
    void createTestResources(ExtensionContext testContext) {
        if (testContext.getTestMethod().isPresent()) {
            testName = testContext.getTestMethod().get().getName();
        }
        ResourceManager.setMethodResources();
    }

    @BeforeAll
    void setTestClassName(ExtensionContext testContext) {
        cluster = KubeClusterResource.getInstance();
        if (testContext.getTestClass().isPresent()) {
            testClass = testContext.getTestClass().get().getName();
        }
    }

    @AfterEach
    void teardownEnvironmentMethod(ExtensionContext testContext) throws Exception {
        TimeMeasuringSystem.getInstance().stopOperation(Operation.TEST_EXECUTION);
        AssertionError assertionError = null;
        try {
            long testDuration = timeMeasuringSystem.getDurationInSeconds(testContext.getRequiredTestClass().getName(), testContext.getRequiredTestMethod().getName(), Operation.TEST_EXECUTION.name());
            assertNoCoErrorsLogged(testDuration);
        } catch (AssertionError e) {
            LOGGER.error("Cluster Operator contains unexpected errors!");
            assertionError = new AssertionError(e);
        }

        if (!Environment.SKIP_TEARDOWN) {
            tearDownEnvironmentAfterEach();
        }

        if (assertionError != null) {
            throw assertionError;
        }
    }

    @AfterAll
    void teardownEnvironmentClass() {
        if (!Environment.SKIP_TEARDOWN) {
            tearDownEnvironmentAfterAll();
            teardownEnvForOperator();
        }
    }
}
