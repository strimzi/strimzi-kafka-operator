/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.exceptions.KubernetesClusterUnstableException;
import io.strimzi.systemtest.interfaces.IndicativeSentences;
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.parallel.SuiteThreadController;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.parallel.TestSuiteNamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;

import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.k8s.KubeClusterResource;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({TestExecutionWatcher.class})
@DisplayNameGeneration(IndicativeSentences.class)
public abstract class AbstractST implements TestSeparator {
    public static final List<String> LB_FINALIZERS;
    static {
        LB_FINALIZERS = Environment.LB_FINALIZERS ? List.of(TestConstants.LOAD_BALANCER_CLEANUP) : null;
    }

    protected final ResourceManager resourceManager = ResourceManager.getInstance();
    protected final TestSuiteNamespaceManager testSuiteNamespaceManager = TestSuiteNamespaceManager.getInstance();
    private final SuiteThreadController parallelSuiteController = SuiteThreadController.getInstance();
    protected SetupClusterOperator clusterOperator = SetupClusterOperator.getInstance();
    protected KubeClusterResource cluster;
    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);

    // {thread-safe} this needs to be static because when more threads spawns diff. TestSuites it might produce race conditions
    private static final Object LOCK = new Object();

    protected static ConcurrentHashMap<ExtensionContext, TestStorage> storageMap = new ConcurrentHashMap<>();
    protected static final String CLUSTER_NAME_PREFIX = "my-cluster-";
    protected static final String KAFKA_IMAGE_MAP = "STRIMZI_KAFKA_IMAGES";
    protected static final String KAFKA_CONNECT_IMAGE_MAP = "STRIMZI_KAFKA_CONNECT_IMAGES";
    protected static final String KAFKA_MIRROR_MAKER_2_IMAGE_MAP = "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES";
    protected static final String TO_IMAGE = "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE";
    protected static final String UO_IMAGE = "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE";
    protected static final String KAFKA_INIT_IMAGE = "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE";
    protected static final String TLS_SIDECAR_EO_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE";

    public static Random rng = new Random();

    public static final int MESSAGE_COUNT = TestConstants.MESSAGE_COUNT;
    public static final String USER_NAME = KafkaUserUtils.generateRandomNameOfKafkaUser();
    public static final String TOPIC_NAME = KafkaTopicUtils.generateRandomNameOfTopic();

    protected void assertResources(String namespace, String podName, String containerName, String memoryLimit, String cpuLimit, String memoryRequest, String cpuRequest) {
        Pod po = kubeClient(namespace).getPod(namespace, podName);
        assertThat("Not found an expected Pod  " + namespace + "/" + podName + " but found " +
            kubeClient(namespace).listPods(namespace).stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()), po, is(notNullValue()));

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

    private List<List<String>> commandLines(String namespaceName, String podName, String containerName) {
        List<List<String>> result = new ArrayList<>();
        String output = cmdKubeClient().namespace(namespaceName).execInPodContainer(podName, containerName, "/bin/bash", "-c",
                "for proc in $(ls -1 /proc/ | grep [0-9]); do if echo \"$(ls -lh /proc/$proc/exe 2>/dev/null || true)\" | grep -q java; then cat /proc/$proc/cmdline; fi; done"
        ).out();
        for (String cmdLine : output.split("\n")) {
            result.add(asList(cmdLine.split("\0")));
        }
        return result;
    }

    protected void assertExpectedJavaOpts(String namespaceName, String podName, String containerName, String expectedXmx, String expectedXms, String expectedXx) {
        List<List<String>> cmdLines = commandLines(namespaceName, podName, containerName);
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

    public Map<String, String> getImagesFromConfig(String namespaceName) {
        Map<String, String> images = new HashMap<>();
        for (Container c : kubeClient(namespaceName).getDeployment(namespaceName, ResourceManager.getCoDeploymentName()).getSpec().getTemplate().getSpec().getContainers()) {
            for (EnvVar envVar : c.getEnv()) {
                images.put(envVar.getName(), envVar.getValue());
            }
        }
        return images;
    }

    /**
     * Verifies container configuration for specific component (kafka/zookeeper/bridge/mm) by environment key.
     * @param namespaceName Namespace name where container is located
     * @param podNamePrefix Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param configKey Expected configuration key
     * @param config Expected component configuration
     */
    protected void checkComponentConfiguration(String namespaceName, String podNamePrefix, String containerName, String configKey, Map<String, Object> config) {
        LOGGER.info("Getting Pods by prefix: {} in Pod name", podNamePrefix);
        List<Pod> pods = kubeClient(namespaceName).listPodsByPrefixInName(podNamePrefix);

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
     * @param namespaceName Namespace name where container is located
     * @param podNamePrefix Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param config Expected environment variables with values
     */
    protected void checkSpecificVariablesInContainer(String namespaceName, String podNamePrefix, String containerName, Map<String, String> config) {
        LOGGER.info("Getting Pods by prefix: {} in Pod name", podNamePrefix);
        List<Pod> pods = kubeClient(namespaceName).listPodsByPrefixInName(podNamePrefix);

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
     * @param namespaceName Namespace name where is container is located
     * @param podNamePrefix Prefix of pod name where container is located
     * @param containerName The container where verifying is expected
     * @param initialDelaySeconds expected value for property initialDelaySeconds
     * @param timeoutSeconds expected value for property timeoutSeconds
     * @param periodSeconds expected value for property periodSeconds
     * @param successThreshold expected value for property successThreshold
     * @param failureThreshold expected value for property failureThreshold
     */
    protected void checkReadinessLivenessProbe(String namespaceName, String podNamePrefix, String containerName, int initialDelaySeconds, int timeoutSeconds,
                                               int periodSeconds, int successThreshold, int failureThreshold) {
        LOGGER.info("Getting Pods by prefix: {} in Pod name", podNamePrefix);
        List<Pod> pods = kubeClient(namespaceName).listPodsByPrefixInName(podNamePrefix);

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

    protected void verifyLabelsForKafkaCluster(String clusterOperatorNamespaceName, String componentsNamespaceName, String clusterName, String appName) {
        verifyLabelsOnPods(componentsNamespaceName, clusterName, "zookeeper", Kafka.RESOURCE_KIND);
        verifyLabelsOnPods(componentsNamespaceName, clusterName, "kafka", Kafka.RESOURCE_KIND);
        verifyLabelsOnCOPod(clusterOperatorNamespaceName);
        verifyLabelsOnPods(componentsNamespaceName, clusterName, "entity-operator", Kafka.RESOURCE_KIND);
        verifyLabelsForCRDs(componentsNamespaceName);
        verifyLabelsForKafkaAndZKServices(componentsNamespaceName, clusterName, appName);
        verifyLabelsForSecrets(componentsNamespaceName, clusterName, appName);
        verifyLabelsForConfigMaps(componentsNamespaceName, clusterName, appName, "");
        verifyLabelsForRoleBindings(componentsNamespaceName, clusterName, appName);
        verifyLabelsForServiceAccounts(componentsNamespaceName, clusterName, appName);
    }

    void verifyLabelsOnCOPod(String namespaceName) {
        LOGGER.info("Verifying labels for cluster-operator Pod");

        Map<String, String> coLabels = kubeClient(namespaceName).listPods("name", ResourceManager.getCoDeploymentName()).get(0).getMetadata().getLabels();
        assertThat(coLabels.get("name"), is(ResourceManager.getCoDeploymentName()));
        assertThat(coLabels.get(Labels.STRIMZI_KIND_LABEL), is("cluster-operator"));
    }

    protected void verifyLabelsOnPods(String namespaceName, String clusterName, String podType, String kind) {
        LOGGER.info("Verifying labels on Pod type {}", podType);
        kubeClient(namespaceName).listPods().stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName.concat("-" + podType)))
            .forEach(pod -> {
                LOGGER.info("Verifying labels for pod: " + pod.getMetadata().getName());
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(kind));
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(clusterName.concat("-" + podType)));
            });
    }

    void verifyLabelsForCRDs(String namespaceName) {
        LOGGER.info("Verifying labels for CRDs");
        String crds = cmdKubeClient(namespaceName).exec("get", "crds", "--selector=app=strimzi", "-o", "jsonpath='{.items[*].metadata.name}'").out();
        crds = crds.replace(" ", "\n").trim();
        assertThat(crds.split("\n").length, is(Crds.getNumCrds()));

    }

    void verifyLabelsForKafkaAndZKServices(String namespaceName, String clusterName, String appName) {
        LOGGER.info("Verifying labels for Services");
        String kafkaServiceName = clusterName + "-kafka";
        String zookeeperServiceName = clusterName + "-zookeeper";

        Map<String, String> servicesMap = new HashMap<>();
        servicesMap.put(kafkaServiceName + "-bootstrap", kafkaServiceName);
        servicesMap.put(kafkaServiceName + "-brokers", kafkaServiceName);

        servicesMap.put(zookeeperServiceName + "-nodes", zookeeperServiceName);
        servicesMap.put(zookeeperServiceName + "-client", zookeeperServiceName + "-client");

        for (String serviceName : servicesMap.keySet()) {
            kubeClient(namespaceName).listServices(namespaceName).stream()
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

    protected void verifyLabelsForService(String namespaceName, String clusterName, String nameLabel, String serviceToTest, String kind) {
        LOGGER.info("Verifying labels for KafkaConnect Services");

        String serviceName = clusterName.concat("-").concat(serviceToTest);
        Service service = kubeClient(namespaceName).getService(serviceName);

        assertThat(service, is(notNullValue()));

        LOGGER.info("Verifying labels for service {}", service.getMetadata().getName());
        assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
        assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(kind));
        assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(clusterName.concat("-").concat(nameLabel)));
    }

    void verifyLabelsForSecrets(String namespaceName, String clusterName, String appName) {
        LOGGER.info("Verifying labels for Secrets");
        kubeClient(namespaceName).listSecrets(namespaceName).stream()
            .filter(p -> p.getMetadata().getName().matches("(" + clusterName + ")-(clients|cluster|(entity))(-operator)?(-ca)?(-certs?)?"))
            .forEach(p -> {
                LOGGER.info("Verifying Secret: {}", p.getMetadata().getName());
                assertThat(p.getMetadata().getLabels().get("app"), is(appName));
                assertThat(p.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is("Kafka"));
                assertThat(p.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
            }
        );
    }

    protected void verifyLabelsForConfigMaps(String namespaceName, String clusterName, String appName, String additionalClusterName) {
        LOGGER.info("Verifying labels for Config maps");

        kubeClient(namespaceName).listConfigMaps()
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

    protected void verifyLabelsForServiceAccounts(String namespaceName, String clusterName, String appName) {
        LOGGER.info("Verifying labels for Service Accounts");

        kubeClient(namespaceName).listServiceAccounts(namespaceName).stream()
            .filter(sa -> sa.getMetadata().getName().equals("strimzi-cluster-operator"))
            .forEach(sa -> {
                LOGGER.info("Verifying labels for service account {}", sa.getMetadata().getName());
                assertThat(sa.getMetadata().getLabels().get("app"), is("strimzi"));
            }
        );

        kubeClient(namespaceName).listServiceAccounts(namespaceName).stream()
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

    void verifyLabelsForRoleBindings(String namespaceName, String clusterName, String appName) {
        LOGGER.info("Verifying labels for Cluster Role bindings");
        kubeClient(namespaceName).listRoleBindings(namespaceName).stream()
            .filter(rb -> rb.getMetadata().getName().startsWith("strimzi-cluster-operator"))
            .forEach(rb -> {
                LOGGER.info("Verifying labels for cluster role {}", rb.getMetadata().getName());
                assertThat(rb.getMetadata().getLabels().get("app"), is("strimzi"));
            });

        kubeClient(namespaceName).listRoleBindings(namespaceName).stream()
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

    protected void assertNoCoErrorsLogged(String namespaceName, long sinceSeconds) {
        LOGGER.info("Search in strimzi-cluster-operator log for errors in last {} second(s)", sinceSeconds);
        String clusterOperatorLog = cmdKubeClient(namespaceName).searchInLog(TestConstants.DEPLOYMENT, ResourceManager.getCoDeploymentName(), sinceSeconds, "Exception", "Error", "Throwable", "OOM");
        assertThat(clusterOperatorLog, logHasNoUnexpectedErrors());
    }

    protected void testDockerImagesForKafkaCluster(String clusterName, String clusterOperatorNamespaceName, String kafkaNamespaceName,
                                                   int kafkaPods, int zkPods, boolean rackAwareEnabled) {
        LOGGER.info("Verifying docker image names");
        //Verifying docker image for cluster-operator

        Map<String, String> imgFromDeplConf = getImagesFromConfig(clusterOperatorNamespaceName);

        String kafkaVersion = Crds.kafkaOperation(kubeClient(kafkaNamespaceName).getClient()).inNamespace(kafkaNamespaceName).withName(clusterName).get().getSpec().getKafka().getVersion();
        if (kafkaVersion == null) {
            kafkaVersion = Environment.ST_KAFKA_VERSION;
        }

        if (!Environment.isKRaftModeEnabled()) {
            //Verifying docker image for zookeeper pods
            for (int i = 0; i < zkPods; i++) {
                String imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, KafkaResources.zookeeperPodName(clusterName, i), "zookeeper");
                assertThat("ZooKeeper Pod: " + i + " uses wrong image", imgFromPod, containsString(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion)));
            }
        }

        //Verifying docker image for kafka pods
        for (int i = 0; i < kafkaPods; i++) {
            String imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, KafkaResource.getKafkaPodName(clusterName, i), "kafka");
            assertThat("Kafka Pod: " + i + " uses wrong image", imgFromPod, containsString(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion)));
            if (rackAwareEnabled) {
                String initContainerImage = PodUtils.getInitContainerImageName(KafkaResources.kafkaPodName(clusterName, i));
                assertThat(initContainerImage, is(imgFromDeplConf.get(KAFKA_INIT_IMAGE)));
            }
        }

        //Verifying docker image for entity-operator
        String entityOperatorPodName = cmdKubeClient(kafkaNamespaceName).listResourcesByLabel("pod",
                Labels.STRIMZI_NAME_LABEL + "=" + clusterName + "-entity-operator").get(0);

        String imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, entityOperatorPodName, "user-operator");
        assertThat(imgFromPod, containsString(imgFromDeplConf.get(UO_IMAGE)));

        if (!Environment.isKRaftModeEnabled()) {
            imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, entityOperatorPodName, "topic-operator");
            assertThat(imgFromPod, containsString(imgFromDeplConf.get(TO_IMAGE)));
            if (!Environment.isUnidirectionalTopicOperatorEnabled()) {
                imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, entityOperatorPodName, "tls-sidecar");
                assertThat(imgFromPod, containsString(imgFromDeplConf.get(TLS_SIDECAR_EO_IMAGE)));
            }
        }

        LOGGER.info("Docker images verified");
    }

    private void afterEachMustExecute(ExtensionContext extensionContext) {
        if (cluster.cluster().isClusterUp()) {
            if (StUtils.isParallelTest(extensionContext) ||
                StUtils.isParallelNamespaceTest(extensionContext)) {
                parallelSuiteController.notifyParallelTestToAllowExecution(extensionContext);
                parallelSuiteController.removeParallelTest(extensionContext);
            }
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    protected void afterEachMayOverride(ExtensionContext extensionContext) throws Exception {
        if (!Environment.SKIP_TEARDOWN) {
            ResourceManager.getInstance().deleteResources(extensionContext);
            testSuiteNamespaceManager.deleteParallelNamespace(extensionContext);
        }
    }

    private void afterAllMustExecute(ExtensionContext extensionContext)  {
        if (cluster.cluster().isClusterUp()) {
            clusterOperator = SetupClusterOperator.getInstance();
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    protected synchronized void afterAllMayOverride(ExtensionContext extensionContext) {
        if (!Environment.SKIP_TEARDOWN) {
            ResourceManager.getInstance().deleteResources(extensionContext);
            testSuiteNamespaceManager.deleteTestSuiteNamespace(extensionContext);
            NamespaceManager.getInstance().deleteAllNamespacesFromSet();
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
        synchronized (LOCK) {
            LOGGER.info("Not first test we are gonna generate cluster name");
            testSuiteNamespaceManager.createParallelNamespace(extensionContext);
            storageMap.put(extensionContext, new TestStorage(extensionContext));
        }
    }

    private void beforeEachMustExecute(ExtensionContext extensionContext) {
        if (cluster.cluster().isClusterUp()) {
            if (StUtils.isParallelNamespaceTest(extensionContext) ||
                StUtils.isParallelTest(extensionContext)) {
                parallelSuiteController.addParallelTest(extensionContext);
                parallelSuiteController.waitUntilAllowedNumberTestCasesParallel(extensionContext);
            }
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    private void beforeAllMustExecute(ExtensionContext extensionContext) {
        if (cluster.cluster().isClusterUp()) {
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
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
        testSuiteNamespaceManager.createTestSuiteNamespace(extensionContext);
    }

    @BeforeEach
    void setUpTestCase(ExtensionContext extensionContext) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("————————————  {}@Before Each - Setup TestCase environment ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        beforeEachMustExecute(extensionContext);
        beforeEachMayOverride(extensionContext);
    }

    @BeforeAll
    void setUpTestSuite(ExtensionContext extensionContext) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@Before All - Setup TestSuite environment ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        beforeAllMayOverride(extensionContext);
        beforeAllMustExecute(extensionContext);
    }

    @AfterEach
    void tearDownTestCase(ExtensionContext extensionContext) throws Exception {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@After Each - Clean up after test ————————————", StUtils.removePackageName(this.getClass().getName()));
        // try with finally is needed because in worst case possible if the Cluster is unable to delete namespaces, which
        // results in `Timeout after 480000 ms waiting for Namespace namespace-136 removal` it throws WaitException and
        // does not proceed with the next method (i.e., afterEachMustExecute()). This ensures that if such problem happen
        // it will always execute the second method.
        try {
            assertNoCoErrorsLogged(clusterOperator.getDeploymentNamespace(), storageMap.get(extensionContext).getTestExecutionStartTime());
            afterEachMayOverride(extensionContext);
        } finally {
            afterEachMustExecute(extensionContext);
        }
    }

    @AfterAll
    void tearDownTestSuite(ExtensionContext extensionContext) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@After All - Clean up after TestSuite ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        afterAllMayOverride(extensionContext);
        afterAllMustExecute(extensionContext);
    }
}
