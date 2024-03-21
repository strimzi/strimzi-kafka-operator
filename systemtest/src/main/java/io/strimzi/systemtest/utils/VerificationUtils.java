/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Utility class for verification and validation tasks related to resource requests, labels, environment variables, and component configurations.
 * This class provides static methods for performing checks commonly used for containers.
 */
public class VerificationUtils {

    private static final Logger LOGGER = LogManager.getLogger(VerificationUtils.class);

    /**
     * Verifies expected and current resources requests inside selected pods container
     * @param namespace Namespace name where is container is located
     * @param podName Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param memoryLimit expected value for allowed memory resource limitation
     * @param cpuLimit expected value for allowed CPU resource limitation
     * @param memoryRequest expected value for requested memory size
     * @param cpuRequest expected value for requested CPU in CPU units
     */
    public static void assertPodResourceRequests(String namespace, String podName, String containerName, String memoryLimit, String cpuLimit, String memoryRequest, String cpuRequest) {
        Pod po = kubeClient().getPod(namespace, podName);
        assertThat("Not found an expected Pod  " + namespace + "/" + podName + " but found " +
            kubeClient().listPods(namespace).stream().map(p -> p.getMetadata().getName()).toList(), po, is(notNullValue()));

        Optional<Container> optional = po.getSpec().getContainers().stream().filter(c -> c.getName().equals(containerName)).findFirst();
        assertThat("Not found an expected container " + containerName, optional.isPresent(), is(true));

        Container container = optional.get();
        Map<String, Quantity> limits = container.getResources().getLimits();
        assertThat(limits.get("memory"), is(new Quantity(memoryLimit)));
        assertThat(limits.get("cpu"), is(new Quantity(cpuLimit)));
        Map<String, Quantity> requests = container.getResources().getRequests();
        assertThat(requests.get("memory"), is(new Quantity(memoryRequest)));
        assertThat(requests.get("cpu"), is(new Quantity(cpuRequest)));
    }

    /**
     * Returns currently running java process commandlines with options as arguments and flags from the pod's container
     * @param namespaceName Namespace name where is container is located
     * @param podName Name of pod where container is located
     * @param containerName The container where java command lines are searched for
     */
    private static List<List<String>> containerJavaCmdLines(String namespaceName, String podName, String containerName) {
        List<List<String>> result = new ArrayList<>();
        String output = cmdKubeClient().namespace(namespaceName).execInPodContainer(podName, containerName, "/bin/bash", "-c",
                "for proc in $(ls -1 /proc/ | grep [0-9]); do if echo \"$(ls -lh /proc/$proc/exe 2>/dev/null || true)\" | grep -q java; then cat /proc/$proc/cmdline; fi; done"
        ).out();
        for (String cmdLine : output.split("\n")) {
            result.add(asList(cmdLine.split("\0")));
        }
        return result;
    }

    /**
     * Asserts expected options, compares it with java process commandline output
     * @param currentOptions List of retrieved java commandline options from container
     * @param expectedOption Java option value expected to be inside the command line list
     */
    private static void assertCmdOption(List<String> currentOptions, String expectedOption) {
        if (!currentOptions.contains(expectedOption)) {
            fail("Failed to find argument matching " + expectedOption + " in java command line " + String.join("\n", currentOptions));
        }
    }

    /**
     * Verifies container jvm options configuration.
     * @param namespaceName Namespace name where container is located
     * @param podName Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param expectedXmx Expected maximal Java heap size configuration
     * @param expectedXms Expected initial Java heap size configuration
     * @param expectedXx Expected extended Java options Configuration (in most test cases garbage collector)
     */
    public static void assertJvmOptions(String namespaceName, String podName, String containerName, String expectedXmx, String expectedXms, String expectedXx) {
        List<List<String>> cmdLines = containerJavaCmdLines(namespaceName, podName, containerName);
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

    /**
     * Verifies container configuration for specific component (kafka/zookeeper/bridge/mm) by environment key.
     * @param namespaceName Namespace name where container is located
     * @param podNamePrefix Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param configKey Expected configuration key
     * @param config Expected component configuration
     */
    public static void verifyComponentConfiguration(String namespaceName, String podNamePrefix, String containerName, String configKey, Map<String, Object> config) {
        LOGGER.info("Getting Pods by prefix: {} in Pod name", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(namespaceName, podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing configuration for container {}", containerName);

            Map<String, Object> actual = pods.stream()
                .flatMap(p -> p.getSpec().getContainers().stream()) // get containers
                .filter(c -> c.getName().equals(containerName))
                .flatMap(c -> c.getEnv().stream().filter(envVar -> envVar.getName().equals(configKey)))
                .map(envVar -> StUtils.loadProperties(envVar.getValue()))
                .toList().get(0);

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
    public static void verifyContainerEnvVariables(String namespaceName, String podNamePrefix, String containerName, Map<String, String> config) {
        LOGGER.info("Getting Pods by prefix: {} in Pod name", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(namespaceName, podNamePrefix);

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
    public static void verifyReadinessAndLivenessProbes(String namespaceName, String podNamePrefix, String containerName, int initialDelaySeconds, int timeoutSeconds, int periodSeconds, int successThreshold, int failureThreshold) {
        LOGGER.info("Getting Pods by prefix: {} in Pod name", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(namespaceName, podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Verifying Readiness and Liveness configuration for container {}", containerName);

            List<Container> containerList = pods.stream()
                .flatMap(p -> p.getSpec().getContainers().stream())
                .filter(c -> c.getName().equals(containerName))
                .toList();

            containerList.forEach(container -> {
                // Initial delay
                assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(initialDelaySeconds));
                assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(initialDelaySeconds));
                // Timeout
                assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(timeoutSeconds));
                assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(timeoutSeconds));
                // Period
                assertThat(container.getLivenessProbe().getPeriodSeconds(), is(periodSeconds));
                assertThat(container.getReadinessProbe().getPeriodSeconds(), is(periodSeconds));
                // Success threshold
                assertThat(container.getLivenessProbe().getSuccessThreshold(), is(successThreshold));
                assertThat(container.getReadinessProbe().getSuccessThreshold(), is(successThreshold));
                // Failure threshold
                assertThat(container.getLivenessProbe().getFailureThreshold(), is(failureThreshold));
                assertThat(container.getReadinessProbe().getFailureThreshold(), is(failureThreshold));
            });
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    /**
     * Verifies metadata labels for pods
     * @param namespaceName Namespace name where pods are located
     * @param podNamePrefix Prefix name of the pods - used to list pods with this prefix
     * @param expectedLabels Expected labels of the tested pods
     */
    public static void verifyPodsLabels(String namespaceName, String podNamePrefix, LabelSelector expectedLabels) {
        LOGGER.info("Verifying labels on pods with prefix {}", podNamePrefix);
        kubeClient().listPods(namespaceName).stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(podNamePrefix))
            .forEach(pod -> {
                LOGGER.info("Verifying labels for pod: " + pod.getMetadata().getName());
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(expectedLabels.getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL)));
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(expectedLabels.getMatchLabels().get(Labels.STRIMZI_KIND_LABEL)));
                assertThat(pod.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(expectedLabels.getMatchLabels().get(Labels.STRIMZI_NAME_LABEL)));
            });
    }

    /**
     * Verifies metadata labels for config maps
     * @param namespaceName Namespace name where config maps are located
     * @param clusterName Name of the cluster linked with configmaps
     * @param additionalClusterName Name of the second cluster - used mainly for source + target cluster verification
     */
    public static void verifyConfigMapsLabels(String namespaceName, String clusterName, String additionalClusterName) {
        LOGGER.info("Verifying labels for Config maps");

        kubeClient().listConfigMaps(namespaceName)
            .forEach(cm -> {
                LOGGER.info("Verifying labels for CM {}", cm.getMetadata().getName());
                if (cm.getMetadata().getName().equals(clusterName.concat("-connect-config"))) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(KafkaConnect.RESOURCE_KIND));
                } else if (cm.getMetadata().getName().contains("-mirror-maker-config")) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(KafkaMirrorMaker.RESOURCE_KIND));
                } else if (cm.getMetadata().getName().contains("-mirrormaker2-config")) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(KafkaMirrorMaker2.RESOURCE_KIND));
                } else if (cm.getMetadata().getName().equals(clusterName.concat("-kafka-config"))) {
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(Kafka.RESOURCE_KIND));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                } else if (cm.getMetadata().getName().equals(additionalClusterName.concat("-kafka-config"))) {
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(Kafka.RESOURCE_KIND));
                    assertThat(cm.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(additionalClusterName));
                } else {
                    LOGGER.info("CM {} is not related to current test", cm.getMetadata().getName());
                }
            }
        );
    }

    /**
     * Verifies metadata labels for service
     * @param namespaceName Namespace name where service is located
     * @param serviceName Name of the service for verification
     * @param expectedLabels Expected labels of the testd component service
     */
    public static void verifyServiceLabels(String namespaceName, String serviceName, LabelSelector expectedLabels) {
        LOGGER.info("Verifying labels for KafkaConnect Services");

        Service service = kubeClient().getService(namespaceName, serviceName);
        assertThat(service, is(notNullValue()));

        LOGGER.info("Verifying labels for service {}", service.getMetadata().getName());
        assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(expectedLabels.getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL)));
        assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(expectedLabels.getMatchLabels().get(Labels.STRIMZI_KIND_LABEL)));
        assertThat(service.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(expectedLabels.getMatchLabels().get(Labels.STRIMZI_NAME_LABEL)));
    }

    /**
     * Verifies metadata labels for service accounts
     * @param namespaceName Namespace name where service accounts are located
     * @param clusterName Name of the cluster linked with service accounts
     */
    public static void verifyServiceAccountsLabels(String namespaceName, String clusterName) {
        LOGGER.info("Verifying labels for Service Accounts");

        kubeClient().listServiceAccounts(namespaceName).stream()
            .filter(sa -> sa.getMetadata().getName().equals("strimzi-cluster-operator"))
            .forEach(sa -> {
                LOGGER.info("Verifying labels for service account {}", sa.getMetadata().getName());
                assertThat(sa.getMetadata().getLabels().get("app"), is("strimzi"));
            }
        );

        kubeClient().listServiceAccounts(namespaceName).stream()
            .filter(sa -> sa.getMetadata().getName().startsWith(clusterName))
            .forEach(sa -> {
                LOGGER.info("Verifying labels for service account {}", sa.getMetadata().getName());
                if (sa.getMetadata().getName().equals(clusterName.concat("-connect"))) {
                    assertThat(sa.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(KafkaConnect.RESOURCE_KIND));
                } else if (sa.getMetadata().getName().equals(clusterName.concat("-mirror-maker"))) {
                    assertThat(sa.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(KafkaMirrorMaker.RESOURCE_KIND));
                } else {
                    assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(Kafka.RESOURCE_KIND));
                }
                assertThat(sa.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
            }
        );
    }

    /**
     * Verifies that the Docker images used by the Kafka cluster in the specified namespaces are correct
     * based on the configured versions and deployment configurations.
     *
     * @param clusterName The name of the Kafka cluster
     * @param kafkaNamespaceName The namespace where Kafka is deployed
     * @param controllerPods The number of Kafka pods in the cluster
     * @param rackAwareEnabled Indicates whether rack-aware configuration is enabled
     */
    public static void verifyClusterOperatorKafkaDockerImages(String clusterName, String clusterOperatorNamespaceName, String kafkaNamespaceName, int controllerPods, boolean rackAwareEnabled) {
        LOGGER.info("Verifying docker image names");
        //Verifying docker image for cluster-operator

        Map<String, String> imgFromDeplConf = getClusterOperatorDeploymentImages(clusterOperatorNamespaceName);
        List<String> brokerPods = kubeClient().listPodNames(clusterOperatorNamespaceName, KafkaResource.getLabelSelector(clusterName, StrimziPodSetResource.getBrokerComponentName(clusterName)));

        final String kafkaVersion = Optional.ofNullable(Crds.kafkaOperation(kubeClient(kafkaNamespaceName).getClient()).inNamespace(kafkaNamespaceName).withName(clusterName).get().getSpec().getKafka().getVersion()).orElse(Environment.ST_KAFKA_VERSION);

        if (!Environment.isKRaftModeEnabled()) {
            //Verifying docker image for zookeeper pods
            for (int i = 0; i < controllerPods; i++) {
                String imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, KafkaResources.zookeeperPodName(clusterName, i), "zookeeper");
                assertThat("ZooKeeper Pod: " + i + " uses wrong image", imgFromPod, containsString(TestUtils.parseImageMap(imgFromDeplConf.get(TestConstants.KAFKA_IMAGE_MAP)).get(kafkaVersion)));
            }
        }

        //Verifying docker image for kafka pods
        brokerPods.forEach(brokerPod -> {
            String imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, brokerPod, "kafka");
            assertThat("Kafka Pod: " + brokerPod + " uses wrong image", imgFromPod, containsString(TestUtils.parseImageMap(imgFromDeplConf.get(TestConstants.KAFKA_IMAGE_MAP)).get(kafkaVersion)));

            if (rackAwareEnabled) {
                String initContainerImage = PodUtils.getInitContainerImageName(brokerPod);
                assertThat(initContainerImage, is(imgFromDeplConf.get(TestConstants.KAFKA_INIT_IMAGE)));
            }
        });

        //Verifying docker image for entity-operator
        String entityOperatorPodName = cmdKubeClient(kafkaNamespaceName).listResourcesByLabel("pod",
                Labels.STRIMZI_NAME_LABEL + "=" + clusterName + "-entity-operator").get(0);

        String imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, entityOperatorPodName, "user-operator");
        assertThat(imgFromPod, containsString(imgFromDeplConf.get(TestConstants.UO_IMAGE)));

        if (!Environment.isKRaftModeEnabled()) {
            imgFromPod = PodUtils.getContainerImageNameFromPod(kafkaNamespaceName, entityOperatorPodName, "topic-operator");
            assertThat(imgFromPod, containsString(imgFromDeplConf.get(TestConstants.TO_IMAGE)));
        }

        LOGGER.info("Docker images names of Kafka verified");
    }

    /**
     * Verifies the Docker images used by Kafka Connect in the specified namespaces
     * based on the configured versions and deployment configurations.
     *
     * @param clusterOperatorNamespace The namespace where the Cluster Operator is deployed
     * @param connectNamespaceName The namespace where Kafka Connect is deployed
     * @param clusterName The name of the Kafka Connect cluster
     */
    public static void verifyClusterOperatorConnectDockerImage(String clusterOperatorNamespace, String connectNamespaceName, String clusterName) {
        LOGGER.info("Verifying docker image name of KafkaConnect in CO");
        Map<String, String> imgFromDeplConf = VerificationUtils.getClusterOperatorDeploymentImages(clusterOperatorNamespace);
        //Verifying docker image for kafka connect
        String connectImageName = PodUtils.getFirstContainerImageNameFromPod(connectNamespaceName, kubeClient().listPodsByPrefixInName(connectNamespaceName, KafkaConnectResources.componentName(clusterName)).
                get(0).getMetadata().getName());

        String connectVersion = Crds.kafkaConnectOperation(kubeClient().namespace(connectNamespaceName).getClient()).inNamespace(connectNamespaceName).withName(clusterName).get().getSpec().getVersion();
        if (connectVersion == null) {
            connectVersion = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(TestConstants.KAFKA_CONNECT_IMAGE_MAP)).get(connectVersion), is(connectImageName));
        LOGGER.info("Docker image name of KafkaConnect verified");
    }

    /**
     * Verifies the Docker images used by MirrorMaker2 in the specified namespace
     * based on the configured versions and deployment configurations.
     *
     * @param clusterName The name of the Kafka MirrorMaker 2 cluster
     * @param clusterOperatorNamespace The namespace where the Cluster Operator is deployed
     * @param mirrorMakerNamespace The namespace where the Kafka MirrorMaker 2 instance is deployed
     */
    public static void verifyClusterOperatorMM2DockerImage(String clusterName, String clusterOperatorNamespace, String mirrorMakerNamespace) {
        LOGGER.info("Verifying docker image of MM2 in CO");
        // we must use INFRA_NAMESPACE because there is CO deployed
        Map<String, String> imgFromDeplConf = VerificationUtils.getClusterOperatorDeploymentImages(clusterOperatorNamespace);
        // Verifying docker image for kafka mirrormaker2
        String mirrormaker2ImageName = PodUtils.getFirstContainerImageNameFromPod(mirrorMakerNamespace, kubeClient().listPods(mirrorMakerNamespace, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND)
            .get(0).getMetadata().getName());

        String mirrormaker2Version = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(mirrorMakerNamespace).withName(clusterName).get().getSpec().getVersion();
        if (mirrormaker2Version == null) {
            mirrormaker2Version = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(TestConstants.KAFKA_MIRROR_MAKER_2_IMAGE_MAP)).get(mirrormaker2Version), is(mirrormaker2ImageName));
        LOGGER.info("Docker image name of MM2 verified");
    }

    /**
     * Retrieves a map of container names and their corresponding image from the CO deployment in a namespace.
     *
     * @param clusterOperatorNamespace The name of the namespace from which images should be retrieved - must be cluster operator namespace
     * @return A map where keys are container names and values are image URLs.
     */
    public static Map<String, String> getClusterOperatorDeploymentImages(String clusterOperatorNamespace) {
        Map<String, String> images = new HashMap<>();
        for (Container container : kubeClient().getDeployment(clusterOperatorNamespace, ResourceManager.getCoDeploymentName()).getSpec().getTemplate().getSpec().getContainers()) {
            for (EnvVar envVar : container.getEnv()) {
                images.put(envVar.getName(), envVar.getValue());
            }
        }
        return images;
    }
}
