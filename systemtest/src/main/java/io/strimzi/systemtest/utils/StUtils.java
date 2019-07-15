/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.strimzi.api.kafka.Crds;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.strimzi.test.BaseITST.kubeClient;

public class StUtils {

    private static final Logger LOGGER = LogManager.getLogger(StUtils.class);
    private static final Pattern KAFKA_COMPONENT_PATTERN = Pattern.compile("([^-|^_]*?)(?<kafka>[-|_]kafka[-|_])(?<version>.*)$");

    private static final Pattern IMAGE_PATTERN_FULL_PATH = Pattern.compile("^(?<registry>[^/]*)/(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");
    private static final Pattern IMAGE_PATTERN = Pattern.compile("^(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");

    private static final Pattern VERSION_IMAGE_PATTERN = Pattern.compile("(?<version>[0-9.]+)=(?<image>[^\\s]*)");

    private StUtils() { }

    /**
     * Returns a map of resource name to resource version for all the pods in the given {@code namespace}
     * matching the given {@code selector}.
     */
    private static Map<String, String> podSnapshot(LabelSelector selector) {
        List<Pod> pods = kubeClient().listPods(selector);
        return pods.stream()
                .collect(
                        Collectors.toMap(pod -> pod.getMetadata().getName(),
                            pod -> pod.getMetadata().getUid()));
    }

    /**
     * Returns a map of pod name to resource version for the pods currently in the given statefulset.
     * @param name  The StatefulSet name
     * @return A map of pod name to resource version for pods in the given StatefulSet.
     */
    public static Map<String, String> ssSnapshot(String name) {
        StatefulSet statefulSet = kubeClient().getStatefulSet(name);
        LabelSelector selector = statefulSet.getSpec().getSelector();
        return podSnapshot(selector);
    }

    /**
     * Returns a map of pod name to resource version for the pods currently in the given deployment.
     * @param name The Deployment name.
     * @return A map of pod name to resource version for pods in the given Deployment.
     */
    public static Map<String, String> depSnapshot(String name) {
        Deployment deployment = kubeClient().getDeployment(name);
        LabelSelector selector = deployment.getSpec().getSelector();
        return podSnapshot(selector);
    }

    /**
     * Method to check that all pods for expected StatefulSet were rolled
     * @param name StatefulSet name
     * @param snapshot Snapshot of pods for StatefulSet before the rolling update
     * @return true when the pods for StatefulSet are recreated
     */
    public static boolean ssHasRolled(String name, Map<String, String> snapshot) {
        boolean log = true;
        if (log) {
            LOGGER.debug("Existing snapshot: {}", new TreeMap<>(snapshot));
        }
        LabelSelector selector = null;
        int times = 60;
        do {
            selector = kubeClient().getStatefulSetSelectors(name);
            if (selector == null) {
                if (times-- == 0) {
                    throw new RuntimeException("Retry failed");
                }
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } while (selector == null);

        Map<String, String> map = podSnapshot(selector);
        if (log) {
            LOGGER.debug("Current snapshot: {}", new TreeMap<>(map));
        }
        // rolled when all the pods in snapshot have a different version in map
        map.keySet().retainAll(snapshot.keySet());
        if (log) {
            LOGGER.debug("Pods in common: {}", new TreeMap<>(map));
        }
        for (Map.Entry<String, String> e : map.entrySet()) {
            String currentResourceVersion = e.getValue();
            String resourceName = e.getKey();
            String oldResourceVersion = snapshot.get(resourceName);
            if (oldResourceVersion.equals(currentResourceVersion)) {
                if (log) {
                    LOGGER.debug("At least {} hasn't rolled", resourceName);
                }
                return false;
            }
        }
        if (log) {
            LOGGER.debug("All pods seem to have rolled");
        }
        return true;
    }

    /**
     * Method to check that all pods for expected Deployment were rolled
     * @param name Deployment name
     * @param snapshot Snapshot of pods for Deployment before the rolling update
     * @return true when the pods for Deployment are recreated
     */
    public static boolean depHasRolled(String name, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}", new TreeMap<>(snapshot));
        Map<String, String> map = podSnapshot(kubeClient().getDeployment(name).getSpec().getSelector());
        LOGGER.debug("Current  snapshot: {}", new TreeMap<>(map));
        int current = map.size();
        map.keySet().retainAll(snapshot.keySet());
        if (current == snapshot.size() && map.isEmpty()) {
            LOGGER.debug("All pods seem to have rolled");
            return true;
        } else {
            LOGGER.debug("Some pods still to roll: {}", map);
            return false;
        }
    }

    /**
     *  Method to wait when StatefulSet will be recreated after rolling update
     * @param name StatefulSet name
     * @param expectedPods Expected number of pods
     * @param snapshot Snapshot of pods for StatefulSet before the rolling update
     * @return The snapshot of the StatefulSet after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillSsHasRolled(String name, int expectedPods, Map<String, String> snapshot) {
        TestUtils.waitFor("StatefulSet " + name + " rolling update",
                Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT, () -> {
                try {
                    return ssHasRolled(name, snapshot);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });
        StUtils.waitForAllStatefulSetPodsReady(name, expectedPods);
        return ssSnapshot(name);
    }

    /**
     * Method to wait when Deployment will be recreated after rolling update
     * @param name Deployment name
     * @param expectedPods Expected number of pods
     * @param snapshot Snapshot of pods for Deployment before the rolling update
     * @return The snapshot of the Deployment after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillDepHasRolled(String name, int expectedPods, Map<String, String> snapshot) {
        TestUtils.waitFor("Deployment " + name + " rolling update",
                Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT, () -> depHasRolled(name, snapshot));
        StUtils.waitForDeploymentReady(name);
        StUtils.waitForPodsReady(kubeClient().getDeployment(name).getSpec().getSelector(), expectedPods, true);
        return depSnapshot(name);
    }

    public static File downloadAndUnzip(String url) throws IOException {
        InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
        File dir = Files.createTempDirectory(StUtils.class.getName()).toFile();
        dir.deleteOnExit();
        ZipInputStream zin = new ZipInputStream(bais);
        ZipEntry entry = zin.getNextEntry();
        byte[] buffer = new byte[8 * 1024];
        int len;
        while (entry != null) {
            File file = new File(dir, entry.getName());
            if (entry.isDirectory()) {
                file.mkdirs();
            } else {
                FileOutputStream fout = new FileOutputStream(file);
                while ((len = zin.read(buffer)) != -1) {
                    fout.write(buffer, 0, len);
                }
                fout.close();
            }
            entry = zin.getNextEntry();
        }
        return dir;
    }

    /**
     *
     * Wait until the SS is ready and all of its Pods are also ready.
     * @param name The name of the StatefulSet
     * @param expectPods The number of pods expected.
     */
    public static void waitForAllStatefulSetPodsReady(String name, int expectPods) {
        LOGGER.debug("Waiting for StatefulSet {} to be ready", name);
        TestUtils.waitFor("statefulset " + name + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getStatefulSetStatus(name));
        LOGGER.debug("StatefulSet {} is ready", name);
        LOGGER.debug("Waiting for Pods of StatefulSet {} to be ready", name);
        waitForPodsReady(kubeClient().getStatefulSetSelectors(name), expectPods, true);
    }

    public static void waitForPodsReady(LabelSelector selector, int expectPods, boolean containers) {
        TestUtils.waitFor("All pods matching " + selector + "to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS, () -> {
            List<Pod> pods = kubeClient().listPods(selector);
            if (pods.isEmpty()) {
                LOGGER.debug("Not ready (no pods matching {})", selector);
                return false;
            }
            if (pods.size() != expectPods) {
                LOGGER.debug("Expected pods not ready");
                return false;
            }
            for (Pod pod : pods) {
                if (!Readiness.isPodReady(pod)) {
                    LOGGER.debug("Not ready (at least 1 pod not ready: {})", pod.getMetadata().getName());
                    return false;
                } else {
                    if (containers) {
                        for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                            LOGGER.debug("Not ready (at least 1 container of pod {} not ready: {})", pod.getMetadata().getName(), cs.getName());
                            if (!Boolean.TRUE.equals(cs.getReady())) {
                                return false;
                            }
                        }
                    }
                }
            }
            LOGGER.debug("Pods {} are ready",
                    pods.stream().map(p -> p.getMetadata().getName()).collect(Collectors.joining(", ")));
            return true;
        });
    }

    public static void waitForPodUpdate(String podName, Date startTime) {
        TestUtils.waitFor(podName + " update", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                startTime.before(kubeClient().getCreationTimestampForPod(podName))
        );
    }

    /**
     * Wait until the given Deployment has been deleted.
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentDeletion(String name) {
        LOGGER.info("Waiting for Deployment deletion {}", name);
        TestUtils.waitFor("Deployment " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getDeploymentStatus(name));
        LOGGER.info("Deployment {} was deleted", name);
    }

    /**
     * Wait until the given Deployment has been recovered.
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentRecovery(String name, String deploymentUid) {
        LOGGER.info("Waiting for Deployment {}-{} recovery in namespace {}", name, deploymentUid, kubeClient().getNamespace());
        TestUtils.waitFor("deployment " + name + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getDeploymentUid(name).equals(deploymentUid));
        LOGGER.info("Deployment {} was recovered", name);
    }

    /**
     * Wait until the given Deployment is ready.
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentReady(String name) {
        LOGGER.debug("Waiting for Deployment {}", name);
        TestUtils.waitFor("deployment " + name, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getDeploymentStatus(name));
        LOGGER.debug("Deployment {} is ready", name);
    }

    /**
     * Wait until the given Deployment is ready.
     * @param name The name of the Deployment.
     * @param expectPods The expected number of pods.
     */
    public static void waitForDeploymentReady(String name, int expectPods) {
        LOGGER.debug("Waiting for Deployment {}", name);
        TestUtils.waitFor("deployment " + name + " pods to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getDeploymentStatus(name));
        LOGGER.debug("Deployment {} is ready", name);
        LOGGER.debug("Waiting for Pods of Deployment {} to be ready", name);
        waitForPodsReady(kubeClient().getDeploymentSelectors(name), expectPods, true);
    }

    /**
     * Wait until the given DeploymentConfig is ready.
     * @param name The name of the DeploymentConfig.
     */
    public static void waitForDeploymentConfigReady(String name) {
        LOGGER.info("Waiting for Deployment Config {}", name);
        TestUtils.waitFor("deployment config "  + name + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getDeploymentConfigStatus(name));
        LOGGER.info("Deployment Config {} is ready", name);
    }

    /**
     * Wait until the given StatefulSet has been deleted.
     * @param name The name of the StatefulSet.
     */
    public static void waitForStatefulSetDeletion(String name) {
        LOGGER.info("Waiting for StatefulSet deletion {}", name);
        TestUtils.waitFor("StatefulSet " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getStatefulSetStatus(name));
        LOGGER.info("StatefulSet {} was deleted", name);
    }

    /**
     * Wait until the given StatefulSet has been recovered.
     * @param name The name of the StatefulSet.
     */
    public static void waitForStatefulSetRecovery(String name, String statefulSetUid) {
        LOGGER.info("Waiting for StatefulSet {}-{} recovery in namespace {}", name, statefulSetUid, kubeClient().getNamespace());
        TestUtils.waitFor("StatefulSet " + name + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getStatefulSetUid(name).equals(statefulSetUid));
        LOGGER.info("StatefulSet {} was recovered", name);
    }

    /**
     * Wait until the config map has been deleted.
     * @param name The name of the ConfigMap.
     */
    public static void waitForConfigMapDeletion(String name) {
        LOGGER.info("Waiting for config map deletion {}", name);
        TestUtils.waitFor("Config map " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getConfigMapStatus(name));
        LOGGER.info("Config map {} was deleted", name);
    }

    /**
     * Wait until the config map has been recovered.
     * @param name The name of the ConfigMap.
     */
    public static void waitForConfigMapRecovery(String name, String configMapUid) {
        LOGGER.info("Waiting for config map {}-{} recovery in namespace {}", name, configMapUid, kubeClient().getNamespace());
        TestUtils.waitFor("Config map " + name + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getConfigMapUid(name).equals(configMapUid));
        LOGGER.info("Config map {} was deleted", name);
    }

    public static void waitForSecretReady(String secretName) {
        TestUtils.waitFor("Expected secret " + secretName + " exists", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getSecret(secretName) != null);
    }

    public static void waitForKafkaUserDeletion(String userName) {
        LOGGER.info("Waiting for Kafka user deletion {}", userName);
        TestUtils.waitFor("Waits for Kafka user deletion " + userName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(userName).get() == null
        );
    }

    /**
     * The method to wait when all pods for Kafka cluster will be deleted.
     * To wait for the cluster to be updated, the following methods must be used: {@link #ssHasRolled(String, Map)}, {@link #waitTillSsHasRolled(String, int, Map)} )}
     * @param clusterName Cluster name where pods should be deleted
     */
    public static void waitForKafkaClusterPodsDeletion(String clusterName) {
        LOGGER.info("Waiting when all pods in Kafka cluster {} will be deleted", clusterName);
        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(clusterName))
                .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));
    }

    public static String getPodNameByPrefix(String prefix) {
        return kubeClient().listPods().stream().filter(pod -> pod.getMetadata().getName().startsWith(prefix))
                .findFirst().get().getMetadata().getName();
    }

    public static void waitForPod(String name) {
        LOGGER.info("Waiting when Pod {} will be ready", name);

        TestUtils.waitFor("pod " + name + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> {
                List<ContainerStatus> statuses =  kubeClient().getPod(name).getStatus().getContainerStatuses();
                for (ContainerStatus containerStatus : statuses) {
                    if (!containerStatus.getReady()) {
                        return false;
                    }
                }
                return true;
            });
    }

    public static void waitForPodDeletion(String name) {
        LOGGER.info("Waiting when Pod {} will be deleted", name);

        TestUtils.waitFor("statefulset " + name, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getPod(name) == null);
    }

    public static void waitForNamespaceDeletion(String name) {
        LOGGER.info("Waiting when Namespace {} to be deleted", name);

        TestUtils.waitFor("namespace " + name, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getNamespaceStatus(name));
    }

    public static void waitForKafkaTopicDeletion(String topicName) {
        LOGGER.info("Waiting for Kafka topic deletion {}", topicName);
        TestUtils.waitFor("Waits for Kafka topic deletion " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
            Crds.topicOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(topicName).get() == null
        );
    }

    public static void waitForLoadBalancerService(String serviceName) {
        LOGGER.info("Waiting when Service {} in namespace {} is ready", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("LoadBalancer service " + serviceName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getClient().services().inNamespace(kubeClient().getNamespace()).withName(serviceName).get().getSpec().getExternalIPs().size() > 0);
    }

    public static void waitForNodePortService(String serviceName) throws InterruptedException {
        LOGGER.info("Waiting when Service {} in namespace {} is ready", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("NodePort service " + serviceName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getClient().services().inNamespace(kubeClient().getNamespace()).withName(serviceName).get().getSpec().getPorts().get(0).getNodePort() != null);

        Thread.sleep(10000);
    }

    /**
     * Wait until Service of the given name will be deleted.
     * @param serviceName service name
     */
    public static void waitForServiceDeletion(String serviceName) {
        LOGGER.info("Waiting when Service {} in namespace {} has been deleted", serviceName, kubeClient().getNamespace());

        TestUtils.waitFor("Service " + serviceName + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getService(serviceName) == null);
    }

    /**
     * Wait until Service of the given name will be recovered.
     * @param serviceName service name
     * @param serviceUid service original uid
     */
    public static void waitForServiceRecovery(String serviceName, String serviceUid) {
        LOGGER.info("Waiting when Service {}-{} in namespace {} is recovered", serviceName, serviceUid, kubeClient().getNamespace());

        TestUtils.waitFor("Service " + serviceName + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getServiceUid(serviceName).equals(serviceUid));
    }

    /**
     * The method to configure docker image to use proper docker registry, docker org and docker tag.
     * @param image Image that needs to be changed
     * @return Updated docker image with a proper registry, org, tag
     */
    public static String changeOrgAndTag(String image) {
        Matcher m = IMAGE_PATTERN_FULL_PATH.matcher(image);
        if (m.find()) {
            String registry = setImageProperties(m.group("registry"), Environment.STRIMZI_REGISTRY, Environment.STRIMZI_REGISTRY_DEFAULT);
            String org = setImageProperties(m.group("org"), Environment.STRIMZI_ORG, Environment.STRIMZI_ORG_DEFAULT);

            return registry + "/" + org + "/" + m.group("image") + ":" + buildTag(m.group("tag"));
        }
        m = IMAGE_PATTERN.matcher(image);
        if (m.find()) {
            String org = setImageProperties(m.group("org"), Environment.STRIMZI_ORG, Environment.STRIMZI_ORG_DEFAULT);

            return Environment.STRIMZI_REGISTRY + "/" + org + "/" + m.group("image") + ":"  + buildTag(m.group("tag"));
        }
        return image;
    }

    public static String changeOrgAndTagInImageMap(String imageMap) {
        Matcher m = VERSION_IMAGE_PATTERN.matcher(imageMap);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, m.group("version") + "=" + StUtils.changeOrgAndTag(m.group("image")));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String setImageProperties(String current, String envVar, String defaultEnvVar) {
        if (!envVar.equals(defaultEnvVar) && !current.equals(envVar)) {
            return envVar;
        }
        return current;
    }

    private static String buildTag(String currentTag) {
        Matcher t = KAFKA_COMPONENT_PATTERN.matcher(currentTag);
        if (t.find()) {
            currentTag = Environment.STRIMZI_TAG + t.group("kafka") + t.group("version");
        } else {
            currentTag = Environment.STRIMZI_TAG;
        }
        return currentTag;
    }
}
