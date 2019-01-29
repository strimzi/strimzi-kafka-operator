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
import io.strimzi.systemtest.Resources;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.Kubernetes;
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
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.strimzi.test.k8s.Kubernetes.getKubernetes;

public class StUtils {

    private static final Logger LOGGER = LogManager.getLogger(StUtils.class);
    private static Kubernetes kubernetes = getKubernetes();

    private StUtils() { }

    /**
     * Returns a map of resource name to resource version for all the pods in the given {@code namespace}
     * matching the given {@code selector}.
     */
    private static Map<String, String> podSnapshot(String namespace, LabelSelector selector) {
        List<Pod> pods = kubernetes.listPods(selector);
        return pods.stream()
                .collect(
                        Collectors.toMap(pod -> pod.getMetadata().getName(),
                            pod -> pod.getMetadata().getResourceVersion()));
    }

    /** Returns a map of pod name to resource version for the pods currently in the given statefulset */
    public static Map<String, String> ssSnapshot(String namespace, String name) {
        StatefulSet statefulSet = kubernetes.getStatefulSet(name);
        LabelSelector selector = statefulSet.getSpec().getSelector();
        return podSnapshot(namespace, selector);
    }

    /** Returns a map of pod name to resource version for the pods currently in the given deployment */
    public static Map<String, String> depSnapshot(String namespace, String name) {
        Deployment deployment = kubernetes.getDeployment(name);
        LabelSelector selector = deployment.getSpec().getSelector();
        return podSnapshot(namespace, selector);
    }

    private static boolean ssHasRolled(String namespace, String name, Map<String, String> snapshot) {
        boolean log = true;
        if (log) {
            LOGGER.debug("Existing snapshot: {}", new TreeMap(snapshot));
        }
        LabelSelector selector = null;
        int times = 60;
        do {
            selector = kubernetes.getStatefulSetSelectors(name);
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

        Map<String, String> map = podSnapshot(namespace, selector);
        if (log) {
            LOGGER.debug("Current snapshot: {}", new TreeMap(map));
        }
        // rolled when all the pods in snapshot have a different version in map
        map.keySet().retainAll(snapshot.keySet());
        if (log) {
            LOGGER.debug("Pods in common: {}", new TreeMap(map));
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

    private static boolean depHasRolled(String namespace, String name, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}", new TreeMap(snapshot));
        Map<String, String> map = podSnapshot(namespace, kubernetes.getDeploymentSelectors(name));
        LOGGER.debug("Current  snapshot: {}", new TreeMap(map));
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


    public static Map<String, String> waitTillSsHasRolled(String namespace, String name, Map<String, String> snapshot) {
        TestUtils.waitFor("SS roll of " + name,
            1_000, 450_000, () -> {
                try {
                    return ssHasRolled(namespace, name, snapshot);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });
        StUtils.waitForAllStatefulSetPodsReady(name);
        return ssSnapshot(namespace, name);
    }

    public static Map<String, String> waitTillDepHasRolled(String namespace, String name, Map<String, String> snapshot) {
        long timeLeft = TestUtils.waitFor("Deployment roll of " + name,
            1_000, 300_000, () -> depHasRolled(namespace, name, snapshot));
        StUtils.waitForDeploymentReady(name);
        StUtils.waitForPodsReady(kubernetes.getDeploymentSelectors(name), true);
        return depSnapshot(namespace, name);
    }

    public static File downloadAndUnzip(String url) throws IOException {
        InputStream bais = (InputStream) URI.create(url).toURL().getContent();
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
     * Wait until the SS is ready and all of its Pods are also ready
     */
    public static void waitForAllStatefulSetPodsReady(String name) {
        LOGGER.info("Waiting for StatefulSet {} to be ready", name);
        TestUtils.waitFor("statefulset " + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubernetes.getStatefulSetStatus(name));
        LOGGER.info("StatefulSet {} is ready", name);
        LOGGER.info("Waiting for Pods of StatefulSet {} to be ready", name);
        waitForPodsReady(kubernetes.getStatefulSetSelectors(name), true);
    }

    public static void waitForSecretReady (String secretName) {
        TestUtils.waitFor("Expected secret exists", Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS, () -> {
            return kubernetes.getSecret(secretName) != null;
        });
    }

    public static void waitForPod(String name) {
        LOGGER.info("Waiting when Pod {} will be ready", name);

        TestUtils.waitFor("pod " + name + " will be ready", Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
                () -> {
                    List<ContainerStatus> statuses =  kubernetes.getPod(name).getStatus().getContainerStatuses();
                    for (ContainerStatus containerStatus : statuses) {
                        if (!containerStatus.getReady()) {
                            return false;
                        }
                    }
                    return true;
                });
    }

    public static void waitForPodsReady(LabelSelector selector, boolean containers) {
        TestUtils.waitFor("All pods matching " + selector + "to be ready", Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS, () -> {
            List<Pod> pods = kubernetes.listPods(selector);
            if (pods.isEmpty()) {
                LOGGER.debug("Not ready (no pods matching {})", selector);
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
        TestUtils.waitFor(podName + " update",Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS, () ->
            startTime.before(kubernetes.getCreationTimestampForPod(podName))
        );
    }

    public static void waitForPodDeletion(String name) {
        LOGGER.info("Waiting when Pod {} will be deleted", name);

        TestUtils.waitFor("statefulset " + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
                () -> kubernetes.getPod(name) == null);
    }

    /**
     * Wait until the deployment is ready
     */
    public static void waitForDeploymentReady(String name) {
        LOGGER.info("Waiting for Deployment {}", name);
        TestUtils.waitFor("deployment " + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubernetes.getDeploymentStatus(name));
        LOGGER.info("Deployment {} is ready", name);
    }

    /**
     * Wait until the deployment will be deleted
     */
    public static void waitForDeploymentDeletion(String name) {
        LOGGER.info("Waiting for Deployment deletion {}", name);
        TestUtils.waitFor("deployment is deleted" + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
                () -> !kubernetes.getDeploymentStatus(name));
        LOGGER.info("Deployment {} was deleted", name);
    }

    /**
     * Wait until the deployment config is ready
     */
    public static void waitForDeploymentConfigReady(String namespace, String name) {
        LOGGER.info("Waiting for Deployment Config {}", name);
        TestUtils.waitFor("deployment config " + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_DEPLOYMENT_CONFIG_READINESS,
                () -> kubernetes.getDeploymentConfigStatus(name));
        LOGGER.info("Deployment Config {} is ready", name);
    }

    /**
     * Wait until the statefulSet is ready
     */
    public static void waitForStatefulSetReady(String name) {
        LOGGER.info("Waiting for StatefulSet {}", name);
        TestUtils.waitFor("StatefulSet " + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
                () -> kubernetes.getStatefulSetStatus(name));
        LOGGER.info("StatefulSet {} is ready", name);
    }


    /**
     * Wait until the stateful set will be deleted
     */
    public static void waitForStatefulSetDeletion(String name) {
        LOGGER.info("Waiting for StatefulSet deletion {}", name);
        TestUtils.waitFor("StatefulSet is deleted" + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
                () -> !kubernetes.getStatefulSetStatus(name));
        LOGGER.info("StatefulSet {} was deleted", name);
    }

    /**
     * Wait until the service is ready
     */
    public static void waitForServiceReady(String name) {
        LOGGER.info("Waiting for service {}", name);
        TestUtils.waitFor("Service " + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
                () -> kubernetes.getServiceStatus(name));
        LOGGER.info("Service {} is ready", name);
    }

    /**
     * Wait until the config map is ready
     */
    public static void waitForConfigMapReady(String name) {
        LOGGER.info("Waiting for config map {}", name);
        TestUtils.waitFor("Config map " + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
                () -> kubernetes.getConfigMapStatus(name));
        LOGGER.info("Config map {} is ready", name);
    }

    /**
     * Wait until the config map will be deleted
     */
    public static void waitForConfigMapDeletion(String name) {
        LOGGER.info("Waiting for config map deletion {}", name);
        TestUtils.waitFor("Config map " + name, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_RESOURCE_READINESS,
                () -> !kubernetes.getConfigMapStatus(name));
        LOGGER.info("Config map {} was deleted", name);
    }

    public static String getPodNameByPrefix (String prefix) {
        return kubernetes.listPods().stream().filter(pod -> pod.getMetadata().getName().startsWith(prefix))
                .findFirst().get().getMetadata().getName();
    }

    public static void waitForKafkaUserDeletion (String userName) {
        LOGGER.info("Waiting for Kafka user deletion {}", userName);
        TestUtils.waitFor("Waits for Kafka user deletion " + userName, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_DEPLOYMENT_CONFIG_READINESS, () ->
            Crds.kafkaUserOperation(kubernetes.getInstance()).inNamespace(kubernetes.getNamespace()).withName(userName).get() == null
        );
    }

    public static void waitForKafkaTopicDeletion (String topicName) {
        LOGGER.info("Waiting for Kafka topic deletion {}", topicName);
        TestUtils.waitFor("Waits for Kafka topic deletion " + topicName, Resources.POLL_INTERVAL_FOR_RESOURCE_READINESS, Resources.TIMEOUT_FOR_DEPLOYMENT_CONFIG_READINESS, () ->
                Crds.topicOperation(kubernetes.getInstance()).inNamespace(kubernetes.getNamespace()).withName(topicName).get() == null
        );
    }
}