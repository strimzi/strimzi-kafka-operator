/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.test.TestUtils;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.strimzi.test.BaseITST.cmdKubeClient;
import static io.strimzi.test.BaseITST.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
     * Returns a map of pod name to resource version for the pods currently in the given DeploymentConfig.
     * @param name The DeploymentConfig name.
     * @return A map of pod name to resource version for pods in the given DeploymentConfig.
     */
    public static Map<String, String> depConfigSnapshot(String name) {
        LabelSelector selector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(name)).build();
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
     * Method to check that all pods for expected DeploymentConfig were rolled
     * @param name DeploymentConfig name
     * @param snapshot Snapshot of pods for DeploymentConfig before the rolling update
     * @return true when the pods for DeploymentConfig are recreated
     */
    public static boolean depConfigHasRolled(String name, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}", new TreeMap<>(snapshot));
        LabelSelector selector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(name)).build();
        Map<String, String> map = podSnapshot(selector);
        LOGGER.debug("Current  snapshot: {}", new TreeMap<>(map));
        int current = map.size();
        map.keySet().retainAll(snapshot.keySet());
        if (current == snapshot.size() && map.isEmpty()) {
            LOGGER.info("All pods seem to have rolled");
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
                Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT * 2, () -> {
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

    /**
     * Method to wait when DeploymentConfig will be recreated after rolling update
     * @param clusterName Kafka Connect S2I cluster name
     * @param snapshot Snapshot of pods for DeploymentConfig before the rolling update
     * @return The snapshot of the DeploymentConfig after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillDepConfigHasRolled(String clusterName, Map<String, String> snapshot) {
        LOGGER.info("Waiting for Kafka Connect S2I cluster {} rolling update", clusterName);
        String name = KafkaConnectS2IResources.deploymentName(clusterName);
        TestUtils.waitFor("DeploymentConfig roll of " + name,
                Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT, () -> depConfigHasRolled(name, snapshot));
        StUtils.waitForConnectS2IReady(clusterName);
        return depConfigSnapshot(name);
    }

    /**
     * Method which return UID for specific topic
     * @param topicName topic name
     * @return topic UID
     */
    public static String topicSnapshot(String topicName) {
        return Crds.topicOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(topicName).get().getMetadata().getUid();
    }

    /**
     * Method which wait until topic has rolled form one generation to another.
     * @param topicName topic name
     * @param topicUid topic UID
     * @return topic new UID
     */
    public static String waitTopicHasRolled(String topicName, String topicUid) {
        TestUtils.waitFor("Topic " + topicName + " has rolled", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> !topicUid.equals(topicSnapshot(topicName)));
        return topicSnapshot(topicName);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public static File downloadAndUnzip(String url) {
        File dir = null;
        FileOutputStream fout = null;
        ZipInputStream zin = null;
        try {
            InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
            dir = Files.createTempDirectory(StUtils.class.getName()).toFile();
            dir.deleteOnExit();
            zin = new ZipInputStream(bais);
            ZipEntry entry = zin.getNextEntry();
            byte[] buffer = new byte[8 * 1024];
            int len;
            while (entry != null) {
                File file = new File(dir, entry.getName());
                if (entry.isDirectory()) {
                    file.mkdirs();
                } else {
                    fout = new FileOutputStream(file);
                    while ((len = zin.read(buffer)) != -1) {
                        fout.write(buffer, 0, len);
                    }
                    fout.close();
                }
                entry = zin.getNextEntry();
            }
        } catch (IOException e) {
            LOGGER.error("IOException {}", e.getMessage());
        } finally {
            if (fout != null) {
                try {
                    fout.close();
                } catch (IOException e) {
                    LOGGER.error("IOException {}", e.getMessage());
                }
            }
            if (zin != null) {
                try {
                    zin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return dir;
    }

    public static File downloadYamlAndReplaceNameSpace(String url, String namespace) {
        try {
            InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader(new InputStreamReader(bais));
            String read;
            while ((read = br.readLine()) != null) {
                sb.append(read + "\n");
            }
            br.close();
            String yaml = sb.toString();
            File yamlFile = File.createTempFile("temp-file", ".yaml");
            BufferedWriter bw = new BufferedWriter(new FileWriter(yamlFile));
            yaml = yaml.replaceAll("namespace: .*", "namespace: " + namespace);
            yaml = yaml.replace("securityContext:\n" +
                    "        runAsNonRoot: true\n" +
                    "        runAsUser: 65534", "");
            bw.write(yaml);
            bw.close();
            return yamlFile;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static File updateNamespaceOfYamlFile(String pathToOrigin, String namespace) {
        byte[] encoded = new byte[0];
        try {
            encoded = Files.readAllBytes(Paths.get(pathToOrigin));

            String yaml = new String(encoded, StandardCharsets.UTF_8);
            yaml = yaml.replaceAll("namespace: .*", "namespace: " + namespace);

            File yamlFile = File.createTempFile("temp-file", ".yaml");
            BufferedWriter bw = new BufferedWriter(new FileWriter(yamlFile));
            bw.write(yaml);
            bw.close();
            return yamlFile.toPath().toFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
        LOGGER.debug("Waiting for Deployment {} deletion", name);
        TestUtils.waitFor("Deployment " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Constants.TIMEOUT_FOR_RESOURCE_DELETION,
            () -> {
                if (kubeClient().getDeployment(name) == null) {
                    return true;
                } else {
                    LOGGER.warn("Deployment {} is not deleted!", name);
                    kubeClient().deleteDeployment(name);
                    return false;
                }
            });
        LOGGER.debug("Deployment {} was deleted", name);
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
     * Wait until the given Kafka Connect S2I cluster is ready.
     * @param name The name of the Kafka Connect S2I cluster.
     */
    public static void waitForConnectS2IReady(String name) {
        LOGGER.info("Waiting for Kafka Connect S2I {}", name);
        TestUtils.waitFor("Test " + name, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> Crds.kafkaConnectS2iOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(name).get().getStatus().getConditions().get(0).getType().equals("Ready"));
        LOGGER.info("Kafka Connect S2I {} is ready", name);
    }

    /**
     * Wait until the given DeploymentConfig has been deleted.
     * @param name The name of the DeploymentConfig.
     */
    public static void waitForDeploymentConfigDeletion(String name) {
        LOGGER.debug("Waiting for DeploymentConfig {} deletion", name);
        TestUtils.waitFor("DeploymentConfig " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Constants.TIMEOUT_FOR_RESOURCE_DELETION,
            () -> {
                if (kubeClient().getDeploymentConfig(name) == null) {
                    return true;
                } else {
                    LOGGER.warn("Deployment {} is not deleted!", name);
                    kubeClient().deleteDeploymentConfig(name);
                    return false;
                }
            });
        LOGGER.debug("Deployment {} was deleted", name);
    }

    /**
     * Wait until the given StatefulSet has been deleted.
     * @param name The name of the StatefulSet.
     */
    public static void waitForStatefulSetDeletion(String name) {
        LOGGER.debug("Waiting for StatefulSet {} deletion", name);
        TestUtils.waitFor("StatefulSet " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Constants.TIMEOUT_FOR_RESOURCE_DELETION,
            () -> {
                if (kubeClient().getStatefulSet(name) == null) {
                    return true;
                } else {
                    LOGGER.warn("StatefulSet {} is not deleted!", name);
                    kubeClient().deleteStatefulSet(name);
                    return false;
                }
            });
        LOGGER.debug("StatefulSet {} was deleted", name);
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
     * Wait until the given ReplicaSet has been deleted.
     * @param name The name of the ReplicaSet
     */
    public static void waitForReplicaSetDeletion(String name) {
        LOGGER.debug("Waiting for deletion of ReplicaSet of Deployment {}", name);
        TestUtils.waitFor("StatefulSet " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Constants.TIMEOUT_FOR_RESOURCE_DELETION,
            () -> {
                if (!kubeClient().replicaSetExists(name)) {
                    return true;
                } else {
                    String rsName = kubeClient().getReplicaSetNameByPrefix(name);
                    LOGGER.warn("ReplicaSet {} is not deleted yet!", rsName);
                    kubeClient().deleteReplicaSet(rsName);
                    return false;
                }
            });
        LOGGER.debug("ReplicaSet of Deployment {} was deleted", name);
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
        LOGGER.info("Waiting for Kafka user secret {}", secretName);
        TestUtils.waitFor("Expected secret " + secretName + " exists", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_SECRET_CREATION,
            () -> kubeClient().getSecret(secretName) != null);
        LOGGER.info("Kafka user secret {} created", secretName);
    }

    public static void waitForKafkaUserDeletion(String userName) {
        LOGGER.info("Waiting for Kafka user deletion {}", userName);
        TestUtils.waitFor("Waits for Kafka user deletion " + userName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(userName).get() == null
        );
        LOGGER.info("Kafka user {} deleted", userName);
    }

    public static void waitForKafkaUserCreationError(String userName, String eoPodName) {
        String errorMessage = "InvalidResourceException: Users with TLS client authentication can have a username (name of the KafkaUser custom resource) only up to 64 characters long.";
        final String messageUserWasNotAdded = "User(" + kubeClient().getNamespace() + "/" + userName + "): createOrUpdate failed";
        TestUtils.waitFor("User operator has expected error", Constants.GLOBAL_POLL_INTERVAL, 60000,
            () -> {
                String logs = kubeClient().logs(eoPodName, "user-operator");
                return logs.contains(errorMessage) && logs.contains(messageUserWasNotAdded);
            });
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

        TestUtils.waitFor("Pod " + name + " could not be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Constants.TIMEOUT_FOR_POD_DELETION,
            () -> {
                Pod pod = kubeClient().getPod(name);
                if (pod == null) {
                    return true;
                } else {
                    LOGGER.debug("Deleting pod {}", pod.getMetadata().getName());
                    cmdKubeClient().deleteByName("pod", pod.getMetadata().getName());
                    return false;
                }
            });
    }

    public static void waitForNamespaceDeletion(String name) {
        LOGGER.info("Waiting when Namespace {} to be deleted", name);

        TestUtils.waitFor("namespace " + name, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getNamespaceStatus(name));
    }

    public static void waitForKafkaTopicCreation(String topicName) {
        LOGGER.info("Waiting for Kafka topic creation {}", topicName);
        TestUtils.waitFor("Waits for Kafka topic creation " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
            Crds.topicOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(topicName).get().getStatus().getConditions().get(0).getType().equals("Ready")
        );
    }

    public static void waitForKafkaTopicDeletion(String topicName) {
        LOGGER.info("Waiting for Kafka topic deletion {}", topicName);
        TestUtils.waitFor("Waits for Kafka topic deletion " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
            Crds.topicOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(topicName).get() == null
        );
    }

    public static void waitForKafkaTopicPartitionChange(String topicName, int partitions) {
        LOGGER.info("Waiting for Kafka topic change {}", topicName);
        TestUtils.waitFor("Waits for Kafka topic change " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                    Crds.topicOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace())
                        .withName(topicName).get().getSpec().getPartitions() == partitions
        );
    }

    public static void waitForKafkaServiceLabelsChange(String serviceName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith("strimzi.io/");
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for Kafka service label change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Waits for Kafka service label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                        Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                                kubeClient().getService(serviceName).getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    public static void waitForKafkaServiceLabelsDeletion(String serviceName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for Kafka service label {} change to {}", labelKey, null);
            TestUtils.waitFor("Waiting for Kafka service label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                            kubeClient().getService(serviceName).getMetadata().getLabels().get(labelKey) == null
            );
        }
    }

    public static void waitForKafkaConfigMapLabelsChange(String configMapName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith("strimzi.io/");
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for Kafka config map label change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Waits for Kafka config map label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                        Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                                kubeClient().getConfigMap(configMapName).getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    public static void waitForKafkaStatefulSetLabelsChange(String statefulSetName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith("strimzi.io/");
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for Kafka stateful set label change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Waits for Kafka stateful set label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                        Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                                kubeClient().getStatefulSet(statefulSetName).getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    public static void waitForReconciliation(String testClass, String testName, String namespace) {
        LOGGER.info("Waiting for reconciliation");
        String reconciliation = TimeMeasuringSystem.startOperation(Operation.NEXT_RECONCILIATION);
        TestUtils.waitFor("Wait till another rolling update starts", Constants.CO_OPERATION_TIMEOUT_POLL, Constants.RECONCILIATION_INTERVAL + 20000,
            () -> !cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator",
                    TimeMeasuringSystem.getCurrentDuration(testClass, testName, reconciliation),
                        "'Triggering periodic reconciliation for namespace " + namespace + "'").isEmpty());
        TimeMeasuringSystem.stopOperation(reconciliation);
    }

    public static void waitForRollingUpdateTimeout(String testClass, String testName, String logPattern, String operationID) {
        TestUtils.waitFor("Wait till rolling update timeout", Constants.CO_OPERATION_TIMEOUT_POLL, Constants.CO_OPERATION_TIMEOUT_WAIT,
            () -> !cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), logPattern).isEmpty());
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

    public static void waitForMessagesInKafkaConnectFileSink(String kafkaConnectPodName, String message) {
        LOGGER.info("Waiting for messages in file sink");
        TestUtils.waitFor("messages in file sink", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat /tmp/test-file-sink.txt").out().contains(message));
    }

    public static void waitForMessagesInKafkaConnectFileSink(String kafkaConnectPodName) {
        waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, "0\n1\n");
    }

    private static String setImageProperties(String current, String envVar, String defaultEnvVar) {
        if (!envVar.equals(defaultEnvVar) && !current.equals(envVar)) {
            return envVar;
        }
        return current;
    }

    private static String buildTag(String currentTag) {
        if (!currentTag.equals(Environment.STRIMZI_TAG) && !Environment.STRIMZI_TAG_DEFAULT.equals(Environment.STRIMZI_TAG)) {
            Matcher t = KAFKA_COMPONENT_PATTERN.matcher(currentTag);
            if (t.find()) {
                currentTag = Environment.STRIMZI_TAG + t.group("kafka") + t.group("version");
            } else {
                currentTag = Environment.STRIMZI_TAG;
            }
        }
        return currentTag;
    }

    public static void waitUntilAddressIsReachable(String address) {
        LOGGER.info("Waiting till address {} is reachable", address);
        TestUtils.waitFor("", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                try {
                    InetAddress.getByName(kubeClient().getService("my-cluster-kafka-external-bootstrap").getStatus().getLoadBalancer().getIngress().get(0).getHostname());
                    return true;
                } catch (IOException e) {
                    return false;
                }
            });
        LOGGER.info("Address {} is reachable", address);
    }

    public static void waitUntilMessageIsInLogs(String podName, String containerName, String message) {
        LOGGER.info("Waiting for message will be in the log");
        TestUtils.waitFor("Waiting for message will be in the log", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_LOG,
            () -> kubeClient().logs(podName, containerName).contains(message));
    }

    public static void waitUntilMessageIsInPodLogs(String podName, String message) {
        LOGGER.info("Waiting for message will be in the log");
        TestUtils.waitFor("Waiting for message will be in the log", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_LOG,
            () -> kubeClient().logs(podName).contains(message));
    }

    /**
     * Method for check if test is allowed on current Kubernetes version
     * @param desiredKubernetesVersion kubernetes version which test needs
     * @return true if test is allowed, false if not
     */
    public static boolean isAllowedOnCurrentK8sVersion(String desiredKubernetesVersion) {
        if (desiredKubernetesVersion.equals("latest")) {
            return true;
        }
        return Double.parseDouble(kubeClient().clusterKubernetesVersion()) < Double.parseDouble(desiredKubernetesVersion);
    }

    public static void createSecretFromFile(String pathToOrigin, String key, String name, String namespace) {
        byte[] encoded = new byte[0];
        try {
            encoded = Files.readAllBytes(Paths.get(pathToOrigin));

            Map<String, String> data = new HashMap<>();
            Base64.Encoder encoder = Base64.getEncoder();
            data.put(key, encoder.encodeToString(encoded));

            Secret secret = new SecretBuilder()
                    .withData(data)
                    .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .endMetadata()
                    .build();
            kubeClient().namespace(namespace).createSecret(secret);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createFileSinkConnector(String podName, String topicName) {
        cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
                "curl -X POST -H \"Content-Type: application/json\" " + "--data '{ \"name\": \"sink-test\", " +
                        "\"config\": " + "{ \"connector.class\": \"FileStreamSink\", " +
                        "\"tasks.max\": \"1\", \"topics\": \"" + topicName + "\"," + " \"file\": \"/tmp/test-file-sink.txt\" } }' " +
                        "http://localhost:8083/connectors"
        );
    }

    public static void waitUntilPodsCountIsPresent(String podNamePrefix, int numberOfPods) {
        LOGGER.info("Waiting till pods with prefix {} count is present", podNamePrefix);
        TestUtils.waitFor("", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().listPodsByPrefixInName(podNamePrefix).size() == numberOfPods);
        LOGGER.info("Pods with count {} are present", numberOfPods);
    }

    public static void createSecret(String secretName, String dataKey, String dataValue) {
        LOGGER.info("Creating secret {}", secretName);
        kubeClient().createSecret(new SecretBuilder()
                .withNewApiVersion("v1")
                .withNewKind("Secret")
                .withNewMetadata()
                    .withName(secretName)
                .endMetadata()
                .withNewType("Opaque")
                    .withData(Collections.singletonMap(dataKey, dataValue))
                .build());
    }

    public static List<ContainerEnvVar> createContainerEnvVarsFromMap(Map<String, String> envVars) {
        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        for (Map.Entry<String, String> entry : envVars.entrySet()) {
            testEnvs.add(new ContainerEnvVarBuilder()
                    .withName(entry.getKey())
                    .withValue(entry.getValue()).build());
        }

        return testEnvs;
    }

    public static void checkCOlogForUsedVariable(String varName) {
        LOGGER.info("Check if ClusterOperator logs already defined variable occurrence");
        String coLog = kubeClient().logs(kubeClient().listPodNames("name", "strimzi-cluster-operator").get(0));
        assertThat(coLog.contains("User defined container template environment variable " + varName + " is already in use and will be ignored"), is(true));
        LOGGER.info("ClusterOperator logs contains proper warning");
    }

    public static void waitUntilPodContainersCount(String podNamePrefix, int numberOfContainers) {
        LOGGER.info("Waiting till pod {} will have {} containers", podNamePrefix, numberOfContainers);
        TestUtils.waitFor("Waiting till pod" + podNamePrefix + " will have " + numberOfContainers + " containers",
                Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().listPodsByPrefixInName(podNamePrefix).get(0).getSpec().getContainers().size() == numberOfContainers);
        LOGGER.info("Waiting till pod {} will have {} containers", podNamePrefix, numberOfContainers);
    }

    public static void waitUntilPodIsInCrashLoopBackOff(String podName) {
        LOGGER.info("Waiting till pod {} is in CrashLoopBackOff state", podName);
        TestUtils.waitFor("Waiting till pod {} is in CrashLoopBackOff state",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().getPod(podName).getStatus().getContainerStatuses().get(0)
                    .getState().getWaiting().getReason().equals("CrashLoopBackOff"));
        LOGGER.info("Pod {} is in CrashLoopBackOff state", podName);
    }

    public static void waitUntilPodIsPresent(String podNamePrefix) {
        LOGGER.info("Waiting till pod {} is present but not ready", podNamePrefix);
        TestUtils.waitFor("Waiting till pod {} is in CrashLoopBackOff state",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().listPodsByPrefixInName(podNamePrefix).get(0) != null);
        LOGGER.info("Pod {} is present but not ready", podNamePrefix);
    }

    public static void waitUntilPVCLabelsChange(Map<String, String> newLabels, String labelKey) {
        LOGGER.info("Waiting till PVC labels will change {}", newLabels.toString());
        TestUtils.waitFor("Waiting till PVC labels will change {}", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                for (PersistentVolumeClaim pvc : kubeClient().listPersistentVolumeClaims()) {
                    if (!pvc.getMetadata().getLabels().get(labelKey).equals(newLabels.get(labelKey))) {
                        return false;
                    }
                }
                return true;
            });
        LOGGER.info("PVC labels has changed {}", newLabels.toString());
    }

    public static void waitUntilPVCAnnotationChange(Map<String, String> newAnnotation, String annotationKey) {
        LOGGER.info("Waiting till PVC annotation will change {}", newAnnotation.toString());
        TestUtils.waitFor("Waiting till PVC labels will change {}", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                for (PersistentVolumeClaim pvc : kubeClient().listPersistentVolumeClaims()) {
                    if (!pvc.getMetadata().getLabels().get(annotationKey).equals(newAnnotation.get(annotationKey))) {
                        return false;
                    }
                }
                return true;
            });
        LOGGER.info("PVC annotation has changed {}", newAnnotation.toString());
    }

    public static void waitUntilKafkaStatusConditionIsPresent(String clusterName) {
        LOGGER.info("Waiting till kafka resource status is present");
        TestUtils.waitFor("Waiting for Kafka resource status is ready", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () ->  Crds.kafkaOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getStatus().getConditions().get(0) != null
        );
        LOGGER.info("Kafka resource status is present");
    }

    public static void waitUntilPodIsInPendingStatus(String podName) {
        LOGGER.info("Waiting till pod:" + podName + " is in pending status");
        TestUtils.waitFor("Waiting till pod:" + podName + " is in pending status", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () ->  kubeClient().getPod(podName).getStatus().getPhase().equals("Pending")
        );
        LOGGER.info("Pod:" + podName + " is in pending status");
    }

    public static void waitForKafkaConfigMapLabelsDeletion(String configMapName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for Kafka configMap label {} change to {}", labelKey, null);
            TestUtils.waitFor("Waiting for Kafka configMap label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                            kubeClient().getConfigMap(configMapName).getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("Kafka configMap label {} change to {}", labelKey, null);
        }
    }

    public static void waitUntilKafkaUserStatusConditionIsPresent(String userName) {
        LOGGER.info("Waiting till kafka user name:{} is created in CRDs", userName);
        TestUtils.waitFor("Waiting for " + userName + " to be created in CRDs", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(userName).get().getStatus().getConditions() != null
        );
        LOGGER.info("Kafka user name:{} is created in CRDs", userName);
    }

    public static void waitForKafkaStatefulSetLabelsDeletion(String statefulSet, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for Kafka statefulSet label {} change to {}", labelKey, null);
            TestUtils.waitFor("Waiting for Kafka statefulSet label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                            kubeClient().getStatefulSet(statefulSet).getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("Kafka statefulSet label {} change to {}", labelKey, null);
        }
    }

    public static void waitUntilKafkaPodLabelsDeletion(String podName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for Kafka pod label {} change to {}", labelKey, null);
            TestUtils.waitFor("Waiting for Kafka pod label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
                            kubeClient().getPod(podName).getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("Kafka pod label {} change to {}", labelKey, null);
        }
    }

}