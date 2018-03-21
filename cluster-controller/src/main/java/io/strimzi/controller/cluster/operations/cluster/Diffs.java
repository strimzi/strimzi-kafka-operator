/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import static io.strimzi.controller.cluster.resources.AbstractCluster.containerEnvVars;

class Diffs {
    static boolean differingScale(StatefulSet a, StatefulSet b) {
        return (a == null) != (b == null)
                || !a.getSpec().getReplicas().equals(b.getSpec().getReplicas());
    }

    static boolean differingScale(Deployment a, Deployment b) {
        return (a == null) != (b == null)
                || !a.getSpec().getReplicas().equals(b.getSpec().getReplicas());
    }

    static <T extends HasMetadata> boolean differingLabels(T a, T b) {
        return (a == null) != (b == null)
                || !a.getMetadata().getLabels().equals(b.getMetadata().getLabels());
    }

    private static <T> boolean differingLists(List<T> a, List<T> b, BiPredicate<T, T> elementsDiffer) {
        if ((a == null) != (b == null)) {
            return true;
        }
        if (a == null && b == null) {
            return false;
        }
        if (a.size() != b.size()) {
            return true;
        }
        Iterator<T> ai = a.iterator();
        Iterator<T> bi = b.iterator();
        while (ai.hasNext()) {
            if (elementsDiffer.test(ai.next(), bi.next())) {
                return true;
            }
        }
        return false;
    }

    static boolean differingContainers(StatefulSet a, StatefulSet b) {
        return (a == null) != (b == null)
                || differingLists(a.getSpec().getTemplate().getSpec().getContainers(),
                    b.getSpec().getTemplate().getSpec().getContainers(),
                    Diffs::differingContainers);
    }

    static boolean differingContainers(Deployment a, Deployment b) {
        return (a == null) != (b == null)
                || differingLists(a.getSpec().getTemplate().getSpec().getContainers(),
                    b.getSpec().getTemplate().getSpec().getContainers(),
                    Diffs::differingContainers);
    }

    static boolean differingContainers(Container a, Container b) {
        return (a == null) != (b == null)
                || !Objects.equals(a.getImage(), b.getImage())
                || (a.getReadinessProbe() == null) != (b.getReadinessProbe() == null)
                || !Objects.equals(a.getReadinessProbe().getInitialDelaySeconds(), b.getReadinessProbe().getInitialDelaySeconds())
                || !Objects.equals(a.getReadinessProbe().getTimeoutSeconds(), b.getReadinessProbe().getTimeoutSeconds());
    }

    static boolean differingEnvironments(Container a, Container b, Set<String> keys) {
        Map<String, String> aVars = containerEnvVars(a);
        Map<String, String> bVars = containerEnvVars(b);
        if (keys != null) {
            aVars.keySet().retainAll(keys);
            bVars.keySet().retainAll(keys);
        }
        return !aVars.equals(bVars);
    }
/*
    public ClusterDiffResult diff(
            ConfigMap metricsConfigMap,
            StatefulSet ss)  {

        boolean scaleDown = false;
        boolean scaleUp = false;
        boolean different = false;
        boolean rollingUpdate = false;
        boolean metricsChanged = false;
        if (replicas > ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            scaleUp = true;
        } else if (replicas < ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            scaleDown = true;
        }

        if (!getLabelsWithName().equals(ss.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), ss.getMetadata().getLabels());
            different = true;
            rollingUpdate = true;
        }

        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);
        if (!image.equals(container.getImage())) {
            log.info("Diff: Expected image {}, actual image {}", image, container.getImage());
            different = true;
            rollingUpdate = true;
        }

        Map<String, String> vars = containerEnvVars(container);

        if (!zookeeperConnect.equals(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, DEFAULT_KAFKA_ZOOKEEPER_CONNECT))
                || defaultReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR)))
                || offsetsTopicReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR)))
                || transactionStateLogReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR)))) {
            log.info("Diff: Kafka options changed");
            different = true;
            rollingUpdate = true;
        }

        if (healthCheckInitialDelay != container.getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != container.getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Kafka healthcheck timing changed");
            different = true;
            rollingUpdate = true;
        }

        if (isMetricsEnabled != Boolean.parseBoolean(vars.getOrDefault(KEY_KAFKA_METRICS_ENABLED, String.valueOf(DEFAULT_KAFKA_METRICS_ENABLED)))) {
            log.info("Diff: Kafka metrics enabled/disabled");
            metricsChanged = true;
            rollingUpdate = true;
        } else {

            if (isMetricsEnabled) {
                JsonObject metricsConfig = new JsonObject(metricsConfigMap.getData().get(METRICS_CONFIG_FILE));
                if (!this.metricsConfig.equals(metricsConfig)) {
                    metricsChanged = true;
                }
            }
        }

        // get the current (deployed) kind of storage
        Storage ssStorage;
        if (ss.getSpec().getVolumeClaimTemplates().isEmpty()) {
            ssStorage = new Storage(Storage.StorageType.EPHEMERAL);
        } else {
            ssStorage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            // the delete-claim flack is backed by the StatefulSets
            if (ss.getMetadata().getAnnotations() != null) {
                String deleteClaimAnnotation = String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Storage.DELETE_CLAIM_FIELD);
                ssStorage.withDeleteClaim(Boolean.valueOf(ss.getMetadata().getAnnotations().computeIfAbsent(deleteClaimAnnotation, s -> "false")));
            }
        }

        // compute the differences with the requested storage (from the updated ConfigMap)
        Storage.StorageDiffResult storageDiffResult = storage.diff(ssStorage);

        // check for all the not allowed changes to the storage
        boolean isStorageRejected = storageDiffResult.isType() || storageDiffResult.isSize() ||
                storageDiffResult.isStorageClass() || storageDiffResult.isSelector();

        // only delete-claim flag can be changed
        if (!isStorageRejected && (storage.type() == Storage.StorageType.PERSISTENT_CLAIM)) {
            if (storageDiffResult.isDeleteClaim()) {
                different = true;
            }
        } else if (isStorageRejected) {
            log.warn("Changing storage configuration other than delete-claim is not supported !");
        }

        return new ClusterDiffResult(different, rollingUpdate, scaleUp, scaleDown, metricsChanged);
    }
*/
}
