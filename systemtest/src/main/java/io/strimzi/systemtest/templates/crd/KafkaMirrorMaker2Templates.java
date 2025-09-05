/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;

public class KafkaMirrorMaker2Templates {

    private KafkaMirrorMaker2Templates() {}

    private static final String METRICS_MM2_CONFIG_MAP_SUFFIX = "-mm2-metrics";
    private static final String CONFIG_MAP_KEY = "metrics-config.yml";

    public static KafkaMirrorMaker2Builder kafkaMirrorMaker2(TestStorage testStorage, int kafkaMirrorMaker2Replicas, boolean tlsListener) {
        return kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), kafkaMirrorMaker2Replicas, tlsListener);
    }

    public static KafkaMirrorMaker2Builder kafkaMirrorMaker2(
        String namespaceName,
        String kafkaMirrorMaker2Name,
        String sourceKafkaClusterName,
        String targetKafkaClusterName,
        int kafkaMirrorMaker2Replicas,
        boolean tlsListener
    ) {
        return defaultKafkaMirrorMaker2(namespaceName, kafkaMirrorMaker2Name, sourceKafkaClusterName, targetKafkaClusterName, kafkaMirrorMaker2Replicas, tlsListener);
    }

    public static KafkaMirrorMaker2Builder kafkaMirrorMaker2WithMetrics(
        String namespaceName,
        String mm2name,
        String sourceKafkaClusterName,
        String targetKafkaClusterName,
        int kafkaMirrorMaker2Replicas,
        String sourceNs,
        String targetNs
    ) {
        return defaultKafkaMirrorMaker2(namespaceName, mm2name, sourceKafkaClusterName, targetKafkaClusterName, kafkaMirrorMaker2Replicas, false, sourceNs, targetNs)
            .editOrNewSpec()
                .withNewJmxPrometheusExporterMetricsConfig()
                    .withNewValueFrom()
                        .withNewConfigMapKeyRef(CONFIG_MAP_KEY, getConfigMapName(mm2name), false)
                    .endValueFrom()
                .endJmxPrometheusExporterMetricsConfig()
                .editFirstMirror()
                    .withNewHeartbeatConnector()
                        .addToConfig("checkpoints.topic.replication.factor", -1)
                    .endHeartbeatConnector()
                .endMirror()
            .endSpec();
    }

    public static ConfigMap mirrorMaker2MetricsConfigMap(String namespaceName, String kafkaMirrorMaker2Name) {
        return new ConfigMapBuilder(FileUtils.extractConfigMapFromYAMLWithResources(TestConstants.PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG, "mirror-maker-2-metrics"))
            .editOrNewMetadata()
                .withNamespace(namespaceName)
                .withName(getConfigMapName(kafkaMirrorMaker2Name))
            .endMetadata()
            .build();
    }

    private static String getConfigMapName(String kafkaMirrorMaker2Name) {
        return kafkaMirrorMaker2Name + METRICS_MM2_CONFIG_MAP_SUFFIX;
    }

    private static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(
        String namespaceName,
        String kafkaMirrorMaker2Name,
        String sourceKafkaClusterName,
        String targetKafkaClusterName,
        int kafkaMirrorMaker2Replicas,
        boolean tlsListener
    ) {
        return defaultKafkaMirrorMaker2(namespaceName, kafkaMirrorMaker2Name, sourceKafkaClusterName, targetKafkaClusterName, kafkaMirrorMaker2Replicas, tlsListener, null, null);
    }

    private static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(
        String namespaceName,
        String kafkaMirrorMaker2Name,
        String sourceKafkaClusterName,
        String targetKafkaClusterName,
        int kafkaMirrorMaker2Replicas,
        boolean tlsListener,
        String sourceNs,
        String targetNs
    ) {
        KafkaMirrorMaker2ClusterSpec targetClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(targetKafkaClusterName)
            .withBootstrapServers(targetNs == null ? KafkaResources.plainBootstrapAddress(targetKafkaClusterName) : KafkaUtils.namespacedPlainBootstrapAddress(targetNs, targetKafkaClusterName))
            .addToConfig("config.storage.replication.factor", -1)
            .addToConfig("offset.storage.replication.factor", -1)
            .addToConfig("status.storage.replication.factor", -1)
            .build();

        KafkaMirrorMaker2ClusterSpec sourceClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(sourceKafkaClusterName)
            .withBootstrapServers(sourceNs == null ? KafkaResources.plainBootstrapAddress(sourceKafkaClusterName) : KafkaUtils.namespacedPlainBootstrapAddress(sourceNs, sourceKafkaClusterName))
            .build();

        if (tlsListener) {
            targetClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder(targetClusterSpec)
                .withBootstrapServers(targetNs == null ? KafkaResources.tlsBootstrapAddress(targetKafkaClusterName) : KafkaUtils.namespacedTlsBootstrapAddress(targetNs, targetKafkaClusterName))
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName(KafkaResources.clusterCaCertificateSecretName(targetKafkaClusterName)).withCertificate("ca.crt").build())
                .endTls()
                .build();

            sourceClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder(sourceClusterSpec)
                .withBootstrapServers(sourceNs == null ? KafkaResources.tlsBootstrapAddress(sourceKafkaClusterName) : KafkaUtils.namespacedTlsBootstrapAddress(sourceNs, sourceKafkaClusterName))
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName(KafkaResources.clusterCaCertificateSecretName(sourceKafkaClusterName)).withCertificate("ca.crt").build())
                .endTls()
                .build();
        }

        KafkaMirrorMaker2Builder kmm2b = new KafkaMirrorMaker2Builder()
            .withNewMetadata()
                .withName(kafkaMirrorMaker2Name)
                .withNamespace(namespaceName)
            .endMetadata()
            .editOrNewSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withReplicas(kafkaMirrorMaker2Replicas)
                .withConnectCluster(targetKafkaClusterName)
                .withClusters(targetClusterSpec, sourceClusterSpec)
                .addNewMirror()
                    .withSourceCluster(sourceKafkaClusterName)
                    .withTargetCluster(targetKafkaClusterName)
                    .withNewSourceConnector()
                        .withTasksMax(1)
                        .addToConfig("replication.factor", -1)
                        .addToConfig("offset-syncs.topic.replication.factor", -1)
                        .addToConfig("sync.topic.acls.enabled", "false")
                        .addToConfig("refresh.topics.interval.seconds", 600)
                    .endSourceConnector()
                    .withNewCheckpointConnector()
                        .withTasksMax(1)
                        .addToConfig("checkpoints.topic.replication.factor", -1)
                        .addToConfig("sync.group.offsets.enabled", "false")
                        .addToConfig("refresh.groups.interval.seconds", 600)
                    .endCheckpointConnector()
                    .withTopicsPattern(".*")
                    .withGroupsPattern(".*")
                .endMirror()
                .withNewInlineLogging()
                    .addToLoggers("rootLogger.level", "DEBUG")
                .endInlineLogging()
            .endSpec();

        if (!Environment.isSharedMemory()) {
            kmm2b.editSpec().withResources(new ResourceRequirementsBuilder()
                // we use such values, because on environments where it is limited to 7Gi, we are unable to deploy
                // Cluster Operator, two Kafka clusters and MirrorMaker/2. Such situation may result in an OOM problem.
                // Using 1Gi is too much and on the other hand 512Mi is causing OOM problem at the start.
                .addToLimits("memory", new Quantity("784Mi"))
                .addToRequests("memory", new Quantity("784Mi"))
                .build());
        }

        return kmm2b;
    }
}
