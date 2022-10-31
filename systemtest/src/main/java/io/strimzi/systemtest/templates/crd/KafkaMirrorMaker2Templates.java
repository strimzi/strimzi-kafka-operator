/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;

public class KafkaMirrorMaker2Templates {

    private KafkaMirrorMaker2Templates() {}

    public static KafkaMirrorMaker2Builder kafkaMirrorMaker2(String name, String targetClusterName, String sourceClusterName, int kafkaMirrorMaker2Replicas, boolean tlsListener) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = getKafkaMirrorMaker2FromYaml(Constants.PATH_TO_KAFKA_MIRROR_MAKER_2_CONFIG);
        return defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, targetClusterName, sourceClusterName, kafkaMirrorMaker2Replicas, tlsListener);
    }

    public static KafkaMirrorMaker2Builder kafkaMirrorMaker2WithMetrics(String namespaceName, String name, String targetClusterName, String sourceClusterName, int kafkaMirrorMaker2Replicas, String sourceNs, String targetNs) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = getKafkaMirrorMaker2FromYaml(Constants.PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG);
        ConfigMap metricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG, "mirror-maker-2-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(metricsCm).createOrReplace();
        return defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, targetClusterName, sourceClusterName, kafkaMirrorMaker2Replicas, false, sourceNs, targetNs);
    }

    private static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(KafkaMirrorMaker2 kafkaMirrorMaker2, String name, String kafkaTargetClusterName, String kafkaSourceClusterName, int kafkaMirrorMaker2Replicas, boolean tlsListener) {
        return defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, kafkaTargetClusterName, kafkaSourceClusterName, kafkaMirrorMaker2Replicas, tlsListener, null, null);
    }

    private static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(KafkaMirrorMaker2 kafkaMirrorMaker2, String name, String kafkaTargetClusterName, String kafkaSourceClusterName, int kafkaMirrorMaker2Replicas, boolean tlsListener, String sourceNs, String targetNs) {

        KafkaMirrorMaker2ClusterSpec targetClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaTargetClusterName)
            .withBootstrapServers(targetNs == null ? KafkaResources.plainBootstrapAddress(kafkaTargetClusterName) : KafkaUtils.namespacedPlainBootstrapAddress(kafkaTargetClusterName, targetNs))
            .addToConfig("config.storage.replication.factor", -1)
            .addToConfig("offset.storage.replication.factor", -1)
            .addToConfig("status.storage.replication.factor", -1)
            .build();

        KafkaMirrorMaker2ClusterSpec sourceClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaSourceClusterName)
            .withBootstrapServers(sourceNs == null ? KafkaResources.plainBootstrapAddress(kafkaSourceClusterName) : KafkaUtils.namespacedPlainBootstrapAddress(kafkaSourceClusterName, sourceNs))
            .build();

        if (tlsListener) {
            targetClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder(targetClusterSpec)
                .withBootstrapServers(targetNs == null ? KafkaResources.tlsBootstrapAddress(kafkaTargetClusterName) : KafkaUtils.namespacedTlsBootstrapAddress(kafkaTargetClusterName, targetNs))
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaTargetClusterName)).withCertificate("ca.crt").build())
                .endTls()
                .build();

            sourceClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder(sourceClusterSpec)
                .withBootstrapServers(sourceNs == null ? KafkaResources.tlsBootstrapAddress(kafkaSourceClusterName) : KafkaUtils.namespacedTlsBootstrapAddress(kafkaSourceClusterName, sourceNs))
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaSourceClusterName)).withCertificate("ca.crt").build())
                .endTls()
                .build();
        }

        KafkaMirrorMaker2Builder kmm2b = new KafkaMirrorMaker2Builder(kafkaMirrorMaker2)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
            .endMetadata()
            .editOrNewSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withReplicas(kafkaMirrorMaker2Replicas)
                .withConnectCluster(kafkaTargetClusterName)
                .withClusters(targetClusterSpec, sourceClusterSpec)
                .editFirstMirror()
                    .withSourceCluster(kafkaSourceClusterName)
                    .withTargetCluster(kafkaTargetClusterName)
                .endMirror()
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "DEBUG")
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

    private static KafkaMirrorMaker2 getKafkaMirrorMaker2FromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaMirrorMaker2.class);
    }
}
