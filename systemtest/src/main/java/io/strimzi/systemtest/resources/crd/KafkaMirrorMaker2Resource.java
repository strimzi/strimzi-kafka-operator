/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

public class KafkaMirrorMaker2Resource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMaker2Resource.class);

    public static final String PATH_TO_KAFKA_MIRROR_MAKER_2_CONFIG = "../examples/kafka-mirror-maker-2/kafka-mirror-maker-2.yaml";
    public static final String PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG = "../metrics/examples/kafka/kafka-mirror-maker-2-metrics.yaml";

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, DoneableKafkaMirrorMaker2, Resource<KafkaMirrorMaker2, DoneableKafkaMirrorMaker2>> kafkaMirrorMaker2Client() {
        return Crds.kafkaMirrorMaker2Operation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaMirrorMaker2 kafkaMirrorMaker2(String name, int kafkaMirrorMaker2Replicas) {
        return kafkaMirrorMaker2(name, name, name, kafkaMirrorMaker2Replicas);
    }

    public static DoneableKafkaMirrorMaker2 kafkaMirrorMaker2(String name, String targetClusterName, String sourceClusterName, int kafkaMirrorMaker2Replicas) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = getKafkaMirrorMaker2FromYaml(PATH_TO_KAFKA_MIRROR_MAKER_2_CONFIG);
        return deployKafkaMirrorMaker2(defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, targetClusterName, sourceClusterName, kafkaMirrorMaker2Replicas).build());
    }

    public static DoneableKafkaMirrorMaker2 kafkaMirrorMaker2WithMetrics(String name, int kafkaMirrorMaker2Replicas) {
        return kafkaMirrorMaker2WithMetrics(name, name, name, kafkaMirrorMaker2Replicas);
    }

    public static DoneableKafkaMirrorMaker2 kafkaMirrorMaker2WithMetrics(String name, String targetClusterName, String sourceClusterName, int kafkaMirrorMaker2Replicas) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = getKafkaMirrorMaker2FromYaml(PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG);
        return deployKafkaMirrorMaker2(defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, targetClusterName, sourceClusterName, kafkaMirrorMaker2Replicas).build());
    }

    public static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(String name, String targetClusterName, String sourceClusterName, int kafkaMirrorMaker2Replicas) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = getKafkaMirrorMaker2FromYaml(PATH_TO_KAFKA_MIRROR_MAKER_2_CONFIG);
        return defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, targetClusterName, sourceClusterName, kafkaMirrorMaker2Replicas);
    }

    private static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(KafkaMirrorMaker2 kafkaMirrorMaker2, String name, String kafkaTargetClusterName, String kafkaSourceClusterName, int kafkaMirrorMaker2Replicas) {

        KafkaMirrorMaker2ClusterSpec targetClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaTargetClusterName)
            .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaTargetClusterName))
            .withNewTls()
                .withTrustedCertificates(new CertSecretSourceBuilder().withNewSecretName(kafkaTargetClusterName + "-cluster-ca-cert").withCertificate("ca.crt").build())
            .endTls()
            .build();

        KafkaMirrorMaker2ClusterSpec sourceClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaSourceClusterName)
            .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaSourceClusterName))
            .withNewTls()
                .withTrustedCertificates(new CertSecretSourceBuilder().withNewSecretName(kafkaSourceClusterName + "-cluster-ca-cert").withCertificate("ca.crt").build())
            .endTls()
            .build();

        return new KafkaMirrorMaker2Builder(kafkaMirrorMaker2)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(kafkaTargetClusterName)
            .endMetadata()
            .editOrNewSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withReplicas(kafkaMirrorMaker2Replicas)
                .withConnectCluster(kafkaTargetClusterName)
                .withClusters(targetClusterSpec, sourceClusterSpec)                
            .endSpec();
    }

    private static DoneableKafkaMirrorMaker2 deployKafkaMirrorMaker2(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        return new DoneableKafkaMirrorMaker2(kafkaMirrorMaker2, kC -> {
            TestUtils.waitFor("KafkaMirrorMaker2 creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
                () -> {
                    try {
                        kafkaMirrorMaker2Client().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kC);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return waitFor(deleteLater(kC));
        });
    }

    public static KafkaMirrorMaker2 kafkaMirrorMaker2WithoutWait(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        kafkaMirrorMaker2Client().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaMirrorMaker2);
        return kafkaMirrorMaker2;
    }

    public static void deleteKafkaMirrorMaker2WithoutWait(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        kafkaMirrorMaker2Client().inNamespace(ResourceManager.kubeClient().getNamespace()).delete(kafkaMirrorMaker2);
    }

    private static KafkaMirrorMaker2 getKafkaMirrorMaker2FromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaMirrorMaker2.class);
    }

    private static KafkaMirrorMaker2 waitFor(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        LOGGER.info("Waiting for Kafka MirrorMaker2 {}", kafkaMirrorMaker2.getMetadata().getName());
        DeploymentUtils.waitForDeploymentReady(kafkaMirrorMaker2.getMetadata().getName() + "-mirrormaker2", kafkaMirrorMaker2.getSpec().getReplicas());
        LOGGER.info("Kafka MirrorMaker2 {} is ready", kafkaMirrorMaker2.getMetadata().getName());
        return kafkaMirrorMaker2;
    }

    private static KafkaMirrorMaker2 deleteLater(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        return ResourceManager.deleteLater(kafkaMirrorMaker2Client(), kafkaMirrorMaker2);
    }

    public static void replaceKafkaMirrorMaker2Resource(String resourceName, Consumer<KafkaMirrorMaker2> editor) {
        ResourceManager.replaceCrdResource(KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, DoneableKafkaMirrorMaker2.class, resourceName, editor);
    }

}
