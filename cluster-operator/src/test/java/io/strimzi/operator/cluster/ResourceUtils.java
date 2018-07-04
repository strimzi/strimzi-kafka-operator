/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.KafkaAssemblySpec;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.api.kafka.model.KafkaConnectAssemblyBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2IAssembly;
import io.strimzi.api.kafka.model.KafkaConnectS2IAssemblyBuilder;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.api.kafka.model.TopicOperator;
import io.strimzi.api.kafka.model.Zookeeper;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.assembly.AbstractAssemblyOperator;
import io.strimzi.test.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class ResourceUtils {

    private ResourceUtils() {

    }

    /**
     * Creates a map of labels
     * @param pairs (key, value) pairs. There must be an even number, obviously.
     * @return a map of labels
     * @deprecated Use method method in TestUtils
     */
    @Deprecated
    public static Map<String, String> labels(String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        HashMap<String, String> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }

    /** @deprecated use the {@link io.strimzi.api.kafka.model.KafkaAssemblyBuilder} */
    @Deprecated
    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                   String image, int healthDelay, int healthTimeout,
                                                   String metricsCmJson) {
        return createKafkaCluster(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCmJson, "{}", "{}",
                "{\"type\": \"ephemeral\"}", null, null, null, null);
    }

    /** @deprecated use the {@link io.strimzi.api.kafka.model.KafkaAssemblyBuilder} */
    @Deprecated
    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout,
                                                        String metricsCmJson, String kafkaConfigurationJson,
                                                        String zooConfigurationJson) {
        return createKafkaCluster(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCmJson, kafkaConfigurationJson, zooConfigurationJson,
                "{\"type\": \"ephemeral\"}", null, null, null, null);
    }

    public static List<Secret> createKafkaClusterInitialSecrets(String clusterCmNamespace) {

        List<Secret> secrets = new ArrayList<>();

        secrets.add(
                new SecretBuilder()
                .withNewMetadata()
                    .withName(AbstractAssemblyOperator.INTERNAL_CA_NAME)
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .addToData("internal-ca.key", Base64.getEncoder().encodeToString("internal-ca-base64key".getBytes()))
                .addToData("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()))
                .build()
        );
        return secrets;
    }

    public static List<Secret> createKafkaClusterSecretsWithReplicas(String clusterCmNamespace, String clusterCmName, int kafkaReplicas, int zkReplicas) {

        List<Secret> secrets = new ArrayList<>();

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(AbstractAssemblyOperator.INTERNAL_CA_NAME)
                        .withNamespace(clusterCmNamespace)
                        .endMetadata()
                        .addToData("internal-ca.key", Base64.getEncoder().encodeToString("internal-ca-base64key".getBytes()))
                        .addToData("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()))
                        .build()
        );

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsCASecretName(clusterCmName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("clients-ca.key", Base64.getEncoder().encodeToString("clients-ca-base64key".getBytes()))
                        .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-base64crt".getBytes()))
                        .build()
        );

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsPublicKeyName(clusterCmName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-base64crt".getBytes()))
                        .build()
        );

        SecretBuilder builder =
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.brokersInternalSecretName(clusterCmName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()));

        for (int i = 0; i < kafkaReplicas; i++) {
            builder.addToData(KafkaCluster.kafkaPodName(clusterCmName, i) + ".key", Base64.getEncoder().encodeToString("brokers-internal-base64key".getBytes()))
                    .addToData(KafkaCluster.kafkaPodName(clusterCmName, i) + ".crt", Base64.getEncoder().encodeToString("brokers-internal-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        builder = new SecretBuilder()
                        .withNewMetadata()
                            .withName(KafkaCluster.brokersClientsSecretName(clusterCmName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()))
                        .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-base64crt".getBytes()));


        for (int i = 0; i < kafkaReplicas; i++) {
            builder.addToData(KafkaCluster.kafkaPodName(clusterCmName, i) + ".key", Base64.getEncoder().encodeToString("brokers-clients-base64key".getBytes()))
                    .addToData(KafkaCluster.kafkaPodName(clusterCmName, i) + ".crt", Base64.getEncoder().encodeToString("brokers-clients-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        builder = new SecretBuilder()
                        .withNewMetadata()
                            .withName(ZookeeperCluster.nodesSecretName(clusterCmName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()));

        for (int i = 0; i < zkReplicas; i++) {
            builder.addToData(ZookeeperCluster.zookeeperPodName(clusterCmName, i) + ".key", Base64.getEncoder().encodeToString("nodes-base64key".getBytes()))
                    .addToData(ZookeeperCluster.zookeeperPodName(clusterCmName, i) + ".crt", Base64.getEncoder().encodeToString("nodes-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        return secrets;
    }

    /** @deprecated use the {@link io.strimzi.api.kafka.model.KafkaAssemblyBuilder} */
    @Deprecated
    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                   String image, int healthDelay, int healthTimeout,
                                                   String metricsCmJson, String kafkaConfigurationJson) {
        return createKafkaCluster(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCmJson, kafkaConfigurationJson, "{}",
                "{\"type\": \"ephemeral\"}", null, null, null, null);
    }

    /** @deprecated use the {@link io.strimzi.api.kafka.model.KafkaAssemblyBuilder} */
    @Deprecated
    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                   String image, int healthDelay, int healthTimeout,
                                                   String metricsCmJson, String kafkaConfigurationJson,
                                                   Logging kafkaLogging, Logging zkLogging) {
        return createKafkaCluster(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCmJson, kafkaConfigurationJson, "{}",
                "{\"type\": \"ephemeral\"}", null, null, kafkaLogging, zkLogging);
    }

    /** @deprecated use the {@link io.strimzi.api.kafka.model.KafkaAssemblyBuilder} */
    @Deprecated
    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                   String image, int healthDelay, int healthTimeout, String metricsCmJson,
                                                   String kafkaConfigurationJson, String zooConfigurationJson,
                                                   String storageJson, String topicOperator, String rackJson,
                                                   Logging kafkaLogging, Logging zkLogging) {
        try {
            KafkaAssembly result = new KafkaAssembly();
            ObjectMeta meta = new ObjectMeta();
            meta.setNamespace(clusterCmNamespace);
            meta.setName(clusterCmName);
            meta.setLabels(Labels.userLabels(singletonMap("my-user-label", "cromulent")).toMap());
            result.setMetadata(meta);

            KafkaAssemblySpec spec = new KafkaAssemblySpec();

            Kafka kafka = new Kafka();
            kafka.setReplicas(replicas);
            kafka.setImage(image);
            if (kafkaLogging != null) {
                kafka.setLogging(kafkaLogging);
            }
            Probe livenessProbe = new Probe();
            livenessProbe.setInitialDelaySeconds(healthDelay);
            livenessProbe.setTimeoutSeconds(healthTimeout);
            kafka.setLivenessProbe(livenessProbe);
            kafka.setReadinessProbe(livenessProbe);
            ObjectMapper om = new ObjectMapper();
            TypeReference<HashMap<String, Object>> typeRef
                    = new TypeReference<HashMap<String, Object>>() { };
            if (metricsCmJson != null) {
                kafka.setMetrics(om.readValue(metricsCmJson, typeRef));
            }
            if (kafkaConfigurationJson != null) {
                kafka.setConfig(om.readValue(kafkaConfigurationJson, typeRef));
            }
            kafka.setStorage(TestUtils.fromJson(storageJson, Storage.class));
            Rack rack = TestUtils.fromJson(rackJson, Rack.class);
            if (rack != null && (rack.getTopologyKey() == null || rack.getTopologyKey().equals(""))) {
                throw new IllegalArgumentException("In rack configuration the 'topologyKey' field is mandatory");
            }
            kafka.setRack(rack);
            spec.setKafka(kafka);

            Zookeeper zk = new Zookeeper();
            zk.setReplicas(replicas);
            zk.setImage(image + "-zk");
            if (zkLogging != null) {
                zk.setLogging(zkLogging);
            }
            zk.setLivenessProbe(livenessProbe);
            zk.setReadinessProbe(livenessProbe);
            if (zooConfigurationJson != null) {
                zk.setConfig(om.readValue(zooConfigurationJson, typeRef));
            }
            zk.setStorage(TestUtils.fromJson(storageJson, Storage.class));
            if (metricsCmJson != null) {
                zk.setMetrics(om.readValue(metricsCmJson, typeRef));
            }

            spec.setTopicOperator(TestUtils.fromJson(topicOperator, TopicOperator.class));

            spec.setZookeeper(zk);
            result.setSpec(spec);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Generate ConfigMap for Kafka Connect S2I cluster
     * @deprecated use the {@link io.strimzi.api.kafka.model.KafkaConnectS2IAssemblyBuilder}
     */
    @Deprecated
    public static KafkaConnectS2IAssembly createKafkaConnectS2ICluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                                       String image, int healthDelay, int healthTimeout, String metricsCmJson,
                                                                       String connectConfig, boolean insecureSourceRepo) {

        return new KafkaConnectS2IAssemblyBuilder(createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName))
                .withNewSpec()
                    .withImage(image)
                    .withReplicas(replicas)
                    .withLivenessProbe(new Probe(healthDelay, healthTimeout))
                    .withReadinessProbe(new Probe(healthDelay, healthTimeout))
                    .withMetrics((Map<String, Object>) TestUtils.fromJson(metricsCmJson, Map.class))
                    .withConfig((Map<String, Object>) TestUtils.fromJson(connectConfig, Map.class))
                    .withInsecureSourceRepository(insecureSourceRepo)
                .endSpec().build();
    }

    /**
     * Generate empty Kafka Connect S2I ConfigMap
     * @deprecated use the {@link io.strimzi.api.kafka.model.KafkaConnectS2IAssemblyBuilder}
     */
    @Deprecated
    public static KafkaConnectS2IAssembly createEmptyKafkaConnectS2ICluster(String clusterCmNamespace, String clusterCmName) {
        return new KafkaConnectS2IAssemblyBuilder()
                .withMetadata(new ObjectMetaBuilder()
                .withName(clusterCmName)
                .withNamespace(clusterCmNamespace)
                .withLabels(labels(Labels.STRIMZI_KIND_LABEL, "cluster",
                        Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                        "my-user-label", "cromulent"))
                .build())
                .withNewSpec().endSpec()
                .build();
    }

    /*** @deprecated use the {@link io.strimzi.api.kafka.model.KafkaConnectAssemblyBuilder} */
    @Deprecated
    public static KafkaConnectAssembly createKafkaConnectCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                                 String image, int healthDelay, int healthTimeout, String metricsCmJson, String connectConfig) {

        KafkaConnectAssembly cm = createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        return new KafkaConnectAssemblyBuilder(cm)
                .withNewSpec()
                    .withMetrics((Map<String, Object>) TestUtils.fromJson(metricsCmJson, Map.class))
                    .withConfig((Map<String, Object>) TestUtils.fromJson(connectConfig, Map.class))
                    .withImage(image)
                    .withReplicas(replicas)
                    .withReadinessProbe(new Probe(healthDelay, healthTimeout))
                    .withLivenessProbe(new Probe(healthDelay, healthTimeout))
                .endSpec()
            .build();

    }

    /**
     * Generate empty Kafka Connect ConfigMap
     * @deprecated use the {@link io.strimzi.api.kafka.model.KafkaConnectAssemblyBuilder}
     */
    @Deprecated
    public static KafkaConnectAssembly createEmptyKafkaConnectCluster(String clusterCmNamespace, String clusterCmName) {
        return new KafkaConnectAssemblyBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(clusterCmName)
                        .withNamespace(clusterCmNamespace)
                        .withLabels(labels(Labels.STRIMZI_KIND_LABEL, "cluster",
                                Labels.STRIMZI_TYPE_LABEL, "kafka-connect",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec().endSpec()
                .build();
    }
}
