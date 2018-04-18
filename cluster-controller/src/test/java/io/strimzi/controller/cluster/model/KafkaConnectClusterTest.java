/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.controller.cluster.ResourceUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KafkaConnectClusterTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final int healthDelay = 100;
    private final int healthTimeout = 10;
    private final String bootstrapServers = "my-cluster-kafka:9092";
    private final String groupID = "my-cluster-group";
    private final int configReplicationFactor = 1;
    private final int offsetReplicationFactor = 1;
    private final int statusReplicationFactor = 1;
    private final String keyConverter = "org.apache.kafka.connect.json.AvroConverter";
    private final String valueConverter = "org.apache.kafka.connect.json.AvroConverter";
    private final boolean keyConverterSchemas = false;
    private final boolean valuesConverterSchema = false;

    private final ConfigMap cm = ResourceUtils.createKafkaConnectClusterConfigMap(namespace, cluster, replicas, image,
            healthDelay, healthTimeout, bootstrapServers, groupID, configReplicationFactor, offsetReplicationFactor,
            statusReplicationFactor, keyConverter, valueConverter, keyConverterSchemas, valuesConverterSchema);
    private final KafkaConnectCluster kc = KafkaConnectCluster.fromConfigMap(cm);

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<EnvVar>();
        expected.add(new EnvVarBuilder().withName(kc.KEY_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_GROUP_ID).withValue(groupID).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_KEY_CONVERTER).withValue(keyConverter).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE).withValue(Boolean.toString(keyConverterSchemas)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_VALUE_CONVERTER).withValue(valueConverter).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE).withValue(Boolean.toString(valuesConverterSchema)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_CONFIG_STORAGE_REPLICATION_FACTOR).withValue(Integer.toString(configReplicationFactor)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_OFFSET_STORAGE_REPLICATION_FACTOR).withValue(Integer.toString(offsetReplicationFactor)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_STATUS_STORAGE_REPLICATION_FACTOR).withValue(Integer.toString(statusReplicationFactor)).build());
        expected.add(new EnvVarBuilder().withName(kc.KEY_KAFKA_HEAP_OPTS).withValue("-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=1").build());
        return expected;
    }

    @Test
    public void testDefaultValues() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectClusterConfigMap(namespace, cluster));

        assertEquals(KafkaConnectCluster.DEFAULT_IMAGE, kc.image);
        assertEquals(KafkaConnectCluster.DEFAULT_REPLICAS, kc.replicas);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_DELAY, kc.healthCheckInitialDelay);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_TIMEOUT, kc.healthCheckTimeout);
        assertEquals(KafkaConnectCluster.DEFAULT_BOOTSTRAP_SERVERS, kc.bootstrapServers);
        assertEquals(KafkaConnectCluster.DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR, kc.configStorageReplicationFactor);
        assertEquals(KafkaConnectCluster.DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR, kc.offsetStorageReplicationFactor);
        assertEquals(KafkaConnectCluster.DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR, kc.statusStorageReplicationFactor);
        assertEquals(KafkaConnectCluster.DEFAULT_GROUP_ID, kc.groupId);
        assertEquals(KafkaConnectCluster.DEFAULT_KEY_CONVERTER, kc.keyConverter);
        assertEquals(KafkaConnectCluster.DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE, kc.keyConverterSchemasEnable);
        assertEquals(KafkaConnectCluster.DEFAULT_VALUE_CONVERTER, kc.valueConverter);
        assertEquals(KafkaConnectCluster.DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE, kc.valueConverterSchemasEnable);
    }

    @Test
    public void testFromConfigMap() {
        assertEquals(replicas, kc.replicas);
        assertEquals(image, kc.image);
        assertEquals(healthDelay, kc.healthCheckInitialDelay);
        assertEquals(healthTimeout, kc.healthCheckTimeout);
        assertEquals(bootstrapServers, kc.bootstrapServers);
        assertEquals(configReplicationFactor, kc.configStorageReplicationFactor);
        assertEquals(offsetReplicationFactor, kc.offsetStorageReplicationFactor);
        assertEquals(statusReplicationFactor, kc.statusStorageReplicationFactor);
        assertEquals(groupID, kc.groupId);
        assertEquals(keyConverter, kc.keyConverter);
        assertEquals(keyConverterSchemas, kc.keyConverterSchemasEnable);
        assertEquals(valueConverter, kc.valueConverter);
        assertEquals(valuesConverterSchema, kc.valueConverterSchemasEnable);
    }

    @Test
    public void testFromDeployment() {
        KafkaConnectCluster newKc = KafkaConnectCluster.fromAssembly(namespace, cluster, kc.generateDeployment());

        assertEquals(replicas, newKc.replicas);
        assertEquals(image, newKc.image);
        assertEquals(healthDelay, newKc.healthCheckInitialDelay);
        assertEquals(healthTimeout, newKc.healthCheckTimeout);
        assertEquals(bootstrapServers, newKc.bootstrapServers);
        assertEquals(configReplicationFactor, newKc.configStorageReplicationFactor);
        assertEquals(offsetReplicationFactor, newKc.offsetStorageReplicationFactor);
        assertEquals(statusReplicationFactor, newKc.statusStorageReplicationFactor);
        assertEquals(groupID, newKc.groupId);
        assertEquals(keyConverter, newKc.keyConverter);
        assertEquals(keyConverterSchemas, newKc.keyConverterSchemasEnable);
        assertEquals(valueConverter, newKc.valueConverter);
        assertEquals(valuesConverterSchema, newKc.valueConverterSchemasEnable);
    }

    @Test
    public void testFromDeploymentWithDefaultValues() {
        KafkaConnectCluster defaultsKc = KafkaConnectCluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectClusterConfigMap(namespace, cluster));
        KafkaConnectCluster newKc = KafkaConnectCluster.fromAssembly(namespace, cluster, defaultsKc.generateDeployment());

        assertEquals(KafkaConnectCluster.DEFAULT_REPLICAS, newKc.replicas);
        assertEquals(KafkaConnectCluster.DEFAULT_IMAGE, newKc.image);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_DELAY, newKc.healthCheckInitialDelay);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_TIMEOUT, newKc.healthCheckTimeout);
        assertEquals(KafkaConnectCluster.DEFAULT_BOOTSTRAP_SERVERS, newKc.bootstrapServers);
        assertEquals(KafkaConnectCluster.DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR, newKc.configStorageReplicationFactor);
        assertEquals(KafkaConnectCluster.DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR, newKc.offsetStorageReplicationFactor);
        assertEquals(KafkaConnectCluster.DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR, newKc.statusStorageReplicationFactor);
        assertEquals(KafkaConnectCluster.DEFAULT_GROUP_ID, newKc.groupId);
        assertEquals(KafkaConnectCluster.DEFAULT_KEY_CONVERTER, newKc.keyConverter);
        assertEquals(KafkaConnectCluster.DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE, newKc.keyConverterSchemasEnable);
        assertEquals(KafkaConnectCluster.DEFAULT_VALUE_CONVERTER, newKc.valueConverter);
        assertEquals(KafkaConnectCluster.DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE, newKc.valueConverterSchemasEnable);
    }

    @Test
    public void testEnvVars()   {
        assertEquals(getExpectedEnvVars(), kc.getEnvVars());
    }

    @Test
    public void testGenerateService()   {
        Service svc = kc.generateService();

        assertEquals("ClusterIP", svc.getSpec().getType());
        Map<String, String> expectedLabels = ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster));
        assertEquals(expectedLabels, svc.getMetadata().getLabels());
        assertEquals(expectedLabels, svc.getSpec().getSelector());
        assertEquals(1, svc.getSpec().getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), svc.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, svc.getSpec().getPorts().get(0).getName());
        assertEquals("TCP", svc.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testGenerateDeployment()   {
        Deployment dep = kc.generateDeployment();

        assertEquals(kc.kafkaConnectClusterName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        Map<String, String> expectedLabels = ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka-connect",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, kc.kafkaConnectClusterName(cluster));
        assertEquals(expectedLabels, dep.getMetadata().getLabels());
        assertEquals(new Integer(replicas), dep.getSpec().getReplicas());
        assertEquals(expectedLabels, dep.getSpec().getTemplate().getMetadata().getLabels());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().size());
        assertEquals(kc.kafkaConnectClusterName(this.cluster), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
        assertEquals(kc.image, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(getExpectedEnvVars(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size());
        assertEquals(new Integer(KafkaConnectCluster.REST_API_PORT), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort());
        assertEquals(KafkaConnectCluster.REST_API_PORT_NAME, dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName());
        assertEquals("TCP", dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol());
        assertEquals("RollingUpdate", dep.getSpec().getStrategy().getType());
        assertEquals(new Integer(1), dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge().getIntVal());
        assertEquals(new Integer(0), dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable().getIntVal());
    }
}
