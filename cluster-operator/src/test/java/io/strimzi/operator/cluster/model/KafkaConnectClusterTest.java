/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.operator.cluster.InvalidConfigMapException;
import io.strimzi.operator.cluster.ResourceUtils;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaConnectClusterTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final int healthDelay = 100;
    private final int healthTimeout = 10;
    private final String configurationJson = "{\"foo\":\"bar\"}";
    private final String expectedConfiguration = "group.id=connect-cluster\n" +
            "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "internal.key.converter.schemas.enable=false\n" +
            "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "config.storage.topic=connect-cluster-configs\n" +
            "status.storage.topic=connect-cluster-status\n" +
            "offset.storage.topic=connect-cluster-offsets\n" +
            "foo=bar\n" +
            "internal.key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "internal.value.converter.schemas.enable=false\n" +
            "internal.value.converter=org.apache.kafka.connect.json.JsonConverter\n";
    private final String defaultConfiguration = "group.id=connect-cluster\n" +
            "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "internal.key.converter.schemas.enable=false\n" +
            "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "config.storage.topic=connect-cluster-configs\n" +
            "status.storage.topic=connect-cluster-status\n" +
            "offset.storage.topic=connect-cluster-offsets\n" +
            "internal.key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "internal.value.converter.schemas.enable=false\n" +
            "internal.value.converter=org.apache.kafka.connect.json.JsonConverter\n";

    private final ConfigMap cm = ResourceUtils.createKafkaConnectClusterConfigMap(namespace, cluster, replicas, image,
            healthDelay, healthTimeout, configurationJson);
    private final KafkaConnectCluster kc = KafkaConnectCluster.fromConfigMap(cm);

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<EnvVar>();
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION).withValue(expectedConfiguration).build());
        expected.add(new EnvVarBuilder().withName(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION).withValue("1.0").build());
        return expected;
    }

    @Test
    public void testDefaultValues() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectClusterConfigMap(namespace, cluster));

        assertEquals(KafkaConnectCluster.DEFAULT_IMAGE, kc.image);
        assertEquals(KafkaConnectCluster.DEFAULT_REPLICAS, kc.replicas);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_DELAY, kc.healthCheckInitialDelay);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_TIMEOUT, kc.healthCheckTimeout);
        assertEquals(defaultConfiguration, kc.getConfiguration().getConfiguration());
    }

    @Test
    public void testFromConfigMap() {
        assertEquals(replicas, kc.replicas);
        assertEquals(image, kc.image);
        assertEquals(healthDelay, kc.healthCheckInitialDelay);
        assertEquals(healthTimeout, kc.healthCheckTimeout);
        assertEquals(expectedConfiguration, kc.getConfiguration().getConfiguration());
    }

    @Test
    public void testFromDeployment() {
        KafkaConnectCluster newKc = KafkaConnectCluster.fromAssembly(namespace, cluster, kc.generateDeployment());

        assertEquals(replicas, newKc.replicas);
        assertEquals(image, newKc.image);
        assertEquals(healthDelay, newKc.healthCheckInitialDelay);
        assertEquals(healthTimeout, newKc.healthCheckTimeout);
        assertEquals(expectedConfiguration, kc.getConfiguration().getConfiguration());
    }

    @Test
    public void testFromDeploymentWithDefaultValues() {
        KafkaConnectCluster defaultsKc = KafkaConnectCluster.fromConfigMap(ResourceUtils.createEmptyKafkaConnectClusterConfigMap(namespace, cluster));
        KafkaConnectCluster newKc = KafkaConnectCluster.fromAssembly(namespace, cluster, defaultsKc.generateDeployment());

        assertEquals(KafkaConnectCluster.DEFAULT_REPLICAS, newKc.replicas);
        assertEquals(KafkaConnectCluster.DEFAULT_IMAGE, newKc.image);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_DELAY, newKc.healthCheckInitialDelay);
        assertEquals(KafkaConnectCluster.DEFAULT_HEALTHCHECK_TIMEOUT, newKc.healthCheckTimeout);
        assertEquals(defaultsKc.getConfiguration().getConfiguration(), newKc.getConfiguration().getConfiguration());
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

    @Test
    public void testCorruptedValues() {
        ConfigMap cm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(namespace, cluster);

        // type mismatch
        cm.getData().put("kafka-healthcheck-delay", "1z");
        try {
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals(e.getKey(), "kafka-healthcheck-delay");
        }

        // unknown type
        cm.getData().clear();
        cm.getData().put("kafka-storage", "{ \"type\": \"zidan\" }");
        try {
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals(e.getKey(), "kafka-storage");
        }

        // corrupted JSON (missing quotes)
        cm.getData().clear();
        cm.getData().put("kafka-config", "{" +
                "\"num.recovery.threads.per.data.dir\": \"1\",\n" +
                "\"default.replication.factor\": e,\n" +
                "\"num.io.threads\": \"1\"" +
                "}");
        try {
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("default.replication.factor", e.getKey());
        }

        // corrupted JSON (missing quotes)
        cm.getData().clear();
        cm.getData().put("kafka-config", "{" +
                "\"num.recovery.threads.per.data.dir\": \"1\",\n" +
                "\"num.io.threads\": \"1\"");
        try {
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("JSON braces", e.getKey());
        }
    }

    @Test
    public void testCorruptedBooleans() {
        ConfigMap cm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(namespace, cluster);

        // typo in boolean value
        cm.getData().put("kafka-config", "{" +
                "\"num.recovery.threads.per.data.dir\": \"1\",\n" +
                "\"default.replication.factor\": 3,\n" +
                "\"num.io.threads\": \"1\",\n" +
                "\"bool.value\": tru" +
                "}");
        try {
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("bool.value", e.getKey());
        }

        // test parsing boolean values
        cm.getData().clear();
        cm.getData().put("kafka-config", "{" +
                "\"num.recovery.threads.per.data.dir\": \"1\",\n" +
                "\"default.replication.factor\": 3,\n" +
                "\"num.io.threads\": \"1\",\n" +
                "\"bool.value\": true,\n" +
                "\"bool.value2\": \"true\",\n" +
                "\"bool.value3\": false,\n" +
                "\"bool.value4\": \"truuue\"" +
                "}");

        // we have to prepare map before parsing booleans
        Map<String, String> data = cm.getData();
        String config = data.get("kafka-config");
        Map<String, Object> map = new JsonObject(config).getMap();
        Map<String, String> newMap = new HashMap<String, String>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof String) {
                newMap.put(entry.getKey(), (String) entry.getValue());
            } else if (entry.getValue() instanceof Integer || entry.getValue() instanceof Long || entry.getValue() instanceof Boolean || entry.getValue() instanceof Double || entry.getValue() instanceof Float) {
                newMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        assertTrue(Utils.getBoolean(newMap, "bool.value", false));
        assertTrue(Utils.getBoolean(newMap, "bool.value2", true));
        assertFalse(Utils.getBoolean(newMap, "bool.value3", true));

        try {
            assertTrue(Utils.getBoolean(newMap, "bool.value4", false));
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("bool.value4", e.getKey());
        }
    }

    @Test
    public void testEmptyValue() {
        ConfigMap cm = ResourceUtils.createEmptyKafkaConnectClusterConfigMap(namespace, cluster);

        // in the middle
        cm.getData().put("kafka-config", "{" +
                "\"offsets .topic.replication.factor\": \"3\",\n" +
                "\"transaction.state.log.min.isr\": ,\n" +
                "\"default.replication.factor\": 2 }");
        try {
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("Unexpected character - ,", e.getKey());
        }

        // end
        cm.getData().clear();
        cm.getData().put("kafka-config", "{" +
                "\"offsets .topic.replication.factor\": \"3\",\n" +
                "\"transaction.state.log.min.isr\": 7,\n" +
                "\"default.replication.factor\": }");
        try {
            KafkaCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("Unexpected character - }", e.getKey());
        }
    }
}
