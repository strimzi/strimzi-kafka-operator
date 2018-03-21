/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ResourceUtils;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.controller.cluster.resources.AbstractCluster.containerEnvVars;
import static io.strimzi.controller.cluster.resources.KafkaCluster.KEY_KAFKA_DEFAULT_REPLICATION_FACTOR;
import static io.strimzi.controller.cluster.resources.KafkaCluster.KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR;
import static io.strimzi.controller.cluster.resources.KafkaCluster.KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR;
import static io.strimzi.controller.cluster.resources.KafkaCluster.KEY_KAFKA_ZOOKEEPER_CONNECT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaSetOperationsTest {

    public static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";

    private StatefulSet a;
    private StatefulSet b;

    @Before
    public void before() {
        a = KafkaCluster.fromConfigMap(getConfigMap()).generateStatefulSet(true);
        b = KafkaCluster.fromConfigMap(getConfigMap()).generateStatefulSet(true);
    }

    private ConfigMap getConfigMap() {
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        int replicas = 3;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        return ResourceUtils.createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay, healthTimeout, METRICS_CONFIG);
    }

    @Test
    public void testNotNeedsRollingUpdateIdentical() {
        assertFalse(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNotNeedsRollingUpdateReplicas() {
        a.getSpec().setReplicas(b.getSpec().getReplicas() + 1);
        assertFalse(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateLabels() {
        Map<String, String> labels = new HashMap(b.getMetadata().getLabels());
        labels.put("foo", "bar");
        a.getMetadata().setLabels(labels);
        assertTrue(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateImage() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getImage() + "-foo");
        assertTrue(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateReadinessDelay() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setInitialDelaySeconds(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds() + 1);
        assertTrue(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateReadinessTimeout() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setTimeoutSeconds(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds() + 1);
        assertTrue(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateEnvZkConnect() {
        String envVar = KEY_KAFKA_ZOOKEEPER_CONNECT;
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                containerEnvVars(a.getSpec().getTemplate().getSpec().getContainers().get(0)).get(envVar) + "-foo", null));
        assertTrue(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateEnvDefaultRepFactor() {
        String envVar = KEY_KAFKA_DEFAULT_REPLICATION_FACTOR;
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                containerEnvVars(a.getSpec().getTemplate().getSpec().getContainers().get(0)).get(envVar) + "-foo", null));
        assertTrue(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateEnvOffsetsRepFactor() {
        String envVar = KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR;
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                containerEnvVars(a.getSpec().getTemplate().getSpec().getContainers().get(0)).get(envVar) + "-foo", null));
        assertTrue(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateEnvTxnRepFactor() {
        String envVar = KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR;
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                containerEnvVars(a.getSpec().getTemplate().getSpec().getContainers().get(0)).get(envVar) + "-foo", null));
        assertTrue(KafkaSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNotNeedsRollingUpdateEnvSomeOtherThing() {
        String envVar = "SOME_RANDOM_ENV";
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                "foo", null));
        assertFalse(KafkaSetOperations.needsRollingUpdate(a, b));
    }
}
