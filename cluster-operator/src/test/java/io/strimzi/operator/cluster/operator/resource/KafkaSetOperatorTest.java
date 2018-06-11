/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.operator.cluster.model.AbstractModel.containerEnvVars;
import static io.strimzi.operator.cluster.model.KafkaCluster.ENV_VAR_KAFKA_ZOOKEEPER_CONNECT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaSetOperatorTest {

    public static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";

    private StatefulSet a;
    private StatefulSet b;

    @Before
    public void before() {
        a = KafkaCluster.fromDescription(getConfigMap(), Collections.EMPTY_LIST).generateStatefulSet(true);
        b = KafkaCluster.fromDescription(getConfigMap(), Collections.EMPTY_LIST).generateStatefulSet(true);
    }

    private ConfigMap getConfigMap() {
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        int replicas = 3;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        return ResourceUtils.createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay, healthTimeout, METRICS_CONFIG,
                "{\"type\": \"persistent-claim\", " +
                        "\"size\": \"123\", " +
                        "\"class\": \"foo\"," +
                        "\"delete-claim\": true}");
    }

    private StatefulSetDiff diff() {
        return new StatefulSetDiff(a, b);
    }

    @Test
    public void testNotNeedsRollingUpdateIdentical() {
        assertFalse(KafkaSetOperator.needsRollingUpdate(diff()));
    }

    @Test
    public void testNotNeedsRollingUpdateReplicas() {
        a.getSpec().setReplicas(b.getSpec().getReplicas() + 1);
        assertFalse(KafkaSetOperator.needsRollingUpdate(diff()));
    }

    @Test
    public void testNeedsRollingUpdateLabels() {
        Map<String, String> labels = new HashMap(b.getMetadata().getLabels());
        labels.put("foo", "bar");
        a.getMetadata().setLabels(labels);
        assertTrue(KafkaSetOperator.needsRollingUpdate(diff()));
    }

    @Test
    public void testNeedsRollingUpdateImage() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getImage() + "-foo");
        assertTrue(KafkaSetOperator.needsRollingUpdate(diff()));
    }

    @Test
    public void testNeedsRollingUpdateReadinessDelay() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setInitialDelaySeconds(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds() + 1);
        assertTrue(KafkaSetOperator.needsRollingUpdate(diff()));
    }

    @Test
    public void testNeedsRollingUpdateReadinessTimeout() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setTimeoutSeconds(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds() + 1);
        assertTrue(KafkaSetOperator.needsRollingUpdate(diff()));
    }

    @Test
    public void testNeedsRollingUpdateEnvZkConnect() {
        String envVar = ENV_VAR_KAFKA_ZOOKEEPER_CONNECT;
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                containerEnvVars(a.getSpec().getTemplate().getSpec().getContainers().get(0)).get(envVar) + "-foo", null));
        assertTrue(KafkaSetOperator.needsRollingUpdate(diff()));
    }

    @Test
    public void testNeedsRollingUpdateEnvSomeOtherThing() {
        String envVar = "SOME_RANDOM_ENV";
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                "foo", null));
        assertTrue(KafkaSetOperator.needsRollingUpdate(diff()));
    }
}
