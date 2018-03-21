/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ResourceUtils;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.controller.cluster.resources.AbstractCluster.containerEnvVars;
import static io.strimzi.controller.cluster.resources.ZookeeperCluster.KEY_ZOOKEEPER_METRICS_ENABLED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZookeeperSetOperationsTest {

    public static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";

    private StatefulSet a;
    private StatefulSet b;

    @Before
    public void before() {
        a = ZookeeperCluster.fromConfigMap(getConfigMap()).generateStatefulSet(true);
        b = ZookeeperCluster.fromConfigMap(getConfigMap()).generateStatefulSet(true);
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
        assertFalse(ZookeeperSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateReplicas() {
        a.getSpec().setReplicas(b.getSpec().getReplicas() + 1);
        assertTrue(ZookeeperSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateLabels() {
        Map<String, String> labels = new HashMap<>(b.getMetadata().getLabels());
        labels.put("foo", "bar");
        a.getMetadata().setLabels(labels);
        assertTrue(ZookeeperSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateImage() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getImage() + "-foo");
        assertTrue(ZookeeperSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateReadinessDelay() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setInitialDelaySeconds(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds() + 1);
        assertTrue(ZookeeperSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateReadinessTimeout() {
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setTimeoutSeconds(
                a.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds() + 1);
        assertTrue(ZookeeperSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNeedsRollingUpdateEnvZkMetricsEnabled() {
        String envVar = KEY_ZOOKEEPER_METRICS_ENABLED;
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                containerEnvVars(a.getSpec().getTemplate().getSpec().getContainers().get(0)).get(envVar) + "-foo", null));
        assertTrue(ZookeeperSetOperations.needsRollingUpdate(a, b));
    }

    @Test
    public void testNotNeedsRollingUpdateEnvSomeOtherThing() {
        String envVar = "SOME_RANDOM_ENV";
        a.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                "foo", null));
        assertFalse(ZookeeperSetOperations.needsRollingUpdate(a, b));
    }
}
