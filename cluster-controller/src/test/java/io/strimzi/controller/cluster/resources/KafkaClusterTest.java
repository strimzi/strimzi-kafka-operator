/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ResourceUtils;
import org.junit.Test;

import static io.strimzi.controller.cluster.ResourceUtils.labels;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaClusterTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson);
    private final KafkaCluster kc = KafkaCluster.fromConfigMap(cm);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsConfigMap();
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractCluster.METRICS_CONFIG_FILE));
    }

    @Test
    public void testGenerateService() {
        Service headful = kc.generateService();
        checkService(headful);
    }

    private void checkService(Service headful) {
        assertEquals("ClusterIP", headful.getSpec().getType());
        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster)), headful.getSpec().getSelector());
        assertEquals(2, headful.getSpec().getPorts().size());
        assertEquals(KafkaCluster.CLIENT_PORT_NAME, headful.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_PORT), headful.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaCluster.REPLICATION_PORT_NAME, headful.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(KafkaCluster.REPLICATION_PORT), headful.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testGenerateHeadlessService() {
        Service headless = kc.generateHeadlessService();
        checkHeadlessService(headless);
    }

    private void checkHeadlessService(Service headless) {
        assertEquals(KafkaCluster.headlessName(cluster), headless.getMetadata().getName());
        assertEquals("ClusterIP", headless.getSpec().getType());
        assertEquals("None", headless.getSpec().getClusterIP());
        assertEquals(labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster)), headless.getSpec().getSelector());
        assertEquals(2, headless.getSpec().getPorts().size());
        assertEquals(KafkaCluster.CLIENT_PORT_NAME, headless.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_PORT), headless.getSpec().getPorts().get(0).getPort());
        assertEquals(KafkaCluster.REPLICATION_PORT_NAME, headless.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(KafkaCluster.REPLICATION_PORT), headless.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headless.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet ss = kc.generateStatefulSet(true);
        checkStatefulSet(ss);
    }

    private void checkStatefulSet(StatefulSet ss) {
        assertEquals(KafkaCluster.kafkaClusterName(cluster), ss.getMetadata().getName());
        // ... in the same namespace ...
        assertEquals(namespace, ss.getMetadata().getNamespace());
        // ... with these labels
        assertEquals(labels("strimzi.io/cluster", cluster,
                "strimzi.io/type", "kafka",
                "my-user-label", "cromulent",
                "strimzi.io/name", KafkaCluster.kafkaClusterName(cluster)),
                ss.getMetadata().getLabels());

        assertEquals(new Integer(replicas), ss.getSpec().getReplicas());
        assertEquals(image, ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(new Integer(healthTimeout), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
    }

    /**
     * Check that a KafkaCluster from a statefulset matches the one from a ConfigMap
     */
    @Test
    public void testClusterFromStatefulSet() {
        StatefulSet ss = kc.generateStatefulSet(true);
        KafkaCluster kc2 = KafkaCluster.fromStatefulSet(ss, namespace, cluster);
        // Don't check the metrics CM, since this isn't restored from the StatefulSet
        checkService(kc2.generateService());
        checkHeadlessService(kc2.generateHeadlessService());
        checkStatefulSet(kc2.generateStatefulSet(true));
    }

    // TODO test volume claim templates

    @Test
    public void testDiffNoDiffs() {
        ClusterDiffResult diff = kc.diff(kc.generateMetricsConfigMap(), kc.generateStatefulSet(true));
        assertFalse(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffMetrics() {
        KafkaCluster other = KafkaCluster.fromConfigMap(ResourceUtils.createKafkaClusterConfigMap(namespace, cluster,
                replicas, image, healthDelay, healthTimeout, "{\"something\":\"different\"}"));
        ClusterDiffResult diff = kc.diff(other.generateMetricsConfigMap(), other.generateStatefulSet(true));
        assertFalse(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertTrue(diff.isMetricsChanged());
    }

    @Test
    public void testDiffScaleDown() {
        KafkaCluster other = KafkaCluster.fromConfigMap(ResourceUtils.createKafkaClusterConfigMap(namespace, cluster,
                replicas + 1, image, healthDelay, healthTimeout, metricsCmJson));
        ClusterDiffResult diff = kc.diff(other.generateMetricsConfigMap(), other.generateStatefulSet(true));
        assertFalse(diff.isDifferent());
        assertTrue(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffScaleUp() {
        KafkaCluster other = KafkaCluster.fromConfigMap(ResourceUtils.createKafkaClusterConfigMap(namespace, cluster,
                replicas - 1, image, healthDelay, healthTimeout, metricsCmJson));
        ClusterDiffResult diff = kc.diff(other.generateMetricsConfigMap(), other.generateStatefulSet(true));
        assertFalse(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertTrue(diff.isScaleUp());
        assertFalse(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffImage() {
        KafkaCluster other = KafkaCluster.fromConfigMap(ResourceUtils.createKafkaClusterConfigMap(namespace, cluster,
                replicas, "differentimage", healthDelay, healthTimeout, metricsCmJson));
        ClusterDiffResult diff = kc.diff(other.generateMetricsConfigMap(), other.generateStatefulSet(true));
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffHealthDelay() {
        KafkaCluster other = KafkaCluster.fromConfigMap(ResourceUtils.createKafkaClusterConfigMap(namespace, cluster,
                replicas, image, healthDelay + 1, healthTimeout, metricsCmJson));
        ClusterDiffResult diff = kc.diff(other.generateMetricsConfigMap(), other.generateStatefulSet(true));
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testDiffHealthTimeout() {
        KafkaCluster other = KafkaCluster.fromConfigMap(ResourceUtils.createKafkaClusterConfigMap(namespace, cluster,
                replicas, image, healthDelay, healthTimeout + 1, metricsCmJson));
        ClusterDiffResult diff = kc.diff(other.generateMetricsConfigMap(), other.generateStatefulSet(true));
        assertTrue(diff.isDifferent());
        assertFalse(diff.isScaleDown());
        assertFalse(diff.isScaleUp());
        assertTrue(diff.isRollingUpdate());
        assertFalse(diff.isMetricsChanged());
    }

    @Test
    public void testPodNames() {

        for (int i = 0; i < replicas; i++) {
            assertEquals(KafkaCluster.kafkaClusterName(cluster) + "-" + i, kc.getPodName(i));
        }
    }

    @Test
    public void testPvcNames() {

        for (int i = 0; i < replicas; i++) {
            assertEquals(kc.volumeName + "-" + KafkaCluster.kafkaClusterName(cluster) + "-" + i, kc.getPersistentVolumeClaimName(i));
        }
    }

}
