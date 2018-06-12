/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.InvalidConfigMapException;
import io.strimzi.operator.cluster.ResourceUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static io.strimzi.operator.cluster.ResourceUtils.labels;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class ZookeeperClusterTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String configurationJson = "{}";
    private final String zooConfigurationJson = "{\"foo\":\"bar\"}";
    private final ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson);
    private final ZookeeperCluster zc = ZookeeperCluster.fromConfigMap(cm);
    @Rule
    public ResourceTester<ZookeeperCluster> resourceTester = new ResourceTester<>(ZookeeperCluster::fromConfigMap);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = zc.generateMetricsConfigMap();
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractModel.METRICS_CONFIG_FILE));
    }

    @Test
    public void testGenerateService() {
        Service headful = zc.generateService();
        checkService(headful);
    }

    private void checkService(Service headful) {
        assertEquals("ClusterIP", headful.getSpec().getType());
        assertEquals(ResourceUtils.labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(cluster)), headful.getSpec().getSelector());
        assertEquals(2, headful.getSpec().getPorts().size());
        assertEquals(ZookeeperCluster.METRICS_PORT_NAME, headful.getSpec().getPorts().get(0).getName());
        assertEquals(ZookeeperCluster.CLIENT_PORT_NAME, headful.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(ZookeeperCluster.METRICS_PORT), headful.getSpec().getPorts().get(0).getPort());
        assertEquals(new Integer(ZookeeperCluster.CLIENT_PORT), headful.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testGenerateHeadlessService() {
        Service headless = zc.generateHeadlessService();
        checkHeadlessService(headless);
    }

    private void checkHeadlessService(Service headless) {
        assertEquals(ZookeeperCluster.zookeeperHeadlessName(cluster), headless.getMetadata().getName());
        assertEquals("ClusterIP", headless.getSpec().getType());
        assertEquals("None", headless.getSpec().getClusterIP());
        assertEquals(labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_TYPE_LABEL, "kafka",
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(cluster)), headless.getSpec().getSelector());
        assertEquals(3, headless.getSpec().getPorts().size());
        assertEquals(ZookeeperCluster.CLIENT_PORT_NAME, headless.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(ZookeeperCluster.CLIENT_PORT), headless.getSpec().getPorts().get(0).getPort());
        assertEquals(ZookeeperCluster.CLUSTERING_PORT_NAME, headless.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(ZookeeperCluster.CLUSTERING_PORT), headless.getSpec().getPorts().get(1).getPort());
        assertEquals(ZookeeperCluster.LEADER_ELECTION_PORT_NAME, headless.getSpec().getPorts().get(2).getName());
        assertEquals(new Integer(ZookeeperCluster.LEADER_ELECTION_PORT), headless.getSpec().getPorts().get(2).getPort());
        assertEquals("TCP", headless.getSpec().getPorts().get(0).getProtocol());
    }

    @Test
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet ss = zc.generateStatefulSet(true);
        checkStatefulSet(ss);
    }

    private void checkStatefulSet(StatefulSet ss) {
        assertEquals(ZookeeperCluster.zookeeperClusterName(cluster), ss.getMetadata().getName());
        // ... in the same namespace ...
        assertEquals(namespace, ss.getMetadata().getNamespace());
        // ... with these labels
        assertEquals(labels("strimzi.io/cluster", cluster,
                "strimzi.io/type", "kafka",
                "my-user-label", "cromulent",
                "strimzi.io/name", ZookeeperCluster.zookeeperClusterName(cluster)),
                ss.getMetadata().getLabels());

        assertEquals(new Integer(replicas), ss.getSpec().getReplicas());
        assertEquals(image + "-zk", ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(new Integer(healthTimeout), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals("timeTick=2000\nautopurge.purgeInterval=1\nsyncLimit=2\ninitLimit=5\nfoo=bar\n", AbstractModel.containerEnvVars(ss.getSpec().getTemplate().getSpec().getContainers().get(0)).get(ZookeeperCluster.ENV_VAR_ZOOKEEPER_CONFIGURATION));
    }

    /**
     * Check that a ZookeeperCluster from a statefulset matches the one from a ConfigMap
     */
    @Test
    public void testClusterFromStatefulSet() {
        StatefulSet ss = zc.generateStatefulSet(true);
        ZookeeperCluster zc2 = ZookeeperCluster.fromAssembly(ss, namespace, cluster);
        // Don't check the metrics CM, since this isn't restored from the StatefulSet
        checkService(zc2.generateService());
        checkHeadlessService(zc2.generateHeadlessService());
        checkStatefulSet(zc2.generateStatefulSet(true));
    }

    // TODO test volume claim templates

    @Test
    public void testPodNames() {

        for (int i = 0; i < replicas; i++) {
            assertEquals(ZookeeperCluster.zookeeperPodName(cluster, i), zc.getPodName(i));
        }
    }

    @Test
    public void testPvcNames() {

        for (int i = 0; i < replicas; i++) {
            assertEquals(zc.VOLUME_NAME + "-" + ZookeeperCluster.zookeeperClusterName(cluster) + "-" + i, zc.getPersistentVolumeClaimName(i));
        }
    }

    @Test
    public void testCorruptedConfigMap() {
        try {
            ConfigMap cm = ResourceUtils.createKafkaClusterConfigMap(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, "{\"key.name\":oops}");
            ZookeeperCluster.fromConfigMap(cm);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("key.name", e.getKey());
        }
    }

    @Test
    public void withAffinity() throws IOException {
        resourceTester.assertDesiredResource("-SS.yaml", zc -> zc.generateStatefulSet(true).getSpec().getTemplate().getSpec().getAffinity());
    }

}
