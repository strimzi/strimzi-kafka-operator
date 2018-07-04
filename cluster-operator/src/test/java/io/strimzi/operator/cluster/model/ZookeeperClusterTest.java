/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.assembly.MockCertManager;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static io.strimzi.operator.cluster.ResourceUtils.labels;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZookeeperClusterTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String configurationJson = "{}";
    private final InlineLogging kafkaLogConfigJson = new InlineLogging();
    private final InlineLogging zooLogConfigJson = new InlineLogging();
    {
        kafkaLogConfigJson.setLoggers(Collections.singletonMap("kafka.root.logger.level", "OFF"));
        zooLogConfigJson.setLoggers(Collections.singletonMap("zookeeper.root.logger", "OFF"));
    }
    private final String zooConfigurationJson = "{\"foo\":\"bar\"}";

    private final CertManager certManager = new MockCertManager();
    private final KafkaAssembly cm = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson, null, null, null, kafkaLogConfigJson, zooLogConfigJson);
    private final ZookeeperCluster zc = ZookeeperCluster.fromCrd(certManager, cm, ResourceUtils.createKafkaClusterInitialSecrets(namespace));

    @Rule
    public ResourceTester<KafkaAssembly, ZookeeperCluster> resourceTester = new ResourceTester<>(KafkaAssembly.class, ZookeeperCluster::fromCrd);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = zc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(metricsCmJson, metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    @Test
    public void testGenerateService() {
        Service headful = zc.generateService();
        checkService(headful);
    }

    private void checkService(Service headful) {
        assertEquals("ClusterIP", headful.getSpec().getType());
        assertEquals(labels(Labels.STRIMZI_CLUSTER_LABEL, cluster,
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
    public void testDeleteClaim() {
        KafkaAssembly cm = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson,
                "{\"type\": \"ephemeral\"}", null, null, null, null);
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(certManager, cm, ResourceUtils.createKafkaClusterInitialSecrets(namespace));
        StatefulSet ss = zc.generateStatefulSet(true);
        assertFalse(ZookeeperCluster.deleteClaim(ss));

        cm = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson,
                "{\"type\": \"persistent-claim\", \"deleteClaim\": false}", null, null, null, null);
        zc = ZookeeperCluster.fromCrd(certManager, cm, ResourceUtils.createKafkaClusterInitialSecrets(namespace));
        ss = zc.generateStatefulSet(true);
        assertFalse(ZookeeperCluster.deleteClaim(ss));

        cm = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson,
                "{\"type\": \"persistent-claim\", \"deleteClaim\": true}", null, null, null, null);
        zc = ZookeeperCluster.fromCrd(certManager, cm, ResourceUtils.createKafkaClusterInitialSecrets(namespace));
        ss = zc.generateStatefulSet(true);
        assertTrue(ZookeeperCluster.deleteClaim(ss));
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
    public void withAffinity() throws IOException {
        resourceTester.assertDesiredResource("-SS.yaml", zc -> zc.generateStatefulSet(true).getSpec().getTemplate().getSpec().getAffinity());
    }

}
