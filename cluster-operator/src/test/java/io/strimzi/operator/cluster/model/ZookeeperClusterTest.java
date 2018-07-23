/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.KafkaAssemblyBuilder;
import io.strimzi.api.kafka.model.Zookeeper;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.assembly.MockCertManager;
import io.strimzi.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
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
    private final Map<String, Object> metricsCmJson = singletonMap("animal", "wombat");
    private final Map<String, Object> configurationJson = emptyMap();
    private final InlineLogging kafkaLogConfigJson = new InlineLogging();
    private final InlineLogging zooLogConfigJson = new InlineLogging();
    {
        kafkaLogConfigJson.setLoggers(Collections.singletonMap("kafka.root.logger.level", "OFF"));
        zooLogConfigJson.setLoggers(Collections.singletonMap("zookeeper.root.logger", "OFF"));
    }
    private final Map<String, Object> zooConfigurationJson = singletonMap("foo", "bar");

    private final CertManager certManager = new MockCertManager();
    private final KafkaAssembly ka = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson, null, null, kafkaLogConfigJson, zooLogConfigJson);
    private final ZookeeperCluster zc = ZookeeperCluster.fromCrd(certManager, ka, ResourceUtils.createKafkaClusterInitialSecrets(namespace, ka.getMetadata().getName()));

    @Rule
    public ResourceTester<KafkaAssembly, ZookeeperCluster> resourceTester = new ResourceTester<>(KafkaAssembly.class, ZookeeperCluster::fromCrd);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = zc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(TestUtils.toJsonString(metricsCmJson), metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    private Map<String, String> expectedLabels()    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, cluster, "my-user-label", "cromulent", Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(cluster), Labels.STRIMZI_KIND_LABEL, KafkaAssembly.RESOURCE_KIND);
    }

    @Test
    public void testGenerateService() {
        Service headful = zc.generateService();
        checkService(headful);
    }

    private void checkService(Service headful) {
        assertEquals("ClusterIP", headful.getSpec().getType());
        assertEquals(expectedLabels(), headful.getSpec().getSelector());
        assertEquals(2, headful.getSpec().getPorts().size());
        assertEquals(ZookeeperCluster.METRICS_PORT_NAME, headful.getSpec().getPorts().get(0).getName());
        assertEquals(ZookeeperCluster.CLIENT_PORT_NAME, headful.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(ZookeeperCluster.METRICS_PORT), headful.getSpec().getPorts().get(0).getPort());
        assertEquals(new Integer(ZookeeperCluster.CLIENT_PORT), headful.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(0).getProtocol());
        assertEquals(zc.getPrometheusAnnotations(), headful.getMetadata().getAnnotations());
    }

    @Test
    public void testGenerateHeadlessService() {
        Service headless = zc.generateHeadlessService();
        checkHeadlessService(headless);
    }

    private void checkHeadlessService(Service headless) {
        assertEquals(ZookeeperCluster.headlessServiceName(cluster), headless.getMetadata().getName());
        assertEquals("ClusterIP", headless.getSpec().getType());
        assertEquals("None", headless.getSpec().getClusterIP());
        assertEquals(expectedLabels(), headless.getSpec().getSelector());
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
        assertEquals(expectedLabels(), ss.getMetadata().getLabels());

        List<Container> containers = ss.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(2, containers.size());

        // checks on the main Zookeeper container
        assertEquals(new Integer(replicas), ss.getSpec().getReplicas());
        assertEquals(image + "-zk", containers.get(0).getImage());
        assertEquals(new Integer(healthTimeout), containers.get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), containers.get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), containers.get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), containers.get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals("timeTick=2000\nautopurge.purgeInterval=1\nsyncLimit=2\ninitLimit=5\nfoo=bar\n", AbstractModel.containerEnvVars(containers.get(0)).get(ZookeeperCluster.ENV_VAR_ZOOKEEPER_CONFIGURATION));
        // checks on the TLS sidecar container
        assertEquals(Zookeeper.DEFAULT_TLS_SIDECAR_IMAGE, containers.get(1).getImage());
        assertEquals(new Integer(replicas), Integer.valueOf(AbstractModel.containerEnvVars(containers.get(1)).get(ZookeeperCluster.ENV_VAR_ZOOKEEPER_NODE_COUNT)));
        assertEquals(ZookeeperCluster.CLUSTERING_PORT_NAME, containers.get(1).getPorts().get(0).getName());
        assertEquals(new Integer(ZookeeperCluster.CLUSTERING_PORT), containers.get(1).getPorts().get(0).getContainerPort());
        assertEquals(ZookeeperCluster.LEADER_ELECTION_PORT_NAME, containers.get(1).getPorts().get(1).getName());
        assertEquals(new Integer(ZookeeperCluster.LEADER_ELECTION_PORT), containers.get(1).getPorts().get(1).getContainerPort());
        assertEquals(ZookeeperCluster.CLIENT_PORT_NAME, containers.get(1).getPorts().get(2).getName());
        assertEquals(new Integer(ZookeeperCluster.CLIENT_PORT), containers.get(1).getPorts().get(2).getContainerPort());
        assertEquals(ZookeeperCluster.TLS_SIDECAR_VOLUME_NAME, containers.get(1).getVolumeMounts().get(0).getName());
        assertEquals(ZookeeperCluster.TLS_SIDECAR_VOLUME_MOUNT, containers.get(1).getVolumeMounts().get(0).getMountPath());
    }

    /**
     * Check that a ZookeeperCluster from a statefulset matches the one from a ConfigMap
     */
    @Test
    public void testDeleteClaim() {
        KafkaAssembly ka = new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson))
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorageStorage().endEphemeralStorageStorage()
                    .endKafka()
                .endSpec()
            .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(certManager, ka, ResourceUtils.createKafkaClusterInitialSecrets(namespace, ka.getMetadata().getName()));
        StatefulSet ss = zc.generateStatefulSet(true);
        assertFalse(ZookeeperCluster.deleteClaim(ss));

        ka = new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson))
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorageStorage().withDeleteClaim(false).endPersistentClaimStorageStorage()
                    .endKafka()
                .endSpec()
            .build();
        zc = ZookeeperCluster.fromCrd(certManager, ka, ResourceUtils.createKafkaClusterInitialSecrets(namespace, ka.getMetadata().getName()));
        ss = zc.generateStatefulSet(true);
        assertFalse(ZookeeperCluster.deleteClaim(ss));

        ka = new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson))
                .editSpec()
                    .editZookeeper()
                        .withNewPersistentClaimStorageStorage().withDeleteClaim(true).endPersistentClaimStorageStorage()
                    .endZookeeper()
                .endSpec()
            .build();
        zc = ZookeeperCluster.fromCrd(certManager, ka, ResourceUtils.createKafkaClusterInitialSecrets(namespace, ka.getMetadata().getName()));
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
