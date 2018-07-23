/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTerm;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.KafkaAssemblyBuilder;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.Rack;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KafkaClusterTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final Map<String, Object> configuration = singletonMap("foo", "bar");
    private final InlineLogging kafkaLog = new InlineLogging();
    private final InlineLogging zooLog = new InlineLogging();
    {
        kafkaLog.setLoggers(Collections.singletonMap("kafka.root.logger.level", "OFF"));
        zooLog.setLoggers(Collections.singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final CertManager certManager = new MockCertManager();
    private final KafkaAssembly kafkaAssembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCm, configuration, kafkaLog, zooLog);
    private final KafkaCluster kc = KafkaCluster.fromCrd(certManager, kafkaAssembly, ResourceUtils.createKafkaClusterInitialSecrets(namespace, kafkaAssembly.getMetadata().getName()));

    @Rule
    public ResourceTester<KafkaAssembly, KafkaCluster> resourceTester = new ResourceTester<>(KafkaAssembly.class, KafkaCluster::fromCrd);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(TestUtils.toJsonString(this.metricsCm), metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    private Map<String, String> expectedLabels()    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, cluster, "my-user-label", "cromulent", Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster), Labels.STRIMZI_KIND_LABEL, KafkaAssembly.RESOURCE_KIND);
    }

    @Test
    public void testGenerateService() {
        Service headful = kc.generateService();
        checkService(headful);
    }

    private void checkService(Service headful) {
        assertEquals("ClusterIP", headful.getSpec().getType());
        assertEquals(expectedLabels(), headful.getSpec().getSelector());
        assertEquals(4, headful.getSpec().getPorts().size());
        assertEquals(KafkaCluster.CLIENT_PORT_NAME, headful.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_PORT), headful.getSpec().getPorts().get(0).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(0).getProtocol());
        assertEquals(kc.getPrometheusAnnotations(), headful.getMetadata().getAnnotations());
    }

    @Test
    public void testGenerateHeadlessService() {
        Service headless = kc.generateHeadlessService();
        checkHeadlessService(headless);
    }

    private void checkHeadlessService(Service headless) {
        assertEquals(KafkaCluster.headlessServiceName(cluster), headless.getMetadata().getName());
        assertEquals("ClusterIP", headless.getSpec().getType());
        assertEquals("None", headless.getSpec().getClusterIP());
        assertEquals(expectedLabels(), headless.getSpec().getSelector());
        assertEquals(3, headless.getSpec().getPorts().size());
        assertEquals(KafkaCluster.CLIENT_PORT_NAME, headless.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_PORT), headless.getSpec().getPorts().get(0).getPort());
        assertEquals("TCP", headless.getSpec().getPorts().get(0).getProtocol());
        assertEquals(KafkaCluster.REPLICATION_PORT_NAME, headless.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(KafkaCluster.REPLICATION_PORT), headless.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headless.getSpec().getPorts().get(1).getProtocol());
    }

    @Test
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet ss = kc.generateStatefulSet(true);
        checkStatefulSet(ss, kafkaAssembly, true);
    }

    @Test
    public void testGenerateStatefulSetWithSetStorageSelector() {
        Map<String, String> selector = TestUtils.map("foo", "bar");
        KafkaAssembly kafkaAssembly = new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorageStorage().withSelector(selector).endPersistentClaimStorageStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(certManager, kafkaAssembly, ResourceUtils.createKafkaClusterInitialSecrets(namespace, kafkaAssembly.getMetadata().getName()));
        StatefulSet ss = kc.generateStatefulSet(false);
        assertEquals(selector, ss.getSpec().getVolumeClaimTemplates().get(0).getSpec().getSelector().getMatchLabels());
    }

    @Test
    public void testGenerateStatefulSetWithEmptyStorageSelector() {
        KafkaAssembly kafkaAssembly = new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorageStorage().withSelector(emptyMap()).endPersistentClaimStorageStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(certManager, kafkaAssembly, ResourceUtils.createKafkaClusterInitialSecrets(namespace, kafkaAssembly.getMetadata().getName()));
        StatefulSet ss = kc.generateStatefulSet(false);
        assertEquals(null, ss.getSpec().getVolumeClaimTemplates().get(0).getSpec().getSelector());
    }

    @Test
    public void testGenerateStatefulSetWithRack() {
        KafkaAssembly kafkaAssembly = new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewRack().withTopologyKey("rack-key").endRack()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(certManager, kafkaAssembly, ResourceUtils.createKafkaClusterInitialSecrets(namespace, kafkaAssembly.getMetadata().getName()));
        StatefulSet ss = kc.generateStatefulSet(true);
        checkStatefulSet(ss, kafkaAssembly, true);
    }

    @Test
    public void testGenerateStatefulSetWithInitContainers() {
        KafkaAssembly kafkaAssembly =
                new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                        metricsCm, configuration, emptyMap()))
                        .editSpec()
                            .editKafka()
                                .withNewPersistentClaimStorageStorage().withSize("1Gi").endPersistentClaimStorageStorage()
                                .withNewRack().withTopologyKey("rack-key").endRack()
                            .endKafka()
                        .endSpec().build();
        KafkaCluster kc = KafkaCluster.fromCrd(certManager, kafkaAssembly, ResourceUtils.createKafkaClusterInitialSecrets(namespace, kafkaAssembly.getMetadata().getName()));
        StatefulSet ss = kc.generateStatefulSet(false);
        checkStatefulSet(ss, kafkaAssembly, false);
    }

    private void checkStatefulSet(StatefulSet ss, KafkaAssembly cm, boolean isOpenShift) {
        assertEquals(KafkaCluster.kafkaClusterName(cluster), ss.getMetadata().getName());
        // ... in the same namespace ...
        assertEquals(namespace, ss.getMetadata().getNamespace());
        // ... with these labels
        assertEquals(expectedLabels(), ss.getMetadata().getLabels());

        List<Container> containers = ss.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(2, containers.size());

        // checks on the main Kafka container
        assertEquals(new Integer(replicas), ss.getSpec().getReplicas());
        assertEquals(image, containers.get(0).getImage());
        assertEquals(new Integer(healthTimeout), containers.get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), containers.get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), containers.get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), containers.get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals("foo=bar\n", AbstractModel.containerEnvVars(containers.get(0)).get(KafkaCluster.ENV_VAR_KAFKA_CONFIGURATION));
        assertEquals(KafkaCluster.BROKER_CERTS_VOLUME, containers.get(0).getVolumeMounts().get(1).getName());
        assertEquals(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT, containers.get(0).getVolumeMounts().get(1).getMountPath());
        assertEquals(KafkaCluster.CLIENT_CA_CERTS_VOLUME, containers.get(0).getVolumeMounts().get(2).getName());
        assertEquals(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT, containers.get(0).getVolumeMounts().get(2).getMountPath());
        // checks on the TLS sidecar
        assertEquals(Kafka.DEFAULT_TLS_SIDECAR_IMAGE, containers.get(1).getImage());
        assertEquals(ZookeeperCluster.serviceName(cluster) + ":2181", AbstractModel.containerEnvVars(containers.get(1)).get(KafkaCluster.ENV_VAR_KAFKA_ZOOKEEPER_CONNECT));
        assertEquals(KafkaCluster.BROKER_CERTS_VOLUME, containers.get(1).getVolumeMounts().get(0).getName());
        assertEquals(KafkaCluster.TLS_SIDECAR_VOLUME_MOUNT, containers.get(1).getVolumeMounts().get(0).getMountPath());

        if (cm.getSpec().getKafka().getStorage() != null) {

            io.strimzi.api.kafka.model.Storage storage = cm.getSpec().getKafka().getStorage();

            if (storage instanceof PersistentClaimStorage && !isOpenShift) {

                PodSpec podSpec = ss.getSpec().getTemplate().getSpec();

                // check that pod spec contains the volume hack container for Kubernetes
                List<Container> initContainers = podSpec.getInitContainers();
                assertNotNull(initContainers);
                assertTrue(initContainers.size() > 0);

                boolean isVolumeHack =
                        initContainers.stream().anyMatch(container -> container.getName().equals(AbstractModel.VOLUME_MOUNT_HACK_NAME));
                assertTrue(isVolumeHack);
            }
        }

        if (cm.getSpec().getKafka().getRack() != null) {

            Rack rack = cm.getSpec().getKafka().getRack();

            // check that the pod spec contains anti-affinity rules with the right topology key
            PodSpec podSpec = ss.getSpec().getTemplate().getSpec();
            assertNotNull(podSpec.getAffinity());
            assertNotNull(podSpec.getAffinity().getPodAntiAffinity());
            assertNotNull(podSpec.getAffinity().getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution());
            List<WeightedPodAffinityTerm> terms = podSpec.getAffinity().getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution();
            assertNotNull(terms);
            assertTrue(terms.size() > 0);

            boolean isTopologyKey =
                    terms.stream().anyMatch(term -> term.getPodAffinityTerm().getTopologyKey().equals(rack.getTopologyKey()));
            assertTrue(isTopologyKey);

            // check that pod spec contains the init Kafka container
            List<Container> initContainers = podSpec.getInitContainers();
            assertNotNull(initContainers);
            assertTrue(initContainers.size() > 0);

            boolean isInitKafka =
                    initContainers.stream().anyMatch(container -> container.getName().equals(KafkaCluster.INIT_NAME));
            assertTrue(isInitKafka);
        }
    }

    @Test
    public void testDeleteClaim() {
        KafkaAssembly assembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap());
        KafkaCluster kc = KafkaCluster.fromCrd(certManager, assembly, ResourceUtils.createKafkaClusterInitialSecrets(namespace, assembly.getMetadata().getName()));
        StatefulSet ss = kc.generateStatefulSet(true);
        assertFalse(KafkaCluster.deleteClaim(ss));

        assembly = new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withStorage(new PersistentClaimStorageBuilder().withDeleteClaim(false).build())
                    .endKafka()
                .endSpec()
                .build();
        kc = KafkaCluster.fromCrd(certManager, assembly, ResourceUtils.createKafkaClusterInitialSecrets(namespace, assembly.getMetadata().getName()));
        ss = kc.generateStatefulSet(true);
        assertFalse(KafkaCluster.deleteClaim(ss));

        assembly = new KafkaAssemblyBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withStorage(new PersistentClaimStorageBuilder().withDeleteClaim(true).build())
                    .endKafka()
                .endSpec()
                .build();
        kc = KafkaCluster.fromCrd(certManager, assembly, ResourceUtils.createKafkaClusterInitialSecrets(namespace, assembly.getMetadata().getName()));
        ss = kc.generateStatefulSet(true);
        assertTrue(KafkaCluster.deleteClaim(ss));
    }

    // TODO test volume claim templates

    @Test
    public void testPodNames() {

        for (int i = 0; i < replicas; i++) {
            assertEquals(KafkaCluster.kafkaClusterName(cluster) + "-" + i, kc.getPodName(i));
        }
    }

    @Test
    public void testPvcNames() {

        for (int i = 0; i < replicas; i++) {
            assertEquals(kc.VOLUME_NAME + "-" + KafkaCluster.kafkaPodName(cluster, i), kc.getPersistentVolumeClaimName(i));
        }
    }

    @Test
    public void withAffinityWithoutRack() throws IOException {
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withRackWithoutAffinity() throws IOException {
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withRackAndAffinity() throws IOException {
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withTolerations() throws IOException {
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true).getSpec().getTemplate().getSpec().getTolerations());
    }

    @Test
    public void testCreateTcpSocketProbe()  {
        Probe probe = kc.createTcpSocketProbe(1234, 10, 20);

        assertEquals(new Integer(1234), probe.getTcpSocket().getPort().getIntVal());
        assertEquals(new Integer(10), probe.getInitialDelaySeconds());
        assertEquals(new Integer(20), probe.getTimeoutSeconds());
    }
}
