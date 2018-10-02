/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTerm;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static io.strimzi.test.TestUtils.LINE_SEPARATOR;
import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
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

    private final Kafka kafkaAssembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCm, configuration, kafkaLog, zooLog);
    private final KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly);

    @Rule
    public ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, KafkaCluster::fromCrd);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
        checkOwnerReference(kc.createOwnerReference(), metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(TestUtils.toJsonString(this.metricsCm), metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    private Map<String, String> expectedLabels()    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, cluster, "my-user-label", "cromulent", Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster), Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziLabels().toMap();
    }

    @Test
    public void testGenerateService() {
        Service headful = kc.generateService();
        checkService(headful);
        checkOwnerReference(kc.createOwnerReference(), headful);
    }

    private void checkService(Service headful) {
        assertEquals("ClusterIP", headful.getSpec().getType());
        assertEquals(expectedSelectorLabels(), headful.getSpec().getSelector());
        assertEquals(4, headful.getSpec().getPorts().size());
        assertEquals(KafkaCluster.REPLICATION_PORT_NAME, headful.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.REPLICATION_PORT), headful.getSpec().getPorts().get(0).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(0).getProtocol());
        assertEquals(KafkaCluster.CLIENT_PORT_NAME, headful.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_PORT), headful.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(1).getProtocol());
        assertEquals(KafkaCluster.CLIENT_TLS_PORT_NAME, headful.getSpec().getPorts().get(2).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_TLS_PORT), headful.getSpec().getPorts().get(2).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(2).getProtocol());
        assertEquals(AbstractModel.METRICS_PORT_NAME, headful.getSpec().getPorts().get(3).getName());
        assertEquals(new Integer(KafkaCluster.METRICS_PORT), headful.getSpec().getPorts().get(3).getPort());
        assertEquals("TCP", headful.getSpec().getPorts().get(2).getProtocol());
        assertEquals(kc.getPrometheusAnnotations(), headful.getMetadata().getAnnotations());
    }

    @Test
    public void testGenerateHeadlessService() {
        Service headless = kc.generateHeadlessService();
        checkHeadlessService(headless);
        checkOwnerReference(kc.createOwnerReference(), headless);
    }

    private void checkHeadlessService(Service headless) {
        assertEquals(KafkaCluster.headlessServiceName(cluster), headless.getMetadata().getName());
        assertEquals("ClusterIP", headless.getSpec().getType());
        assertEquals("None", headless.getSpec().getClusterIP());
        assertEquals(expectedSelectorLabels(), headless.getSpec().getSelector());
        assertEquals(3, headless.getSpec().getPorts().size());
        assertEquals(KafkaCluster.REPLICATION_PORT_NAME, headless.getSpec().getPorts().get(0).getName());
        assertEquals(new Integer(KafkaCluster.REPLICATION_PORT), headless.getSpec().getPorts().get(0).getPort());
        assertEquals("TCP", headless.getSpec().getPorts().get(0).getProtocol());
        assertEquals(KafkaCluster.CLIENT_PORT_NAME, headless.getSpec().getPorts().get(1).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_PORT), headless.getSpec().getPorts().get(1).getPort());
        assertEquals("TCP", headless.getSpec().getPorts().get(1).getProtocol());
        assertEquals(KafkaCluster.CLIENT_TLS_PORT_NAME, headless.getSpec().getPorts().get(2).getName());
        assertEquals(new Integer(KafkaCluster.CLIENT_TLS_PORT), headless.getSpec().getPorts().get(2).getPort());
        assertEquals("TCP", headless.getSpec().getPorts().get(2).getProtocol());
    }

    @Test
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet ss = kc.generateStatefulSet(true);
        checkStatefulSet(ss, kafkaAssembly, true);
        checkOwnerReference(kc.createOwnerReference(), ss);
    }

    @Test
    public void testGenerateStatefulSetWithSetStorageSelector() {
        Map<String, String> selector = TestUtils.map("foo", "bar");
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorageStorage().withSelector(selector).endPersistentClaimStorageStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly);
        StatefulSet ss = kc.generateStatefulSet(false);
        assertEquals(selector, ss.getSpec().getVolumeClaimTemplates().get(0).getSpec().getSelector().getMatchLabels());
    }

    @Test
    public void testGenerateStatefulSetWithEmptyStorageSelector() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorageStorage().withSelector(emptyMap()).endPersistentClaimStorageStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly);
        StatefulSet ss = kc.generateStatefulSet(false);
        assertEquals(null, ss.getSpec().getVolumeClaimTemplates().get(0).getSpec().getSelector());
    }

    @Test
    public void testGenerateStatefulSetWithRack() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewRack().withTopologyKey("rack-key").endRack()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly);
        StatefulSet ss = kc.generateStatefulSet(true);
        checkStatefulSet(ss, kafkaAssembly, true);
    }

    @Test
    public void testGenerateStatefulSetWithInitContainers() {
        Kafka kafkaAssembly =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout,
                        metricsCm, configuration, emptyMap()))
                        .editSpec()
                            .editKafka()
                                .withNewPersistentClaimStorageStorage().withSize("1Gi").endPersistentClaimStorageStorage()
                                .withNewRack().withTopologyKey("rack-key").endRack()
                            .endKafka()
                        .endSpec().build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly);
        StatefulSet ss = kc.generateStatefulSet(false);
        checkStatefulSet(ss, kafkaAssembly, false);
    }

    private void checkStatefulSet(StatefulSet ss, Kafka cm, boolean isOpenShift) {
        assertEquals(KafkaCluster.kafkaClusterName(cluster), ss.getMetadata().getName());
        // ... in the same namespace ...
        assertEquals(namespace, ss.getMetadata().getNamespace());
        // ... with these labels
        assertEquals(expectedLabels(), ss.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), ss.getSpec().getSelector().getMatchLabels());

        List<Container> containers = ss.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(2, containers.size());

        // checks on the main Kafka container
        assertEquals(new Integer(replicas), ss.getSpec().getReplicas());
        assertEquals(image, containers.get(0).getImage());
        assertEquals(new Integer(healthTimeout), containers.get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), containers.get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), containers.get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), containers.get(0).getReadinessProbe().getInitialDelaySeconds());
        assertEquals("foo=bar" + LINE_SEPARATOR, AbstractModel.containerEnvVars(containers.get(0)).get(KafkaCluster.ENV_VAR_KAFKA_CONFIGURATION));
        assertEquals(KafkaCluster.BROKER_CERTS_VOLUME, containers.get(0).getVolumeMounts().get(2).getName());
        assertEquals(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT, containers.get(0).getVolumeMounts().get(2).getMountPath());
        assertEquals(KafkaCluster.CLUSTER_CA_CERTS_VOLUME, containers.get(0).getVolumeMounts().get(1).getName());
        assertEquals(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT, containers.get(0).getVolumeMounts().get(1).getMountPath());
        assertEquals(KafkaCluster.CLIENT_CA_CERTS_VOLUME, containers.get(0).getVolumeMounts().get(3).getName());
        assertEquals(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT, containers.get(0).getVolumeMounts().get(3).getMountPath());
        // checks on the TLS sidecar
        assertEquals(KafkaClusterSpec.DEFAULT_TLS_SIDECAR_IMAGE, containers.get(1).getImage());
        assertEquals(ZookeeperCluster.serviceName(cluster) + ":2181", AbstractModel.containerEnvVars(containers.get(1)).get(KafkaCluster.ENV_VAR_KAFKA_ZOOKEEPER_CONNECT));
        assertEquals(TlsSidecarLogLevel.NOTICE.toValue(), AbstractModel.containerEnvVars(containers.get(1)).get(KafkaCluster.ENV_VAR_TLS_SIDECAR_LOG_LEVEL));
        assertEquals(KafkaCluster.BROKER_CERTS_VOLUME, containers.get(1).getVolumeMounts().get(0).getName());
        assertEquals(KafkaCluster.TLS_SIDECAR_KAFKA_CERTS_VOLUME_MOUNT, containers.get(1).getVolumeMounts().get(0).getMountPath());
        assertEquals(KafkaCluster.TLS_SIDECAR_CLUSTER_CA_CERTS_VOLUME_MOUNT, containers.get(1).getVolumeMounts().get(1).getMountPath());

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
        Kafka assembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap());
        KafkaCluster kc = KafkaCluster.fromCrd(assembly);
        StatefulSet ss = kc.generateStatefulSet(true);
        assertFalse(KafkaCluster.deleteClaim(ss));

        assembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withStorage(new PersistentClaimStorageBuilder().withDeleteClaim(false).build())
                    .endKafka()
                .endSpec()
                .build();
        kc = KafkaCluster.fromCrd(assembly);
        ss = kc.generateStatefulSet(true);
        assertFalse(KafkaCluster.deleteClaim(ss));

        assembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withStorage(new PersistentClaimStorageBuilder().withDeleteClaim(true).build())
                    .endKafka()
                .endSpec()
                .build();
        kc = KafkaCluster.fromCrd(assembly);
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

    @Test
    public void testExternalRoutes() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewListeners()
                    .withNewKafkaListenerExternalRouteExternal()
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                    .endKafkaListenerExternalRouteExternal()
                .endListeners()
                .endKafka()
                .endSpec()
                .build();

        ClusterCa clusterCa = ResourceUtils.createInitialClusterCa(namespace, cluster);

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly);

        SortedMap<Integer, String> addresses = new TreeMap<>();
        addresses.put(0, "my-address-0");
        addresses.put(1, "my-address-1");
        addresses.put(2, "my-address-2");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "route")));
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses.values()))));
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION, KafkaListenerAuthenticationTls.TYPE_TLS)));

        List<ContainerPort> ports = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts();
        assertTrue(ports.contains(kc.createContainerPort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, "TCP")));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapService();
        assertEquals(KafkaCluster.externalBootstrapServiceName(cluster), ext.getMetadata().getName());
        assertEquals("ClusterIP", ext.getSpec().getType());
        assertEquals(kc.getSelectorLabels(), ext.getSpec().getSelector());
        assertEquals(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP")), ext.getSpec().getPorts());
        checkOwnerReference(kc.createOwnerReference(), ext);

        // Check per pod services
        for (int i = 0; i < replicas; i++)  {
            Service srv = kc.generateExternalService(i);
            assertEquals(KafkaCluster.externalServiceName(cluster, i), srv.getMetadata().getName());
            assertEquals("ClusterIP", srv.getSpec().getType());
            assertEquals(KafkaCluster.kafkaPodName(cluster, i), srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL));
            assertEquals(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP")), srv.getSpec().getPorts());
            checkOwnerReference(kc.createOwnerReference(), srv);
        }

        // Check bootstrap route
        Route brt = kc.generateExternalBootstrapRoute();
        assertEquals(KafkaCluster.serviceName(cluster), brt.getMetadata().getName());
        assertEquals("passthrough", brt.getSpec().getTls().getTermination());
        assertEquals("Service", brt.getSpec().getTo().getKind());
        assertEquals(KafkaCluster.externalBootstrapServiceName(cluster), brt.getSpec().getTo().getName());
        assertEquals(new IntOrString(KafkaCluster.EXTERNAL_PORT), brt.getSpec().getPort().getTargetPort());
        checkOwnerReference(kc.createOwnerReference(), brt);

        // Check per pod router
        for (int i = 0; i < replicas; i++)  {
            Route rt = kc.generateExternalRoute(i);
            assertEquals(KafkaCluster.externalServiceName(cluster, i), rt.getMetadata().getName());
            assertEquals("passthrough", rt.getSpec().getTls().getTermination());
            assertEquals("Service", rt.getSpec().getTo().getKind());
            assertEquals(KafkaCluster.externalServiceName(cluster, i), rt.getSpec().getTo().getName());
            assertEquals(new IntOrString(KafkaCluster.EXTERNAL_PORT), rt.getSpec().getPort().getTargetPort());
            checkOwnerReference(kc.createOwnerReference(), rt);
        }
    }

    @Test
    public void testExternalLoadBalancers() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalLoadBalancerExternal()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerExternalLoadBalancerExternal()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        ClusterCa clusterCa = ResourceUtils.createInitialClusterCa(namespace, cluster);
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly);

        SortedMap<Integer, String> addresses = new TreeMap<>();
        addresses.put(0, "my-address-0");
        addresses.put(1, "my-address-1");
        addresses.put(2, "my-address-2");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "loadbalancer")));
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses.values()))));
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION, KafkaListenerAuthenticationTls.TYPE_TLS)));

        List<ContainerPort> ports = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts();
        assertTrue(ports.contains(kc.createContainerPort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, "TCP")));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapService();
        assertEquals(KafkaCluster.externalBootstrapServiceName(cluster), ext.getMetadata().getName());
        assertEquals("LoadBalancer", ext.getSpec().getType());
        assertEquals(kc.getSelectorLabels(), ext.getSpec().getSelector());
        assertEquals(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP")), ext.getSpec().getPorts());
        checkOwnerReference(kc.createOwnerReference(), ext);

        // Check per pod services
        for (int i = 0; i < replicas; i++)  {
            Service srv = kc.generateExternalService(i);
            assertEquals(KafkaCluster.externalServiceName(cluster, i), srv.getMetadata().getName());
            assertEquals("LoadBalancer", srv.getSpec().getType());
            assertEquals(KafkaCluster.kafkaPodName(cluster, i), srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL));
            assertEquals(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP")), srv.getSpec().getPorts());
            checkOwnerReference(kc.createOwnerReference(), srv);
        }
    }

    @Test
    public void testExternalNodePorts() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalNodePortExternal()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerExternalNodePortExternal()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        ClusterCa clusterCa = ResourceUtils.createInitialClusterCa(namespace, cluster);
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly);

        SortedMap<Integer, String> addresses = new TreeMap<>();
        addresses.put(0, "32123");
        addresses.put(1, "32456");
        addresses.put(2, "32789");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "nodeport")));
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses.values()))));
        assertTrue(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION, KafkaListenerAuthenticationTls.TYPE_TLS)));

        List<ContainerPort> ports = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts();
        assertTrue(ports.contains(kc.createContainerPort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, "TCP")));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapService();
        assertEquals(KafkaCluster.externalBootstrapServiceName(cluster), ext.getMetadata().getName());
        assertEquals("NodePort", ext.getSpec().getType());
        assertEquals(kc.getSelectorLabels(), ext.getSpec().getSelector());
        assertEquals(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP")), ext.getSpec().getPorts());
        checkOwnerReference(kc.createOwnerReference(), ext);

        // Check per pod services
        for (int i = 0; i < replicas; i++)  {
            Service srv = kc.generateExternalService(i);
            assertEquals(KafkaCluster.externalServiceName(cluster, i), srv.getMetadata().getName());
            assertEquals("NodePort", srv.getSpec().getType());
            assertEquals(KafkaCluster.kafkaPodName(cluster, i), srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL));
            assertEquals(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP")), srv.getSpec().getPorts());
            checkOwnerReference(kc.createOwnerReference(), srv);
        }
    }

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertEquals(1, resource.getMetadata().getOwnerReferences().size());
        assertEquals(ownerRef, resource.getMetadata().getOwnerReferences().get(0));
    }

    @Test
    public void testGenerateBrokerSecret() throws CertificateParsingException {
        ClusterCa clusterCa = new ClusterCa(new OpenSslCertManager(), cluster, null, null);
        clusterCa.createOrRenew(namespace, cluster, emptyMap(), null);

        ClientsCa clientsCa = new ClientsCa(new OpenSslCertManager(), KafkaCluster.getClusterCaKeyName(cluster), null, KafkaCluster.clientsCASecretName(cluster), null, 365, 30, true);

        kc.generateCertificates(kafkaAssembly, clusterCa, clientsCa, null, emptyMap());
        Secret secret = kc.generateBrokersSecret();
        assertEquals(set(
                "foo-kafka-0.crt",  "foo-kafka-0.key",
                "foo-kafka-1.crt", "foo-kafka-1.key",
                "foo-kafka-2.crt", "foo-kafka-2.key"),
                secret.getData().keySet());
        X509Certificate cert = Ca.cert(secret, "foo-kafka-0.crt");
        assertEquals("CN=foo-kafka, O=io.strimzi", cert.getSubjectDN().getName());
        assertEquals(set(
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc.cluster.local"),
                asList(2, "foo-kafka-bootstrap"),
                asList(2, "foo-kafka-bootstrap.test"),
                asList(2, "foo-kafka-bootstrap.test.svc.cluster.local")),
                new HashSet<Object>(cert.getSubjectAlternativeNames()));

    }
}
