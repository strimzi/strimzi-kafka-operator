/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Lifecycle;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.RackBuilder;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarBuilder;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ZookeeperClusterTest {

    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef\n" +
                    "2.1.0         2.1 2.0 1234567890abcdef"),
            map("2.0.0", "strimzi/kafka:latest-kafka-2.0.0",
                    "2.1.0", "strimzi/kafka:latest-kafka-2.1.0"), emptyMap(), emptyMap(), emptyMap()) { };
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final int tlsHealthDelay = 120;
    private final int tlsHealthTimeout = 30;
    private final Map<String, Object> metricsCmJson = singletonMap("animal", "wombat");
    private final Map<String, Object> configurationJson = emptyMap();
    private final InlineLogging kafkaLogConfigJson = new InlineLogging();
    private final InlineLogging zooLogConfigJson = new InlineLogging();
    {
        kafkaLogConfigJson.setLoggers(Collections.singletonMap("kafka.root.logger.level", "OFF"));
        zooLogConfigJson.setLoggers(Collections.singletonMap("zookeeper.root.logger", "OFF"));
    }
    private final Map<String, Object> zooConfigurationJson = singletonMap("foo", "bar");

    private final TlsSidecar tlsSidecar = new TlsSidecarBuilder()
            .withLivenessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .withReadinessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .build();

    private final Kafka ka = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson, null, null, null, kafkaLogConfigJson, zooLogConfigJson))
            .editSpec()
                .editZookeeper()
                    .withTlsSidecar(tlsSidecar)
                .endZookeeper()
            .endSpec()
            .build();

    private final ZookeeperCluster zc = ZookeeperCluster.fromCrd(ka, VERSIONS);

    @Rule
    public ResourceTester<Kafka, ZookeeperCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, ZookeeperCluster::fromCrd);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = zc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
        checkOwnerReference(zc.createOwnerReference(), metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertEquals(TestUtils.toJsonString(metricsCmJson), metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS));
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, cluster, "my-user-label", "cromulent", Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(cluster), Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
    }

    @Test
    public void testGenerateService() {
        Service headful = zc.generateService();
        checkService(headful);
        checkOwnerReference(zc.createOwnerReference(), headful);
    }

    private void checkService(Service headful) {
        assertEquals("ClusterIP", headful.getSpec().getType());
        assertEquals(expectedSelectorLabels(), headful.getSpec().getSelector());
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
        checkOwnerReference(zc.createOwnerReference(), headless);
    }

    private void checkHeadlessService(Service headless) {
        assertEquals(ZookeeperCluster.headlessServiceName(cluster), headless.getMetadata().getName());
        assertEquals("ClusterIP", headless.getSpec().getType());
        assertEquals("None", headless.getSpec().getClusterIP());
        assertEquals(expectedSelectorLabels(), headless.getSpec().getSelector());
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
        StatefulSet ss = zc.generateStatefulSet(true, null);
        checkStatefulSet(ss);
        checkOwnerReference(zc.createOwnerReference(), ss);
    }

    private void checkStatefulSet(StatefulSet ss) {
        assertEquals(ZookeeperCluster.zookeeperClusterName(cluster), ss.getMetadata().getName());
        // ... in the same namespace ...
        assertEquals(namespace, ss.getMetadata().getNamespace());
        // ... with these labels
        assertEquals(expectedLabels(), ss.getMetadata().getLabels());
        assertEquals(expectedSelectorLabels(), ss.getSpec().getSelector().getMatchLabels());

        List<Container> containers = ss.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(2, containers.size());

        // checks on the main Zookeeper container
        assertEquals(new Integer(replicas), ss.getSpec().getReplicas());
        assertEquals(image + "-zk", containers.get(0).getImage());
        assertEquals(new Integer(healthTimeout), containers.get(0).getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), containers.get(0).getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(healthTimeout), containers.get(0).getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(healthDelay), containers.get(0).getReadinessProbe().getInitialDelaySeconds());
        OrderedProperties expectedConfig = new OrderedProperties()
                .addPair("timeTick", "2000")
                .addPair("autopurge.purgeInterval", "1")
                .addPair("syncLimit", "2")
                .addPair("initLimit", "5")
                .addPair("foo", "bar");
        OrderedProperties actual = new OrderedProperties()
                .addStringPairs(AbstractModel.containerEnvVars(containers.get(0)).get(ZookeeperCluster.ENV_VAR_ZOOKEEPER_CONFIGURATION));
        assertEquals(expectedConfig, actual);
        assertEquals(ZookeeperCluster.DEFAULT_KAFKA_GC_LOG_ENABLED, AbstractModel.containerEnvVars(containers.get(0)).get(ZookeeperCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED));
        // checks on the TLS sidecar container
        Container tlsSidecarContainer = containers.get(1);
        assertEquals(image, tlsSidecarContainer.getImage());
        assertEquals(new Integer(replicas), Integer.valueOf(AbstractModel.containerEnvVars(tlsSidecarContainer).get(ZookeeperCluster.ENV_VAR_ZOOKEEPER_NODE_COUNT)));
        assertEquals(TlsSidecarLogLevel.NOTICE.toValue(), AbstractModel.containerEnvVars(tlsSidecarContainer).get(ModelUtils.TLS_SIDECAR_LOG_LEVEL));
        assertEquals(ZookeeperCluster.CLUSTERING_PORT_NAME, tlsSidecarContainer.getPorts().get(0).getName());
        assertEquals(new Integer(ZookeeperCluster.CLUSTERING_PORT), tlsSidecarContainer.getPorts().get(0).getContainerPort());
        assertEquals(ZookeeperCluster.LEADER_ELECTION_PORT_NAME, tlsSidecarContainer.getPorts().get(1).getName());
        assertEquals(new Integer(ZookeeperCluster.LEADER_ELECTION_PORT), tlsSidecarContainer.getPorts().get(1).getContainerPort());
        assertEquals(ZookeeperCluster.CLIENT_PORT_NAME, tlsSidecarContainer.getPorts().get(2).getName());
        assertEquals(new Integer(ZookeeperCluster.CLIENT_PORT), tlsSidecarContainer.getPorts().get(2).getContainerPort());
        assertEquals(ZookeeperCluster.TLS_SIDECAR_NODES_VOLUME_NAME, tlsSidecarContainer.getVolumeMounts().get(0).getName());
        assertEquals(ZookeeperCluster.TLS_SIDECAR_NODES_VOLUME_MOUNT, tlsSidecarContainer.getVolumeMounts().get(0).getMountPath());
        assertEquals(ZookeeperCluster.TLS_SIDECAR_CLUSTER_CA_VOLUME_NAME, tlsSidecarContainer.getVolumeMounts().get(1).getName());
        assertEquals(ZookeeperCluster.TLS_SIDECAR_CLUSTER_CA_VOLUME_MOUNT, tlsSidecarContainer.getVolumeMounts().get(1).getMountPath());
        assertEquals(new Integer(tlsHealthDelay), tlsSidecarContainer.getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(tlsHealthTimeout), tlsSidecarContainer.getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(tlsHealthDelay), tlsSidecarContainer.getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(tlsHealthTimeout), tlsSidecarContainer.getLivenessProbe().getTimeoutSeconds());
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
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCmJson, configurationJson, zooConfigurationJson))
                .editSpec()
                    .editZookeeper()
                        .withNewPersistentClaimStorage().withDeleteClaim(false).withSize("100Gi").endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(ka, VERSIONS);

        PersistentVolumeClaim pvc = zc.getVolumeClaims().get(0);

        for (int i = 0; i < replicas; i++) {
            assertEquals(zc.VOLUME_NAME + "-" + ZookeeperCluster.zookeeperPodName(cluster, i),
                    pvc.getMetadata().getName() + "-" + ZookeeperCluster.zookeeperPodName(cluster, i));
        }
    }

    @Test
    public void withAffinity() throws IOException {
        resourceTester.assertDesiredResource("-SS.yaml", zc -> zc.generateStatefulSet(true, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertEquals(1, resource.getMetadata().getOwnerReferences().size());
        assertEquals(ownerRef, resource.getMetadata().getOwnerReferences().get(0));
    }

    @Test
    public void testGenerateBrokerSecret() throws CertificateParsingException {
        ClusterCa clusterCa = new ClusterCa(new OpenSslCertManager(), cluster, null, null);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), null);

        Secret secret = zc.generateNodesSecret(clusterCa, ka);
        assertEquals(set(
                "foo-zookeeper-0.crt",  "foo-zookeeper-0.key",
                "foo-zookeeper-1.crt", "foo-zookeeper-1.key",
                "foo-zookeeper-2.crt", "foo-zookeeper-2.key"),
                secret.getData().keySet());
        X509Certificate cert = Ca.cert(secret, "foo-zookeeper-0.crt");
        assertEquals("CN=foo-zookeeper, O=io.strimzi", cert.getSubjectDN().getName());
        assertEquals(set(
                asList(2, "foo-zookeeper-0.foo-zookeeper-nodes.test.svc.cluster.local"),
                asList(2, "foo-zookeeper-client"),
                asList(2, "foo-zookeeper-client.test"),
                asList(2, "foo-zookeeper-client.test.svc"),
                asList(2, "foo-zookeeper-client.test.svc.cluster.local")),
                new HashSet<Object>(cert.getSubjectAlternativeNames()));

    }

    @Test
    public void testTemplate() {
        Map<String, String> ssLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> ssAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> hSvcLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> hSvcAnots = TestUtils.map("a7", "v7", "a8", "v8");

        Map<String, String> pdbLabels = TestUtils.map("l9", "v9", "l10", "v10");
        Map<String, String> pdbAnots = TestUtils.map("a9", "v9", "a10", "v10");

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewStatefulset()
                                .withNewMetadata()
                                    .withLabels(ssLabels)
                                    .withAnnotations(ssAnots)
                                .endMetadata()
                            .endStatefulset()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnots)
                                .endMetadata()
                            .endPod()
                            .withNewClientService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnots)
                                .endMetadata()
                            .endClientService()
                            .withNewNodesService()
                                .withNewMetadata()
                                    .withLabels(hSvcLabels)
                                    .withAnnotations(hSvcAnots)
                                .endMetadata()
                            .endNodesService()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withLabels(pdbLabels)
                                    .withAnnotations(pdbAnots)
                                .endMetadata()
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check StatefulSet
        StatefulSet ss = zc.generateStatefulSet(true, null);
        assertTrue(ss.getMetadata().getLabels().entrySet().containsAll(ssLabels.entrySet()));
        assertTrue(ss.getMetadata().getAnnotations().entrySet().containsAll(ssAnots.entrySet()));

        // Check Pods
        assertTrue(ss.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()));
        assertTrue(ss.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()));

        // Check Service
        Service svc = zc.generateService();
        assertTrue(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()));
        assertTrue(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()));

        // Check Headless Service
        svc = zc.generateHeadlessService();
        assertTrue(svc.getMetadata().getLabels().entrySet().containsAll(hSvcLabels.entrySet()));
        assertTrue(svc.getMetadata().getAnnotations().entrySet().containsAll(hSvcAnots.entrySet()));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget();
        assertTrue(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()));
        assertTrue(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()));
    }

    @Test
    public void testGracePeriod() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withTerminationGracePeriodSeconds(123)
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = zc.generateStatefulSet(true, null);
        assertEquals(Long.valueOf(123), ss.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
        Lifecycle lifecycle = ss.getSpec().getTemplate().getSpec().getContainers().get(1).getLifecycle();
        assertNotNull(lifecycle);
        assertTrue(lifecycle.getPreStop().getExec().getCommand().contains("/opt/stunnel/zookeeper_stunnel_pre_stop.sh"));
        assertTrue(lifecycle.getPreStop().getExec().getCommand().contains("123"));
    }

    @Test
    public void testDefaultGracePeriod() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = zc.generateStatefulSet(true, null);
        assertEquals(Long.valueOf(30), ss.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
        Lifecycle lifecycle = ss.getSpec().getTemplate().getSpec().getContainers().get(1).getLifecycle();
        assertNotNull(lifecycle);
        assertTrue(lifecycle.getPreStop().getExec().getCommand().contains("/opt/stunnel/zookeeper_stunnel_pre_stop.sh"));
        assertTrue(lifecycle.getPreStop().getExec().getCommand().contains("30"));
    }

    @Test
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret1, secret2)
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = zc.generateStatefulSet(true, null);
        assertEquals(2, ss.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testDefaultImagePullSecrets() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = zc.generateStatefulSet(true, null);
        assertEquals(0, ss.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
    }

    /**
     * Verify the lookup order is:<ul>
     * <li>Kafka.spec.zookeeper.tlsSidecar.image</li>
     * <li>Kafka.spec.kafka.image</li>
     * <li>image for default version of Kafka</li></ul>
     */
    @Test
    public void testStunnelImage() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap());

        Kafka kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editZookeeper()
                        .editOrNewTlsSidecar()
                            .withImage("foo1")
                        .endTlsSidecar()
                    .endZookeeper()
                    .editKafka()
                        .withImage("foo2")
                    .endKafka()
                .endSpec()
            .build();
        assertEquals("foo1", ZookeeperCluster.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(1).getImage());

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editZookeeper()
                        .withImage("bar")
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endZookeeper()
                    .editKafka()
                        .withImage("foo2")
                    .endKafka()
                .endSpec()
            .build();
        assertEquals("foo2", ZookeeperCluster.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(1).getImage());

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editZookeeper()
                        .withImage("bar")
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endZookeeper()
                    .editKafka()
                        .withVersion("2.0.0")
                        .withImage(null)
                    .endKafka()
                .endSpec()
            .build();
        assertEquals("strimzi/kafka:latest-kafka-2.0.0", ZookeeperCluster.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(1).getImage());

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editZookeeper()
                        .withImage("bar")
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endZookeeper()
                    .editKafka()
                        .withVersion("2.1.0")
                        .withImage(null)
                    .endKafka()
                .endSpec()
            .build();
        assertEquals("strimzi/kafka:latest-kafka-2.0.0", ZookeeperCluster.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(1).getImage());
    }

    @Test
    public void testSecurityContext() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withNewRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = zc.generateStatefulSet(true, null);
        assertNotNull(ss.getSpec().getTemplate().getSpec().getSecurityContext());
        assertEquals(Long.valueOf(123), ss.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup());
        assertEquals(Long.valueOf(456), ss.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup());
        assertEquals(Long.valueOf(789), ss.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser());
    }

    @Test
    public void testDefaultSecurityContext() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = zc.generateStatefulSet(true, null);
        assertNull(ss.getSpec().getTemplate().getSpec().getSecurityContext());
    }

    @Test
    public void testPodDisruptionBudget() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPodDisruptionBudget()
                                .withMaxUnavailable(2)
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget();
        assertEquals(new IntOrString(2), pdb.getSpec().getMaxUnavailable());
    }

    @Test
    public void testDefaultPodDisruptionBudget() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap()))
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget();
        assertEquals(new IntOrString(1), pdb.getSpec().getMaxUnavailable());
    }

    @Test
    public void testImagePullPolicy() {
        Kafka kafkaAssembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap());
        kafkaAssembly.getSpec().getKafka().setRack(new RackBuilder().withTopologyKey("topology-key").build());
        ZookeeperCluster kc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, ImagePullPolicy.ALWAYS);
        assertEquals(ImagePullPolicy.ALWAYS.toString(), sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
        assertEquals(ImagePullPolicy.ALWAYS.toString(), sts.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy());

        sts = zc.generateStatefulSet(true, ImagePullPolicy.IFNOTPRESENT);
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), sts.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy());
    }

    @Test
    public void testNetworkPolicy() {
        Kafka kafkaAssembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCmJson, configurationJson, emptyMap());
        kafkaAssembly.getSpec().getKafka().setRack(new RackBuilder().withTopologyKey("topology-key").build());
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Network Policies
        NetworkPolicy np = zc.generateNetworkPolicy(true);

        LabelSelector podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(zc.getCluster())));
        assertEquals(podSelector, np.getSpec().getPodSelector());

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress();
        assertEquals(2, rules.size());

        NetworkPolicyIngressRule metricsRule = rules.get(1);
        assertEquals(1, metricsRule.getPorts().size());
        assertEquals(new IntOrString(9404), metricsRule.getPorts().get(0).getPort());
        assertEquals(0, metricsRule.getFrom().size());

        NetworkPolicyIngressRule zooRule = rules.get(0);
        assertEquals(3, zooRule.getPorts().size());
        assertEquals(new IntOrString(2181), zooRule.getPorts().get(0).getPort());
        assertEquals(new IntOrString(2888), zooRule.getPorts().get(1).getPort());
        assertEquals(new IntOrString(3888), zooRule.getPorts().get(2).getPort());

        assertEquals(4, zooRule.getFrom().size());

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(zc.getCluster())));
        assertEquals(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build(), zooRule.getFrom().get(0));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(zc.getCluster())));
        assertEquals(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build(), zooRule.getFrom().get(1));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(zc.getCluster())));
        assertEquals(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build(), zooRule.getFrom().get(2));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        assertEquals(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).withNamespaceSelector(new LabelSelector()).build(), zooRule.getFrom().get(3));

        // Check NetworkPolicy for older OCP versions
        np = zc.generateNetworkPolicy(false);
        rules = np.getSpec().getIngress();
        zooRule = rules.get(0);

        assertEquals(4, zooRule.getFrom().size());

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(zc.getCluster())));
        assertEquals(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build(), zooRule.getFrom().get(0));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(zc.getCluster())));
        assertEquals(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build(), zooRule.getFrom().get(1));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(zc.getCluster())));
        assertEquals(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build(), zooRule.getFrom().get(2));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        assertEquals(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build(), zooRule.getFrom().get(3));
    }
}
