/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.io.IOException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.AbstractModel.JMX_PORT;
import static io.strimzi.operator.cluster.model.AbstractModel.JMX_PORT_NAME;
import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
@ParallelSuite
public class ZookeeperClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final String NAMESPACE = "test";
    private static final String CLUSTER = "foo";
    private static final int REPLICAS = 3;
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewZookeeper()
                    .withReplicas(REPLICAS)
                    .withNewPersistentClaimStorage()
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                    .withConfig(Map.of("foo", "bar"))
                .endZookeeper()
                .withNewKafka()
                    .withReplicas(REPLICAS)
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build())
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build())
                    .endJbodStorage()
                .endKafka()
            .endSpec()
            .build();
    private final static ZookeeperCluster ZC = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, VERSIONS);

    //////////
    // Utility methods
    //////////

    private Map<String, String> expectedSelectorLabels()    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
                Labels.STRIMZI_NAME_LABEL, KafkaResources.zookeeperStatefulSetName(CLUSTER),
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
    }

    private void checkHeadlessService(Service headless) {
        assertThat(headless.getMetadata().getName(), is(KafkaResources.zookeeperHeadlessServiceName(CLUSTER)));
        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getClusterIP(), is("None"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(3));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(ZookeeperCluster.CLIENT_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(ZookeeperCluster.CLIENT_TLS_PORT));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(ZookeeperCluster.CLUSTERING_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(ZookeeperCluster.CLUSTERING_PORT));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(ZookeeperCluster.LEADER_ELECTION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(ZookeeperCluster.LEADER_ELECTION_PORT));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(headless.getSpec().getIpFamilies(), is(nullValue()));
    }

    private Secret generateCertificatesSecret() {
        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), CLUSTER, null, null);
        clusterCa.createRenewOrReplace(NAMESPACE, CLUSTER, emptyMap(), emptyMap(), emptyMap(), null, true);

        return ZC.generateCertificatesSecret(clusterCa, true);
    }

    //////////
    // Tests
    //////////

    @ParallelTest
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm("{\"animal\":\"wombat\"}", "zoo-metrics-config", "zoo-metrics-config.yml");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("zoo-metrics-config.yml", "zoo-metrics-config", false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ConfigMap brokerCm = zc.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCm, null));
        TestUtils.checkOwnerReference(brokerCm, KAFKA);
        assertThat(brokerCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS), is("{\"animal\":\"wombat\"}"));
    }

    @ParallelTest
    public void testGenerateService() {
        Service svc = ZC.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(ZookeeperCluster.CLIENT_TLS_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(ZookeeperCluster.CLIENT_TLS_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of()));

        TestUtils.checkOwnerReference(svc, KAFKA);
    }

    @ParallelTest
    public void testGenerateServiceWithoutMetrics() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withMetricsConfig(null)
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        Service svc = zc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(ZookeeperCluster.CLIENT_TLS_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(ZookeeperCluster.CLIENT_TLS_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        assertThat(svc.getMetadata().getAnnotations(), is(Map.of()));

        TestUtils.checkOwnerReference(svc, KAFKA);
    }

    @ParallelTest
    public void testGenerateHeadlessService() {
        Service headless = ZC.generateHeadlessService();
        checkHeadlessService(headless);
        TestUtils.checkOwnerReference(headless, KAFKA);
    }

    @ParallelTest
    public void testGenerateHeadlessServiceWithJmxMetrics() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().build())
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        Service headless = zc.generateHeadlessService();

        assertThat(headless.getMetadata().getName(), is(KafkaResources.zookeeperHeadlessServiceName(CLUSTER)));
        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getClusterIP(), is("None"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(4));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(ZookeeperCluster.CLIENT_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(ZookeeperCluster.CLIENT_TLS_PORT));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(ZookeeperCluster.CLUSTERING_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(ZookeeperCluster.CLUSTERING_PORT));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(ZookeeperCluster.LEADER_ELECTION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(ZookeeperCluster.LEADER_ELECTION_PORT));
        assertThat(headless.getSpec().getPorts().get(3).getName(), is(ZookeeperCluster.JMX_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(3).getPort(), is(ZookeeperCluster.JMX_PORT));
        assertThat(headless.getSpec().getPorts().get(3).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(headless.getSpec().getIpFamilies(), is(nullValue()));

        TestUtils.checkOwnerReference(headless, KAFKA);
    }

    @ParallelTest
    public void testExposesJmxContainerPortWhenJmxEnabled() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().build())
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ContainerPort jmxContainerPort = ContainerUtils.createContainerPort(JMX_PORT_NAME, JMX_PORT);
        assertThat(zc.createContainer(ImagePullPolicy.IFNOTPRESENT).getPorts().contains(jmxContainerPort), is(true));
    }

    @ParallelTest
    public void testCreateClusterWithZookeeperJmxEnabled() {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withJmxOptions(new KafkaJmxOptionsBuilder()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder()
                                .build())
                            .build())
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, KafkaVersionTestUtils.getKafkaVersionLookup());
        Secret jmxSecret = zookeeperCluster.generateJmxSecret(null);

        assertThat(jmxSecret.getData(), hasKey("jmx-username"));
        assertThat(jmxSecret.getData(), hasKey("jmx-password"));

        Secret newJmxSecret = zookeeperCluster.generateJmxSecret(jmxSecret);

        assertThat(newJmxSecret.getData(), hasKey("jmx-username"));
        assertThat(newJmxSecret.getData(), hasKey("jmx-password"));
        assertThat(newJmxSecret.getData().get("jmx-username"), is(jmxSecret.getData().get("jmx-username")));
        assertThat(newJmxSecret.getData().get("jmx-password"), is(jmxSecret.getData().get("jmx-password")));
    }

    @ParallelTest
    public void testJmxSecretCustomLabelsAndAnnotations() {
        Map<String, String> customLabels = new HashMap<>(2);
        customLabels.put("label1", "value1");
        customLabels.put("label2", "value2");

        Map<String, String> customAnnotations = new HashMap<>(2);
        customAnnotations.put("anno1", "value3");
        customAnnotations.put("anno2", "value4");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withJmxOptions(new KafkaJmxOptionsBuilder()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder()
                                .build())
                            .build())
                        .withNewTemplate()
                            .withNewJmxSecret()
                                .withNewMetadata()
                                    .withAnnotations(customAnnotations)
                                    .withLabels(customLabels)
                                .endMetadata()
                            .endJmxSecret()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        Secret jmxSecret = zookeeperCluster.generateJmxSecret(null);

        for (Map.Entry<String, String> entry : customAnnotations.entrySet()) {
            assertThat(jmxSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry : customLabels.entrySet()) {
            assertThat(jmxSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    @ParallelTest
    public void testInvalidVersion() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka ka = new KafkaBuilder(KAFKA)
                    .editSpec()
                        .editKafka()
                            .withVersion("10000.0.0")
                        .endKafka()
                    .endSpec()
                    .build();

            ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);
        });
    }

    @ParallelTest
    public void testPodNames() {
        for (int i = 0; i < REPLICAS; i++) {
            assertThat(ZC.getPodName(i), is(KafkaResources.zookeeperPodName(CLUSTER, i)));
        }
    }

    @ParallelTest
    public void testPvcNames() {
        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewPersistentClaimStorage().withDeleteClaim(false).withSize("100Gi").endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        PersistentVolumeClaim pvc = zc.getPersistentVolumeClaimTemplates().get(0);

        for (int i = 0; i < REPLICAS; i++) {
            assertThat(pvc.getMetadata().getName() + "-" + KafkaResources.zookeeperPodName(CLUSTER, i),
                    is(ZookeeperCluster.VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(CLUSTER, i)));
        }
    }

    @ParallelTest
    public void withAffinity() throws IOException {
        ResourceTester<Kafka, ZookeeperCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly, versions) -> ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, versions), this.getClass().getSimpleName() + ".withAffinity");
        resourceTester.assertDesiredResource(".yaml", cr -> cr.getSpec().getZookeeper().getTemplate().getPod().getAffinity());
    }

    @ParallelTest
    public void withTolerations() throws IOException {
        ResourceTester<Kafka, ZookeeperCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly, versions) -> ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, versions), this.getClass().getSimpleName() + ".withTolerations");
        resourceTester.assertDesiredResource(".yaml", cr -> cr.getSpec().getZookeeper().getTemplate().getPod().getTolerations());
    }

    @ParallelTest
    public void testGenerateBrokerSecret() throws CertificateParsingException {
        Secret secret = generateCertificatesSecret();
        assertThat(secret.getData().keySet(), is(set(
                "foo-zookeeper-0.crt",  "foo-zookeeper-0.key", "foo-zookeeper-0.p12", "foo-zookeeper-0.password",
                "foo-zookeeper-1.crt", "foo-zookeeper-1.key", "foo-zookeeper-1.p12", "foo-zookeeper-1.password",
                "foo-zookeeper-2.crt", "foo-zookeeper-2.key", "foo-zookeeper-2.p12", "foo-zookeeper-2.password")));
        X509Certificate cert = Ca.cert(secret, "foo-zookeeper-0.crt");
        assertThat(cert.getSubjectDN().getName(), is("CN=foo-zookeeper, O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(set(
                asList(2, "foo-zookeeper-0.foo-zookeeper-nodes.test.svc"),
                asList(2, "foo-zookeeper-0.foo-zookeeper-nodes.test.svc.cluster.local"),
                asList(2, "foo-zookeeper-client"),
                asList(2, "foo-zookeeper-client.test"),
                asList(2, "foo-zookeeper-client.test.svc"),
                asList(2, "foo-zookeeper-client.test.svc.cluster.local"),
                asList(2, "*.foo-zookeeper-client.test.svc"),
                asList(2, "*.foo-zookeeper-client.test.svc.cluster.local"),
                asList(2, "*.foo-zookeeper-nodes.test.svc"),
                asList(2, "*.foo-zookeeper-nodes.test.svc.cluster.local"))));
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnnotations = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> hSvcLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> hSvcAnnotations = TestUtils.map("a7", "v7", "a8", "v8");

        Map<String, String> pdbLabels = TestUtils.map("l9", "v9", "l10", "v10");
        Map<String, String> pdbAnnotations = TestUtils.map("a9", "v9", "a10", "v10");

        Map<String, String> saLabels = TestUtils.map("l11", "v11", "l12", "v12");
        Map<String, String> saAnnotations = TestUtils.map("a11", "v11", "a12", "v12");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewClientService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                            .endClientService()
                            .withNewNodesService()
                                .withNewMetadata()
                                    .withLabels(hSvcLabels)
                                    .withAnnotations(hSvcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.SINGLE_STACK)
                                .withIpFamilies(IpFamily.IPV6)
                            .endNodesService()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withLabels(pdbLabels)
                                    .withAnnotations(pdbAnnotations)
                                .endMetadata()
                            .endPodDisruptionBudget()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnnotations)
                                .endMetadata()
                            .endServiceAccount()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check Service
        Service svc = zc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check Headless Service
        svc = zc.generateHeadlessService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(hSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(hSvcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("SingleStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget(false);
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));

        // Check PodDisruptionBudgetV1Beta1
        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdbV1Beta1 = zc.generatePodDisruptionBudgetV1Beta1(false);
        assertThat(pdbV1Beta1.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdbV1Beta1.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = zc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnotations.entrySet()), is(true));
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
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
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget(false);
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));

        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdbV1Beta1 = zc.generatePodDisruptionBudgetV1Beta1(false);
        assertThat(pdbV1Beta1.getSpec().getMaxUnavailable(), is(new IntOrString(2)));        
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget(false);
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @ParallelTest
    public void testNetworkPolicyNewKubernetesVersions() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("zoo-metrics-config.yml", "zoo-metrics-config", false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endZookeeper()
                    .editKafka()
                        .withNewRack().withTopologyKey("rack-key").endRack()
                    .endKafka()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check Network Policies => Other namespace
        NetworkPolicy np = zc.generateNetworkPolicy("operator-namespace", null);

        LabelSelector podSelector = new LabelSelector();
        podSelector.setMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.zookeeperStatefulSetName(CLUSTER)));
        assertThat(np.getSpec().getPodSelector(), is(podSelector));

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress();
        assertThat(rules.size(), is(4));

        // Ports 2888
        NetworkPolicyIngressRule zooRule = rules.get(0);
        assertThat(zooRule.getPorts().size(), is(1));
        assertThat(zooRule.getPorts().get(0).getPort(), is(new IntOrString(2888)));

        assertThat(zooRule.getFrom().size(), is(1));
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.zookeeperStatefulSetName(CLUSTER)));
        assertThat(zooRule.getFrom().get(0), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        // Ports 3888
        NetworkPolicyIngressRule zooRule2 = rules.get(1);
        assertThat(zooRule2.getPorts().size(), is(1));
        assertThat(zooRule2.getPorts().get(0).getPort(), is(new IntOrString(3888)));

        assertThat(zooRule2.getFrom().size(), is(1));
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.zookeeperStatefulSetName(CLUSTER)));
        assertThat(zooRule2.getFrom().get(0), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        // Port 2181
        NetworkPolicyIngressRule clientsRule = rules.get(2);
        assertThat(clientsRule.getPorts().size(), is(1));
        assertThat(clientsRule.getPorts().get(0).getPort(), is(new IntOrString(ZookeeperCluster.CLIENT_TLS_PORT)));

        assertThat(clientsRule.getFrom().size(), is(4));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaStatefulSetName(zc.getCluster())));
        assertThat(clientsRule.getFrom().get(0), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.zookeeperStatefulSetName(CLUSTER)));
        assertThat(clientsRule.getFrom().get(1), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(zc.getCluster())));
        assertThat(clientsRule.getFrom().get(2), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        assertThat(clientsRule.getFrom().get(3), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).withNamespaceSelector(new LabelSelector()).build()));

        // Port 9404
        NetworkPolicyIngressRule metricsRule = rules.get(3);
        assertThat(metricsRule.getPorts().size(), is(1));
        assertThat(metricsRule.getPorts().get(0).getPort(), is(new IntOrString(9404)));
        assertThat(metricsRule.getFrom().size(), is(0));

        // Check Network Policies => The same namespace
        np = zc.generateNetworkPolicy(NAMESPACE, null);
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        assertThat(np.getSpec().getIngress().get(2).getFrom().get(3), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        // Check Network Policies => The same namespace with namespace labels
        np = zc.generateNetworkPolicy(NAMESPACE, Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        assertThat(np.getSpec().getIngress().get(2).getFrom().get(3), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        // Check Network Policies => Other namespace with namespace labels
        np = zc.generateNetworkPolicy("operator-namespace", Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        LabelSelector namespaceSelector = new LabelSelector();
        namespaceSelector.setMatchLabels(Collections.singletonMap("nsLabelKey", "nsLabelValue"));
        assertThat(np.getSpec().getIngress().get(2).getFrom().get(3), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).withNamespaceSelector(namespaceSelector).build()));
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithClaimDeletion() {
        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewPersistentClaimStorage().withStorageClass("gp2-ssd").withDeleteClaim(true).withSize("100Gi").endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(ZookeeperCluster.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithoutClaimDeletion() {
        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewPersistentClaimStorage().withStorageClass("gp2-ssd").withDeleteClaim(false).withSize("100Gi").endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(ZookeeperCluster.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithOverride() {
        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withStorageClass("gp2-ssd")
                            .withDeleteClaim(false)
                            .withSize("100Gi")
                            .withOverrides(new PersistentClaimStorageOverrideBuilder()
                                    .withBroker(1)
                                    .withStorageClass("gp2-ssd-az1")
                                    .build())
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);

            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));

            if (i != 1) {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            } else {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd-az1"));
            }

            assertThat(pvc.getMetadata().getName().startsWith(ZookeeperCluster.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsWithTemplate() {
        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPersistentVolumeClaim()
                                .withNewMetadata()
                                    .withLabels(singletonMap("testLabel", "testValue"))
                                    .withAnnotations(singletonMap("testAnno", "testValue"))
                                .endMetadata()
                            .endPersistentVolumeClaim()
                        .endTemplate()
                        .withStorage(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                        .withDeleteClaim(false)
                                        .withId(0)
                                        .withSize("100Gi")
                                        .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(1).withStorageClass("gp2-ssd-az1").build())
                                        .build())
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getMetadata().getLabels().get("testLabel"), is("testValue"));
            assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is("testValue"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsEphemeral()    {
        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(0));
    }

    @ParallelTest
    public void testStorageReverting() {
        SingleVolumeStorage ephemeral = new EphemeralStorageBuilder().build();
        SingleVolumeStorage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        // Test Storage changes and how the are reverted

        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withStorage(ephemeral)
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS, persistent, REPLICAS);
        assertThat(zc.getStorage(), is(persistent));

        ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withStorage(persistent)
                    .endZookeeper()
                .endSpec()
                .build();
        zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS, ephemeral, REPLICAS);

        // Storage is reverted
        assertThat(zc.getStorage(), is(ephemeral));

        // Warning status condition is set
        assertThat(zc.getWarningConditions().size(), is(1));
        assertThat(zc.getWarningConditions().get(0).getReason(), is("ZooKeeperStorage"));
    }

    @ParallelTest
    public void testStorageValidationAfterInitialDeployment() {
        assertThrows(InvalidResourceException.class, () -> {
            Storage oldStorage = new PersistentClaimStorageBuilder()
                    .withSize("100Gi")
                    .build();

            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                        .editZookeeper()
                            .withStorage(new PersistentClaimStorageBuilder().build())
                        .endZookeeper()
                    .endSpec()
                    .build();
            ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS, oldStorage, REPLICAS);
        });
    }

    @ParallelTest
    public void testZookeeperContainerEnvVars() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate zookeeperContainer = new ContainerTemplate();
        zookeeperContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withZookeeperContainer(zookeeperContainer)
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        List<EnvVar> zkEnvVars = zc.getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                zkEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                zkEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));

    }

    @ParallelTest
    public void testZookeeperContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = ZookeeperCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = ZookeeperCluster.ENV_VAR_ZOOKEEPER_METRICS_ENABLED;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate zookeeperContainer = new ContainerTemplate();
        zookeeperContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withZookeeperContainer(zookeeperContainer)
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        List<EnvVar> zkEnvVars = zc.getEnvVars();
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                zkEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                zkEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));

    }

    @ParallelTest
    public void testZookeeperContainerSecurityContext() {
        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewZookeeperContainer()
                                .withSecurityContext(securityContext)
                            .endZookeeperContainer()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        assertThat(zc.createContainer(null),
                allOf(
                        hasProperty("name", equalTo(ZookeeperCluster.ZOOKEEPER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                ));
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("zoo-metrics-config").withKey("zoo-metrics-config.yml").build())
                .endValueFrom()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withMetricsConfig(metrics)
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        assertThat(zc.isMetricsEnabled(), is(true));
        assertThat(zc.getMetricsConfigInCm(), is(metrics));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        assertThat(ZC.isMetricsEnabled(), is(false));
        assertThat(ZC.getMetricsConfigInCm(), is(nullValue()));
    }

    @ParallelTest
    public void testCustomImage() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withImage("my-image:my-tag")
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check container
        Container cont = zc.createContainer(null);
        assertThat(cont.getImage(), is("my-image:my-tag"));
    }

    @ParallelTest
    public void testHealthChecks() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withLivenessProbe(new ProbeBuilder()
                                .withInitialDelaySeconds(1)
                                .withPeriodSeconds(2)
                                .withTimeoutSeconds(3)
                                .withSuccessThreshold(4)
                                .withFailureThreshold(5)
                                .build())
                        .withReadinessProbe(new ProbeBuilder()
                                .withInitialDelaySeconds(6)
                                .withPeriodSeconds(7)
                                .withTimeoutSeconds(8)
                                .withSuccessThreshold(9)
                                .withFailureThreshold(10)
                                .build())
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check container
        Container cont = zc.createContainer(null);
        assertThat(cont.getLivenessProbe().getInitialDelaySeconds(), is(1));
        assertThat(cont.getLivenessProbe().getPeriodSeconds(), is(2));
        assertThat(cont.getLivenessProbe().getTimeoutSeconds(), is(3));
        assertThat(cont.getLivenessProbe().getSuccessThreshold(), is(4));
        assertThat(cont.getLivenessProbe().getFailureThreshold(), is(5));
        assertThat(cont.getReadinessProbe().getInitialDelaySeconds(), is(6));
        assertThat(cont.getReadinessProbe().getPeriodSeconds(), is(7));
        assertThat(cont.getReadinessProbe().getTimeoutSeconds(), is(8));
        assertThat(cont.getReadinessProbe().getSuccessThreshold(), is(9));
        assertThat(cont.getReadinessProbe().getFailureThreshold(), is(10));
    }

    @ParallelTest
    public void testDefaultCustomControllerPodDisruptionBudget()   {
        PodDisruptionBudget pdb = ZC.generatePodDisruptionBudget(true);
        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdbV1Beta1 = ZC.generatePodDisruptionBudgetV1Beta1(true);

        assertThat(pdb.getMetadata().getName(), is(KafkaResources.zookeeperStatefulSetName(CLUSTER)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(2));
        assertThat(pdb.getSpec().getSelector().getMatchLabels(), is(ZC.getSelectorLabels().toMap()));

        assertThat(pdbV1Beta1.getMetadata().getName(), is(KafkaResources.zookeeperStatefulSetName(CLUSTER)));
        assertThat(pdbV1Beta1.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdbV1Beta1.getSpec().getMinAvailable().getIntVal(), is(2));
        assertThat(pdbV1Beta1.getSpec().getSelector().getMatchLabels(), is(ZC.getSelectorLabels().toMap()));
    }

    @ParallelTest
    public void testCustomizedCustomControllerPodDisruptionBudget()   {
        Map<String, String> pdbLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> pdbAnnos = TestUtils.map("a1", "v1", "a2", "v2");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withAnnotations(pdbAnnos)
                                    .withLabels(pdbLabels)
                                .endMetadata()
                                .withMaxUnavailable(2)
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget(true);
        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdbV1Beta1 = zc.generatePodDisruptionBudgetV1Beta1(true);

        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnos.entrySet()), is(true));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(1));
        assertThat(pdb.getSpec().getSelector().getMatchLabels(), is(zc.getSelectorLabels().toMap()));

        assertThat(pdbV1Beta1.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdbV1Beta1.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnos.entrySet()), is(true));
        assertThat(pdbV1Beta1.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdbV1Beta1.getSpec().getMinAvailable().getIntVal(), is(1));
        assertThat(pdbV1Beta1.getSpec().getSelector().getMatchLabels(), is(zc.getSelectorLabels().toMap()));
    }
}
