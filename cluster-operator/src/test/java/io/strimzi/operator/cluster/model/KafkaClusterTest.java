/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.Lifecycle;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTerm;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.listener.NodePortListenerBootstrapOverrideBuilder;
import io.strimzi.api.kafka.model.listener.NodePortListenerBrokerOverrideBuilder;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.RackBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarBuilder;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.listener.IngressListenerBrokerConfiguration;
import io.strimzi.api.kafka.model.listener.IngressListenerBrokerConfigurationBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBootstrapOverride;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBootstrapOverrideBuilder;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBrokerOverrideBuilder;
import io.strimzi.api.kafka.model.listener.NodePortListenerBootstrapOverride;
import io.strimzi.api.kafka.model.listener.NodePortListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.RouteListenerBrokerOverride;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.LINE_SEPARATOR;
import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings({
        "checkstyle:ClassDataAbstractionCoupling",
        "checkstyle:ClassFanOutComplexity"
})
public class KafkaClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final int tlsHealthDelay = 120;
    private final int tlsHealthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final Map<String, Object> configuration = singletonMap("foo", "bar");
    private final InlineLogging kafkaLog = new InlineLogging();
    private final InlineLogging zooLog = new InlineLogging();
    {
        kafkaLog.setLoggers(Collections.singletonMap("kafka.root.logger.level", "OFF"));
        zooLog.setLoggers(Collections.singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final TlsSidecar tlsSidecar = new TlsSidecarBuilder()
            .withLivenessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .withReadinessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .build();

    private final Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCm, configuration, kafkaLog, zooLog))
            .editSpec()
                .editKafka()
                    .withTlsSidecar(tlsSidecar)
                .endKafka()
            .endSpec()
            .build();

    private final KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

    @Test
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsAndLogConfigMap(null);
        checkMetricsConfigMap(metricsCm);
        checkOwnerReference(kc.createOwnerReference(), metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertThat(metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS), is(TestUtils.toJsonString(this.metricsCm)));
    }

    private Map<String, String> expectedLabels()    {
        return TestUtils.map(
            Labels.STRIMZI_CLUSTER_LABEL, cluster,
            "my-user-label", "cromulent",
            Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster),
            Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
            Labels.KUBERNETES_NAME_LABEL, Labels.KUBERNETES_NAME,
            Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
            Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziLabels().toMap();
    }

    @Test
    public void testGenerateService() {
        Service headful = kc.generateService();

        assertThat(headful.getSpec().getType(), is("ClusterIP"));
        assertThat(headful.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headful.getSpec().getPorts().size(), is(4));
        assertThat(headful.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(0).getPort(), is(new Integer(KafkaCluster.REPLICATION_PORT)));
        assertThat(headful.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headful.getSpec().getPorts().get(1).getName(), is(KafkaCluster.CLIENT_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(1).getPort(), is(new Integer(KafkaCluster.CLIENT_PORT)));
        assertThat(headful.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headful.getSpec().getPorts().get(2).getName(), is(KafkaCluster.CLIENT_TLS_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(2).getPort(), is(new Integer(KafkaCluster.CLIENT_TLS_PORT)));
        assertThat(headful.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(headful.getSpec().getPorts().get(3).getName(), is(AbstractModel.METRICS_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(3).getPort(), is(new Integer(KafkaCluster.METRICS_PORT)));
        assertThat(headful.getSpec().getPorts().get(3).getProtocol(), is("TCP"));
        assertThat(headful.getMetadata().getAnnotations(), is(kc.getPrometheusAnnotations()));

        checkOwnerReference(kc.createOwnerReference(), headful);
    }

    @Test
    public void testGenerateServiceWithoutMetrics() {
        Kafka kafka = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editKafka()
                        .withMetrics(null)
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafka, VERSIONS);
        Service headful = kc.generateService();

        assertThat(headful.getSpec().getType(), is("ClusterIP"));
        assertThat(headful.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headful.getSpec().getPorts().size(), is(3));
        assertThat(headful.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(0).getPort(), is(new Integer(KafkaCluster.REPLICATION_PORT)));
        assertThat(headful.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headful.getSpec().getPorts().get(1).getName(), is(KafkaCluster.CLIENT_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(1).getPort(), is(new Integer(KafkaCluster.CLIENT_PORT)));
        assertThat(headful.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headful.getSpec().getPorts().get(2).getName(), is(KafkaCluster.CLIENT_TLS_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(2).getPort(), is(new Integer(KafkaCluster.CLIENT_TLS_PORT)));
        assertThat(headful.getSpec().getPorts().get(2).getProtocol(), is("TCP"));

        assertThat(headful.getMetadata().getAnnotations().containsKey("prometheus.io/port"), is(false));
        assertThat(headful.getMetadata().getAnnotations().containsKey("prometheus.io/scrape"), is(false));
        assertThat(headful.getMetadata().getAnnotations().containsKey("prometheus.io/path"), is(false));

        checkOwnerReference(kc.createOwnerReference(), headful);
    }

    @Test
    public void testGenerateHeadlessService() {
        Service headless = kc.generateHeadlessService();
        checkHeadlessService(headless);
        checkOwnerReference(kc.createOwnerReference(), headless);
    }

    private void checkHeadlessService(Service headless) {
        assertThat(headless.getMetadata().getName(), is(KafkaCluster.headlessServiceName(cluster)));
        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getClusterIP(), is("None"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(3));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(new Integer(KafkaCluster.REPLICATION_PORT)));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(KafkaCluster.CLIENT_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(new Integer(KafkaCluster.CLIENT_PORT)));
        assertThat(headless.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(KafkaCluster.CLIENT_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(new Integer(KafkaCluster.CLIENT_TLS_PORT)));
        assertThat(headless.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
    }

    @Test
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet ss = kc.generateStatefulSet(true, null, null);
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
                .withNewPersistentClaimStorage().withSelector(selector).withSize("100Gi").endPersistentClaimStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        StatefulSet ss = kc.generateStatefulSet(false, null, null);
        assertThat(ss.getSpec().getVolumeClaimTemplates().get(0).getSpec().getSelector().getMatchLabels(), is(selector));
    }

    @Test
    public void testGenerateStatefulSetWithEmptyStorageSelector() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorage().withSelector(emptyMap()).withSize("100Gi").endPersistentClaimStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        StatefulSet ss = kc.generateStatefulSet(false, null, null);
        assertThat(ss.getSpec().getVolumeClaimTemplates().get(0).getSpec().getSelector(), is(nullValue()));
    }

    @Test
    public void testGenerateStatefulSetWithSetSizeLimit() {
        String sizeLimit = "1Gi";
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorage().withNewSizeLimit(sizeLimit).endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        StatefulSet ss = kc.generateStatefulSet(false, null, null);
        assertThat(ss.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit().getAmount(), is(sizeLimit));
    }

    @Test
    public void testGenerateStatefulSetWithEmptySizeLimit() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        StatefulSet ss = kc.generateStatefulSet(false, null, null);
        assertThat(ss.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(nullValue()));
    }

    @Test
    public void testGenerateStatefulSetWithRack() {
        Kafka editKafkaAssembly = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editKafka()
                        .withNewRack().withTopologyKey("rack-key").endRack()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(editKafkaAssembly, VERSIONS);
        StatefulSet ss = kc.generateStatefulSet(true, null, null);
        checkStatefulSet(ss, editKafkaAssembly, true);
    }

    @Test
    public void testGenerateStatefulSetWithInitContainers() {
        Kafka editKafkaAssembly =
                new KafkaBuilder(kafkaAssembly)
                        .editSpec()
                            .editKafka()
                                .withNewPersistentClaimStorage().withSize("1Gi").endPersistentClaimStorage()
                                .withNewRack().withTopologyKey("rack-key").endRack()
                            .endKafka()
                        .endSpec().build();
        KafkaCluster kc = KafkaCluster.fromCrd(editKafkaAssembly, VERSIONS);
        StatefulSet ss = kc.generateStatefulSet(false, null, null);
        checkStatefulSet(ss, editKafkaAssembly, false);
    }

    private void checkStatefulSet(StatefulSet ss, Kafka cm, boolean isOpenShift) {
        assertThat(ss.getMetadata().getName(), is(KafkaCluster.kafkaClusterName(cluster)));
        // ... in the same namespace ...
        assertThat(ss.getMetadata().getNamespace(), is(namespace));
        // ... with these labels
        assertThat(ss.getMetadata().getLabels(), is(expectedLabels()));
        assertThat(ss.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));

        assertThat(ss.getSpec().getTemplate().getSpec().getSchedulerName(), is("default-scheduler"));

        List<Container> containers = ss.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(2));

        // checks on the main Kafka container
        assertThat(ss.getSpec().getReplicas(), is(new Integer(replicas)));
        assertThat(containers.get(0).getImage(), is(image));
        assertThat(containers.get(0).getLivenessProbe().getTimeoutSeconds(), is(new Integer(healthTimeout)));
        assertThat(containers.get(0).getLivenessProbe().getInitialDelaySeconds(), is(new Integer(healthDelay)));
        assertThat(containers.get(0).getLivenessProbe().getFailureThreshold(), is(new Integer(10)));
        assertThat(containers.get(0).getLivenessProbe().getSuccessThreshold(), is(new Integer(4)));
        assertThat(containers.get(0).getLivenessProbe().getPeriodSeconds(), is(new Integer(33)));
        assertThat(containers.get(0).getReadinessProbe().getTimeoutSeconds(), is(new Integer(healthTimeout)));
        assertThat(containers.get(0).getReadinessProbe().getInitialDelaySeconds(), is(new Integer(healthDelay)));
        assertThat(containers.get(0).getReadinessProbe().getFailureThreshold(), is(new Integer(10)));
        assertThat(containers.get(0).getReadinessProbe().getSuccessThreshold(), is(new Integer(4)));
        assertThat(containers.get(0).getReadinessProbe().getPeriodSeconds(), is(new Integer(33)));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaCluster.ENV_VAR_KAFKA_CONFIGURATION), is("foo=bar" + LINE_SEPARATOR));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED), is(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaCluster.ENV_VAR_KAFKA_LOG_DIRS),
                is(kc.dataVolumeMountPaths.stream().map(volumeMount -> volumeMount.getMountPath()).collect(Collectors.joining(","))));
        assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
        assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
        assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
        assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
        // checks on the TLS sidecar
        Container tlsSidecarContainer = containers.get(1);
        assertThat(tlsSidecarContainer.getImage(), is(image));
        assertThat(AbstractModel.containerEnvVars(tlsSidecarContainer).get(KafkaCluster.ENV_VAR_KAFKA_ZOOKEEPER_CONNECT), is(ZookeeperCluster.serviceName(cluster) + ":2181"));
        assertThat(AbstractModel.containerEnvVars(tlsSidecarContainer).get(ModelUtils.TLS_SIDECAR_LOG_LEVEL), is(TlsSidecarLogLevel.NOTICE.toValue()));
        assertThat(tlsSidecarContainer.getVolumeMounts().get(0).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
        assertThat(tlsSidecarContainer.getVolumeMounts().get(0).getMountPath(), is(KafkaCluster.TLS_SIDECAR_KAFKA_CERTS_VOLUME_MOUNT));
        assertThat(tlsSidecarContainer.getVolumeMounts().get(1).getMountPath(), is(KafkaCluster.TLS_SIDECAR_CLUSTER_CA_CERTS_VOLUME_MOUNT));
        assertThat(tlsSidecarContainer.getReadinessProbe().getInitialDelaySeconds(), is(new Integer(tlsHealthDelay)));
        assertThat(tlsSidecarContainer.getReadinessProbe().getTimeoutSeconds(), is(new Integer(tlsHealthTimeout)));
        assertThat(tlsSidecarContainer.getLivenessProbe().getInitialDelaySeconds(), is(new Integer(tlsHealthDelay)));
        assertThat(tlsSidecarContainer.getLivenessProbe().getTimeoutSeconds(), is(new Integer(tlsHealthTimeout)));

        if (cm.getSpec().getKafka().getRack() != null) {

            Rack rack = cm.getSpec().getKafka().getRack();

            // check that the pod spec contains anti-affinity rules with the right topology key
            PodSpec podSpec = ss.getSpec().getTemplate().getSpec();
            assertThat(podSpec.getAffinity(), is(notNullValue()));
            assertThat(podSpec.getAffinity().getPodAntiAffinity(), is(notNullValue()));
            assertThat(podSpec.getAffinity().getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution(), is(notNullValue()));
            List<WeightedPodAffinityTerm> terms = podSpec.getAffinity().getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution();
            assertThat(terms, is(notNullValue()));
            assertThat(terms.size() > 0, is(true));

            boolean isTopologyKey =
                    terms.stream().anyMatch(term -> term.getPodAffinityTerm().getTopologyKey().equals(rack.getTopologyKey()));
            assertThat(isTopologyKey, is(true));

            // check that pod spec contains the init Kafka container
            List<Container> initContainers = podSpec.getInitContainers();
            assertThat(initContainers, is(notNullValue()));
            assertThat(initContainers.size() > 0, is(true));

            boolean isInitKafka =
                    initContainers.stream().anyMatch(container -> container.getName().equals(KafkaCluster.INIT_NAME));
            assertThat(isInitKafka, is(true));
        }
    }

    // TODO test volume claim templates

    @Test
    public void testPodNames() {

        for (int i = 0; i < replicas; i++) {
            assertThat(kc.getPodName(i), is(KafkaCluster.kafkaClusterName(cluster) + "-" + i));
        }
    }

    @Test
    public void testPvcNames() {
        Kafka assembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withStorage(new PersistentClaimStorageBuilder().withDeleteClaim(false).withSize("100Gi").build())
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(assembly, VERSIONS);

        List<PersistentVolumeClaim> pvcs = kc.getVolumeClaims();

        for (int i = 0; i < replicas; i++) {
            assertThat(pvcs.get(0).getMetadata().getName() + "-" + KafkaCluster.kafkaPodName(cluster, i),
                    is(kc.VOLUME_NAME + "-" + KafkaCluster.kafkaPodName(cluster, i)));
        }

        assembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(
                            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize("100Gi").build())
                            .build())
                    .endKafka()
                .endSpec()
                .build();
        kc = KafkaCluster.fromCrd(assembly, VERSIONS);

        pvcs = kc.getVolumeClaims();

        for (int i = 0; i < replicas; i++) {
            int id = 0;
            for (PersistentVolumeClaim pvc : pvcs) {
                assertThat(pvc.getMetadata().getName() + "-" + KafkaCluster.kafkaPodName(cluster, i),
                        is(kc.VOLUME_NAME + "-" + id++ + "-" + KafkaCluster.kafkaPodName(cluster, i)));
            }
        }
    }

    @Test
    public void withOldAffinityWithoutRack() throws IOException {
        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, KafkaCluster::fromCrd, this.getClass().getSimpleName() + ".withOldAffinityWithoutRack");
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withAffinityWithoutRack() throws IOException {
        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, KafkaCluster::fromCrd, this.getClass().getSimpleName() + ".withAffinityWithoutRack");
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withRackWithoutAffinity() throws IOException {
        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, KafkaCluster::fromCrd, this.getClass().getSimpleName() + ".withRackWithoutAffinity");
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withRackAndOldAffinity() throws IOException {
        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, KafkaCluster::fromCrd, this.getClass().getSimpleName() + ".withRackAndOldAffinity");
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withRackAndAffinity() throws IOException {
        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, KafkaCluster::fromCrd, this.getClass().getSimpleName() + ".withRackAndAffinity");
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withOldTolerations() throws IOException {
        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, KafkaCluster::fromCrd, this.getClass().getSimpleName() + ".withOldTolerations");
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    @Test
    public void withTolerations() throws IOException {
        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, KafkaCluster::fromCrd, this.getClass().getSimpleName() + ".withTolerations");
        resourceTester.assertDesiredResource("-SS.yaml",
            kc -> kc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    @Test
    public void testExternalRoutes() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewListeners()
                    .withNewKafkaListenerExternalRoute()
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                    .endKafkaListenerExternalRoute()
                .endListeners()
                .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        Set<String> addresses = new HashSet<>();
        addresses.add("my-address-0");
        addresses.add("my-address-1");
        addresses.add("my-address-2");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true, null, null);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "route")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_TLS, "true")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses))), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION, KafkaListenerAuthenticationTls.TYPE_TLS)), is(true));

        List<ContainerPort> ports = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts();
        assertThat(ports.contains(kc.createContainerPort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, "TCP")), is(true));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapService();
        assertThat(ext.getMetadata().getName(), is(KafkaCluster.externalBootstrapServiceName(cluster)));
        assertThat(ext.getSpec().getType(), is("ClusterIP"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels()));
        assertThat(ext.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
        checkOwnerReference(kc.createOwnerReference(), ext);

        // Check per pod services
        for (int i = 0; i < replicas; i++)  {
            Service srv = kc.generateExternalService(i);
            assertThat(srv.getMetadata().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(srv.getSpec().getType(), is("ClusterIP"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaCluster.kafkaPodName(cluster, i)));
            assertThat(srv.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
            checkOwnerReference(kc.createOwnerReference(), srv);
        }

        // Check bootstrap route
        Route brt = kc.generateExternalBootstrapRoute();
        assertThat(brt.getMetadata().getName(), is(KafkaCluster.serviceName(cluster)));
        assertThat(brt.getSpec().getTls().getTermination(), is("passthrough"));
        assertThat(brt.getSpec().getTo().getKind(), is("Service"));
        assertThat(brt.getSpec().getTo().getName(), is(KafkaCluster.externalBootstrapServiceName(cluster)));
        assertThat(brt.getSpec().getPort().getTargetPort(), is(new IntOrString(KafkaCluster.EXTERNAL_PORT)));
        checkOwnerReference(kc.createOwnerReference(), brt);

        // Check per pod router
        for (int i = 0; i < replicas; i++)  {
            Route rt = kc.generateExternalRoute(i);
            assertThat(rt.getMetadata().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(rt.getSpec().getTls().getTermination(), is("passthrough"));
            assertThat(rt.getSpec().getTo().getKind(), is("Service"));
            assertThat(rt.getSpec().getTo().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(rt.getSpec().getPort().getTargetPort(), is(new IntOrString(KafkaCluster.EXTERNAL_PORT)));
            checkOwnerReference(kc.createOwnerReference(), rt);
        }
    }

    @Test
    public void testExternalRoutesWithHostOverrides() {
        RouteListenerBrokerOverride routeListenerBrokerOverride0 = new RouteListenerBrokerOverride();
        routeListenerBrokerOverride0.setBroker(0);
        routeListenerBrokerOverride0.setHost("my-host-0.cz");

        RouteListenerBrokerOverride routeListenerBrokerOverride1 = new RouteListenerBrokerOverride();
        routeListenerBrokerOverride1.setBroker(1);
        routeListenerBrokerOverride1.setHost("my-host-1.cz");

        RouteListenerBrokerOverride routeListenerBrokerOverride2 = new RouteListenerBrokerOverride();
        routeListenerBrokerOverride2.setBroker(2);
        routeListenerBrokerOverride2.setHost("my-host-2.cz");

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                        .withNewKafkaListenerExternalRoute()
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .withNewOverrides()
                                .withNewBootstrap()
                                    .withHost("my-boostrap.cz")
                                .endBootstrap()
                                .withBrokers(routeListenerBrokerOverride0, routeListenerBrokerOverride1, routeListenerBrokerOverride2)
                            .endOverrides()
                        .endKafkaListenerExternalRoute()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        Set<String> addresses = new HashSet<>();
        addresses.add("my-address-0");
        addresses.add("my-address-1");
        addresses.add("my-address-2");
        kc.setExternalAddresses(addresses);

        // Check bootstrap route
        Route brt = kc.generateExternalBootstrapRoute();
        assertThat(brt.getMetadata().getName(), is(KafkaCluster.serviceName(cluster)));
        assertThat(brt.getSpec().getHost(), is("my-boostrap.cz"));

        // Check per pod router
        for (int i = 0; i < replicas; i++)  {
            Route rt = kc.generateExternalRoute(i);
            assertThat(rt.getMetadata().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(rt.getSpec().getHost(), is("my-host-" + i + ".cz"));
        }
    }

    @Test
    public void testExternalLoadBalancers() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalLoadBalancer()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerExternalLoadBalancer()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        Set<String> addresses = new HashSet<>();
        addresses.add("my-address-0");
        addresses.add("my-address-1");
        addresses.add("my-address-2");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true, null, null);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "loadbalancer")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_TLS, "true")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses))), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION, KafkaListenerAuthenticationTls.TYPE_TLS)), is(true));

        List<ContainerPort> ports = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts();
        assertThat(ports.contains(kc.createContainerPort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, "TCP")), is(true));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapService();
        assertThat(ext.getMetadata().getName(), is(KafkaCluster.externalBootstrapServiceName(cluster)));
        assertThat(ext.getSpec().getType(), is("LoadBalancer"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels()));
        assertThat(ext.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
        assertThat(ext.getSpec().getLoadBalancerIP(), is(nullValue()));
        checkOwnerReference(kc.createOwnerReference(), ext);

        // Check per pod services
        for (int i = 0; i < replicas; i++)  {
            Service srv = kc.generateExternalService(i);
            assertThat(srv.getMetadata().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(srv.getSpec().getType(), is("LoadBalancer"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaCluster.kafkaPodName(cluster, i)));
            assertThat(srv.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
            assertThat(srv.getSpec().getLoadBalancerIP(), is(nullValue()));
            checkOwnerReference(kc.createOwnerReference(), srv);
        }
    }

    @Test
    public void testExternalLoadBalancersWithoutTls() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalLoadBalancer()
                                .withTls(false)
                            .endKafkaListenerExternalLoadBalancer()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        Set<String> addresses = new HashSet<>();
        addresses.add("my-address-0");
        addresses.add("my-address-1");
        addresses.add("my-address-2");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true, null, null);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "loadbalancer")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_TLS, "false")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses))), is(true));
    }

    @Test
    public void testExternalLoadBalancersWithDnsAnnotations() {
        LoadBalancerListenerBootstrapOverride bootstrapOverride = new LoadBalancerListenerBootstrapOverrideBuilder()
                .withDnsAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "bootstrap.myingress.com."))
                .build();

        LoadBalancerListenerBrokerOverride brokerOverride0 = new LoadBalancerListenerBrokerOverrideBuilder()
                .withBroker(0)
                .withDnsAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-0.myingress.com."))
                .build();

        LoadBalancerListenerBrokerOverride brokerOverride2 = new LoadBalancerListenerBrokerOverrideBuilder()
                .withBroker(2)
                .withDnsAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-2.myingress.com."))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalLoadBalancer()
                                .withNewOverrides()
                                    .withBootstrap(bootstrapOverride)
                                    .withBrokers(brokerOverride0, brokerOverride2)
                                .endOverrides()
                            .endKafkaListenerExternalLoadBalancer()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check annotations
        assertThat(kc.generateExternalBootstrapService().getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "bootstrap.myingress.com.")));
        assertThat(kc.generateExternalService(0).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-0.myingress.com.")));
        assertThat(kc.generateExternalService(1).getMetadata().getAnnotations().isEmpty(), is(true));
        assertThat(kc.generateExternalService(2).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-2.myingress.com.")));
    }

    @Test
    public void testExternalLoadBalancersWithLoadBalancerIPOverride() {
        LoadBalancerListenerBootstrapOverride bootstrapOverride = new LoadBalancerListenerBootstrapOverrideBuilder()
                .withLoadBalancerIP("10.0.0.1")
                .build();

        LoadBalancerListenerBrokerOverride brokerOverride0 = new LoadBalancerListenerBrokerOverrideBuilder()
                .withBroker(0)
                .withLoadBalancerIP("10.0.0.2")
                .build();

        LoadBalancerListenerBrokerOverride brokerOverride2 = new LoadBalancerListenerBrokerOverrideBuilder()
                .withBroker(2)
                .withLoadBalancerIP("10.0.0.3")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalLoadBalancer()
                                .withNewOverrides()
                                    .withBootstrap(bootstrapOverride)
                                    .withBrokers(brokerOverride0, brokerOverride2)
                                .endOverrides()
                            .endKafkaListenerExternalLoadBalancer()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check annotations
        assertThat(kc.generateExternalBootstrapService().getSpec().getLoadBalancerIP(), is("10.0.0.1"));
        assertThat(kc.generateExternalService(0).getSpec().getLoadBalancerIP(), is("10.0.0.2"));
        assertThat(kc.generateExternalService(1).getSpec().getLoadBalancerIP(), is(nullValue()));
        assertThat(kc.generateExternalService(2).getSpec().getLoadBalancerIP(), is("10.0.0.3"));
    }

    @Test
    public void testExternalNodePortWithDnsAnnotations() {
        NodePortListenerBootstrapOverride bootstrapOverride = new NodePortListenerBootstrapOverrideBuilder()
                .withDnsAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "bootstrap.myingress.com."))
                .build();

        NodePortListenerBrokerOverride brokerOverride0 = new NodePortListenerBrokerOverrideBuilder()
                .withBroker(0)
                .withDnsAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-0.myingress.com."))
                .build();

        NodePortListenerBrokerOverride brokerOverride2 = new NodePortListenerBrokerOverrideBuilder()
                .withBroker(2)
                .withDnsAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-2.myingress.com."))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withNewOverrides()
                                    .withBootstrap(bootstrapOverride)
                                    .withBrokers(brokerOverride0, brokerOverride2)
                                .endOverrides()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check annotations
        assertThat(kc.generateExternalBootstrapService().getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "bootstrap.myingress.com.")));
        assertThat(kc.generateExternalService(0).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-0.myingress.com.")));
        assertThat(kc.generateExternalService(1).getMetadata().getAnnotations().isEmpty(), is(true));
        assertThat(kc.generateExternalService(2).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-2.myingress.com.")));

    }

    @Test
    public void testExternalNodePorts() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        Set<String> addresses = new HashSet<>();
        addresses.add("32123");
        addresses.add("32456");
        addresses.add("32789");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true, null, null);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "nodeport")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_TLS, "true")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses))), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION, KafkaListenerAuthenticationTls.TYPE_TLS)), is(true));

        List<ContainerPort> ports = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts();
        assertThat(ports.contains(kc.createContainerPort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, "TCP")), is(true));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapService();
        assertThat(ext.getMetadata().getName(), is(KafkaCluster.externalBootstrapServiceName(cluster)));
        assertThat(ext.getSpec().getType(), is("NodePort"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels()));
        assertThat(ext.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
        checkOwnerReference(kc.createOwnerReference(), ext);

        // Check per pod services
        for (int i = 0; i < replicas; i++)  {
            Service srv = kc.generateExternalService(i);
            assertThat(srv.getMetadata().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(srv.getSpec().getType(), is("NodePort"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaCluster.kafkaPodName(cluster, i)));
            assertThat(srv.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
            checkOwnerReference(kc.createOwnerReference(), srv);
        }
    }

    @Test
    public void testExternalNodePortOverrides() {
        NodePortListenerBrokerOverride nodePortListenerBrokerOverride = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride.setBroker(0);
        nodePortListenerBrokerOverride.setNodePort(32101);
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
            image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                            .withNewOverrides()
                                .withNewBootstrap()
                                    .withNodePort(32001)
                                .endBootstrap()
                                .withBrokers(nodePortListenerBrokerOverride)
                            .endOverrides()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        Set<String> addresses = new HashSet<>();
        addresses.add("32123");
        addresses.add("32456");
        addresses.add("32789");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true, null, null);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "nodeport")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_TLS, "false")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses))), is(true));

        List<ContainerPort> ports = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts();
        assertThat(ports.contains(kc.createContainerPort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, "TCP")), is(true));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapService();
        assertThat(ext.getMetadata().getName(), is(KafkaCluster.externalBootstrapServiceName(cluster)));
        assertThat(ext.getSpec().getType(), is("NodePort"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels()));
        assertThat(ext.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, 32001, "TCP"))));
        checkOwnerReference(kc.createOwnerReference(), ext);

        // Check per pod services
        for (int i = 0; i < replicas; i++)  {
            Service srv = kc.generateExternalService(i);
            assertThat(srv.getMetadata().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(srv.getSpec().getType(), is("NodePort"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaCluster.kafkaPodName(cluster, i)));
            if (i == 0) { // pod with index 0 will have overriden port
                assertThat(srv.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, 32101, "TCP"))));
            } else {
                assertThat(srv.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
            }
            checkOwnerReference(kc.createOwnerReference(), srv);
        }
    }

    @Test
    public void testExternalNodePortsWithoutTls() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        Set<String> addresses = new HashSet<>();
        addresses.add("32123");
        addresses.add("32456");
        addresses.add("32789");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true, null, null);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "nodeport")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_TLS, "false")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses))), is(true));
    }

    @Test
    public void testGetExternalNodePortServiceAddressOverrideWithNullAdvertisedHost() {
        NodePortListenerBrokerOverride nodePortListenerBrokerOverride = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride.setBroker(0);
        nodePortListenerBrokerOverride.setNodePort(32101);
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
            image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                            .withNewOverrides()
                                .withNewBootstrap()
                                    .withNodePort(32001)
                                .endBootstrap()
                                .withBrokers(nodePortListenerBrokerOverride)
                            .endOverrides()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        assertThat(kc.generateExternalService(0).getSpec().getPorts().get(0).getNodePort(), is(new Integer(32101)));
        assertThat(kc.generateExternalBootstrapService().getSpec().getPorts().get(0).getNodePort(), is(new Integer(32001)));
        assertThat(((NodePortListenerBootstrapOverride) kc.getExternalListenerBootstrapOverride()).getNodePort(), is(new Integer(32001)));
    }

    @Test
    public void testGetExternalNodePortServiceAddressOverrideWithNonNullAdvertisedHost() {
        NodePortListenerBrokerOverride nodePortListenerBrokerOverride = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride.setBroker(0);
        nodePortListenerBrokerOverride.setNodePort(32101);
        nodePortListenerBrokerOverride.setAdvertisedHost("advertised.host");
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
            image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewKafkaListenerExternalNodePort()
                        .withTls(false)
                        .withNewOverrides()
                            .withNewBootstrap()
                                .withNodePort(32001)
                            .endBootstrap()
                            .withBrokers(nodePortListenerBrokerOverride)
                        .endOverrides()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        assertThat(kc.generateExternalService(0).getSpec().getPorts().get(0).getNodePort(), is(new Integer(32101)));
        assertThat(kc.generateExternalBootstrapService().getSpec().getPorts().get(0).getNodePort(), is(new Integer(32001)));

        assertThat(((NodePortListenerBootstrapOverride) kc.getExternalListenerBootstrapOverride()).getNodePort(), is(new Integer(32001)));
        assertThat(kc.getExternalServiceAdvertisedHostOverride(0), is("advertised.host"));
    }

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(resource.getMetadata().getOwnerReferences().get(0), is(ownerRef));
    }

    @Test
    public void testGenerateBrokerSecret() throws CertificateParsingException {
        Secret secret = generateBrokerSecret(null, emptyMap());
        assertThat(secret.getData().keySet(), is(set(
                "foo-kafka-0.crt",  "foo-kafka-0.key",
                "foo-kafka-1.crt", "foo-kafka-1.key",
                "foo-kafka-2.crt", "foo-kafka-2.key")));
        X509Certificate cert = Ca.cert(secret, "foo-kafka-0.crt");
        assertThat(cert.getSubjectDN().getName(), is("CN=foo-kafka, O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(set(
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc.cluster.local"),
                asList(2, "foo-kafka-bootstrap"),
                asList(2, "foo-kafka-bootstrap.test"),
                asList(2, "foo-kafka-bootstrap.test.svc"),
                asList(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                asList(2, "foo-kafka-brokers"),
                asList(2, "foo-kafka-brokers.test"),
                asList(2, "foo-kafka-brokers.test.svc"),
                asList(2, "foo-kafka-brokers.test.svc.cluster.local"))));

    }

    @Test
    public void testGenerateBrokerSecretExternal() throws CertificateParsingException {
        Map<Integer, Set<String>> externalAddresses = new HashMap<>();
        externalAddresses.put(0, Collections.singleton("123.10.125.130"));
        externalAddresses.put(1, Collections.singleton("123.10.125.131"));
        externalAddresses.put(2, Collections.singleton("123.10.125.132"));

        Secret secret = generateBrokerSecret(Collections.singleton("123.10.125.140"), externalAddresses);
        assertThat(secret.getData().keySet(), is(set(
                "foo-kafka-0.crt",  "foo-kafka-0.key",
                "foo-kafka-1.crt", "foo-kafka-1.key",
                "foo-kafka-2.crt", "foo-kafka-2.key")));
        X509Certificate cert = Ca.cert(secret, "foo-kafka-0.crt");
        assertThat(cert.getSubjectDN().getName(), is("CN=foo-kafka, O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(set(
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc.cluster.local"),
                asList(2, "foo-kafka-bootstrap"),
                asList(2, "foo-kafka-bootstrap.test"),
                asList(2, "foo-kafka-bootstrap.test.svc"),
                asList(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                asList(2, "foo-kafka-brokers"),
                asList(2, "foo-kafka-brokers.test"),
                asList(2, "foo-kafka-brokers.test.svc"),
                asList(2, "foo-kafka-brokers.test.svc.cluster.local"),
                asList(7, "123.10.125.140"),
                asList(7, "123.10.125.130"))));
    }

    @Test
    public void testGenerateBrokerSecretExternalWithManyDNS() throws CertificateParsingException {
        Map<Integer, Set<String>> externalAddresses = new HashMap<>();
        externalAddresses.put(0, TestUtils.set("123.10.125.130", "my-broker-0"));
        externalAddresses.put(1, TestUtils.set("123.10.125.131", "my-broker-1"));
        externalAddresses.put(2, TestUtils.set("123.10.125.132", "my-broker-2"));

        Secret secret = generateBrokerSecret(TestUtils.set("123.10.125.140", "my-bootstrap"), externalAddresses);
        assertThat(secret.getData().keySet(), is(set(
                "foo-kafka-0.crt",  "foo-kafka-0.key",
                "foo-kafka-1.crt", "foo-kafka-1.key",
                "foo-kafka-2.crt", "foo-kafka-2.key")));
        X509Certificate cert = Ca.cert(secret, "foo-kafka-0.crt");
        assertThat(cert.getSubjectDN().getName(), is("CN=foo-kafka, O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(set(
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc.cluster.local"),
                asList(2, "foo-kafka-bootstrap"),
                asList(2, "foo-kafka-bootstrap.test"),
                asList(2, "foo-kafka-bootstrap.test.svc"),
                asList(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                asList(2, "foo-kafka-brokers"),
                asList(2, "foo-kafka-brokers.test"),
                asList(2, "foo-kafka-brokers.test.svc"),
                asList(2, "foo-kafka-brokers.test.svc.cluster.local"),
                asList(2, "my-broker-0"),
                asList(2, "my-bootstrap"),
                asList(7, "123.10.125.140"),
                asList(7, "123.10.125.130"))));
    }

    private Secret generateBrokerSecret(Set<String> externalBootstrapAddress, Map<Integer, Set<String>> externalAddresses) {
        ClusterCa clusterCa = new ClusterCa(new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), cluster, null, null);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), null, true);

        kc.generateCertificates(kafkaAssembly, clusterCa, externalBootstrapAddress, externalAddresses);
        return kc.generateBrokersSecret();
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

        Map<String, String> exSvcLabels = TestUtils.map("l9", "v9", "l10", "v10");
        Map<String, String> exSvcAnots = TestUtils.map("a9", "v9", "a10", "v10");

        Map<String, String> perPodSvcLabels = TestUtils.map("l11", "v11", "l12", "v12");
        Map<String, String> perPodSvcAnots = TestUtils.map("a11", "v11", "a12", "v12");

        Map<String, String> exRouteLabels = TestUtils.map("l13", "v13", "l14", "v14");
        Map<String, String> exRouteAnots = TestUtils.map("a13", "v13", "a14", "v14");

        Map<String, String> perPodRouteLabels = TestUtils.map("l15", "v15", "l16", "v16");
        Map<String, String> perPodRouteAnots = TestUtils.map("a15", "v15", "a16", "v16");

        Map<String, String> pdbLabels = TestUtils.map("l17", "v17", "l18", "v18");
        Map<String, String> pdbAnots = TestUtils.map("a17", "v17", "a18", "v18");

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                        .withNewKafkaListenerExternalRoute()
                        .endKafkaListenerExternalRoute()
                        .endListeners()
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
                                .withNewPriorityClassName("top-priority")
                                .withNewSchedulerName("my-scheduler")
                            .endPod()
                            .withNewBootstrapService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnots)
                                .endMetadata()
                            .endBootstrapService()
                            .withNewBrokersService()
                                .withNewMetadata()
                                    .withLabels(hSvcLabels)
                                    .withAnnotations(hSvcAnots)
                                .endMetadata()
                            .endBrokersService()
                            .withNewExternalBootstrapService()
                                .withNewMetadata()
                                    .withLabels(exSvcLabels)
                                    .withAnnotations(exSvcAnots)
                                .endMetadata()
                            .endExternalBootstrapService()
                            .withNewPerPodService()
                                .withNewMetadata()
                                    .withLabels(perPodSvcLabels)
                                    .withAnnotations(perPodSvcAnots)
                                .endMetadata()
                            .endPerPodService()
                            .withNewExternalBootstrapRoute()
                                .withNewMetadata()
                                .withLabels(exRouteLabels)
                                .withAnnotations(exRouteAnots)
                                .endMetadata()
                            .endExternalBootstrapRoute()
                            .withNewPerPodRoute()
                                .withNewMetadata()
                                .withLabels(perPodRouteLabels)
                                .withAnnotations(perPodRouteAnots)
                                .endMetadata()
                            .endPerPodRoute()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withLabels(pdbLabels)
                                    .withAnnotations(pdbAnots)
                                .endMetadata()
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check StatefulSet
        StatefulSet ss = kc.generateStatefulSet(true, null, null);
        assertThat(ss.getMetadata().getLabels().entrySet().containsAll(ssLabels.entrySet()), is(true));
        assertThat(ss.getMetadata().getAnnotations().entrySet().containsAll(ssAnots.entrySet()), is(true));
        assertThat(ss.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));

        // Check Pods
        assertThat(ss.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(ss.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(ss.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));

        // Check Service
        Service svc = kc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()), is(true));

        // Check Headless Service
        svc = kc.generateHeadlessService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(hSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(hSvcAnots.entrySet()), is(true));

        // Check External Bootstrap service
        svc = kc.generateExternalBootstrapService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(exSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(exSvcAnots.entrySet()), is(true));

        // Check per pod service
        svc = kc.generateExternalService(0);
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnots.entrySet()), is(true));

        // Check Bootstrap Route
        Route rt = kc.generateExternalBootstrapRoute();
        assertThat(rt.getMetadata().getLabels().entrySet().containsAll(exRouteLabels.entrySet()), is(true));
        assertThat(rt.getMetadata().getAnnotations().entrySet().containsAll(exRouteAnots.entrySet()), is(true));

        // Check PerPodRoute
        rt = kc.generateExternalRoute(0);
        assertThat(rt.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels.entrySet()), is(true));
        assertThat(rt.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnots.entrySet()), is(true));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));
    }

    @Test
    public void testReplicationPortNetworkPolicy() {
        NetworkPolicyPeer kafkaBrokersPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer eoPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(cluster)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer kafkaExporterPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaExporter.kafkaExporterName(cluster)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .build();

        Kafka kafkaAssembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap());
        KafkaCluster k = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Network Policies
        NetworkPolicy np = k.generateNetworkPolicy(false);

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertEquals(4, rules.size());
        assertTrue(rules.contains(kafkaBrokersPeer));
        assertTrue(rules.contains(eoPeer));
        assertTrue(rules.contains(kafkaExporterPeer));
        assertTrue(rules.contains(clusterOperatorPeer));
    }

    @Test
    public void testNetworkPolicyPeers() {
        NetworkPolicyPeer peer1 = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchExpressions(new LabelSelectorRequirementBuilder().withKey("my-key1").withValues("my-value1").build())
                .endPodSelector()
                .build();

        NetworkPolicyPeer peer2 = new NetworkPolicyPeerBuilder()
                .withNewNamespaceSelector()
                    .withMatchExpressions(new LabelSelectorRequirementBuilder().withKey("my-key2").withValues("my-value2").build())
                .endNamespaceSelector()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewPlain()
                                .withNetworkPolicyPeers(peer1)
                            .endPlain()
                            .withNewTls()
                                .withNetworkPolicyPeers(peer2)
                            .endTls()
                            .withNewKafkaListenerExternalRoute()
                                .withNetworkPolicyPeers(peer1, peer2)
                            .endKafkaListenerExternalRoute()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster k = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Network Policies
        NetworkPolicy np = k.generateNetworkPolicy(false);

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CLIENT_PORT))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().get(0), is(peer1));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CLIENT_TLS_PORT))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().get(0), is(peer2));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.EXTERNAL_PORT))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().size(), is(2));
        assertThat(rules.get(0).getFrom().contains(peer1), is(true));
        assertThat(rules.get(0).getFrom().contains(peer2), is(true));
    }

    @Test
    public void testNoNetworkPolicyPeers() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewPlain()
                            .endPlain()
                            .withNewTls()
                            .endTls()
                            .withNewKafkaListenerExternalRoute()
                            .endKafkaListenerExternalRoute()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster k = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Network Policies
        NetworkPolicy np = k.generateNetworkPolicy(false);

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CLIENT_PORT))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().size(), is(0));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CLIENT_TLS_PORT))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().size(), is(0));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.EXTERNAL_PORT))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().size(), is(0));
    }

    @Test
    public void testGracePeriod() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withTerminationGracePeriodSeconds(123)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = kc.generateStatefulSet(true, null, null);
        assertThat(ss.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(123)));
        Lifecycle lifecycle = ss.getSpec().getTemplate().getSpec().getContainers().get(1).getLifecycle();
        assertThat(lifecycle, is(notNullValue()));
        assertThat(lifecycle.getPreStop().getExec().getCommand().contains("/opt/stunnel/kafka_stunnel_pre_stop.sh"), is(true));
    }

    @Test
    public void testDefaultGracePeriod() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = kc.generateStatefulSet(true, null, null);
        assertThat(ss.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(30)));
        Lifecycle lifecycle = ss.getSpec().getTemplate().getSpec().getContainers().get(1).getLifecycle();
        assertThat(lifecycle, is(notNullValue()));
        assertThat(lifecycle.getPreStop().getExec().getCommand().contains("/opt/stunnel/kafka_stunnel_pre_stop.sh"), is(true));
    }

    /**
     * Verify the lookup order is:<ul>
     * <li>Kafka.spec.kafka.tlsSidecar.image</li>
     * <li>Kafka.spec.kafka.image</li>
     * <li>image for default version of Kafka</li></ul>
     */
    @Test
    public void testStunnelImage() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap());

        Kafka kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editKafka()
                        .editOrNewTlsSidecar()
                            .withImage("foo1")
                        .endTlsSidecar()
                        .withImage("foo2")
                    .endKafka()
                .endSpec()
            .build();
        assertThat(KafkaCluster.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(1).getImage(), is("foo1"));

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editKafka()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                        .withImage("foo2")
                    .endKafka()
                .endSpec()
                .build();
        assertThat(KafkaCluster.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(1).getImage(), is("foo2"));

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editKafka()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                        .withVersion("2.2.1")
                        .withImage(null)
                    .endKafka()
                .endSpec()
                .build();
        assertThat(KafkaCluster.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(1).getImage(), is(KafkaVersionTestUtils.DEFAULT_KAFKA_IMAGE));

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editKafka()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                        .withVersion("2.3.0")
                        .withImage(null)
                    .endKafka()
                .endSpec()
            .build();
        assertThat(KafkaCluster.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(1).getImage(), is(KafkaVersionTestUtils.DEFAULT_KAFKA_IMAGE));
    }

    @Test
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret1, secret2)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = kc.generateStatefulSet(true, null, null);
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @Test
    public void testImagePullSecretsFromCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        Kafka kafkaAssembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap());
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = kc.generateStatefulSet(true, null, secrets);
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @Test
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                                .withNewPod()
                                .withImagePullSecrets(secret2)
                                .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = kc.generateStatefulSet(true, null, singletonList(secret1));
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @Test
    public void testDefaultImagePullSecrets() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = kc.generateStatefulSet(true, null, null);
        assertThat(ss.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(0));
    }

    @Test
    public void testSecurityContext() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = kc.generateStatefulSet(true, null, null);
        assertThat(ss.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(ss.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(Long.valueOf(123)));
        assertThat(ss.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(Long.valueOf(456)));
        assertThat(ss.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(Long.valueOf(789)));
    }

    @Test
    public void testDefaultSecurityContext() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet ss = kc.generateStatefulSet(true, null, null);
        assertThat(ss.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @Test
    public void testPodDisruptionBudget() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
    }

    @Test
    public void testDefaultPodDisruptionBudget() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @Test
    public void testImagePullPolicy() {
        Kafka kafkaAssembly = ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap());
        kafkaAssembly.getSpec().getKafka().setRack(new RackBuilder().withTopologyKey("topology-key").build());
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(true, ImagePullPolicy.ALWAYS, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getInitContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        sts = kc.generateStatefulSet(true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getInitContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @Test
    public void testGetExternalServiceAdvertisedHostAndPortOverride() {
        NodePortListenerBrokerOverride nodePortListenerBrokerOverride0 = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride0.setBroker(0);
        nodePortListenerBrokerOverride0.setAdvertisedHost("my-host-0.cz");
        nodePortListenerBrokerOverride0.setAdvertisedPort(10000);

        NodePortListenerBrokerOverride nodePortListenerBrokerOverride1 = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride1.setBroker(1);
        nodePortListenerBrokerOverride1.setAdvertisedHost("my-host-1.cz");
        nodePortListenerBrokerOverride1.setAdvertisedPort(10001);

        NodePortListenerBrokerOverride nodePortListenerBrokerOverride2 = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride2.setBroker(2);
        nodePortListenerBrokerOverride2.setAdvertisedHost("my-host-2.cz");
        nodePortListenerBrokerOverride2.setAdvertisedPort(10002);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withNewOverrides()
                                    .withBrokers(nodePortListenerBrokerOverride0, nodePortListenerBrokerOverride1, nodePortListenerBrokerOverride2)
                                .endOverrides()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        assertThat(kc.getExternalServiceAdvertisedPortOverride(0), is(new Integer(10000)));
        assertThat(kc.getExternalServiceAdvertisedHostOverride(0), is("my-host-0.cz"));

        assertThat(kc.getExternalServiceAdvertisedPortOverride(1), is(new Integer(10001)));
        assertThat(kc.getExternalServiceAdvertisedHostOverride(1), is("my-host-1.cz"));

        assertThat(kc.getExternalServiceAdvertisedPortOverride(2), is(new Integer(10002)));
        assertThat(kc.getExternalServiceAdvertisedHostOverride(2), is("my-host-2.cz"));
    }

    @Test
    public void testGetExternalServiceWithoutAdvertisedHostAndPortOverride() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withNewKafkaListenerExternalNodePort()
                .endKafkaListenerExternalNodePort()
                .endListeners()
                .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        assertThat(kc.getExternalServiceAdvertisedPortOverride(0), is(nullValue()));
        assertThat(kc.getExternalServiceAdvertisedHostOverride(0), is(nullValue()));

        assertThat(kc.getExternalServiceAdvertisedPortOverride(1), is(nullValue()));
        assertThat(kc.getExternalServiceAdvertisedHostOverride(1), is(nullValue()));

        assertThat(kc.getExternalServiceAdvertisedPortOverride(2), is(nullValue()));
        assertThat(kc.getExternalServiceAdvertisedHostOverride(2), is(nullValue()));
    }

    @Test
    public void testGetExternalAdvertisedUrlWithOverrides() {
        NodePortListenerBrokerOverride nodePortListenerBrokerOverride0 = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride0.setBroker(0);
        nodePortListenerBrokerOverride0.setAdvertisedHost("my-host-0.cz");
        nodePortListenerBrokerOverride0.setAdvertisedPort(10000);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withNewOverrides()
                                    .withBrokers(nodePortListenerBrokerOverride0)
                                .endOverrides()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        assertThat(kc.getExternalAdvertisedUrl(0, "some-host.com", "12345"), is("0://my-host-0.cz:10000"));
        assertThat(kc.getExternalAdvertisedUrl(0, "", "12345"), is("0://my-host-0.cz:10000"));

        assertThat(kc.getExternalAdvertisedUrl(1, "some-host.com", "12345"), is("1://some-host.com:12345"));
        assertThat(kc.getExternalAdvertisedUrl(1, "", "12345"), is("1://:12345"));
    }

    @Test
    public void testGetExternalAdvertisedUrlWithoutOverrides() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalNodePort()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        assertThat(kc.getExternalAdvertisedUrl(0, "some-host.com", "12345"), is("0://some-host.com:12345"));
        assertThat(kc.getExternalAdvertisedUrl(0, "", "12345"), is("0://:12345"));
    }

    @Test
    public void testGeneratePersistentVolumeClaimsPersistentWithClaimDeletion() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorage().withStorageClass("gp2-ssd").withDeleteClaim(true).withSize("100Gi").endPersistentClaimStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Storage annotation on STS
        assertThat(kc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(kafkaAssembly.getSpec().getKafka().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(kc.getStorage());

        assertThat(pvcs.size(), is(3));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(kc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
        }
    }

    @Test
    public void testGeneratePersistentVolumeClaimsPersistentWithoutClaimDeletion() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorage().withStorageClass("gp2-ssd").withDeleteClaim(false).withSize("100Gi").endPersistentClaimStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Storage annotation on STS
        assertThat(kc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(kafkaAssembly.getSpec().getKafka().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(kc.getStorage());

        assertThat(pvcs.size(), is(3));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(kc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }
    }

    @Test
    public void testGeneratePersistentVolumeClaimsPersistentWithOverride() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewPersistentClaimStorage()
                    .withStorageClass("gp2-ssd")
                    .withDeleteClaim(false)
                    .withSize("100Gi")
                    .withOverrides(new PersistentClaimStorageOverrideBuilder()
                            .withBroker(1)
                            .withStorageClass("gp2-ssd-az1")
                            .build())
                .endPersistentClaimStorage()
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Storage annotation on STS
        assertThat(kc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(kafkaAssembly.getSpec().getKafka().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(kc.getStorage());

        assertThat(pvcs.size(), is(3));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);

            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));

            if (i != 1) {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            } else {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd-az1"));
            }

            assertThat(pvc.getMetadata().getName().startsWith(kc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }
    }

    @Test
    public void testGeneratePersistentVolumeClaimsJbod() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withStorage(new JbodStorageBuilder().withVolumes(
                        new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                .withDeleteClaim(false)
                                .withId(0)
                                .withSize("100Gi")
                                .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(1).withStorageClass("gp2-ssd-az1").build())
                                .build(),
                        new PersistentClaimStorageBuilder()
                                .withStorageClass("gp2-st1")
                                .withDeleteClaim(true)
                                .withId(1)
                                .withSize("1000Gi")
                                .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(1).withStorageClass("gp2-st1-az1").build())
                                .build())
                        .build())
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Storage annotation on STS
        assertThat(kc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(kafkaAssembly.getSpec().getKafka().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(kc.getStorage());

        assertThat(pvcs.size(), is(6));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));

            if (i != 1) {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            } else {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd-az1"));
            }

            assertThat(pvc.getMetadata().getName().startsWith(kc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }

        for (int i = 3; i < 6; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("1000Gi")));

            if (i != 4) {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1"));
            } else {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1-az1"));
            }

            assertThat(pvc.getMetadata().getName().startsWith(kc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
        }
    }

    @Test
    public void testGeneratePersistentVolumeClaimsJbodWithTemplate() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPersistentVolumeClaim()
                                .withNewMetadata()
                                    .withLabels(singletonMap("testLabel", "testValue"))
                                    .withAnnotations(singletonMap("testAnno", "testValue"))
                                .endMetadata()
                            .endPersistentVolumeClaim()
                        .endTemplate()
                        .withStorage(new JbodStorageBuilder().withVolumes(
                            new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                    .withDeleteClaim(false)
                                    .withId(0)
                                    .withSize("100Gi")
                                    .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(1).withStorageClass("gp2-ssd-az1").build())
                                    .build(),
                            new PersistentClaimStorageBuilder()
                                    .withStorageClass("gp2-st1")
                                    .withDeleteClaim(true)
                                    .withId(1)
                                    .withSize("1000Gi")
                                    .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(1).withStorageClass("gp2-st1-az1").build())
                                    .build())
                            .build())
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(kc.getStorage());

        assertThat(pvcs.size(), is(6));

        for (int i = 0; i < 6; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getMetadata().getLabels().get("testLabel"), is("testValue"));
            assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is("testValue"));
        }
    }

    @Test
    public void testGeneratePersistentVolumeClaimsJbodWithOverrides() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withStorage(new JbodStorageBuilder().withVolumes(
                        new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                        new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                        .build())
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Storage annotation on STS
        assertThat(kc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(kafkaAssembly.getSpec().getKafka().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(kc.getStorage());

        assertThat(pvcs.size(), is(6));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(kc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }

        for (int i = 3; i < 6; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("1000Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1"));
            assertThat(pvc.getMetadata().getName().startsWith(kc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
        }
    }

    @Test
    public void testGeneratePersistentVolumeClaimsJbodWithoutVolumes() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                    .editKafka()
                    .withStorage(new JbodStorageBuilder().withVolumes(Collections.EMPTY_LIST)
                            .build())
                    .endKafka()
                    .endSpec()
                    .build();
            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testStorageValidationAfterInitialDeployment() {
        assertThrows(InvalidResourceException.class, () -> {
            Storage oldStorage = new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withSize("100Gi").build())
                    .build();

            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                    .editKafka()
                    .withStorage(new JbodStorageBuilder().withVolumes(Collections.EMPTY_LIST)
                            .build())
                    .endKafka()
                    .endSpec()
                    .build();
            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS, oldStorage);
        });
    }

    @Test
    public void testGeneratePersistentVolumeClaimsEphemeral()    {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        // Check Storage annotation on STS
        assertThat(kc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(kafkaAssembly.getSpec().getKafka().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(kc.getStorage());

        assertThat(pvcs.size(), is(0));
    }

    @Test
    public void testStorageReverting() {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage ephemeral = new EphemeralStorageBuilder().build();

        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        // Test Storage changes and how the are reverted

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withStorage(jbod)
                .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS, ephemeral);
        assertThat(kc.getStorage(), is(ephemeral));

        kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withStorage(jbod)
                .endKafka()
                .endSpec()
                .build();
        kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS, persistent);
        assertThat(kc.getStorage(), is(persistent));

        kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withStorage(ephemeral)
                .endKafka()
                .endSpec()
                .build();
        kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS, jbod);
        assertThat(kc.getStorage(), is(jbod));

        kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withStorage(persistent)
                .endKafka()
                .endSpec()
                .build();
        kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS, jbod);
        assertThat(kc.getStorage(), is(jbod));
    }

    @Test
    public void testExternalIngress() {
        IngressListenerBrokerConfiguration broker0 = new IngressListenerBrokerConfigurationBuilder()
                .withHost("my-broker-kafka-0.com")
                .withBroker(0)
                .build();

        IngressListenerBrokerConfiguration broker1 = new IngressListenerBrokerConfigurationBuilder()
                .withHost("my-broker-kafka-1.com")
                .withBroker(1)
                .build();

        IngressListenerBrokerConfiguration broker2 = new IngressListenerBrokerConfigurationBuilder()
                .withHost("my-broker-kafka-2.com")
                .withBroker(2)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalIngress()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withDnsAnnotations(Collections.singletonMap("dns-annotation", "my-kafka-bootstrap.com"))
                                    .endBootstrap()
                                    .withBrokers(broker0, broker1, broker2)
                                .endConfiguration()
                            .endKafkaListenerExternalIngress()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        assertThat(kc.isExposedWithIngress(), is(true));

        Set<String> addresses = new HashSet<>();
        addresses.add("my-broker-kafka-0.com");
        addresses.add("my-broker-kafka-1.com");
        addresses.add("my-broker-kafka-2.com");
        kc.setExternalAddresses(addresses);

        // Check StatefulSet changes
        StatefulSet ss = kc.generateStatefulSet(true, null, null);

        List<EnvVar> envs = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED, "ingress")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_TLS, "true")), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", addresses))), is(true));
        assertThat(envs.contains(kc.buildEnvVar(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION, KafkaListenerAuthenticationTls.TYPE_TLS)), is(true));

        List<ContainerPort> ports = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts();
        assertThat(ports.contains(kc.createContainerPort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, "TCP")), is(true));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapService();
        assertThat(ext.getMetadata().getName(), is(KafkaCluster.externalBootstrapServiceName(cluster)));
        assertThat(ext.getSpec().getType(), is("ClusterIP"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels()));
        assertThat(ext.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
        checkOwnerReference(kc.createOwnerReference(), ext);

        // Check per pod services
        for (int i = 0; i < replicas; i++)  {
            Service srv = kc.generateExternalService(i);
            assertThat(srv.getMetadata().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(srv.getSpec().getType(), is("ClusterIP"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaCluster.kafkaPodName(cluster, i)));
            assertThat(srv.getSpec().getPorts(), is(Collections.singletonList(kc.createServicePort(KafkaCluster.EXTERNAL_PORT_NAME, KafkaCluster.EXTERNAL_PORT, KafkaCluster.EXTERNAL_PORT, "TCP"))));
            checkOwnerReference(kc.createOwnerReference(), srv);
        }

        // Check bootstrap ingress
        Ingress bing = kc.generateExternalBootstrapIngress();
        assertThat(bing.getMetadata().getName(), is(KafkaCluster.serviceName(cluster)));
        assertThat(bing.getMetadata().getAnnotations().get("kubernetes.io/ingress.class"), is("nginx"));
        assertThat(bing.getSpec().getTls().size(), is(1));
        assertThat(bing.getSpec().getTls().get(0).getHosts().size(), is(1));
        assertThat(bing.getSpec().getTls().get(0).getHosts().get(0), is("my-kafka-bootstrap.com"));
        assertThat(bing.getSpec().getRules().size(), is(1));
        assertThat(bing.getSpec().getRules().get(0).getHost(), is("my-kafka-bootstrap.com"));
        assertThat(bing.getSpec().getRules().get(0).getHttp().getPaths().size(), is(1));
        assertThat(bing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getPath(), is("/"));
        assertThat(bing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getServiceName(), is(KafkaCluster.externalBootstrapServiceName(cluster)));
        assertThat(bing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getServicePort(), is(new IntOrString(KafkaCluster.EXTERNAL_PORT)));
        checkOwnerReference(kc.createOwnerReference(), bing);

        // Check per pod ingress
        for (int i = 0; i < replicas; i++)  {
            Ingress ing = kc.generateExternalIngress(i);
            assertThat(ing.getMetadata().getName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(ing.getMetadata().getAnnotations().get("kubernetes.io/ingress.class"), is("nginx"));
            assertThat(ing.getSpec().getTls().size(), is(1));
            assertThat(ing.getSpec().getTls().get(0).getHosts().size(), is(1));
            assertThat(addresses.contains(ing.getSpec().getTls().get(0).getHosts().get(0)), is(true));
            assertThat(ing.getSpec().getRules().size(), is(1));
            assertThat(addresses.contains(ing.getSpec().getRules().get(0).getHost()), is(true));
            assertThat(ing.getSpec().getRules().get(0).getHttp().getPaths().size(), is(1));
            assertThat(ing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getPath(), is("/"));
            assertThat(ing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getServiceName(), is(KafkaCluster.externalServiceName(cluster, i)));
            assertThat(ing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getServicePort(), is(new IntOrString(KafkaCluster.EXTERNAL_PORT)));
            checkOwnerReference(kc.createOwnerReference(), ing);
        }
    }

    @Test
    public void testExternalIngressClass() {
        IngressListenerBrokerConfiguration broker0 = new IngressListenerBrokerConfigurationBuilder()
                .withHost("my-broker-kafka-0.com")
                .withBroker(0)
                .build();

        IngressListenerBrokerConfiguration broker1 = new IngressListenerBrokerConfigurationBuilder()
                .withHost("my-broker-kafka-1.com")
                .withBroker(1)
                .build();

        IngressListenerBrokerConfiguration broker2 = new IngressListenerBrokerConfigurationBuilder()
                .withHost("my-broker-kafka-2.com")
                .withBroker(2)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalIngress()
                                .withNewIngressClass("nginx-internal")
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withDnsAnnotations(Collections.singletonMap("dns-annotation", "my-kafka-bootstrap.com"))
                                    .endBootstrap()
                                    .withBrokers(broker0, broker1, broker2)
                                .endConfiguration()
                            .endKafkaListenerExternalIngress()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

         // Check bootstrap ingress
        Ingress bing = kc.generateExternalBootstrapIngress();
        assertThat(bing.getMetadata().getAnnotations().get("kubernetes.io/ingress.class"), is("nginx-internal"));

        // Check per pod ingress
        for (int i = 0; i < replicas; i++)  {
            Ingress ing = kc.generateExternalIngress(i);
            assertThat(ing.getMetadata().getAnnotations().get("kubernetes.io/ingress.class"), is("nginx-internal"));
        }
    }

    @Test
    public void testExternalIngressMissingConfiguration() {
        IngressListenerBrokerConfiguration broker0 = new IngressListenerBrokerConfigurationBuilder()
                .withBroker(0)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalIngress()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .withNewConfiguration()
                                    .withBrokers(broker0)
                                .endConfiguration()
                            .endKafkaListenerExternalIngress()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        try {
            kc.generateExternalBootstrapIngress();
            fail("Expected exception was not thrown");
        } catch (InvalidResourceException e)    {
            // pass
        }

        // Check per pod router
        for (int i = 0; i < replicas; i++)  {
            try {
                kc.generateExternalIngress(i);
                fail("Expected exception was not thrown");
            } catch (InvalidResourceException e)    {
                // pass
            }
        }
    }

    @Test
    public void testClusterRoleBindingNodePort() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(testNamespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb.getMetadata().getName(), is(KafkaCluster.initContainerClusterRoleBindingName(testNamespace, cluster)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(KafkaCluster.initContainerServiceAccountName(cluster)));
    }

    @Test
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(testNamespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewRack("my-topology-label")
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb.getMetadata().getName(), is(KafkaCluster.initContainerClusterRoleBindingName(testNamespace, cluster)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(KafkaCluster.initContainerServiceAccountName(cluster)));
    }

    @Test
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = ResourceUtils.createKafkaCluster(testNamespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap());

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb, is(nullValue()));
    }

    @Test
    public void testKafkaContainerEnvVars() {

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
        ContainerTemplate kafkaContainer = new ContainerTemplate();
        kafkaContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewTemplate()
                .withKafkaContainer(kafkaContainer)
                .endTemplate()
                .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        List<EnvVar> kafkaEnvVars = kc.getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));

    }

    @Test
    public void testKafkaContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaCluster.ENV_VAR_KAFKA_LOG_DIRS;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaCluster.ENV_VAR_KAFKA_CONFIGURATION;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaContainer = new ContainerTemplate();
        kafkaContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewTemplate()
                .withKafkaContainer(kafkaContainer)
                .endTemplate()
                .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);

        List<EnvVar> kafkaEnvVars = kc.getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));

    }

    @Test
    public void testTlsSideCarContainerEnvVars() {

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
        ContainerTemplate tlsContainer = new ContainerTemplate();
        tlsContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewTemplate()
                .withTlsSidecarContainer(tlsContainer)
                .endTemplate()
                .endKafka()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS).getTlsSidevarEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));

    }

    @Test
    public void testTlsSidecarContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaCluster.ENV_VAR_KAFKA_ZOOKEEPER_CONNECT;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        ContainerTemplate tlsContainer = new ContainerTemplate();
        tlsContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewTemplate()
                .withTlsSidecarContainer(tlsContainer)
                .endTemplate()
                .endKafka()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS).getTlsSidevarEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));

    }

    @Test
    public void testInitContainerEnvVars() {

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
        ContainerTemplate initContainer = new ContainerTemplate();
        initContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewTemplate()
                .withInitContainer(initContainer)
                .endTemplate()
                .endKafka()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS).getInitContainerEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));

    }

    @Test
    public void testInitContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaCluster.ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaCluster.ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate initContainer = new ContainerTemplate();
        initContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                .editKafka()
                .withNewTemplate()
                .withInitContainer(initContainer)
                .endTemplate()
                .withNewListeners()
                .withNewKafkaListenerExternalNodePort()
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerExternalNodePort()
                .endListeners()
                .endKafka()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS).getInitContainerEnvVars();

        System.err.println(kafkaEnvVars);

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));

    }

    @Test
    public void testGenerateDeploymentWithOAuthWithClientSecret() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewPlain()
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                .endClientSecret()
                                                .build())
                            .endPlain()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(true, null, null);
        Container cont = sts.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_KAFKA_CLIENT_AUTHENTICATION.equals(var.getName())).findFirst().orElse(null).getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_OPTIONS.equals(var.getName())).findFirst().orElse(null).getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"",
                        ServerConfig.OAUTH_CLIENT_ID, "my-client-id",
                        ServerConfig.OAUTH_VALID_ISSUER_URI, "http://valid-issuer",
                        ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, "http://introspection")));
    }

    @Test
    public void testOAuthValidationMissingValidIssuerUriPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withClientId("my-client-id")
                                            .withIntrospectionEndpointUri("http://introspection")
                                            .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                            .endClientSecret().build())
                                .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationMissingValidIssuerUriTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withClientId("my-client-id")
                                            .withIntrospectionEndpointUri("http://introspection")
                                            .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                            .endClientSecret().build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationMissingValidIssuerUriExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalIngress()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withClientId("my-client-id")
                                            .withIntrospectionEndpointUri("http://introspection")
                                            .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                            .endClientSecret().build())
                                    .endKafkaListenerExternalIngress()
                                .endListeners()
                            .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsRelationWithExpirySecondsPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withValidIssuerUri("http://valid-issuer")
                                            .withJwksEndpointUri("http://jwks-endpoint")
                                            .withJwksRefreshSeconds(30)
                                            .withJwksExpirySeconds(89)
                                            .build())
                                .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsRelationWithExpirySecondsTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withValidIssuerUri("http://valid-issuer")
                                            .withJwksEndpointUri("http://jwks-endpoint")
                                            .withJwksRefreshSeconds(30)
                                            .withJwksExpirySeconds(89)
                                            .build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsRelationWithExpirySecondsExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalLoadBalancer()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withValidIssuerUri("http://valid-issuer")
                                            .withJwksEndpointUri("http://jwks-endpoint")
                                            .withJwksRefreshSeconds(30)
                                            .withJwksExpirySeconds(89)
                                            .build())
                                .endKafkaListenerExternalLoadBalancer()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsSetWithExpirySecondsNotSetPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withValidIssuerUri("http://valid-issuer")
                                            .withJwksEndpointUri("http://jwks-endpoint")
                                            .withJwksRefreshSeconds(333).build())
                                .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsSetWithExpirySecondsNotSetTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withValidIssuerUri("http://valid-issuer")
                                            .withJwksEndpointUri("http://jwks-endpoint")
                                            .withJwksRefreshSeconds(333).build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsSetWithExpirySecondsNotSetExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalNodePort()
                                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                            .withValidIssuerUri("http://valid-issuer")
                                            .withJwksEndpointUri("http://jwks-endpoint")
                                            .withJwksRefreshSeconds(333)
                                            .build())
                                .endKafkaListenerExternalNodePort()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsNotSetWithExpirySecondsSetPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withValidIssuerUri("http://valid-issuer")
                                                    .withJwksEndpointUri("http://jwks-endpoint")
                                                    .withJwksExpirySeconds(150)
                                                    .build())
                                .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsNotSetWithExpirySecondsSetTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withValidIssuerUri("http://valid-issuer")
                                                    .withJwksEndpointUri("http://jwks-endpoint")
                                                    .withJwksExpirySeconds(150)
                                                    .build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsNotSetWithExpirySecondsSetExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalRoute()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withValidIssuerUri("http://valid-issuer")
                                                    .withJwksEndpointUri("http://jwks-endpoint")
                                                    .withJwksExpirySeconds(150)
                                                    .build())
                                .endKafkaListenerExternalRoute()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationNoUriSpecifiedPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .build())
                                    .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationNoUriSpecifiedTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationNoUriSpecifiedExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalIngress()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .build())
                                .endKafkaListenerExternalIngress()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientIdPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withNewClientSecret()
                                                        .withSecretName("my-secret-secret")
                                                        .withKey("my-secret-key")
                                                    .endClientSecret()
                                                    .build())
                                .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientIdTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                    .endClientSecret()
                                                    .build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientIdExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalIngress()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                    .endClientSecret()
                                                    .build())
                                .endKafkaListenerExternalIngress()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientSecretPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withClientId("my-client-id")
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .build())
                                .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientSecretTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withClientId("my-client-id")
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientSecretExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalIngress()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withClientId("my-client-id")
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .build())
                                .endKafkaListenerExternalIngress()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationExpirySecondsWithoutEndpointUriPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withClientId("my-client-id")
                                                    .withJwksExpirySeconds(100)
                                                    .build())
                                    .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationExpirySecondsWithoutEndpointUriTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withClientId("my-client-id")
                                                    .withJwksExpirySeconds(100)
                                                    .build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationExpirySecondsWithoutEndpointUriExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalIngress()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withClientId("my-client-id")
                                                    .withJwksExpirySeconds(100)
                                                    .build())
                                .endKafkaListenerExternalIngress()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsWithoutEndpointUriPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewPlain()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withClientId("my-client-id")
                                                    .withJwksRefreshSeconds(40)
                                                    .build())
                                    .endPlain()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsWithoutEndpointUriTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewTls()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withClientId("my-client-id")
                                                    .withJwksRefreshSeconds(40)
                                                    .build())
                                .endTls()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsWithoutEndpointUriExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                    image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                    .editSpec()
                        .editKafka()
                            .withNewListeners()
                                .withNewKafkaListenerExternalIngress()
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withClientId("my-client-id")
                                                    .withJwksRefreshSeconds(40)
                                                    .build())
                                .endKafkaListenerExternalIngress()
                            .endListeners()
                        .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testGenerateDeploymentWithOAuthWithJwks() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewPlain()
                            .withAuth(
                                    new KafkaListenerAuthenticationOAuthBuilder()
                                            .withValidIssuerUri("http://valid-issuer")
                                            .withJwksEndpointUri("http://jwks-endpoint")
                                            .withJwksExpirySeconds(160)
                                            .withJwksRefreshSeconds(50)
                                            .withUserNameClaim("preferred_username")
                                            .build())
                            .endPlain()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(true, null, null);
        Container cont = sts.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_KAFKA_CLIENT_AUTHENTICATION.equals(var.getName())).findFirst().orElse(null).getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_OPTIONS.equals(var.getName())).findFirst().orElse(null).getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"",
                        ServerConfig.OAUTH_VALID_ISSUER_URI, "http://valid-issuer",
                        ServerConfig.OAUTH_JWKS_ENDPOINT_URI, "http://jwks-endpoint",
                        ServerConfig.OAUTH_JWKS_REFRESH_SECONDS, 50,
                        ServerConfig.OAUTH_JWKS_EXPIRY_SECONDS, 160,
                        ServerConfig.OAUTH_USERNAME_CLAIM, "preferred_username")));
    }

    @Test
    public void testGenerateDeploymentWithOAuthWithClientSecretAndTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewPlain()
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                .endClientSecret()
                                                .withDisableTlsHostnameVerification(true)
                                                .withTlsTrustedCertificates(cert1, cert2, cert3)
                                                .build())
                            .endPlain()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(true, null, null);
        Container cont = sts.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_KAFKA_CLIENT_AUTHENTICATION.equals(var.getName())).findFirst().orElse(null).getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_OPTIONS.equals(var.getName())).findFirst().orElse(null).getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"",
                        ServerConfig.OAUTH_CLIENT_ID, "my-client-id",
                        ServerConfig.OAUTH_VALID_ISSUER_URI, "http://valid-issuer",
                        ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, "http://introspection",
                        ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName())).findFirst().orElse(null).getMountPath(), is(KafkaCluster.OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/client"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName())).findFirst().orElse(null).getMountPath(), is(KafkaCluster.OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/client"));

        // Volumes
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size(), is(2));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey(), is("ca.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath(), is("first-certificate/ca.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getKey(), is("ca2.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getPath(), is("first-certificate/ca2.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size(), is(1));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey(), is("tls.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath(), is("second-certificate/tls.crt"));
    }

    @Test
    public void testGenerateDeploymentWithOAuthEverywhere() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, metricsCm, configuration, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewPlain()
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                .endClientSecret()
                                                .withDisableTlsHostnameVerification(true)
                                                .withTlsTrustedCertificates(cert1, cert2, cert3)
                                                .build())
                            .endPlain()
                            .withNewTls()
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                .endClientSecret()
                                                .withDisableTlsHostnameVerification(true)
                                                .withTlsTrustedCertificates(cert1, cert2, cert3)
                                                .build())
                            .endTls()
                            .withNewKafkaListenerExternalNodePort()
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                .endClientSecret()
                                                .withDisableTlsHostnameVerification(true)
                                                .withTlsTrustedCertificates(cert1, cert2, cert3)
                                                .build())
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(true, null, null);
        Container cont = sts.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_KAFKA_CLIENT_AUTHENTICATION.equals(var.getName())).findFirst().orElse(null).getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENT_OAUTH_OPTIONS.equals(var.getName())).findFirst().orElse(null).getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"",
                        ServerConfig.OAUTH_CLIENT_ID, "my-client-id",
                        ServerConfig.OAUTH_VALID_ISSUER_URI, "http://valid-issuer",
                        ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, "http://introspection",
                        ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_KAFKA_CLIENTTLS_AUTHENTICATION.equals(var.getName())).findFirst().orElse(null).getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENTTLS_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENTTLS_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_CLIENTTLS_OAUTH_OPTIONS.equals(var.getName())).findFirst().orElse(null).getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"",
                        ServerConfig.OAUTH_CLIENT_ID, "my-client-id",
                        ServerConfig.OAUTH_VALID_ISSUER_URI, "http://valid-issuer",
                        ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, "http://introspection",
                        ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION.equals(var.getName())).findFirst().orElse(null).getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_EXTERNAL_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_EXTERNAL_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElse(null).getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaCluster.ENV_VAR_STRIMZI_EXTERNAL_OAUTH_OPTIONS.equals(var.getName())).findFirst().orElse(null).getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"",
                        ServerConfig.OAUTH_CLIENT_ID, "my-client-id",
                        ServerConfig.OAUTH_VALID_ISSUER_URI, "http://valid-issuer",
                        ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, "http://introspection",
                        ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName()) && mount.getMountPath().endsWith("client")).findFirst().orElse(null).getMountPath(), is(KafkaCluster.OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/client"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName()) && mount.getMountPath().endsWith("client")).findFirst().orElse(null).getMountPath(), is(KafkaCluster.OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/client"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName()) && mount.getMountPath().endsWith("clienttls")).findFirst().orElse(null).getMountPath(), is(KafkaCluster.OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/clienttls"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName()) && mount.getMountPath().endsWith("clienttls")).findFirst().orElse(null).getMountPath(), is(KafkaCluster.OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/clienttls"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "first-certificate".equals(mount.getName()) && mount.getMountPath().endsWith("external")).findFirst().orElse(null).getMountPath(), is(KafkaCluster.OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/external"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "second-certificate".equals(mount.getName()) && mount.getMountPath().endsWith("external")).findFirst().orElse(null).getMountPath(), is(KafkaCluster.OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/external"));

        // Volumes
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size(), is(2));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey(), is("ca.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath(), is("first-certificate/ca.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getKey(), is("ca2.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "first-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(1).getPath(), is("first-certificate/ca2.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().size(), is(1));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getKey(), is("tls.crt"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "second-certificate".equals(vol.getName())).findFirst().orElse(null).getSecret().getItems().get(0).getPath(), is("second-certificate/tls.crt"));
    }
}
