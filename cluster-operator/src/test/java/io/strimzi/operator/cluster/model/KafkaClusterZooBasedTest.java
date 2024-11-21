/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.common.GenericSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.ProbeBuilder;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ExternalTrafficPolicy;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationOpaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBootstrap;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBootstrapBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationCustomBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.jmx.JmxModel;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.operator.cluster.model.jmx.JmxModel.JMX_PORT;
import static io.strimzi.operator.cluster.model.jmx.JmxModel.JMX_PORT_NAME;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
@ParallelSuite
public class KafkaClusterZooBasedTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static String NAMESPACE = "test";
    private final static String CLUSTER = "foo";
    private final static int REPLICAS = 3;
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
                .endZookeeper()
                .withNewKafka()
                    .withReplicas(REPLICAS)
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build())
                    .endJbodStorage()
                    .withConfig(Map.of("log.message.format.version", "3.0", "inter.broker.protocol.version", "3.0"))
                .endKafka()
            .endSpec()
            .build();
    private static final List<KafkaPool> POOLS = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
    private final static KafkaCluster KC = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

    //////////
    // Utility methods
    //////////
    private Map<String, String> expectedSelectorLabels()    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
                Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER),
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
    }

    private void checkHeadlessService(Service headless) {
        assertThat(headless.getMetadata().getName(), is(KafkaResources.brokersServiceName(CLUSTER)));
        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getClusterIP(), is("None"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(5));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(KafkaCluster.CONTROLPLANE_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.CONTROLPLANE_PORT));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(headless.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(KafkaCluster.KAFKA_AGENT_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(KafkaCluster.KAFKA_AGENT_PORT));
        assertThat(headless.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(3).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(3).getPort(), is(9092));
        assertThat(headless.getSpec().getPorts().get(3).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(4).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(4).getPort(), is(9093));
        assertThat(headless.getSpec().getPorts().get(4).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(headless.getSpec().getIpFamilies(), is(nullValue()));

        assertThat(headless.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(false));
    }

    private Secret generateBrokerSecret(Set<String> externalBootstrapAddress, Map<Integer, Set<String>> externalAddresses) {
        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), CLUSTER, null, null);
        clusterCa.createRenewOrReplace(NAMESPACE, emptyMap(), emptyMap(), emptyMap(), null, List.of(), true);
        ClientsCa clientsCa = new ClientsCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), null, null, null, null, 365, 30, true, CertificateExpirationPolicy.RENEW_CERTIFICATE);
        clientsCa.createRenewOrReplace(NAMESPACE, emptyMap(), emptyMap(), emptyMap(), null, List.of(), true);

        return KC.generateCertificatesSecret(clusterCa, clientsCa, null, externalBootstrapAddress, externalAddresses, true);
    }

    //////////
    // Tests
    //////////

    @ParallelTest
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm("{\"animal\":\"wombat\"}", "kafka-metrics-config", "kafka-metrics-config.yml");

        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "broker-0"),
                1, Map.of("PLAIN_9092", "broker-1"),
                2, Map.of("PLAIN_9092", "broker-2")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092"),
                1, Map.of("PLAIN_9092", "9092"),
                2, Map.of("PLAIN_9092", "9092")
        );

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("kafka-metrics-config.yml", "kafka-metrics-config", false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(metricsCm, null), advertisedHostnames, advertisedPorts);

        for (ConfigMap cm : cms)    {
            TestUtils.checkOwnerReference(cm, KAFKA);
            assertThat(cm.getData().get(MetricsModel.CONFIG_MAP_KEY), is("{\"animal\":\"wombat\"}"));
        }
    }

    @ParallelTest
    public void  testJavaSystemProperties() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewJvmOptions()
                            .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                    new SystemPropertyBuilder().withName("something.else").withValue("42").build()))
                        .endJvmOptions()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(1));

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            final Optional<EnvVar> envVarValue = pod.getSpec().getContainers().stream().findAny().orElseThrow().getEnv().stream().filter(env -> env.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findAny();
            assertThat(envVarValue.isPresent(), is(true));
            assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=42"));
        }));
    }

    @ParallelTest
    public void testCustomImage() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withImage("my-image:my-tag")
                        .withBrokerRackInitImage("my-init-image:my-init-tag")
                        .withNewRack().withTopologyKey("rack-key").endRack()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Check container
            assertThat(pod.getSpec().getContainers().stream().findAny().orElseThrow().getImage(), is("my-image:my-tag"));

            // Check Init container
            assertThat(pod.getSpec().getInitContainers().stream().findAny().orElseThrow().getImage(), is("my-init-image:my-init-tag"));
        }));
    }

    @ParallelTest
    public void testHealthChecks() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
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
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Check container
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();

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
        }));
    }

    @ParallelTest
    public void testInitContainer() {
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

        // Test env var conflict
        ContainerEnvVar envVar3 = new ContainerEnvVar();
        String testEnvThreeKey = KafkaCluster.ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS;
        String testEnvThreeValue = "test.env.three";
        envVar3.setName(testEnvThreeKey);
        envVar3.setValue(testEnvThreeValue);

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        // Set a node-port listener to force init-container to be templated
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                        .withNewTemplate()
                            .withNewInitContainer()
                                .withEnv(envVar1, envVar2, envVar3)
                                .withSecurityContext(securityContext)
                                .withVolumeMounts(additionalVolumeMount)
                            .endInitContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getInitContainers().stream().findAny().orElseThrow();
            assertThat(cont.getName(), is(KafkaCluster.INIT_NAME));
            assertThat(cont.getSecurityContext(), is(securityContext));
            assertThat(cont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
            assertThat(cont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
            assertThat(cont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

            assertThat(cont.getVolumeMounts().size(), is(2));
            assertThat(cont.getVolumeMounts().get(0).getName(), is("rack-volume"));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("secret-volume-name"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/mnt/secret-volume"));
        }));
    }

    @ParallelTest
    public void testGenerateService() {
        Service clusterIp = KC.generateService();

        assertThat(clusterIp.getSpec().getType(), is("ClusterIP"));
        assertThat(clusterIp.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(clusterIp.getSpec().getPorts().size(), is(3));
        assertThat(clusterIp.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(clusterIp.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(1).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(1).getPort(), is(9092));
        assertThat(clusterIp.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(2).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(2).getPort(), is(9093));
        assertThat(clusterIp.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(clusterIp.getSpec().getIpFamilies(), is(nullValue()));
        assertThat(clusterIp.getSpec().getPublishNotReadyAddresses(), is(nullValue()));
        
        assertThat(clusterIp.getMetadata().getAnnotations(), hasKey("strimzi.io/discovery"));        
        JsonArray annotation = new JsonArray(clusterIp.getMetadata().getAnnotations().get("strimzi.io/discovery"));
        JsonObject listener1 = annotation.getJsonObject(0);
        assertThat(listener1.getString("port"), is("9092"));
        assertThat(listener1.getString("tls"), is("false"));
        assertThat(listener1.getString("protocol"), is("kafka"));
        assertThat(listener1.getString("auth"), is("none"));
        JsonObject listener2 = annotation.getJsonObject(1);
        assertThat(listener2.getString("port"), is("9093"));
        assertThat(listener2.getString("tls"), is("true"));
        assertThat(listener2.getString("protocol"), is("kafka"));
        assertThat(listener2.getString("auth"), is("none"));

        assertThat(clusterIp.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(true));
        assertThat(clusterIp.getMetadata().getLabels().get(Labels.STRIMZI_DISCOVERY_LABEL), is("true"));

        TestUtils.checkOwnerReference(clusterIp, KAFKA);
    }

    @ParallelTest
    public void testGenerateServiceWithoutMetrics() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetricsConfig(null)
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        Service clusterIp = kc.generateService();

        assertThat(clusterIp.getSpec().getType(), is("ClusterIP"));
        assertThat(clusterIp.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(clusterIp.getSpec().getPorts().size(), is(3));
        assertThat(clusterIp.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(clusterIp.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(1).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(1).getPort(), is(9092));
        assertThat(clusterIp.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(2).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(2).getPort(), is(9093));
        assertThat(clusterIp.getSpec().getPorts().get(2).getProtocol(), is("TCP"));

        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/port"), is(false));
        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/scrape"), is(false));
        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/path"), is(false));

        assertThat(clusterIp.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(true));
        assertThat(clusterIp.getMetadata().getLabels().get(Labels.STRIMZI_DISCOVERY_LABEL), is("true"));

        TestUtils.checkOwnerReference(clusterIp, KAFKA);
    }

    @ParallelTest
    public void testGenerateHeadlessServiceWithJmxMetrics() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        Service headless = kc.generateHeadlessService();

        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(6));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(KafkaCluster.CONTROLPLANE_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.CONTROLPLANE_PORT));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(headless.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(KafkaCluster.KAFKA_AGENT_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(KafkaCluster.KAFKA_AGENT_PORT));
        assertThat(headless.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(3).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(3).getPort(), is(9092));
        assertThat(headless.getSpec().getPorts().get(3).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(4).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(4).getPort(), is(9093));
        assertThat(headless.getSpec().getPorts().get(4).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(5).getName(), is(JmxModel.JMX_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(5).getPort(), is(JmxModel.JMX_PORT));
        assertThat(headless.getSpec().getPorts().get(5).getProtocol(), is("TCP"));

        assertThat(headless.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(false));

        TestUtils.checkOwnerReference(headless, KAFKA);
    }

    @ParallelTest
    public void testExposesJmxContainerPortWhenJmxEnabled() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            ContainerPort jmxPort = cont.getPorts().stream().filter(port -> JMX_PORT_NAME.equals(port.getName())).findFirst().orElseThrow();
            assertThat(jmxPort.getContainerPort(), is(JMX_PORT));
        }));
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testTemplate() {
        Map<String, String> svcLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> svcAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> hSvcLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> hSvcAnnotations = Map.of("a7", "v7", "a8", "v8");

        Map<String, String> exSvcLabels = Map.of("l9", "v9", "l10", "v10");
        Map<String, String> exSvcAnnotations = Map.of("a9", "v9", "a10", "v10");

        Map<String, String> perPodSvcLabels = Map.of("l11", "v11", "l12", "v12");
        Map<String, String> perPodSvcAnnotations = Map.of("a11", "v11", "a12", "v12");

        Map<String, String> exRouteLabels = Map.of("l13", "v13", "l14", "v14");
        Map<String, String> exRouteAnnotations = Map.of("a13", "v13", "a14", "v14");

        Map<String, String> perPodRouteLabels = Map.of("l15", "v15", "l16", "v16");
        Map<String, String> perPodRouteAnnotations = Map.of("a15", "v15", "a16", "v16");

        Map<String, String> pdbLabels = Map.of("l17", "v17", "l18", "v18");
        Map<String, String> pdbAnnotations = Map.of("a17", "v17", "a18", "v18");

        Map<String, String> crbLabels = Map.of("l19", "v19", "l20", "v20");
        Map<String, String> crbAnnotations = Map.of("a19", "v19", "a20", "v20");

        Map<String, String> saLabels = Map.of("l21", "v21", "l22", "v22");
        Map<String, String> saAnnotations = Map.of("a21", "v21", "a22", "v22");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external2")
                                    .withPort(9095)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .build())
                        .withNewTemplate()
                            .withNewBootstrapService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                            .endBootstrapService()
                            .withNewBrokersService()
                                .withNewMetadata()
                                    .withLabels(hSvcLabels)
                                    .withAnnotations(hSvcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.SINGLE_STACK)
                                .withIpFamilies(IpFamily.IPV6)
                            .endBrokersService()
                            .withNewExternalBootstrapService()
                                .withNewMetadata()
                                    .withLabels(exSvcLabels)
                                    .withAnnotations(exSvcAnnotations)
                                .endMetadata()
                            .endExternalBootstrapService()
                            .withNewPerPodService()
                                .withNewMetadata()
                                    .withLabels(perPodSvcLabels)
                                    .withAnnotations(perPodSvcAnnotations)
                                .endMetadata()
                            .endPerPodService()
                            .withNewExternalBootstrapRoute()
                                .withNewMetadata()
                                .withLabels(exRouteLabels)
                                .withAnnotations(exRouteAnnotations)
                                .endMetadata()
                            .endExternalBootstrapRoute()
                            .withNewPerPodRoute()
                                .withNewMetadata()
                                .withLabels(perPodRouteLabels)
                                .withAnnotations(perPodRouteAnnotations)
                                .endMetadata()
                            .endPerPodRoute()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withLabels(pdbLabels)
                                    .withAnnotations(pdbAnnotations)
                                .endMetadata()
                            .endPodDisruptionBudget()
                            .withNewClusterRoleBinding()
                                .withNewMetadata()
                                    .withLabels(crbLabels)
                                    .withAnnotations(crbAnnotations)
                                .endMetadata()
                            .endClusterRoleBinding()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnnotations)
                                .endMetadata()
                            .endServiceAccount()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check Service
        Service svc = kc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check Headless Service
        svc = kc.generateHeadlessService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(hSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(hSvcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("SingleStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6"));

        // Check External Bootstrap service
        svc = kc.generateExternalBootstrapServices().get(0);
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(exSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(exSvcAnnotations.entrySet()), is(true));

        // Check per pod service
        svc = kc.generatePerPodServices().get(0);
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnnotations.entrySet()), is(true));

        // Check Bootstrap Route
        Route rt = kc.generateExternalBootstrapRoutes().get(0);
        assertThat(rt.getMetadata().getLabels().entrySet().containsAll(exRouteLabels.entrySet()), is(true));
        assertThat(rt.getMetadata().getAnnotations().entrySet().containsAll(exRouteAnnotations.entrySet()), is(true));

        // Check PerPodRoute
        rt = kc.generateExternalRoutes().get(0);
        assertThat(rt.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels.entrySet()), is(true));
        assertThat(rt.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnnotations.entrySet()), is(true));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));


        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kc.generateClusterRoleBinding("namespace");
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnnotations.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnotations.entrySet()), is(true));
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
                    .editKafka()
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
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        Secret jmxSecret = kc.jmx().jmxSecret(null);

        for (Map.Entry<String, String> entry : customAnnotations.entrySet()) {
            assertThat(jmxSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry : customLabels.entrySet()) {
            assertThat(jmxSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    @ParallelTest
    public void testJmxSecret() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder()
                                  .build())
                            .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        Secret jmxSecret = kc.jmx().jmxSecret(null);

        assertThat(jmxSecret.getData(), hasKey("jmx-username"));
        assertThat(jmxSecret.getData(), hasKey("jmx-password"));

        Secret newJmxSecret = kc.jmx().jmxSecret(jmxSecret);

        assertThat(newJmxSecret.getData(), hasKey("jmx-username"));
        assertThat(newJmxSecret.getData(), hasKey("jmx-password"));
        assertThat(newJmxSecret.getData().get("jmx-username"), is(jmxSecret.getData().get("jmx-username")));
        assertThat(newJmxSecret.getData().get("jmx-password"), is(jmxSecret.getData().get("jmx-password")));
    }

    @ParallelTest
    public void testGenerateHeadlessService() {
        Service headless = KC.generateHeadlessService();
        checkHeadlessService(headless);
        TestUtils.checkOwnerReference(headless, KAFKA);
    }

    @ParallelTest
    public void testPerBrokerConfiguration() {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "broker-0", "TLS_9093", "broker-0"),
                1, Map.of("PLAIN_9092", "broker-1", "TLS_9093", "broker-1"),
                2, Map.of("PLAIN_9092", "broker-2", "TLS_9093", "broker-2")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092", "TLS_9093", "10000"),
                1, Map.of("PLAIN_9092", "9092", "TLS_9093", "10001"),
                2, Map.of("PLAIN_9092", "9092", "TLS_9093", "10002")
        );

        String config = KC.generatePerBrokerConfiguration(1, advertisedHostnames, advertisedPorts);

        assertThat(config, CoreMatchers.containsString("broker.id=1"));
        assertThat(config, CoreMatchers.containsString("node.id=1"));
        assertThat(config, CoreMatchers.containsString("log.dirs=/var/lib/kafka/data-0/kafka-log1"));
        assertThat(config, CoreMatchers.containsString("advertised.listeners=CONTROLPLANE-9090://foo-kafka-1.foo-kafka-brokers.test.svc:9090,REPLICATION-9091://foo-kafka-1.foo-kafka-brokers.test.svc:9091,PLAIN-9092://broker-1:9092,TLS-9093://broker-1:10001\n"));
    }

    @ParallelTest
    public void testPerBrokerConfigMaps() {
        MetricsAndLogging metricsAndLogging = new MetricsAndLogging(null, null);
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "broker-0"),
                1, Map.of("PLAIN_9092", "broker-1"),
                2, Map.of("PLAIN_9092", "broker-2")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092"),
                1, Map.of("PLAIN_9092", "9092"),
                2, Map.of("PLAIN_9092", "9092")
        );

        List<ConfigMap> cms = KC.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, advertisedHostnames, advertisedPorts);

        assertThat(cms.size(), is(3));

        for (ConfigMap cm : cms)    {
            assertThat(cm.getData().size(), is(4));
            assertThat(cm.getMetadata().getName(), startsWith("foo-kafka-"));
            KC.getSelectorLabels().toMap().forEach((key, value) -> assertThat(cm.getMetadata().getLabels(), hasEntry(key, value)));
            assertThat(cm.getData().get("log4j.properties"), is(notNullValue()));
            assertThat(cm.getData().get("server.config"), is(notNullValue()));
            assertThat(cm.getData().get("listeners.config"), is("PLAIN_9092 TLS_9093"));
            assertThat(cm.getData().get("metadata.state"), is(notNullValue()));
        }
    }

    @ParallelTest
    public void testPvcNames() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withStorage(new PersistentClaimStorageBuilder().withDeleteClaim(false).withSize("100Gi").build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();

        for (int i = 0; i < REPLICAS; i++) {
            assertThat(pvcs.get(i).getMetadata().getName(),
                    is(VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.kafkaPodName(CLUSTER, i)));
        }

        kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(
                            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize("100Gi").build())
                            .build())
                    .endKafka()
                .endSpec()
                .build();
        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        pvcs = kc.generatePersistentVolumeClaims();

        for (int i = 0; i < REPLICAS; i++) {
            for (int id = 0; id < 2; id++) {
                assertThat(pvcs.get(i + (id * REPLICAS)).getMetadata().getName(),
                        is(VolumeUtils.DATA_VOLUME_NAME + "-" + id + "-" + KafkaResources.kafkaPodName(CLUSTER, i)));
            }
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithClaimDeletion() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withStorageClass("gp2-ssd")
                            .withDeleteClaim(true)
                            .withSize("100Gi")
                        .endPersistentClaimStorage()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithoutClaimDeletion() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage().withStorageClass("gp2-ssd").withDeleteClaim(false).withSize("100Gi").endPersistentClaimStorage()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithOverride() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withStorage(new PersistentClaimStorageBuilder()
                                .withStorageClass("gp2-ssd")
                                .withDeleteClaim(false)
                                .withSize("100Gi")
                                .withOverrides(new PersistentClaimStorageOverrideBuilder()
                                        .withBroker(1)
                                        .withStorageClass("gp2-ssd-az1")
                                        .build())
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);

            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));

            if (i != 1) {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            } else {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd-az1"));
            }

            assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbod() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
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
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(6));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));

            if (i != 1) {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            } else {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd-az1"));
            }

            assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }

        for (int i = 3; i < 6; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("1000Gi")));

            if (i != 4) {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1"));
            } else {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1-az1"));
            }

            assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbodWithOverrides() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(
                                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(6));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }

        for (int i = 3; i < 6; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("1000Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-st1"));
            assertThat(pvc.getMetadata().getName().startsWith(VolumeUtils.DATA_VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
            assertThat(pvc.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
        }
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithClientSecret() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
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
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            // Env Vars
            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        }));
    }

    @ParallelTest
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

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
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
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();

            // Env Vars
            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));

            // Volume mounts
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs/second-certificate"));

            // Volumes
            List<Volume> volumes = pod.getSpec().getVolumes();
            assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_PLAIN_9092_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
        }));
    }

    @ParallelTest
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

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
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
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
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
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
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
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            
            // Test Env Vars
            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));

            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_TLS_9093_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_TLS_9093_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));

            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_EXTERNAL_9094_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_EXTERNAL_9094_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));

            // Volume mounts
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs/second-certificate"));

            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-tls-9093-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-tls-9093-certs/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-tls-9093-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-tls-9093-certs/second-certificate"));

            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-external-9094-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-external-9094-certs/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-external-9094-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-external-9094-certs/second-certificate"));

            // Volumes
            List<Volume> volumes = pod.getSpec().getVolumes();

            assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            assertThat(volumes.stream().filter(vol -> "oauth-tls-9093-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(volumes.stream().filter(vol -> "oauth-tls-9093-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            assertThat(volumes.stream().filter(vol -> "oauth-external-9094-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(volumes.stream().filter(vol -> "oauth-external-9094-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_PLAIN_9092_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
            assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_TLS_9093_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
            assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_EXTERNAL_9094_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));

        }));
    }

    @ParallelTest
    public void testCustomAuthSecretsAreMounted() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("plain")
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .withAuth(
                                new KafkaListenerAuthenticationCustomBuilder()
                                        .withSecrets(new GenericSecretSourceBuilder().withSecretName("test").withKey("foo").build(),
                                                new GenericSecretSourceBuilder().withSecretName("test2").withKey("bar").build())
                                        .build())
                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Volume mounts
            Container container = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            assertThat(container.getVolumeMounts().stream().filter(mount -> "custom-listener-plain-9092-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.CUSTOM_AUTHN_SECRETS_VOLUME_MOUNT + "/custom-listener-plain-9092/test"));
            assertThat(container.getVolumeMounts().stream().filter(mount -> "custom-listener-plain-9092-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.CUSTOM_AUTHN_SECRETS_VOLUME_MOUNT + "/custom-listener-plain-9092/test2"));

            // Volumes
            List<Volume> volumes = pod.getSpec().getVolumes();

            assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
            assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("foo"));
            assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("foo"));

            assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
            assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("bar"));
            assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("bar"));
        }));
    }

    @ParallelTest
    public void testExternalCertificateIngress() {
        String cert = "my-external-cert.crt";
        String key = "my.key";
        String secret = "my-secret";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBrokerCertChainAndKey()
                                        .withCertificate(cert)
                                        .withKey(key)
                                        .withSecretName(secret)
                                    .endBrokerCertChainAndKey()
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            Volume vol = volumes.stream().filter(v -> "custom-external-9094-certs".equals(v.getName())).findFirst().orElse(null);

            assertThat(vol, is(notNullValue()));
            assertThat(vol.getSecret().getSecretName(), is(secret));
            assertThat(vol.getSecret().getItems().get(0).getKey(), is(key));
            assertThat(vol.getSecret().getItems().get(0).getPath(), is("tls.key"));
            assertThat(vol.getSecret().getItems().get(1).getKey(), is(cert));
            assertThat(vol.getSecret().getItems().get(1).getPath(), is("tls.crt"));

            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            VolumeMount mount = cont.getVolumeMounts().stream().filter(v -> "custom-external-9094-certs".equals(v.getName())).findFirst().orElse(null);

            assertThat(mount, is(notNullValue()));
            assertThat(mount.getName(), is("custom-external-9094-certs"));
            assertThat(mount.getMountPath(), is("/opt/kafka/certificates/custom-external-9094-certs"));
        }));
    }

    @ParallelTest
    public void testCustomCertificateTls() {
        String cert = "my-external-cert.crt";
        String key = "my.key";
        String secret = "my-secret";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBrokerCertChainAndKey()
                                        .withCertificate(cert)
                                        .withKey(key)
                                        .withSecretName(secret)
                                    .endBrokerCertChainAndKey()
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            Volume vol = volumes.stream().filter(v -> "custom-tls-9093-certs".equals(v.getName())).findFirst().orElse(null);

            assertThat(vol, is(notNullValue()));
            assertThat(vol.getSecret().getSecretName(), is(secret));
            assertThat(vol.getSecret().getItems().get(0).getKey(), is(key));
            assertThat(vol.getSecret().getItems().get(0).getPath(), is("tls.key"));
            assertThat(vol.getSecret().getItems().get(1).getKey(), is(cert));
            assertThat(vol.getSecret().getItems().get(1).getPath(), is("tls.crt"));

            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            VolumeMount mount = cont.getVolumeMounts().stream().filter(v -> "custom-tls-9093-certs".equals(v.getName())).findFirst().orElse(null);

            assertThat(mount, is(notNullValue()));
            assertThat(mount.getName(), is("custom-tls-9093-certs"));
            assertThat(mount.getMountPath(), is("/opt/kafka/certificates/custom-tls-9093-certs"));
        }));
    }

    @ParallelTest
    public void testGenerateDeploymentWithKeycloakAuthorization() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withMaxSecondsWithoutReauthentication(3600)
                                                .withNewClientSecret()
                                                .withSecretName("my-secret-secret")
                                                .withKey("my-secret-key")
                                                .endClientSecret()
                                                .withDisableTlsHostnameVerification(true)
                                                .withTlsTrustedCertificates(cert1, cert2)
                                                .build())
                                .build())
                    .withAuthorization(
                            new KafkaAuthorizationKeycloakBuilder()
                                    .withClientId("my-client-id")
                                    .withTokenEndpointUri("http://token-endpoint-uri")
                                    .withDisableTlsHostnameVerification(true)
                                    .withDelegateToKafkaAcls(false)
                                    .withGrantsRefreshPeriodSeconds(90)
                                    .withGrantsRefreshPoolSize(4)
                                    .withTlsTrustedCertificates(cert1, cert2)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Volume mounts
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "authz-keycloak-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-keycloak-certs/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "authz-keycloak-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-keycloak-certs/second-certificate"));

            // Volumes
            List<Volume> volumes = pod.getSpec().getVolumes();
            assertThat(volumes.stream().filter(vol -> "authz-keycloak-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(volumes.stream().filter(vol -> "authz-keycloak-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_KEYCLOAK_AUTHZ_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt"));
        }));
    }

    @ParallelTest
    public void testPvcsWithEmptyStorageSelector() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage().withSelector(emptyMap()).withSize("100Gi").endPersistentClaimStorage()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(3));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getSelector(), is(nullValue()));
        }
    }

    @ParallelTest
    public void testPvcsWithSetStorageSelector() {
        Map<String, String> selector = Map.of("foo", "bar");
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage().withSelector(selector).withSize("100Gi").endPersistentClaimStorage()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(3));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getSpec().getSelector().getMatchLabels(), is(selector));
        }
    }


    @ParallelTest
    public void testExternalRoutes() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.ROUTE)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                        .build())
                .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check port
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();
            assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
        }));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getName(), is(KafkaResources.externalBootstrapServiceName(CLUSTER)));
        assertThat(ext.getSpec().getType(), is("ClusterIP"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels().toMap()));
        assertThat(ext.getSpec().getPorts().size(), is(1));
        assertThat(ext.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(ext.getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(ext.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        TestUtils.checkOwnerReference(ext, KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(srv.getSpec().getType(), is("ClusterIP"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaResources.kafkaPodName(CLUSTER, i)));
            assertThat(srv.getSpec().getPorts().size(), is(1));
            assertThat(srv.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(srv.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(srv.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            TestUtils.checkOwnerReference(srv, KAFKA);
        }

        // Check bootstrap route
        Route brt = kc.generateExternalBootstrapRoutes().get(0);
        assertThat(brt.getMetadata().getName(), is(KafkaResources.bootstrapServiceName(CLUSTER)));
        assertThat(brt.getSpec().getTls().getTermination(), is("passthrough"));
        assertThat(brt.getSpec().getTo().getKind(), is("Service"));
        assertThat(brt.getSpec().getTo().getName(), is(KafkaResources.externalBootstrapServiceName(CLUSTER)));
        assertThat(brt.getSpec().getPort().getTargetPort(), is(new IntOrString(9094)));
        TestUtils.checkOwnerReference(brt, KAFKA);

        // Check per pod router
        List<Route> routes = kc.generateExternalRoutes();

        for (int i = 0; i < REPLICAS; i++)  {
            Route rt = routes.get(i);
            assertThat(rt.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(rt.getSpec().getTls().getTermination(), is("passthrough"));
            assertThat(rt.getSpec().getTo().getKind(), is("Service"));
            assertThat(rt.getSpec().getTo().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(rt.getSpec().getPort().getTargetPort(), is(new IntOrString(9094)));
            TestUtils.checkOwnerReference(rt, KAFKA);
        }
    }

    @ParallelTest
    public void testExternalRoutesWithHostOverrides() {
        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig0 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig0.setBroker(0);
        routeListenerBrokerConfig0.setHost("my-host-0.cz");

        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig1 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig1.setBroker(1);
        routeListenerBrokerConfig1.setHost("my-host-1.cz");

        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig2 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig2.setBroker(2);
        routeListenerBrokerConfig2.setHost("my-host-2.cz");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.ROUTE)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-boostrap.cz")
                                    .endBootstrap()
                                    .withBrokers(routeListenerBrokerConfig0, routeListenerBrokerConfig1, routeListenerBrokerConfig2)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check bootstrap route
        Route brt = kc.generateExternalBootstrapRoutes().get(0);
        assertThat(brt.getMetadata().getName(), is(KafkaResources.bootstrapServiceName(CLUSTER)));
        assertThat(brt.getSpec().getHost(), is("my-boostrap.cz"));

        // Check per pod router
        List<Route> routes = kc.generateExternalRoutes();

        for (int i = 0; i < REPLICAS; i++)  {
            Route rt = routes.get(i);
            assertThat(rt.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(rt.getSpec().getHost(), is("my-host-" + i + ".cz"));
        }
    }

    @ParallelTest
    public void testExternalRoutesWithLabelsAndAnnotations() {
        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig0 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig0.setBroker(0);
        routeListenerBrokerConfig0.setAnnotations(Collections.singletonMap("anno", "anno-value-0"));
        routeListenerBrokerConfig0.setLabels(Collections.singletonMap("label", "label-value-0"));

        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig1 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig1.setBroker(1);
        routeListenerBrokerConfig1.setAnnotations(Collections.singletonMap("anno", "anno-value-1"));
        routeListenerBrokerConfig1.setLabels(Collections.singletonMap("label", "label-value-1"));

        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig2 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig2.setBroker(2);
        routeListenerBrokerConfig2.setAnnotations(Collections.singletonMap("anno", "anno-value-2"));
        routeListenerBrokerConfig2.setLabels(Collections.singletonMap("label", "label-value-2"));

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.ROUTE)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withAnnotations(Collections.singletonMap("anno", "anno-value"))
                                        .withLabels(Collections.singletonMap("label", "label-value"))
                                    .endBootstrap()
                                    .withBrokers(routeListenerBrokerConfig0, routeListenerBrokerConfig1, routeListenerBrokerConfig2)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check bootstrap route
        Route brt = kc.generateExternalBootstrapRoutes().get(0);
        assertThat(brt.getMetadata().getName(), is(KafkaResources.bootstrapServiceName(CLUSTER)));
        assertThat(brt.getMetadata().getAnnotations().get("anno"), is("anno-value"));
        assertThat(brt.getMetadata().getLabels().get("label"), is("label-value"));

        // Check per pod router
        List<Route> routes = kc.generateExternalRoutes();

        for (int i = 0; i < REPLICAS; i++)  {
            Route rt = routes.get(i);
            assertThat(rt.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(rt.getMetadata().getAnnotations().get("anno"), is("anno-value-" + i));
            assertThat(rt.getMetadata().getLabels().get("label"), is("label-value-" + i));
        }
    }

    @ParallelTest
    public void testExternalLoadBalancers() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check port
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();
            assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
        }));
        
        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getName(), is(KafkaResources.externalBootstrapServiceName(CLUSTER)));
        assertThat(ext.getMetadata().getFinalizers(), is(emptyList()));
        assertThat(ext.getSpec().getType(), is("LoadBalancer"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels().toMap()));
        assertThat(ext.getSpec().getPorts().size(), is(1));
        assertThat(ext.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(ext.getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(ext.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(ext.getSpec().getLoadBalancerIP(), is(nullValue()));
        assertThat(ext.getSpec().getExternalTrafficPolicy(), is("Cluster"));
        assertThat(ext.getSpec().getLoadBalancerSourceRanges(), is(emptyList()));
        TestUtils.checkOwnerReference(ext, KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(srv.getMetadata().getFinalizers(), is(emptyList()));
            assertThat(srv.getSpec().getType(), is("LoadBalancer"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaResources.kafkaPodName(CLUSTER, i)));
            assertThat(srv.getSpec().getPorts().size(), is(1));
            assertThat(srv.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(srv.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(srv.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            assertThat(srv.getSpec().getLoadBalancerIP(), is(nullValue()));
            assertThat(srv.getSpec().getExternalTrafficPolicy(), is("Cluster"));
            assertThat(srv.getSpec().getLoadBalancerSourceRanges(), is(emptyList()));
            TestUtils.checkOwnerReference(srv, KAFKA);
        }
    }

    @ParallelTest
    public void testExternalLoadBalancersWithoutBootstrapService() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                        .withNewConfiguration()
                            .withCreateBootstrapService(false)
                        .endConfiguration()
                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        assertThat(kc.generateExternalBootstrapServices().isEmpty(), is(true));
    }

    @ParallelTest
    public void testLoadBalancerExternalTrafficPolicyLocalFromListener() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getExternalTrafficPolicy(), is(ExternalTrafficPolicy.LOCAL.toValue()));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getSpec().getExternalTrafficPolicy(), is(ExternalTrafficPolicy.LOCAL.toValue()));
        }
    }

    @ParallelTest
    public void testLoadBalancerExternalTrafficPolicyClusterFromListener() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withExternalTrafficPolicy(ExternalTrafficPolicy.CLUSTER)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getExternalTrafficPolicy(), is(ExternalTrafficPolicy.CLUSTER.toValue()));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getSpec().getExternalTrafficPolicy(), is(ExternalTrafficPolicy.CLUSTER.toValue()));
        }
    }

    @ParallelTest
    public void testFinalizersFromListener() {
        List<String> finalizers = List.of("service.kubernetes.io/load-balancer-cleanup", "my-domain.io/my-custom-finalizer");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withFinalizers(finalizers)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getFinalizers(), is(finalizers));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getMetadata().getFinalizers(), is(finalizers));
        }
    }

    @ParallelTest
    public void testLoadBalancerSourceRangeFromListener() {
        List<String> sourceRanges = List.of("10.0.0.0/8", "130.211.204.1/32");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withLoadBalancerSourceRanges(sourceRanges)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getLoadBalancerSourceRanges(), is(sourceRanges));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getSpec().getLoadBalancerSourceRanges(), is(sourceRanges));
        }
    }

    @ParallelTest
    public void testExternalLoadBalancersWithLabelsAndAnnotations() {
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "bootstrap.my-ingress.com."))
                .withLabels(Collections.singletonMap("label", "label-value"))
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-0.my-ingress.com."))
                .withLabels(Collections.singletonMap("label", "label-value"))
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig2 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(2)
                .withAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-2.my-ingress.com."))
                .withLabels(Collections.singletonMap("label", "label-value"))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBootstrap(bootstrapConfig)
                                    .withBrokers(brokerConfig0, brokerConfig2)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check annotations
        assertThat(kc.generateExternalBootstrapServices().get(0).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "bootstrap.my-ingress.com.")));
        assertThat(kc.generateExternalBootstrapServices().get(0).getMetadata().getLabels().get("label"), is("label-value"));

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.get(0).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-0.my-ingress.com.")));
        assertThat(services.get(0).getMetadata().getLabels().get("label"), is("label-value"));
        assertThat(services.get(1).getMetadata().getAnnotations().isEmpty(), is(true));
        assertThat(services.get(1).getMetadata().getLabels().get("label"), is(nullValue()));
        assertThat(services.get(2).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-2.my-ingress.com.")));
        assertThat(services.get(2).getMetadata().getLabels().get("label"), is("label-value"));
    }

    @ParallelTest
    public void testExternalLoadBalancersWithLoadBalancerIPOverride() {
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withLoadBalancerIP("10.0.0.1")
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withLoadBalancerIP("10.0.0.2")
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig2 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(2)
                .withLoadBalancerIP("10.0.0.3")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBootstrap(bootstrapConfig)
                                    .withBrokers(brokerConfig0, brokerConfig2)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check annotations
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getLoadBalancerIP(), is("10.0.0.1"));

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.get(0).getSpec().getLoadBalancerIP(), is("10.0.0.2"));
        assertThat(services.get(1).getSpec().getLoadBalancerIP(), is(nullValue()));
        assertThat(services.get(2).getSpec().getLoadBalancerIP(), is("10.0.0.3"));
    }

    @ParallelTest
    public void testExternalLoadBalancersWithLoadBalancerClass() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .withNewConfiguration()
                            .withControllerClass("metalLB-class")
                        .endConfiguration()
                        .withTls(true)
                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check Service Class
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getLoadBalancerClass(), is("metalLB-class"));

        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++) {
            Service service = services.get(i);
            assertThat(service.getSpec().getLoadBalancerClass(), is("metalLB-class"));
        }
    }

    @ParallelTest
    public void testExternalNodePortWithLabelsAndAnnotations() {
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "bootstrap.my-ingress.com."))
                .withLabels(Collections.singletonMap("label", "label-value"))
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-0.my-ingress.com."))
                .withLabels(Collections.singletonMap("label", "label-value"))
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig2 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(2)
                .withAnnotations(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-2.my-ingress.com."))
                .withLabels(Collections.singletonMap("label", "label-value"))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBootstrap(bootstrapConfig)
                                    .withBrokers(brokerConfig0, brokerConfig2)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check annotations
        assertThat(kc.generateExternalBootstrapServices().get(0).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "bootstrap.my-ingress.com.")));
        assertThat(kc.generateExternalBootstrapServices().get(0).getMetadata().getLabels().get("label"), is("label-value"));

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.get(0).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-0.my-ingress.com.")));
        assertThat(services.get(0).getMetadata().getLabels().get("label"), is("label-value"));
        assertThat(services.get(1).getMetadata().getAnnotations().isEmpty(), is(true));
        assertThat(services.get(1).getMetadata().getLabels().get("label"), is(nullValue()));
        assertThat(services.get(2).getMetadata().getAnnotations(), is(Collections.singletonMap("external-dns.alpha.kubernetes.io/hostname", "broker-2.my-ingress.com.")));
        assertThat(services.get(2).getMetadata().getLabels().get("label"), is("label-value"));
    }

    @ParallelTest
    public void testExternalNodePorts() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check port
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();
            assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
        }));
        
        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getName(), is(KafkaResources.externalBootstrapServiceName(CLUSTER)));
        assertThat(ext.getSpec().getType(), is("NodePort"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels().toMap()));
        assertThat(ext.getSpec().getPorts().size(), is(1));
        assertThat(ext.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(ext.getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(ext.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        TestUtils.checkOwnerReference(ext, KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(srv.getSpec().getType(), is("NodePort"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaResources.kafkaPodName(CLUSTER, i)));
            assertThat(srv.getSpec().getPorts().size(), is(1));
            assertThat(srv.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(srv.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(srv.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            TestUtils.checkOwnerReference(srv, KAFKA);
        }
    }

    @ParallelTest
    public void testExternalNodePortsWithAddressType() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Check Init container
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElseThrow();
            assertThat(initCont, is(notNullValue()));
            assertThat(initCont.getEnv().stream().filter(env -> KafkaCluster.ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse(""), is("TRUE"));
        }));
    }

    @ParallelTest
    public void testExternalNodePortOverrides() {
        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig.setBroker(0);
        nodePortListenerBrokerConfig.setNodePort(32101);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("external")
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                            .withNewConfiguration()
                                .withNewBootstrap()
                                    .withNodePort(32001)
                                .endBootstrap()
                                .withBrokers(nodePortListenerBrokerConfig)
                            .endConfiguration()
                            .build())
                .endKafka()
            .endSpec()
            .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check port
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();
            assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
        }));
        
        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getName(), is(KafkaResources.externalBootstrapServiceName(CLUSTER)));
        assertThat(ext.getSpec().getType(), is("NodePort"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels().toMap()));
        assertThat(ext.getSpec().getPorts().size(), is(1));
        assertThat(ext.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(ext.getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getNodePort(), is(32001));
        assertThat(ext.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        TestUtils.checkOwnerReference(ext, KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(srv.getSpec().getType(), is("NodePort"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaResources.kafkaPodName(CLUSTER, i)));
            if (i == 0) { // pod with index 0 will have overridden port
                assertThat(srv.getSpec().getPorts().size(), is(1));
                assertThat(srv.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
                assertThat(srv.getSpec().getPorts().get(0).getPort(), is(9094));
                assertThat(srv.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
                assertThat(srv.getSpec().getPorts().get(0).getNodePort(), is(32101));
                assertThat(srv.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            } else {
                assertThat(srv.getSpec().getPorts().size(), is(1));
                assertThat(srv.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
                assertThat(srv.getSpec().getPorts().get(0).getPort(), is(9094));
                assertThat(srv.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
                assertThat(srv.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            }
            TestUtils.checkOwnerReference(srv, KAFKA);
        }
    }

    @ParallelTest
    public void testNodePortWithLoadbalancer() {
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withNodePort(32189)
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withNodePort(32001)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .withTls(true)
                        .withNewConfiguration()
                        .withBootstrap(bootstrapConfig)
                        .withBrokers(brokerConfig0)
                        .endConfiguration()
                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().size(), is(1));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getNodePort(), is(32189));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(kc.generatePerPodServices().get(0).getSpec().getPorts().get(0).getNodePort(), is(32001));

        assertThat(ListenersUtils.bootstrapNodePort(kc.getListeners().get(0)), is(32189));
        assertThat(ListenersUtils.brokerNodePort(kc.getListeners().get(0), 0), is(32001));
    }

    @ParallelTest
    public void testGetExternalNodePortServiceAddressOverrideWithNullAdvertisedHost() {
        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig.setBroker(0);
        nodePortListenerBrokerConfig.setNodePort(32101);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("external")
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                            .withNewConfiguration()
                                .withNewBootstrap()
                                    .withNodePort(32001)
                                .endBootstrap()
                                .withBrokers(nodePortListenerBrokerConfig)
                            .endConfiguration()
                            .build())
                .endKafka()
            .endSpec()
            .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        assertThat(kc.generatePerPodServices().get(0).getSpec().getPorts().get(0).getNodePort(), is(32101));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getNodePort(), is(32001));
        assertThat(ListenersUtils.bootstrapNodePort(kc.getListeners().get(0)), is(32001));
        assertThat(ListenersUtils.brokerNodePort(kc.getListeners().get(0), 0), is(32101));
    }

    @ParallelTest
    public void testGetExternalNodePortServiceAddressOverrideWithNonNullAdvertisedHost() {
        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig.setBroker(0);
        nodePortListenerBrokerConfig.setNodePort(32101);
        nodePortListenerBrokerConfig.setAdvertisedHost("advertised.host");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("external")
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                            .withNewConfiguration()
                                .withNewBootstrap()
                                    .withNodePort(32001)
                                .endBootstrap()
                                .withBrokers(nodePortListenerBrokerConfig)
                            .endConfiguration()
                            .build())
                .endKafka()
            .endSpec()
            .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        assertThat(kc.generatePerPodServices().get(0).getSpec().getPorts().get(0).getNodePort(), is(32101));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getNodePort(), is(32001));

        assertThat(ListenersUtils.bootstrapNodePort(kc.getListeners().get(0)), is(32001));
        assertThat(ListenersUtils.brokerNodePort(kc.getListeners().get(0), 0), is(32101));
        assertThat(ListenersUtils.brokerAdvertisedHost(kc.getListeners().get(0), new NodeRef("foo-kafka-0", 0, "kafka", false, true)), is("advertised.host"));
    }

    @ParallelTest
    public void testGenerateBrokerSecret() throws CertificateParsingException {
        Secret secret = generateBrokerSecret(null, emptyMap());
        assertThat(secret.getData().keySet(), is(Set.of(
                "foo-kafka-0.crt",  "foo-kafka-0.key",
                "foo-kafka-1.crt", "foo-kafka-1.key",
                "foo-kafka-2.crt", "foo-kafka-2.key")));
        X509Certificate cert = Ca.cert(secret, "foo-kafka-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc.cluster.local"),
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc"),
                asList(2, "foo-kafka-bootstrap"),
                asList(2, "foo-kafka-bootstrap.test"),
                asList(2, "foo-kafka-bootstrap.test.svc"),
                asList(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                asList(2, "foo-kafka-brokers"),
                asList(2, "foo-kafka-brokers.test"),
                asList(2, "foo-kafka-brokers.test.svc"),
                asList(2, "foo-kafka-brokers.test.svc.cluster.local"))));
    }

    @ParallelTest
    public void testGenerateBrokerSecretExternal() throws CertificateParsingException {
        Map<Integer, Set<String>> externalAddresses = new HashMap<>();
        externalAddresses.put(0, Collections.singleton("123.10.125.130"));
        externalAddresses.put(1, Collections.singleton("123.10.125.131"));
        externalAddresses.put(2, Collections.singleton("123.10.125.132"));

        Secret secret = generateBrokerSecret(Collections.singleton("123.10.125.140"), externalAddresses);
        assertThat(secret.getData().keySet(), is(Set.of(
                "foo-kafka-0.crt",  "foo-kafka-0.key",
                "foo-kafka-1.crt", "foo-kafka-1.key",
                "foo-kafka-2.crt", "foo-kafka-2.key")));
        X509Certificate cert = Ca.cert(secret, "foo-kafka-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc.cluster.local"),
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc"),
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

    @ParallelTest
    public void testGenerateBrokerSecretExternalWithManyDNS() throws CertificateParsingException {
        Map<Integer, Set<String>> externalAddresses = new HashMap<>();
        externalAddresses.put(0, Set.of("123.10.125.130", "my-broker-0"));
        externalAddresses.put(1, Set.of("123.10.125.131", "my-broker-1"));
        externalAddresses.put(2, Set.of("123.10.125.132", "my-broker-2"));

        Secret secret = generateBrokerSecret(Set.of("123.10.125.140", "my-bootstrap"), externalAddresses);
        assertThat(secret.getData().keySet(), is(Set.of(
                "foo-kafka-0.crt",  "foo-kafka-0.key",
                "foo-kafka-1.crt", "foo-kafka-1.key",
                "foo-kafka-2.crt", "foo-kafka-2.key")));
        X509Certificate cert = Ca.cert(secret, "foo-kafka-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc.cluster.local"),
                asList(2, "foo-kafka-0.foo-kafka-brokers.test.svc"),
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

    @ParallelTest
    public void testControlPlanePortNetworkPolicy() {
        NetworkPolicyPeer kafkaBrokersPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER)))
                .endPodSelector()
                .build();

        // Check Network Policies => Different namespace
        NetworkPolicy np = KC.generateNetworkPolicy("operator-namespace", null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CONTROLPLANE_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CONTROLPLANE_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(1));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
    }

    @ParallelTest
    public void testReplicationPortNetworkPolicy() {
        NetworkPolicyPeer kafkaBrokersPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer eoPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer kafkaExporterPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaExporterResources.componentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer cruiseControlPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, CruiseControlResources.componentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeerSameNamespace = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeerNamespaceWithLabels = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector()
                    .withMatchLabels(Collections.singletonMap("nsLabelKey", "nsLabelValue"))
                .endNamespaceSelector()
                .build();

        // Check Network Policies => Different namespace
        NetworkPolicy np = KC.generateNetworkPolicy("operator-namespace", null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeer), is(true));

        // Check Network Policies => Same namespace
        np = KC.generateNetworkPolicy(NAMESPACE, null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeerSameNamespace), is(true));

        // Check Network Policies => Namespace with Labels
        np = KC.generateNetworkPolicy("operator-namespace", Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeerNamespaceWithLabels), is(true));
    }

    @ParallelTest
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

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withNetworkPolicyPeers(peer1)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .withNetworkPolicyPeers(peer2)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .withNetworkPolicyPeers(peer1, peer2)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check Network Policies
        NetworkPolicy np = kc.generateNetworkPolicy(null, null);

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9092))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().get(0), is(peer1));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9093))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().get(0), is(peer2));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9094))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().size(), is(2));
        assertThat(rules.get(0).getFrom().contains(peer1), is(true));
        assertThat(rules.get(0).getFrom().contains(peer2), is(true));
    }

    @ParallelTest
    public void testNoNetworkPolicyPeers() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check Network Policies
        NetworkPolicy np = kc.generateNetworkPolicy(null, null);

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9092))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9093))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9094))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget()   {
        PodDisruptionBudget pdb = KC.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(2));
        assertThat(pdb.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().toMap()));
    }

    @ParallelTest
    public void testCustomizedPodDisruptionBudget()   {
        Map<String, String> pdbLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> pdbAnnotations = Map.of("a1", "v1", "a2", "v2");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withAnnotations(pdbAnnotations)
                                    .withLabels(pdbLabels)
                                .endMetadata()
                                .withMaxUnavailable(2)
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();

        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(1));
        assertThat(pdb.getSpec().getSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
    }

    @ParallelTest
    public void testExternalServiceWithDualStackNetworking() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("np")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                        .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                                    .endConfiguration()
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("lb")
                                    .withPort(9095)
                                    .withType(KafkaListenerType.LOADBALANCER)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                        .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                                    .endConfiguration()
                                    .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        List<Service> services = new ArrayList<>();
        services.addAll(kc.generateExternalBootstrapServices());
        services.addAll(kc.generatePerPodServices());

        for (Service svc : services)    {
            assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
            assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));
        }
    }

    @ParallelTest
    public void testGetExternalServiceAdvertisedHostAndPortOverride() {
        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig0 = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig0.setBroker(0);
        nodePortListenerBrokerConfig0.setAdvertisedHost("my-host-0.cz");
        nodePortListenerBrokerConfig0.setAdvertisedPort(10000);

        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig1 = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig1.setBroker(1);
        nodePortListenerBrokerConfig1.setAdvertisedHost("my-host-1.cz");
        nodePortListenerBrokerConfig1.setAdvertisedPort(10001);

        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig2 = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig2.setBroker(2);
        nodePortListenerBrokerConfig2.setAdvertisedHost("my-host-2.cz");
        nodePortListenerBrokerConfig2.setAdvertisedPort(10002);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBrokers(nodePortListenerBrokerConfig0, nodePortListenerBrokerConfig1, nodePortListenerBrokerConfig2)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        assertThat(ListenersUtils.brokerAdvertisedPort(kc.getListeners().get(0), 0), is(10000));
        assertThat(ListenersUtils.brokerAdvertisedHost(kc.getListeners().get(0), new NodeRef("foo-kafka-0", 0, "kafka", false, true)), is("my-host-0.cz"));

        assertThat(ListenersUtils.brokerAdvertisedPort(kc.getListeners().get(0), 1), is(10001));
        assertThat(ListenersUtils.brokerAdvertisedHost(kc.getListeners().get(0), new NodeRef("foo-kafka-1", 1, "kafka", false, true)), is("my-host-1.cz"));

        assertThat(ListenersUtils.brokerAdvertisedPort(kc.getListeners().get(0), 2), is(10002));
        assertThat(ListenersUtils.brokerAdvertisedHost(kc.getListeners().get(0), new NodeRef("foo-kafka-2", 2, "kafka", false, true)), is("my-host-2.cz"));
    }

    @ParallelTest
    public void testGetExternalServiceWithoutAdvertisedHostAndPortOverride() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(true)
                        .build())
                .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        assertThat(ListenersUtils.brokerAdvertisedPort(kc.getListeners().get(0), 0), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedHost(kc.getListeners().get(0), new NodeRef("foo-kafka-0", 0, "kafka", false, true)), is(nullValue()));

        assertThat(ListenersUtils.brokerAdvertisedPort(kc.getListeners().get(0), 1), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedHost(kc.getListeners().get(0), new NodeRef("foo-kafka-1", 1, "kafka", false, true)), is(nullValue()));

        assertThat(ListenersUtils.brokerAdvertisedPort(kc.getListeners().get(0), 2), is(nullValue()));
        assertThat(ListenersUtils.brokerAdvertisedHost(kc.getListeners().get(0), new NodeRef("foo-kafka-2", 2, "kafka", false, true)), is(nullValue()));
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbodWithTemplate() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
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
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(6));

        for (int i = 0; i < 6; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getMetadata().getLabels().get("testLabel"), is("testValue"));
            assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is("testValue"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsJbodWithoutVolumes() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withStorage(new JbodStorageBuilder().withVolumes(List.of())
                            .build())
                    .endKafka()
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testStorageValidationAfterInitialDeployment() {
        assertThrows(InvalidResourceException.class, () -> {
            Storage oldStorage = new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withSize("100Gi").build())
                    .build();

            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withStorage(new JbodStorageBuilder().withVolumes(List.of())
                            .build())
                    .endKafka()
                    .endSpec()
                    .build();
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", oldStorage),
                Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", IntStream.range(0, REPLICAS).mapToObj(i -> kafkaAssembly.getMetadata().getName() + "-kafka-" + i).toList()), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testStorageReverting() {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage ephemeral = new EphemeralStorageBuilder().build();

        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        // Test Storage changes and how they are reverted

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withStorage(jbod)
                        .withConfig(Map.of("default.replication.factor", 3, "min.insync.replicas", 2))
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", ephemeral),
            Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", IntStream.range(0, REPLICAS).mapToObj(i -> CLUSTER + "-kafka-" + i).toList()), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Storage is reverted
        assertThat(kc.getStorageByPoolName(), is(Map.of("kafka", ephemeral)));

        // Warning status condition is set
        assertThat(kc.getWarningConditions().size(), is(1));
        assertThat(kc.getWarningConditions().get(0).getReason(), is("KafkaStorage"));

        kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withStorage(jbod)
                        .withConfig(Map.of("default.replication.factor", 3, "min.insync.replicas", 2))
                    .endKafka()
                .endSpec()
                .build();
        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", persistent),
            Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", IntStream.range(0, REPLICAS).mapToObj(i -> CLUSTER + "-kafka-" + i).toList()), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Storage is reverted
        assertThat(kc.getStorageByPoolName(), is(Map.of("kafka", persistent)));

        // Warning status condition is set
        assertThat(kc.getWarningConditions().size(), is(1));
        assertThat(kc.getWarningConditions().get(0).getReason(), is("KafkaStorage"));

        kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withStorage(ephemeral)
                        .withConfig(Map.of("default.replication.factor", 3, "min.insync.replicas", 2))
                    .endKafka()
                .endSpec()
                .build();
        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", jbod),
            Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", IntStream.range(0, REPLICAS).mapToObj(i -> CLUSTER + "-kafka-" + i).toList()), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Storage is reverted
        assertThat(kc.getStorageByPoolName(), is(Map.of("kafka", jbod)));

        // Warning status condition is set
        assertThat(kc.getWarningConditions().size(), is(1));
        assertThat(kc.getWarningConditions().get(0).getReason(), is("KafkaStorage"));

        kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withStorage(persistent)
                        .withConfig(Map.of("default.replication.factor", 3, "min.insync.replicas", 2))
                    .endKafka()
                .endSpec()
                .build();
        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", jbod),
            Map.of(kafkaAssembly.getMetadata().getName() + "-kafka", IntStream.range(0, REPLICAS).mapToObj(i -> CLUSTER + "-kafka-" + i).toList()), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Storage is reverted
        assertThat(kc.getStorageByPoolName(), is(Map.of("kafka", jbod)));

        // Warning status condition is set
        assertThat(kc.getWarningConditions().size(), is(1));
        assertThat(kc.getWarningConditions().get(0).getReason(), is("KafkaStorage"));
    }

    @ParallelTest
    public void testExternalIngress() {
        GenericKafkaListenerConfigurationBroker broker0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-broker-kafka-0.com")
                .withLabels(Collections.singletonMap("label", "label-value"))
                .withAnnotations(Collections.singletonMap("dns-annotation", "my-kafka-broker.com"))
                .withBroker(0)
                .build();

        GenericKafkaListenerConfigurationBroker broker1 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-broker-kafka-1.com")
                .withLabels(Collections.singletonMap("label", "label-value"))
                .withAnnotations(Collections.singletonMap("dns-annotation", "my-kafka-broker.com"))
                .withBroker(1)
                .build();

        GenericKafkaListenerConfigurationBroker broker2 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-broker-kafka-2.com")
                .withLabels(Collections.singletonMap("label", "label-value"))
                .withAnnotations(Collections.singletonMap("dns-annotation", "my-kafka-broker.com"))
                .withBroker(2)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.INGRESS)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Collections.singletonMap("dns-annotation", "my-kafka-bootstrap.com"))
                                        .withLabels(Collections.singletonMap("label", "label-value"))
                                    .endBootstrap()
                                    .withBrokers(broker0, broker1, broker2)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        assertThat(kc.getListeners().stream().findFirst().orElseThrow().getType(), is(KafkaListenerType.INGRESS));

        // Check port
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();
            assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
        }));
        
        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getName(), is(KafkaResources.externalBootstrapServiceName(CLUSTER)));
        assertThat(ext.getSpec().getType(), is("ClusterIP"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels().toMap()));
        assertThat(ext.getSpec().getPorts().size(), is(1));
        assertThat(ext.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(ext.getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(ext.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        TestUtils.checkOwnerReference(ext, KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(srv.getSpec().getType(), is("ClusterIP"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaResources.kafkaPodName(CLUSTER, i)));
            assertThat(srv.getSpec().getPorts().size(), is(1));
            assertThat(srv.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(srv.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(srv.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            TestUtils.checkOwnerReference(srv, KAFKA);
        }

        // Check bootstrap ingress
        Ingress bing = kc.generateExternalBootstrapIngresses().get(0);
        assertThat(bing.getMetadata().getName(), is(KafkaResources.bootstrapServiceName(CLUSTER)));
        assertThat(bing.getSpec().getIngressClassName(), is(nullValue()));
        assertThat(bing.getMetadata().getAnnotations().get("dns-annotation"), is("my-kafka-bootstrap.com"));
        assertThat(bing.getMetadata().getLabels().get("label"), is("label-value"));
        assertThat(bing.getSpec().getTls().size(), is(1));
        assertThat(bing.getSpec().getTls().get(0).getHosts().size(), is(1));
        assertThat(bing.getSpec().getTls().get(0).getHosts().get(0), is("my-kafka-bootstrap.com"));
        assertThat(bing.getSpec().getRules().size(), is(1));
        assertThat(bing.getSpec().getRules().get(0).getHost(), is("my-kafka-bootstrap.com"));
        assertThat(bing.getSpec().getRules().get(0).getHttp().getPaths().size(), is(1));
        assertThat(bing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getPath(), is("/"));
        assertThat(bing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getService().getName(), is(KafkaResources.externalBootstrapServiceName(CLUSTER)));
        assertThat(bing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getService().getPort().getNumber(), is(9094));
        TestUtils.checkOwnerReference(bing, KAFKA);

        // Check per pod ingress
        List<Ingress> ingresses = kc.generateExternalIngresses();

        for (int i = 0; i < REPLICAS; i++)  {
            Ingress ing = ingresses.get(i);
            assertThat(ing.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(ing.getSpec().getIngressClassName(), is(nullValue()));
            assertThat(ing.getMetadata().getAnnotations().get("dns-annotation"), is("my-kafka-broker.com"));
            assertThat(ing.getMetadata().getLabels().get("label"), is("label-value"));
            assertThat(ing.getSpec().getTls().size(), is(1));
            assertThat(ing.getSpec().getTls().get(0).getHosts().size(), is(1));
            assertThat(ing.getSpec().getTls().get(0).getHosts().get(0), is(String.format("my-broker-kafka-%d.com", i)));
            assertThat(ing.getSpec().getRules().size(), is(1));
            assertThat(ing.getSpec().getRules().get(0).getHost(), is(String.format("my-broker-kafka-%d.com", i)));
            assertThat(ing.getSpec().getRules().get(0).getHttp().getPaths().size(), is(1));
            assertThat(ing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getPath(), is("/"));
            assertThat(ing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getService().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(ing.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getService().getPort().getNumber(), is(9094));
            TestUtils.checkOwnerReference(ing, KAFKA);
        }
    }

    @ParallelTest
    public void testExternalIngressClass() {
        GenericKafkaListenerConfigurationBroker broker0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-broker-kafka-0.com")
                .withBroker(0)
                .build();

        GenericKafkaListenerConfigurationBroker broker1 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-broker-kafka-1.com")
                .withBroker(1)
                .build();

        GenericKafkaListenerConfigurationBroker broker2 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-broker-kafka-2.com")
                .withBroker(2)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.INGRESS)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withControllerClass("nginx-internal")
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Collections.singletonMap("dns-annotation", "my-kafka-bootstrap.com"))
                                    .endBootstrap()
                                    .withBrokers(broker0, broker1, broker2)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

         // Check bootstrap ingress
        Ingress bing = kc.generateExternalBootstrapIngresses().get(0);
        assertThat(bing.getSpec().getIngressClassName(), is("nginx-internal"));

        // Check per pod ingress
        List<Ingress> ingresses = kc.generateExternalIngresses();

        for (int i = 0; i < REPLICAS; i++)  {
            Ingress ing = ingresses.get(i);
            assertThat(ing.getSpec().getIngressClassName(), is("nginx-internal"));
        }
    }

    @ParallelTest
    public void testExternalIngressMissingConfiguration() {
        GenericKafkaListenerConfigurationBroker broker0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.INGRESS)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withControllerClass("nginx-internal")
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Collections.singletonMap("dns-annotation", "my-kafka-bootstrap.com"))
                                    .endBootstrap()
                                    .withBrokers(broker0)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        assertThrows(InvalidResourceException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }


    @ParallelTest
    public void testClusterIP() {
        GenericKafkaListenerConfigurationBroker broker0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withAdvertisedHost("my-ingress.com")
                .withAdvertisedPort(9990)
                .withLabels(Collections.singletonMap("label", "label-value"))
                .withBroker(0)
                .build();

        GenericKafkaListenerConfigurationBroker broker1 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withAdvertisedHost("my-ingress.com")
                .withAdvertisedPort(9991)
                .withLabels(Collections.singletonMap("label", "label-value"))
                .withBroker(1)
                .build();

        GenericKafkaListenerConfigurationBroker broker2 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withAdvertisedHost("my-ingress.com")
                .withAdvertisedPort(9992)
                .withLabels(Collections.singletonMap("label", "label-value"))
                .withBroker(2)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.CLUSTER_IP)
                        .withTls(true)
                        .withNewConfiguration()
                        .withBrokers(broker0, broker1, broker2)
                        .endConfiguration()
                        .build())
                .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        assertThat(kc.getListeners().stream().findFirst().orElseThrow().getType(), is(KafkaListenerType.CLUSTER_IP));

        // Check port
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();
            assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
        }));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getName(), is(KafkaResources.externalBootstrapServiceName(CLUSTER)));
        assertThat(ext.getSpec().getType(), is("ClusterIP"));
        assertThat(ext.getSpec().getSelector(), is(kc.getSelectorLabels().toMap()));
        assertThat(ext.getSpec().getPorts().size(), is(1));
        assertThat(ext.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(ext.getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(ext.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        TestUtils.checkOwnerReference(ext, KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER) + "-" + i));
            assertThat(srv.getSpec().getType(), is("ClusterIP"));
            assertThat(srv.getSpec().getSelector().get(Labels.KUBERNETES_STATEFULSET_POD_LABEL), is(KafkaResources.kafkaPodName(CLUSTER, i)));
            assertThat(srv.getSpec().getPorts().size(), is(1));
            assertThat(srv.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(srv.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getTargetPort().getIntVal(), is(9094));
            assertThat(srv.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(srv.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            TestUtils.checkOwnerReference(srv, KAFKA);
        }

    }
    @ParallelTest
    public void testClusterIPMissingConfiguration() {
        GenericKafkaListenerConfigurationBroker broker0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withType(KafkaListenerType.CLUSTER_IP)
                        .withTls(false)
                        .withNewConfiguration()
                        .withBrokers(broker0)
                        .endConfiguration()
                        .build())
                .endKafka()
                .endSpec()
                .build();

        assertThrows(InvalidResourceException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }
    @ParallelTest
    public void testClusterRoleBindingNodePort() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb.getMetadata().getName(), is(KafkaResources.initContainerClusterRoleBindingName(CLUSTER, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(kc.componentName));
    }

    @ParallelTest
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withNewRack("my-topology-label")
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb.getMetadata().getName(), is(KafkaResources.initContainerClusterRoleBindingName(CLUSTER, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(kc.componentName));
    }

    @ParallelTest
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        ClusterRoleBinding crb = KC.generateClusterRoleBinding(testNamespace);

        assertThat(crb, is(nullValue()));
    }

    @ParallelTest
    public void testGenerateDeploymentWithKeycloakAuthorizationMissingOAuthListeners() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withAuthorization(
                            new KafkaAuthorizationKeycloakBuilder()
                                    .build())
                    .endKafka()
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testReplicasAndRelatedOptionsValidationNok() {
        String propertyName = "offsets.topic.replication.factor";
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(singletonMap(propertyName, REPLICAS + 1))
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER));
        assertThat(ex.getMessage(), is("Kafka configuration option '" + propertyName + "' should be set to " + REPLICAS + " or less because this cluster has only " + REPLICAS + " Kafka broker(s)."));
    }

    @ParallelTest
    public void testReplicasAndRelatedOptionsValidationOk() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(singletonMap("offsets.topic.replication.factor", REPLICAS - 1))
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);       
    }

    @ParallelTest
    public void testCruiseControl() {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "broker-0", "TLS_9093", "broker-0"),
                1, Map.of("PLAIN_9092", "broker-1", "TLS_9093", "broker-1"),
                2, Map.of("PLAIN_9092", "broker-2", "TLS_9093", "broker-2")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092", "TLS_9093", "10000"),
                1, Map.of("PLAIN_9092", "9092", "TLS_9093", "10001"),
                2, Map.of("PLAIN_9092", "9092", "TLS_9093", "10002")
        );

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .withNewCruiseControl()
                .endCruiseControl()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        String brokerConfig = kafkaCluster.generatePerBrokerConfiguration(1, advertisedHostnames, advertisedPorts);

        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + 1));
    }

    @ParallelTest
    public void testCruiseControlCustomMetricsReporterTopic() {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "broker-0", "TLS_9093", "broker-0"),
                1, Map.of("PLAIN_9092", "broker-1", "TLS_9093", "broker-1"),
                2, Map.of("PLAIN_9092", "broker-2", "TLS_9093", "broker-2")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092", "TLS_9093", "10000"),
                1, Map.of("PLAIN_9092", "9092", "TLS_9093", "10001"),
                2, Map.of("PLAIN_9092", "9092", "TLS_9093", "10002")
        );

        int replicationFactor = 3;
        int minInSync = 2;
        int partitions = 5;
        Map<String, Object> config = new HashMap<>();
        config.put(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS.getValue(), partitions);
        config.put(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue(), replicationFactor);
        config.put(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), minInSync);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(config)
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        String brokerConfig = kafkaCluster.generatePerBrokerConfiguration(1, advertisedHostnames, advertisedPorts);

        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + partitions));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + replicationFactor));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));
    }

    @ParallelTest
    public void testCruiseControlCustomMetricsReporterTopicMinInSync() {
        Map<Integer, Map<String, String>> advertisedHostnames = Map.of(
                0, Map.of("PLAIN_9092", "broker-0", "TLS_9093", "broker-0"),
                1, Map.of("PLAIN_9092", "broker-1", "TLS_9093", "broker-1"),
                2, Map.of("PLAIN_9092", "broker-2", "TLS_9093", "broker-2")
        );
        Map<Integer, Map<String, String>> advertisedPorts = Map.of(
                0, Map.of("PLAIN_9092", "9092", "TLS_9093", "10000"),
                1, Map.of("PLAIN_9092", "9092", "TLS_9093", "10001"),
                2, Map.of("PLAIN_9092", "9092", "TLS_9093", "10002")
        );

        int minInSync = 1;
        Map<String, Object> config = new HashMap<>();
        config.put(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), minInSync);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(config)
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        String brokerConfig = kc.generatePerBrokerConfiguration(1, advertisedHostnames, advertisedPorts);

        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));
    }

    @ParallelTest
    public void testCruiseControlWithSingleNodeKafka() {
        Map<String, Object> config = new HashMap<>();
        config.put("offsets.topic.replication.factor", 1);
        config.put("transaction.state.log.replication.factor", 1);
        config.put("transaction.state.log.min.isr", 1);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withReplicas(1)
                        .withConfig(config)
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });

        assertThat(ex.getMessage(), is("Kafka " + NAMESPACE + "/" + CLUSTER + " has invalid configuration. " +
                "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers."));
    }

    @ParallelTest
    public void testCruiseControlWithMinISRgtReplicas() {
        Map<String, Object> config = new HashMap<>();
        int minInSyncReplicas = 3;
        config.put(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue(), 2);
        config.put(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), minInSyncReplicas);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(config)
                    .endKafka()
                .withNewCruiseControl()
                .endCruiseControl()
                .endSpec()
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testCruiseControlWithMinISRgtDefaultReplicas() {
        Map<String, Object> config = new HashMap<>();
        int minInSyncReplicas = 2;
        config.put(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), minInSyncReplicas);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(config)
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetricsConfig(metrics)
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        assertThat(kc.metrics().isEnabled(), is(true));
        assertThat(kc.metrics().getConfigMapName(), is("my-metrics-configuration"));
        assertThat(kc.metrics().getConfigMapKey(), is("config.yaml"));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        assertThat(KC.metrics().isEnabled(), is(false));
        assertThat(KC.metrics().getConfigMapName(), is(nullValue()));
        assertThat(KC.metrics().getConfigMapKey(), is(nullValue()));
    }

    @ParallelTest
    public void testKafkaInitContainerSectionIsConfigurable() {
        Map<String, Quantity> limits = new HashMap<>();
        limits.put("cpu", Quantity.parse("1"));
        limits.put("memory", Quantity.parse("256Mi"));

        Map<String, Quantity> requirements = new HashMap<>();
        requirements.put("cpu", Quantity.parse("100m"));
        requirements.put("memory", Quantity.parse("128Mi"));

        ResourceRequirements resourceReq = new ResourceRequirementsBuilder()
            .withLimits(limits)
            .withRequests(requirements)
            .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withResources(resourceReq)
                    .withNewRack()
                        .withTopologyKey("rack-key")
                    .endRack()
                .endKafka()
            .endSpec()
            .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            ResourceRequirements initContainersResources = pod.getSpec().getInitContainers().stream().findAny().orElseThrow().getResources();
            assertThat(initContainersResources.getRequests(), is(requirements));
            assertThat(initContainersResources.getLimits(), is(limits));
        }));
    }

    @ParallelTest
    public void testInvalidVersion() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("6.6.6")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 6.6.6. Supported versions are:"));
    }

    @ParallelTest
    public void testUnsupportedVersion() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("2.6.0")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 2.6.0. Supported versions are:"));
    }

    @ParallelTest
    public void testInvalidVersionWithCustomImage() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("2.6.0")
                        .withImage("my-custom/image:latest")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 2.6.0. Supported versions are:"));
    }

    @ParallelTest
    public void withAffinityWithoutRack() throws IOException {
        AtomicReference<KafkaPool> pool = new AtomicReference<>();

        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly1, versions) -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            pool.set(pools.get(0));
            return KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, pools, versions, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        }, getClass().getSimpleName() + ".withAffinityWithoutRack");

        resourceTester.assertDesiredModel(".yaml", model -> PodSetUtils.podSetToPods(model.generatePodSets(true, null, null, node -> Map.of()).stream().findFirst().orElseThrow()).stream().findFirst().orElseThrow().getSpec().getAffinity());
    }

    @ParallelTest
    public void withRackWithoutAffinity() throws IOException {
        AtomicReference<KafkaPool> pool = new AtomicReference<>();

        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly1, versions) -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            pool.set(pools.get(0));
            return KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, pools, versions, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        }, getClass().getSimpleName() + ".withRackWithoutAffinity");

        resourceTester.assertDesiredModel(".yaml", model -> PodSetUtils.podSetToPods(model.generatePodSets(true, null, null, node -> Map.of()).stream().findFirst().orElseThrow()).stream().findFirst().orElseThrow().getSpec().getAffinity());

    }

    @ParallelTest
    public void withRackAndAffinityWithMoreTerms() throws IOException {
        AtomicReference<KafkaPool> pool = new AtomicReference<>();

        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly1, versions) -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            pool.set(pools.get(0));
            return KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, pools, versions, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        }, getClass().getSimpleName() + ".withRackAndAffinityWithMoreTerms");

        resourceTester.assertDesiredModel(".yaml", model -> PodSetUtils.podSetToPods(model.generatePodSets(true, null, null, node -> Map.of()).stream().findFirst().orElseThrow()).stream().findFirst().orElseThrow().getSpec().getAffinity());
    }

    @ParallelTest
    public void withRackAndAffinity() throws IOException {
        AtomicReference<KafkaPool> pool = new AtomicReference<>();

        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly1, versions) -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            pool.set(pools.get(0));
            return KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, pools, versions, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        }, getClass().getSimpleName() + ".withRackAndAffinity");

        resourceTester.assertDesiredModel(".yaml", model -> PodSetUtils.podSetToPods(model.generatePodSets(true, null, null, node -> Map.of()).stream().findFirst().orElseThrow()).stream().findFirst().orElseThrow().getSpec().getAffinity());
    }

    @ParallelTest
    public void withTolerations() throws IOException {
        ResourceTester<Kafka, KafkaCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly1, versions) -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
            return KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly1, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        }, getClass().getSimpleName() + ".withTolerations");

        resourceTester.assertDesiredResource(".yaml", cr -> cr.getSpec().getKafka().getTemplate().getPod().getTolerations());
    }

    @ParallelTest
    public void testInvalidInterBrokerProtocolAndLogMessageFormatOnKRaftMigration() {
        // invalid values ... metadata missing (it gets the Kafka version), inter broker protocol and log message format lower than Kafka version
        Map<String, Object> config = new HashMap<>();
        config.put("inter.broker.protocol.version", "3.6");
        config.put("log.message.format.version", "3.6");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("3.9.0")
                        .withConfig(config)
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

            KafkaVersion kafkaVersion = VERSIONS.supportedVersion(kafka.getSpec().getKafka().getVersion());
            KafkaVersionChange kafkaVersionChange = new KafkaVersionChange(
                    kafkaVersion,
                    kafkaVersion,
                    VERSIONS.version("3.6.0").protocolVersion(),
                    VERSIONS.version("3.6.0").messageVersion(),
                    // as per ZooKeeperVersionChangeCreator, when migration, we set missing metadata version to the Kafka version
                    kafkaVersion.metadataVersion());
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, kafkaVersionChange, KafkaMetadataConfigurationState.PRE_MIGRATION, null, SHARED_ENV_PROVIDER);
        });

        assertThat(ex.getMessage(), containsString("Migration cannot be performed with Kafka version 3.9-IV0, metadata version 3.9-IV0, inter.broker.protocol.version 3.6-IV2, log.message.format.version 3.6-IV2."));
    }

    @ParallelTest
    public void testInvalidMetadataVersionOnKRaftMigration() {
        // invalid values ... metadata lower than Kafka version, inter broker protocol and log message format missing (they get the Kafka version)
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("3.9.0")
                        .withMetadataVersion("3.6-IV2")
                        .withConfig(Map.of())
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

            KafkaVersion kafkaVersion = VERSIONS.supportedVersion(kafka.getSpec().getKafka().getVersion());
            KafkaVersionChange kafkaVersionChange = new KafkaVersionChange(
                    kafkaVersion,
                    kafkaVersion,
                    // as per ZooKeeperVersionChangeCreator, we set missing inter broker protocol and log message format to the Kafka version
                    kafkaVersion.protocolVersion(),
                    kafkaVersion.messageVersion(),
                    kafka.getSpec().getKafka().getMetadataVersion());
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, kafkaVersionChange, KafkaMetadataConfigurationState.PRE_MIGRATION, null, SHARED_ENV_PROVIDER);
        });

        assertThat(ex.getMessage(), containsString("Migration cannot be performed with Kafka version 3.9-IV0, metadata version 3.6-IV2, inter.broker.protocol.version 3.9-IV0, log.message.format.version 3.9-IV0."));
    }

    @ParallelTest
    public void testValidVersionsOnKRaftMigration() {
        Map<String, Object> config = new HashMap<>();
        config.put("inter.broker.protocol.version", "3.7");
        config.put("log.message.format.version", "3.7");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("3.9.0")
                        .withMetadataVersion("3.9-IV0")
                        .withConfig(config)
                .endKafka()
                .endSpec()
                .build();

        assertDoesNotThrow(() -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

            KafkaVersion kafkaVersion = VERSIONS.supportedVersion(kafka.getSpec().getKafka().getVersion());
            KafkaVersionChange kafkaVersionChange = new KafkaVersionChange(
                    kafkaVersion,
                    kafkaVersion,
                    kafkaVersion.protocolVersion(),
                    kafkaVersion.messageVersion(),
                    kafka.getSpec().getKafka().getMetadataVersion());
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, kafkaVersionChange, KafkaMetadataConfigurationState.PRE_MIGRATION, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testNodePortWithBootstrapExternalIPs() {
        // set externalIP
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withNodePort(32100)
                .withExternalIPs(List.of("10.0.0.1"))
                .build();
        
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBootstrap(bootstrapConfig)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
       
        List<Service> services = kc.generateExternalBootstrapServices();
        assertThat(services.get(0).getSpec().getType(), is("NodePort"));
        assertEquals(services.get(0).getSpec().getExternalIPs(), List.of("10.0.0.1"));
    }
    
    @ParallelTest
    public void testNodePortWithBrokerExternalIPs() {    
        //set externalIP 
        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig.setBroker(0);
        nodePortListenerBrokerConfig.setNodePort(32000);
        nodePortListenerBrokerConfig.setAdvertisedHost("advertised.host");
        nodePortListenerBrokerConfig.setExternalIPs(List.of("10.0.0.1"));
        
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                .withBrokers(nodePortListenerBrokerConfig)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
       
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.get(0).getSpec().getType(), is("NodePort"));
        assertEquals(services.get(0).getSpec().getExternalIPs(), List.of("10.0.0.1"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOpa() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withAuthorization(
                            new KafkaAuthorizationOpaBuilder()
                                    .withUrl("http://opa:8080")
                                    .withTlsTrustedCertificates(cert1, cert2)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Volume mounts
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "authz-opa-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-opa-certs/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "authz-opa-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-opa-certs/second-certificate"));

            // Volumes
            List<Volume> volumes = pod.getSpec().getVolumes();
            assertThat(volumes.stream().filter(vol -> "authz-opa-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(volumes.stream().filter(vol -> "authz-opa-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_OPA_AUTHZ_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt"));
        }));
    }

    @ParallelTest
    public void testPublishNotReadyAddressesFromListener() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("nodeport")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withNewConfiguration()
                                    .withPublishNotReadyAddresses(true)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getPublishNotReadyAddresses(), is(true));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();

        for (int i = 0; i < REPLICAS; i++)  {
            Service srv = services.get(i);
            assertThat(srv.getSpec().getPublishNotReadyAddresses(), is(true));
        }
    }

    @ParallelTest
    public void testImagePullPolicy() {
        // Test ALWAYS policy
        StrimziPodSet ps = KC.generatePodSets(true, ImagePullPolicy.ALWAYS, null, node -> Map.of()).get(0);

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods) {
            assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        }

        // Test IFNOTPRESENT policy
        ps = KC.generatePodSets(true, ImagePullPolicy.IFNOTPRESENT, null, node -> Map.of()).get(0);

        // We need to loop through the pods to make sure they have the right values
        pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods) {
            assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        }
    }

    @ParallelTest
    public void testImagePullSecrets() {
        // CR configuration has priority -> CO configuration is ignored if both are set
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafka = new KafkaBuilder(KAFKA)
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        StrimziPodSet ps = kc.generatePodSets(true, null, null, node -> Map.of()).get(0);

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods) {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        }
    }

    @ParallelTest
    public void testImagePullSecretsFromCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        StrimziPodSet ps = KC.generatePodSets(true, null, secrets, node -> Map.of()).get(0);

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods) {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        }
    }

    @ParallelTest
    public void testImagePullSecretsFromBoth() {
        // CR configuration has priority -> CO configuration is ignored if both are set
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafka = new KafkaBuilder(KAFKA)
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        StrimziPodSet ps = kc.generatePodSets(true, null, List.of(secret1), node -> Map.of()).get(0);

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods) {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        }
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        StrimziPodSet ps = KC.generatePodSets(true, null, null, node -> Map.of()).get(0);

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods) {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(0));
        }
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        kc.securityProvider = new RestrictedPodSecurityProvider();
        kc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        // Test generated SPS
        StrimziPodSet ps = kc.generatePodSets(false, null, null, node -> Map.of()).get(0);
        List<Pod> pods = PodSetUtils.podSetToPods(ps);

        for (Pod pod : pods) {
            assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(0L));

            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
        }
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        StrimziPodSet sps = KC.generatePodSets(false, null, null, node -> Map.of()).get(0);

        List<Pod> pods = PodSetUtils.podSetToPods(sps);
        for (Pod pod : pods) {
            assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(0L));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(nullValue()));
        }
    }

    @ParallelTest
    public void testCustomLabelsFromCR() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToLabels("foo", "bar")
                .endMetadata()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        StrimziPodSet sps = kc.generatePodSets(false, null, null, node -> Map.of()).get(0);
        assertThat(sps.getMetadata().getLabels().get("foo"), is("bar"));

        List<Pod> pods = PodSetUtils.podSetToPods(sps);
        for (Pod pod : pods) {
            assertThat(pod.getMetadata().getLabels().get("foo"), is("bar"));
        }
    }

    @ParallelTest
    public void testPodSet()   {
        StrimziPodSet ps = KC.generatePodSets(true, null, null, node -> Map.of("test-anno", KafkaResources.kafkaPodName(CLUSTER, node.nodeId()))).get(0);

        assertThat(ps.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER)));
        assertThat(ps.getMetadata().getLabels().entrySet().containsAll(KC.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(ps.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build()).build())));
        TestUtils.checkOwnerReference(ps, KAFKA);
        assertThat(ps.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().withStrimziPoolName("kafka").toMap()));
        assertThat(ps.getSpec().getPods().size(), is(3));

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(KC.labels.withStrimziPodName(pod.getMetadata().getName()).withStatefulSetPod(pod.getMetadata().getName()).withStrimziPodSetController(KC.getComponentName()).toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(2));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("test-anno"), is(pod.getMetadata().getName()));

            assertThat(pod.getSpec().getHostname(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getSubdomain(), is(KafkaResources.brokersServiceName(CLUSTER)));
            assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
            assertThat(pod.getSpec().getVolumes().stream()
                    .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                    .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(15));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(15));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));

            assertThat(pod.getSpec().getVolumes().size(), is(7));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is("foo-kafka-brokers"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testCustomizedPodSet()   {
        // Prepare various template values
        Map<String, String> spsLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> spsAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = Map.of("a3", "v3", "a4", "v4");

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        TopologySpreadConstraint tsc1 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/zone")
                .withMaxSkew(1)
                .withWhenUnsatisfiable("DoNotSchedule")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
                .build();

        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                        .addNewMatchExpression()
                        .withKey("key1")
                        .withOperator("In")
                        .withValues("value1", "value2")
                        .endMatchExpression()
                        .build())
                .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> toleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

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

        // Used to test env var conflict
        ContainerEnvVar envVar3 = new ContainerEnvVar();
        String testEnvThreeKey = KafkaCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED;
        String testEnvThreeValue = "test.env.three";
        envVar3.setName(testEnvThreeKey);
        envVar3.setValue(testEnvThreeValue);

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(true)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        String image = "my-custom-image:latest";

        Probe livenessProbe = new Probe();
        livenessProbe.setInitialDelaySeconds(1);
        livenessProbe.setTimeoutSeconds(2);
        livenessProbe.setSuccessThreshold(3);
        livenessProbe.setFailureThreshold(4);
        livenessProbe.setPeriodSeconds(5);

        Probe readinessProbe = new Probe();
        readinessProbe.setInitialDelaySeconds(6);
        readinessProbe.setTimeoutSeconds(7);
        readinessProbe.setSuccessThreshold(8);
        readinessProbe.setFailureThreshold(9);
        readinessProbe.setPeriodSeconds(10);

        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();

        AdditionalVolume additionalVolume  = new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        // Use the template values in Kafka CR
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withImage(image)
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                        .withReadinessProbe(readinessProbe)
                        .withLivenessProbe(livenessProbe)
                        .withConfig(Map.of("foo", "bar"))
                        .withNewTemplate()
                            .withNewPodSet()
                                .withNewMetadata()
                                    .withLabels(spsLabels)
                                    .withAnnotations(spsAnnotations)
                                .endMetadata()
                            .endPodSet()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnnotations)
                                .endMetadata()
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withHostAliases(hostAlias1, hostAlias2)
                                .withTopologySpreadConstraints(tsc1, tsc2)
                                .withAffinity(affinity)
                                .withTolerations(toleration)
                                .withEnableServiceLinks(false)
                                .withTmpDirSizeLimit("10Mi")
                                .withTerminationGracePeriodSeconds(123)
                                .withImagePullSecrets(secret1, secret2)
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                                .withVolumes(additionalVolume)
                            .endPod()
                            .withNewKafkaContainer()
                                .withEnv(envVar1, envVar2, envVar3)
                                .withSecurityContext(securityContext)
                                .withVolumeMounts(additionalVolumeMount)
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        // Test the resources
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        StrimziPodSet ps = kc.generatePodSets(true, null, null, node -> Map.of("special", "annotation")).get(0);

        assertThat(ps.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER)));
        assertThat(ps.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(true));
        assertThat(ps.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(true));
        assertThat(ps.getSpec().getSelector().getMatchLabels(), is(kc.getSelectorLabels().withStrimziPoolName("kafka").toMap()));
        assertThat(ps.getSpec().getPods().size(), is(3));

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods)  {
            // Metadata
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().get("special"), is("annotation"));

            // Pod
            assertThat(pod.getSpec().getPriorityClassName(), is("top-priority"));
            assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
            assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
            assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
            assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
            assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
            assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(123L));
            assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(456L));
            assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(789L));
            assertThat(pod.getSpec().getAffinity(), is(affinity));
            assertThat(pod.getSpec().getTolerations(), is(toleration));

            assertThat(pod.getSpec().getVolumes().size(), is(8));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is("foo-kafka-brokers"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(7).getName(), is("secret-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(7).getSecret(), is(notNullValue()));

            // Containers
            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getName(), is(KafkaCluster.KAFKA_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is(image));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(securityContext));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(livenessProbe.getTimeoutSeconds()));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(livenessProbe.getInitialDelaySeconds()));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getFailureThreshold(), is(livenessProbe.getFailureThreshold()));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getSuccessThreshold(), is(livenessProbe.getSuccessThreshold()));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getPeriodSeconds(), is(livenessProbe.getPeriodSeconds()));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(readinessProbe.getTimeoutSeconds()));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(readinessProbe.getInitialDelaySeconds()));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getFailureThreshold(), is(readinessProbe.getFailureThreshold()));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getSuccessThreshold(), is(readinessProbe.getSuccessThreshold()));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getPeriodSeconds(), is(readinessProbe.getPeriodSeconds()));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("true"));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(8));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getName(), is("secret-volume-name"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getMountPath(), is("/mnt/secret-volume"));
        }
    }

    @ParallelTest
    public void testGeneratePodSetWithSetSizeLimit() {
        String sizeLimit = "1Gi";
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorage().withSizeLimit(sizeLimit).endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        StrimziPodSet ps = kc.generatePodSets(false, null, null, node -> Map.of()).get(0);
        List<Pod> pods = PodSetUtils.podSetToPods(ps);

        for (Pod pod : pods) {
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("1", "Gi")));
        }
    }

    @ParallelTest
    public void testEphemeralStorage()    {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        StrimziPodSet ps = kc.generatePodSets(false, null, null, node -> Map.of()).get(0);
        List<Pod> pods = PodSetUtils.podSetToPods(ps);

        for (Pod pod : pods) {
            assertThat(pod.getSpec().getVolumes().stream().filter(v -> "data".equals(v.getName())).findFirst().orElseThrow().getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(nullValue()));
        }

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims();
        assertThat(pvcs.size(), is(0));
    }
}
