/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTermBuilder;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.StrimziProbe;
import io.strimzi.api.kafka.model.common.StrimziProbeBuilder;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalTemplatedVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalTemplatedVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.TestUtils;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.AuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.platform.KubernetesVersion;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.CLUSTER;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.NAMESPACE;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.createBrokerPool;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.createControllerPool;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.createKafka;
import static io.strimzi.operator.cluster.model.KafkaClusterTestUtils.createMixedPool;
import static io.strimzi.operator.cluster.model.jmx.JmxModel.JMX_PORT;
import static io.strimzi.operator.cluster.model.jmx.JmxModel.JMX_PORT_NAME;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
public class KafkaClusterPodTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static Kafka KAFKA = createKafka();
    private final static KafkaNodePool POOL_CONTROLLERS = createControllerPool();
    private final static KafkaNodePool POOL_MIXED = createMixedPool();
    private final static KafkaNodePool POOL_BROKERS = createBrokerPool();

    private static final List<KafkaPool> POOLS = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
    private final static KafkaCluster KC = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

    @Test
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
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            final Optional<EnvVar> envVarValue = pod.getSpec().getContainers().stream().findAny().orElseThrow().getEnv().stream().filter(env -> env.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findAny();
            assertThat(envVarValue.isPresent(), is(true));
        }));
    }

    @Test
    public void  testJavaSystemPropertiesInNodePools() {
        KafkaNodePool controllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("42").build()))
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("1874").build()))
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("1919").build()))
                    .endJvmOptions()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(controllers, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            final Optional<EnvVar> envVarValue = pod.getSpec().getContainers().stream().findAny().orElseThrow().getEnv().stream().filter(env -> env.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findAny();
            assertThat(envVarValue.isPresent(), is(true));
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=42"));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=1874"));
            } else {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=1919"));
            }
        }));
    }

    @Test
    public void  testJavaSystemPropertiesInNodePoolsAndKafka() {
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
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("1874").build()))
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("1919").build()))
                    .endJvmOptions()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            final Optional<EnvVar> envVarValue = pod.getSpec().getContainers().stream().findAny().orElseThrow().getEnv().stream().filter(env -> env.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findAny();
            assertThat(envVarValue.isPresent(), is(true));
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=42"));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=1874"));
            } else {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=1919"));
            }
        }));
    }

    @Test
    public void testCustomImage() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withImage("my-image:my-tag")
                        .withBrokerRackInitImage("my-init-image:my-init-tag")
                        .withNewTopologyLabelRack()
                            .withTopologyKey("rack-key")
                        .endTopologyLabelRack()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Check container
            assertThat(pod.getSpec().getContainers().stream().findAny().orElseThrow().getImage(), is("my-image:my-tag"));

            // Check Init container
            if (!pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(pod.getSpec().getInitContainers().stream().findAny().orElseThrow().getImage(), is("my-init-image:my-init-tag"));
            }
        }));
    }

    @Test
    public void testHealthChecks() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withLivenessProbe(new StrimziProbeBuilder()
                                .withInitialDelaySeconds(1)
                                .withPeriodSeconds(2)
                                .withTimeoutSeconds(3)
                                .withSuccessThreshold(4)
                                .withFailureThreshold(5)
                                .build())
                        .withReadinessProbe(new StrimziProbeBuilder()
                                .withInitialDelaySeconds(6)
                                .withPeriodSeconds(7)
                                .withTimeoutSeconds(8)
                                .withSuccessThreshold(9)
                                .withFailureThreshold(10)
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
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

    @Test
    public void testInitContainerTemplate() {
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
        String testEnvThreeKey = KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME;
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
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));
            } else {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(securityContext));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(3));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                assertThat(initCont.getVolumeMounts().get(1).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/init"));
                assertThat(initCont.getVolumeMounts().get(2).getName(), is("secret-volume-name"));
                assertThat(initCont.getVolumeMounts().get(2).getMountPath(), is("/mnt/secret-volume"));
            }
        }));
    }

    @Test
    public void testExposesJmxContainerPortWhenJmxEnabled() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            ContainerPort jmxPort = cont.getPorts().stream().filter(port -> JMX_PORT_NAME.equals(port.getName())).findFirst().orElseThrow();
            assertThat(jmxPort.getContainerPort(), is(JMX_PORT));
        }));
    }

    @Test
    public void testWithJmxMetricsExporterContainerPorts() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.INTERNAL).withTls().build(),
                                new GenericKafkaListenerBuilder().withName("external").withPort(9094).withType(KafkaListenerType.NODEPORT).withTls().build())
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("metrics-cm", "metrics.json", false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endKafka()
                .endSpec()
                .build();

        assertExpectedContainerPortsAreSet(kafka);
    }

    @Test
    public void testWithStrimziMetricsReporterContainerPorts() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.INTERNAL).withTls().build(),
                                new GenericKafkaListenerBuilder().withName("external").withPort(9094).withType(KafkaListenerType.NODEPORT).withTls().build())
                        .withNewStrimziMetricsReporterConfig()
                            .withNewValues()
                                .withAllowList("kafka_log.*", "kafka_network.*")
                            .endValues()
                        .endStrimziMetricsReporterConfig()
                    .endKafka()
                .endSpec()
                .build();

        assertExpectedContainerPortsAreSet(kafka);
    }

    private void assertExpectedContainerPortsAreSet(Kafka kafka) {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(ports.size(), is(3));
                assertThat(ports.get(0).getContainerPort(), is(8443));
                assertThat(ports.get(1).getContainerPort(), is(9090));
                assertThat(ports.get(2).getContainerPort(), is(9404));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(ports.size(), is(6));
                assertThat(ports.get(0).getContainerPort(), is(8443));
                assertThat(ports.get(1).getContainerPort(), is(9090));
                assertThat(ports.get(2).getContainerPort(), is(9091));
                assertThat(ports.get(3).getContainerPort(), is(9093));
                assertThat(ports.get(4).getContainerPort(), is(9094));
                assertThat(ports.get(5).getContainerPort(), is(9404));
            } else {
                assertThat(ports.size(), is(5));
                assertThat(ports.get(0).getContainerPort(), is(8443));
                assertThat(ports.get(1).getContainerPort(), is(9091));
                assertThat(ports.get(2).getContainerPort(), is(9093));
                assertThat(ports.get(3).getContainerPort(), is(9094));
                assertThat(ports.get(4).getContainerPort(), is(9404));
            }
        }));
    }

    @Test
    public void testInitContainerTemplateInKafkaAndNodePool() {
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
        String testEnvThreeKey = KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME;
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

        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar2, envVar3)
                            .withSecurityContext(securityContext)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar1, envVar3)
                            .withVolumeMounts(additionalVolumeMount)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(securityContext));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(2));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                assertThat(initCont.getVolumeMounts().get(1).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/init"));
            } else {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(Matchers.nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(3));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                assertThat(initCont.getVolumeMounts().get(1).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/init"));
                assertThat(initCont.getVolumeMounts().get(2).getName(), is("secret-volume-name"));
                assertThat(initCont.getVolumeMounts().get(2).getMountPath(), is("/mnt/secret-volume"));
            }
        }));
    }

    @Test
    public void testInitContainerTemplateInNodePool() {
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
        String testEnvThreeKey = KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME;
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
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool controllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar1, envVar2, envVar3)
                            .withSecurityContext(securityContext)
                            .withVolumeMounts(additionalVolumeMount)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar2, envVar3)
                            .withSecurityContext(securityContext)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar1, envVar3)
                            .withVolumeMounts(additionalVolumeMount)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(controllers, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(securityContext));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(2));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                assertThat(initCont.getVolumeMounts().get(1).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/init"));
            } else {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(Matchers.nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(3));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                assertThat(initCont.getVolumeMounts().get(1).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/init"));
                assertThat(initCont.getVolumeMounts().get(2).getName(), is("secret-volume-name"));
                assertThat(initCont.getVolumeMounts().get(2).getMountPath(), is("/mnt/secret-volume"));
            }
        }));
    }

    @Test
    public void testServiceAccountAuthenticationVolumesAndVolumeMounts() {
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER,
                new KafkaClusterSecurityContext(new TlsEncryptionConfiguration(), AuthenticationConfiguration.fromCrd(NAMESPACE, CLUSTER, new ClusterSecurityAuthenticationBuilder().withType(ClusterSecurityAuthenticationType.SERVICE_ACCOUNT).build())));

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        // The token volume and volume mount are added to all nodes => controllers, mixed nodes and brokers
        podSets.forEach(podSet -> PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Volume tokenVolume = pod.getSpec().getVolumes().stream()
                    .filter(volume -> VolumeUtils.STRIMZI_AUTHENTICATION_TOKEN_VOLUME_NAME.equals(volume.getName()))
                    .findFirst()
                    .orElseThrow();
            assertThat(tokenVolume.getProjected().getSources().size(), is(1));
            assertThat(tokenVolume.getProjected().getSources().get(0).getServiceAccountToken().getAudience(), is("strimzi.io/kafka/" + NAMESPACE + "/" + CLUSTER));
            assertThat(tokenVolume.getProjected().getSources().get(0).getServiceAccountToken().getExpirationSeconds(), is(3600L));
            assertThat(tokenVolume.getProjected().getSources().get(0).getServiceAccountToken().getPath(), is("token"));

            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().stream().map(VolumeMount::getName).toList(),
                    hasItem(VolumeUtils.STRIMZI_AUTHENTICATION_TOKEN_VOLUME_NAME));
        }));
    }

    @Test
    public void testVolumesAndVolumeMountsWithoutServiceAccountAuthentication() {
        List<StrimziPodSet> podSets = KC.generatePodSets(null, null, node -> Map.of());

        podSets.forEach(podSet -> PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().stream().map(Volume::getName).toList(),
                    not(hasItem(VolumeUtils.STRIMZI_AUTHENTICATION_TOKEN_VOLUME_NAME)));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().stream().map(VolumeMount::getName).toList(),
                    not(hasItem(VolumeUtils.STRIMZI_AUTHENTICATION_TOKEN_VOLUME_NAME)));
        }));
    }

    @Test
    public void testExternalAddressEnvVarNotSetInControllers() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("external").withPort(9094).withType(KafkaListenerType.NODEPORT).withTls().build())
                        .withNewTopologyLabelRack()
                            .withTopologyKey("my-topology-key")
                        .endTopologyLabelRack()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(pod.getSpec().getInitContainers(), is(empty()));
            } else {
                List<EnvVar> envVars = pod.getSpec().getInitContainers().stream().findAny().orElseThrow().getEnv();
                assertThat(envVars.size(), is(3));
                assertThat(envVars.get(0).getName(), is("NODE_NAME"));
                assertThat(envVars.get(0).getValueFrom(), is(notNullValue()));
                assertThat(envVars.get(1).getName(), is("RACK_TOPOLOGY_KEY"));
                assertThat(envVars.get(1).getValue(), is("my-topology-key"));
                assertThat(envVars.get(2).getName(), is("EXTERNAL_ADDRESS"));
                assertThat(envVars.get(2).getValue(), is("TRUE"));
            }
        }));
    }

    @Test
    public void testKafkaInitContainerResourcesConfiguration() {
        Map<String, Quantity> poolLimits = new HashMap<>();
        poolLimits.put("cpu", Quantity.parse("10"));
        poolLimits.put("memory", Quantity.parse("2560Mi"));

        Map<String, Quantity> poolRequirements = new HashMap<>();
        poolRequirements.put("cpu", Quantity.parse("1000m"));
        poolRequirements.put("memory", Quantity.parse("1280Mi"));

        ResourceRequirements poolResourceReq = new ResourceRequirementsBuilder()
            .withLimits(poolLimits)
            .withRequests(poolRequirements)
            .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withNewTopologyLabelRack()
                        .withTopologyKey("rack-key")
                    .endTopologyLabelRack()
                .endKafka()
            .endSpec()
            .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withResources(poolResourceReq)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(pod.getSpec().getInitContainers(), is(empty()));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-brokers")) {
                ResourceRequirements initContainersResources = pod.getSpec().getInitContainers().get(0).getResources();
                assertThat(initContainersResources.getRequests(), is(poolRequirements));
                assertThat(initContainersResources.getLimits(), is(poolLimits));
            } else {
                assertThat(pod.getSpec().getInitContainers().get(0).getResources(), is(nullValue()));
            }
        }));
    }

    @Test
    public void testKafkaInitContainerSectionIsConfigurableOnlyInNodePool() {
        Map<String, Quantity> poolLimits = new HashMap<>();
        poolLimits.put("cpu", Quantity.parse("1"));
        poolLimits.put("memory", Quantity.parse("256Mi"));

        Map<String, Quantity> poolRequirements = new HashMap<>();
        poolRequirements.put("cpu", Quantity.parse("100m"));
        poolRequirements.put("memory", Quantity.parse("128Mi"));

        ResourceRequirements poolResourceReq = new ResourceRequirementsBuilder()
            .withLimits(poolLimits)
            .withRequests(poolRequirements)
            .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withNewTopologyLabelRack()
                        .withTopologyKey("rack-key")
                    .endTopologyLabelRack()
                .endKafka()
            .endSpec()
            .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withResources(poolResourceReq)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, mixed, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(initCont.getResources().getRequests(), is(poolRequirements));
                assertThat(initCont.getResources().getLimits(), is(poolLimits));
            } else {
                assertThat(initCont.getResources(), is(nullValue()));
            }
        }));
    }

    @Test
    public void testRackAffinity() {
        Affinity rackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("failure-domain.beta.kubernetes.io/zone")
                                    .withOperator("Exists")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTopologyLabelRack()
                            .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                        .endTopologyLabelRack()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("-controllers-"))  {
                    assertThat(pod.getSpec().getAffinity(), is(nullValue()));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(rackAffinity));
                }
            }
        }
    }

    @Test
    public void testAffinityAndTolerations() {
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

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withAffinity(affinity)
                                .withTolerations(toleration)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                assertThat(pod.getSpec().getAffinity(), is(affinity));
                assertThat(pod.getSpec().getTolerations(), is(toleration));
            }
        }
    }

    @Test
    public void testAffinityAndTolerationsInKafkaAndKafkaPool() {
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
        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key2")
                                    .withOperator("In")
                                    .withValues("value3", "value4")
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
        List<Toleration> poolToleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key2")
                .withOperator("Equal")
                .withValue("value2")
                .build());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withAffinity(affinity)
                                .withTolerations(toleration)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                            .withTolerations(poolToleration)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getAffinity(), is(poolAffinity));
                    assertThat(pod.getSpec().getTolerations(), is(poolToleration));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(affinity));
                    assertThat(pod.getSpec().getTolerations(), is(toleration));
                }
            }
        }
    }

    @Test
    public void testAffinityAndTolerationsInKafkaPool() {
        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key2")
                                    .withOperator("In")
                                    .withValues("value3", "value4")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> poolToleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key2")
                .withOperator("Equal")
                .withValue("value2")
                .build());

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                            .withTolerations(poolToleration)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getAffinity(), is(poolAffinity));
                    assertThat(pod.getSpec().getTolerations(), is(poolToleration));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(nullValue()));
                    assertThat(pod.getSpec().getTolerations(), is(List.of()));
                }
            }
        }
    }

    @Test
    public void testAffinityAndRack() {
        Affinity mergedRackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(
                                new NodeSelectorTermBuilder()
                                        .addNewMatchExpression()
                                            .withKey("key1")
                                            .withOperator("In")
                                            .withValues("value1", "value2")
                                        .endMatchExpression()
                                        .addNewMatchExpression()
                                            .withKey("failure-domain.beta.kubernetes.io/zone")
                                            .withOperator("Exists")
                                        .endMatchExpression()
                                        .build()
                        )
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("storage", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

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
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("storage", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTopologyLabelRack()
                            .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                        .endTopologyLabelRack()
                        .withNewTemplate()
                            .withNewPod()
                                .withAffinity(affinity)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("-controllers-"))  {
                    assertThat(pod.getSpec().getAffinity(), is(affinity));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(mergedRackAffinity));
                }
            }
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testAffinityAndRackInKafkaAndKafkaPool() {
        Affinity mergedRackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(
                                new NodeSelectorTermBuilder()
                                        .addNewMatchExpression()
                                            .withKey("key1")
                                            .withOperator("In")
                                            .withValues("value1", "value2")
                                        .endMatchExpression()
                                        .addNewMatchExpression()
                                            .withKey("failure-domain.beta.kubernetes.io/zone")
                                            .withOperator("Exists")
                                        .endMatchExpression()
                                        .build()
                        )
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("storage", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Affinity poolMergedRackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(
                                new NodeSelectorTermBuilder()
                                        .addNewMatchExpression()
                                            .withKey("key2")
                                            .withOperator("In")
                                            .withValues("value3", "value4")
                                        .endMatchExpression()
                                        .addNewMatchExpression()
                                            .withKey("failure-domain.beta.kubernetes.io/zone")
                                            .withOperator("Exists")
                                        .endMatchExpression()
                                        .build()
                        )
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("database", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

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
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("storage", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();
        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key2")
                                    .withOperator("In")
                                    .withValues("value3", "value4")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("database", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTopologyLabelRack()
                            .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                        .endTopologyLabelRack()
                        .withNewTemplate()
                            .withNewPod()
                                .withAffinity(affinity)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getAffinity(), is(poolMergedRackAffinity));
                } else if (pod.getMetadata().getName().contains("-controllers-"))  {
                    assertThat(pod.getSpec().getAffinity(), is(affinity));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(mergedRackAffinity));
                }
            }
        }
    }

    @Test
    public void testAffinityAndRackInKafkaPool() {
        Affinity rackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("failure-domain.beta.kubernetes.io/zone")
                                    .withOperator("Exists")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        Affinity mergedRackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(
                                new NodeSelectorTermBuilder()
                                        .addNewMatchExpression()
                                            .withKey("key2")
                                            .withOperator("In")
                                            .withValues("value3", "value4")
                                        .endMatchExpression()
                                        .addNewMatchExpression()
                                            .withKey("failure-domain.beta.kubernetes.io/zone")
                                            .withOperator("Exists")
                                        .endMatchExpression()
                                        .build()
                        )
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("database", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key2")
                                    .withOperator("In")
                                    .withValues("value3", "value4")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("database", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTopologyLabelRack()
                            .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                        .endTopologyLabelRack()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaNodePool controllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(controllers, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers")) {
                    assertThat(pod.getSpec().getAffinity(), is(mergedRackAffinity));
                } else if (pod.getMetadata().getName().contains("-controllers-"))  {
                    assertThat(pod.getSpec().getAffinity(), is(poolAffinity));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(rackAffinity));
                }
            }
        }
    }

    @Test
    public void testImagePullPolicy() {
        // Test ALWAYS policy
        List<StrimziPodSet> podSets = KC.generatePodSets(ImagePullPolicy.ALWAYS, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
            }
        }

        // Test IFNOTPRESENT policy
        podSets = KC.generatePodSets(ImagePullPolicy.IFNOTPRESENT, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
            }
        }

        // Test NEVER policy
        podSets = KC.generatePodSets(ImagePullPolicy.NEVER, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.NEVER.toString()));
            }
        }
    }

    @Test
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
            }
        }
    }

    @Test
    public void testImagePullSecretsFromCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        List<StrimziPodSet> podSets = KC.generatePodSets(null, secrets, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
            }
        }
    }

    @Test
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, List.of(secret1), node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
            }
        }
    }

    @Test
    public void testImagePullSecretsFromKafkaAndNodePool() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret1)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));

                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
                } else {
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(false));
                }
            }
        }
    }

    @Test
    public void testImagePullSecretsFromCoAndNodePool() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, List.of(secret1), node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));

                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
                } else {
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(false));
                }
            }
        }
    }

    @Test
    public void testDefaultImagePullSecrets() {
        List<StrimziPodSet> podSets = KC.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(0));
            }
        }
    }

    @Test
    public void testSecurityProvider() {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);
        kc.securityProvider = new TestPodSecurityProvider();
        kc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(0L));
                assertThat(pod.getSpec().getHostUsers(), is(false));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
            }
        }
    }

    @Test
    public void testDefaultSecurityContext() {
        List<StrimziPodSet> podSets = KC.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(0L));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(nullValue()));
            }
        }
    }

    @Test
    public void testCustomLabelsFromCR() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editMetadata()
                    .withLabels(Map.of("foo", "bar"))
                .endMetadata()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            if (podSet.getMetadata().getName().contains("brokers")) {
                assertThat(podSet.getMetadata().getLabels().get("foo"), is("bar"));
            } else {
                assertThat(podSet.getMetadata().getLabels().get("foo"), is(nullValue()));
            }

            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers")) {
                    assertThat(pod.getMetadata().getLabels().get("foo"), is("bar"));
                } else {
                    assertThat(pod.getMetadata().getLabels().get("foo"), is(nullValue()));
                }
            }
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testPodSet()   {
        List<StrimziPodSet> podSets = KC.generatePodSets(null, null, node -> Map.of("test-anno", "test-value"));
        assertThat(podSets.size(), is(3));

        // Controllers
        StrimziPodSet podSet = podSets.stream().filter(sps -> (CLUSTER + "-controllers").equals(sps.getMetadata().getName())).findFirst().orElse(null);
        assertThat(podSet, is(notNullValue()));

        TestUtils.checkOwnerReference(podSet, POOL_CONTROLLERS);
        assertThat(podSet.getMetadata().getName(), is(CLUSTER + "-controllers"));
        assertThat(podSet.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().withStrimziPoolName("controllers").toMap()));
        assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(KC.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build()).build())));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING), is(nullValue()));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED), is(nullValue()));

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(podSet);
        assertThat(pods.size(), is(3));

        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(KC.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(CLUSTER + "-controllers").toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(3));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("test-anno"), is("test-value"));

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
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));

            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
            assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
        }

        // Mixed nodes
        podSet = podSets.stream().filter(sps -> (CLUSTER + "-mixed").equals(sps.getMetadata().getName())).findFirst().orElse(null);
        assertThat(podSet, is(notNullValue()));

        TestUtils.checkOwnerReference(podSet, POOL_MIXED);
        assertThat(podSet.getMetadata().getName(), is(CLUSTER + "-mixed"));
        assertThat(podSet.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().withStrimziPoolName("mixed").toMap()));
        assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(KC.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build()).build())));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING), is(nullValue()));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED), is(nullValue()));

        // We need to loop through the pods to make sure they have the right values
        pods = PodSetUtils.podSetToPods(podSet);
        assertThat(pods.size(), is(2));

        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(KC.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(CLUSTER + "-mixed").toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(3));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("test-anno"), is("test-value"));

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
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));

            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
            assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
        }

        // Brokers
        podSet = podSets.stream().filter(sps -> (CLUSTER + "-brokers").equals(sps.getMetadata().getName())).findFirst().orElse(null);
        assertThat(podSet, is(notNullValue()));

        TestUtils.checkOwnerReference(podSet, POOL_BROKERS);
        assertThat(podSet.getMetadata().getName(), is(CLUSTER + "-brokers"));
        assertThat(podSet.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().withStrimziPoolName("brokers").toMap()));
        assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(KC.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build()).build())));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING), is(nullValue()));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED), is(nullValue()));

        // We need to loop through the pods to make sure they have the right values
        pods = PodSetUtils.podSetToPods(podSet);
        assertThat(pods.size(), is(3));

        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(KC.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(CLUSTER + "-brokers").toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(3));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("test-anno"), is("test-value"));

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
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));

            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
            assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
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
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
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

        StrimziProbe livenessProbe = new StrimziProbe();
        livenessProbe.setInitialDelaySeconds(1);
        livenessProbe.setTimeoutSeconds(2);
        livenessProbe.setSuccessThreshold(3);
        livenessProbe.setFailureThreshold(4);
        livenessProbe.setPeriodSeconds(5);

        StrimziProbe readinessProbe = new StrimziProbe();
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

        AdditionalTemplatedVolume additionalTemplatedVolume  = new AdditionalTemplatedVolumeBuilder()
                .withName("pvc-volume-name")
                .withPersistentVolumeClaim(new PersistentVolumeClaimVolumeSourceBuilder().withClaimName("my-pvc-{nodeId}").build())
                .build();

        VolumeMount additionalTemplatedVolumeMount = new VolumeMountBuilder()
                .withName("pvc-volume-name")
                .withMountPath("/mnt/pvc-volume-name")
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
                                .withTemplatedVolumes(additionalTemplatedVolume)
                                .withHostUsers(false)
                            .endPod()
                            .withNewKafkaContainer()
                                .withEnv(envVar1, envVar2, envVar3)
                                .withSecurityContext(securityContext)
                                .withVolumeMounts(additionalVolumeMount, additionalTemplatedVolumeMount)
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        // Test the resources
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of("special", "annotation"));
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(true));
            assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(true));

            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
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
                assertThat(pod.getSpec().getHostUsers(), is(false));

                assertThat(pod.getSpec().getVolumes().size(), is(7));
                assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
                assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
                assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
                assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
                assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(5).getName(), is("secret-volume-name"));
                assertThat(pod.getSpec().getVolumes().get(5).getSecret(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(6).getName(), is("pvc-volume-name"));
                assertThat(pod.getSpec().getVolumes().get(6).getPersistentVolumeClaim().getClaimName(), is("my-pvc-" + pod.getMetadata().getName().substring(pod.getMetadata().getName().lastIndexOf("-") + 1)));

                // Containers
                assertThat(pod.getSpec().getContainers().size(), is(1));
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

                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(7));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("secret-volume-name"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/mnt/secret-volume"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("pvc-volume-name"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/mnt/pvc-volume-name"));
            }
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testCustomizedPodSetInKafkaAndNodePool()   {
        // Prepare various template values
        Map<String, String> spsLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> spsAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> poolSpsLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> poolSpsAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> poolPodLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> poolPodAnnotations = Map.of("a7", "v7", "a8", "v8");

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
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
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

        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                        .addNewMatchExpression()
                        .withKey("key2")
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

        List<Toleration> poolToleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key2")
                .withOperator("Equal")
                .withValue("value2")
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

        SecurityContext poolSecurityContext = new SecurityContextBuilder()
                .withPrivileged(true)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(true)
                .withRunAsNonRoot(false)
                .withNewCapabilities()
                    .addToDrop("NONE")
                .endCapabilities()
                .build();

        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();

        AdditionalVolume additionalVolume  = new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build();

        AdditionalVolume poolAdditionalVolume  = new AdditionalVolumeBuilder()
                .withName("secret-volume-name2")
                .withSecret(secret)
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        VolumeMount poolAdditionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name2")
                .withMountPath("/mnt/secret-volume2")
                .withSubPath("def")
                .build();

        // Use the template values in Kafka CR
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
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
                                .withHostAliases(hostAlias2)
                                .withTopologySpreadConstraints(tsc2)
                                .withAffinity(affinity)
                                .withTolerations(toleration)
                                .withEnableServiceLinks(false)
                                .withTmpDirSizeLimit("13Mi")
                                .withTerminationGracePeriodSeconds(321)
                                .withImagePullSecrets(secret2)
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(1230L).withRunAsGroup(4560L).withRunAsUser(7890L).build())
                                .withVolumes(additionalVolume)
                            .endPod()
                            .withNewKafkaContainer()
                                .withEnv(envVar2, envVar3)
                                .withSecurityContext(securityContext)
                                .withVolumeMounts(additionalVolumeMount)
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder()
                        .withRequests(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi")))
                        .withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi")))
                        .build())
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(false)
                    .endJvmOptions()
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(poolSpsLabels)
                                .withAnnotations(poolSpsAnnotations)
                            .endMetadata()
                        .endPodSet()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(poolPodLabels)
                                .withAnnotations(poolPodAnnotations)
                            .endMetadata()
                            .withPriorityClassName("top-priority2")
                            .withSchedulerName("my-scheduler2")
                            .withHostAliases(hostAlias1)
                            .withTopologySpreadConstraints(tsc1)
                            .withAffinity(poolAffinity)
                            .withTolerations(poolToleration)
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                            .withTerminationGracePeriodSeconds(123)
                            .withImagePullSecrets(secret1)
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .withVolumes(poolAdditionalVolume)
                        .endPod()
                        .withNewKafkaContainer()
                            .withEnv(envVar1, envVar3)
                            .withSecurityContext(poolSecurityContext)
                            .withVolumeMounts(poolAdditionalVolumeMount)
                        .endKafkaContainer()
                    .endTemplate()
                .endSpec()
                .build();

        // Test the resources
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of("special", "annotation"));
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            if (podSet.getMetadata().getName().contains("brokers")) {
                assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(poolSpsLabels.entrySet()), is(true));
                assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(poolSpsAnnotations.entrySet()), is(true));
            } else {
                assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(true));
                assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(true));
            }

            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers")) {
                    // Metadata
                    assertThat(pod.getMetadata().getLabels().entrySet().containsAll(poolPodLabels.entrySet()), is(true));
                    assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(poolPodAnnotations.entrySet()), is(true));
                    assertThat(pod.getMetadata().getAnnotations().get("special"), is("annotation"));

                    // Pod
                    assertThat(pod.getSpec().getPriorityClassName(), is("top-priority2"));
                    assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler2"));
                    assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1));
                    assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1));
                    assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
                    assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
                    assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                    assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
                    assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(123L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(456L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(789L));
                    assertThat(pod.getSpec().getAffinity(), is(poolAffinity));
                    assertThat(pod.getSpec().getTolerations(), is(poolToleration));

                    assertThat(pod.getSpec().getVolumes().size(), is(6));
                    assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
                    assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(5).getName(), is("secret-volume-name2"));
                    assertThat(pod.getSpec().getVolumes().get(5).getSecret(), is(notNullValue()));

                    // Containers
                    assertThat(pod.getSpec().getContainers().size(), is(1));
                    assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(poolSecurityContext));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("false"));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi"))));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi"))));

                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(6));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("secret-volume-name2"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/mnt/secret-volume2"));
                } else {
                    assertThat(pod.getSpec().getPriorityClassName(), is("top-priority"));
                    assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
                    assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias2));
                    assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc2));
                    assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
                    assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(321L));
                    assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
                    assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
                    assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(1230L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(4560L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(7890L));
                    assertThat(pod.getSpec().getAffinity(), is(affinity));
                    assertThat(pod.getSpec().getTolerations(), is(toleration));

                    assertThat(pod.getSpec().getVolumes().size(), is(6));
                    assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("13Mi")));
                    assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(5).getName(), is("secret-volume-name"));
                    assertThat(pod.getSpec().getVolumes().get(5).getSecret(), is(notNullValue()));

                    // Containers
                    assertThat(pod.getSpec().getContainers().size(), is(1));
                    assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(securityContext));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("true"));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));
                    assertThat(pod.getSpec().getContainers().get(0).getResources(), is(nullValue()));

                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(6));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("secret-volume-name"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/mnt/secret-volume"));
                }
            }
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testCustomizedPodSetInNodePool()   {
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
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
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
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder()
                            .withRequests(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi")))
                            .withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi")))
                            .build())
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
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
                .endSpec()
                .build();

        // Test the resources
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of("special", "annotation"));
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            if (podSet.getMetadata().getName().contains("brokers")) {
                assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(true));
                assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(true));
            } else {
                assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(false));
                assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(false));
            }

            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers")) {
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

                    assertThat(pod.getSpec().getVolumes().size(), is(6));
                    assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
                    assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(5).getName(), is("secret-volume-name"));
                    assertThat(pod.getSpec().getVolumes().get(5).getSecret(), is(notNullValue()));

                    // Containers
                    assertThat(pod.getSpec().getContainers().size(), is(1));
                    assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(securityContext));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("true"));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi"))));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi"))));

                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(6));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("secret-volume-name"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/mnt/secret-volume"));
                } else {
                    // Metadata
                    assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(false));
                    assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(false));
                    assertThat(pod.getMetadata().getAnnotations().get("special"), is("annotation"));

                    // Pod
                    assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
                    assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
                    assertThat(pod.getSpec().getHostAliases(), is(List.of()));
                    assertThat(pod.getSpec().getTopologySpreadConstraints(), is(List.of()));
                    assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
                    assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
                    assertThat(pod.getSpec().getImagePullSecrets().size(), is(0));
                    assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(0L));
                    assertThat(pod.getSpec().getAffinity(), is(nullValue()));
                    assertThat(pod.getSpec().getTolerations(), is(List.of()));

                    assertThat(pod.getSpec().getVolumes().size(), is(5));
                    assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("5Mi")));
                    assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));

                    // Containers
                    assertThat(pod.getSpec().getContainers().size(), is(1));
                    assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("false"));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is("false"));
                    assertThat(pod.getSpec().getContainers().get(0).getResources(), is(nullValue()));

                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(5));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));
                }
            }
        }
    }

    @Test
    public void testResourceResizingPodSetAnnotations() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "true")
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED, "false")
                .endMetadata()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of("test-anno", "test-value"));
        podSets.forEach(podSet -> {
            assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING), is("true"));
            assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED), is(nullValue()));
        });

        kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "false")
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED, "true")
                .endMetadata()
                .build();
        kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        podSets = kc.generatePodSets(null, null, node -> Map.of("test-anno", "test-value"));
        podSets.forEach(podSet -> {
            assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING), is(nullValue()));
            assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED), is("true"));
        });
    }

    @Test
    public void testEnvironmentVariableRack()   {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewEnvironmentVariableRack()
                            .withEnvVarName("MY_RACK_ID")
                        .endEnvironmentVariableRack()
                    .endKafka()
                .endSpec()
                .build();

        // Test the resources
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(null, null, node -> Map.of("special", "annotation"));
        assertThat(podSets.size(), is(2));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                // Check init containers
                assertThat(pod.getSpec().getInitContainers().size(), is(0));

                // Check volumes
                assertThat(pod.getSpec().getVolumes().size(), is(5));
                assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(pod.getSpec().getVolumes().get(1).getProjected(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("5Mi")));
                assertThat(pod.getSpec().getVolumes().get(3).getName(), is("kafka-metrics-and-logging"));
                assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is(pod.getMetadata().getName()));
                assertThat(pod.getSpec().getVolumes().get(4).getName(), is("ready-files"));
                assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));

                // Check volume mounts
                assertThat(pod.getSpec().getContainers().size(), is(1));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(5));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.SERVICE_ACCOUNT_TOKEN_VOLUME_NAME));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/var/run/secrets/kubernetes.io/serviceaccount"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("kafka-metrics-and-logging"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/custom-config/"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("ready-files"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/var/opt/kafka"));
            }
        }

        // Test Cluster Role Binding
        assertThat(kc.generateClusterRoleBinding(NAMESPACE), is(nullValue()));
    }
}
