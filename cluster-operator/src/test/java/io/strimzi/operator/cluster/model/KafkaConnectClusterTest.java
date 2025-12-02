/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.PodDNSConfigBuilder;
import io.fabric8.kubernetes.api.model.PodDNSConfigOptionBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
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
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.DnsPolicy;
import io.strimzi.api.kafka.model.common.template.EmptyDirMedium;
import io.strimzi.api.kafka.model.common.template.EmptyDirVolume;
import io.strimzi.api.kafka.model.common.template.EmptyDirVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSourceBuilder;
import io.strimzi.api.kafka.model.connect.ImageArtifactBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.MountedPluginBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class KafkaConnectClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider(Map.of(
        "NO_PROXY", new EnvVarBuilder().withName("NO_PROXY").withValue("127.0.0.1").build(),
        "NON_SHARED_VAR", new EnvVarBuilder().withName("NON_SHARED_VAR").withValue("foo").build()
    ));
    private static final String NAMESPACE = "test";
    private static final String NAME = "foo";
    private static final KafkaConnect RESOURCE = new KafkaConnectBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Map.of("my-user-label", "cromulent"))
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withBootstrapServers("my-kafka:9092")
                .withGroupId("my-group")
                .withConfigStorageTopic("my-config-topic")
                .withOffsetStorageTopic("my-offset-topic")
                .withStatusStorageTopic("my-status-topic")
                .withConfig(Map.of("foo", "bar"))
            .endSpec()
            .build();
    private static final KafkaConnectCluster KC = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, RESOURCE, VERSIONS, SHARED_ENV_PROVIDER);

    @Test
    public void testConnectConfiguration() {
        assertThat(KC.groupId, is("my-group"));
        assertThat(KC.configStorageTopic, is("my-config-topic"));
        assertThat(KC.offsetStorageTopic, is("my-offset-topic"));
        assertThat(KC.statusStorageTopic, is("my-status-topic"));
        assertThat(KC.configuration.getConfigOption("group.id"), is(nullValue()));
        assertThat(KC.configuration.getConfigOption("config.storage.topic"), is(nullValue()));
        assertThat(KC.configuration.getConfigOption("offset.storage.topic"), is(nullValue()));
        assertThat(KC.configuration.getConfigOption("status.storage.topic"), is(nullValue()));

        String connectConfigurations = KC.generateConnectConfigMap(new MetricsAndLogging(null, null)).getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("bootstrap.servers=my-kafka:9092"));
        assertThat(connectConfigurations, containsString("group.id=my-group"));
        assertThat(connectConfigurations, containsString("config.storage.topic=my-config-topic"));
        assertThat(connectConfigurations, containsString("offset.storage.topic=my-offset-topic"));
        assertThat(connectConfigurations, containsString("status.storage.topic=my-status-topic"));
        assertThat(connectConfigurations, containsString("key.converter=org.apache.kafka.connect.json.JsonConverter"));
        assertThat(connectConfigurations, containsString("value.converter=org.apache.kafka.connect.json.JsonConverter"));
        assertThat(connectConfigurations, containsString("foo=bar"));
    }

    // This should stop working after we use v1 API only and the new options in `.spec` are required
    @Test
    public void testConnectConfigurationOldStyle() {
        KafkaConnect oldStyleConnect = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withGroupId(null)
                    .withConfigStorageTopic(null)
                    .withOffsetStorageTopic(null)
                    .withStatusStorageTopic(null)
                    .withConfig(Map.of(
                            "group.id", "my-other-group",
                            "config.storage.topic", "my-other-config-topic",
                            "offset.storage.topic", "my-other-offset-topic",
                            "status.storage.topic", "my-other-status-topic",
                            "foo", "bar"))
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldStyleConnect, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.groupId, is("my-other-group"));
        assertThat(kc.configStorageTopic, is("my-other-config-topic"));
        assertThat(kc.offsetStorageTopic, is("my-other-offset-topic"));
        assertThat(kc.statusStorageTopic, is("my-other-status-topic"));
        assertThat(kc.configuration.getConfigOption("group.id"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("config.storage.topic"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("offset.storage.topic"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("status.storage.topic"), is(nullValue()));

        String connectConfigurations = kc.generateConnectConfigMap(new MetricsAndLogging(null, null)).getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("bootstrap.servers=my-kafka:9092"));
        assertThat(connectConfigurations, containsString("group.id=my-other-group"));
        assertThat(connectConfigurations, containsString("config.storage.topic=my-other-config-topic"));
        assertThat(connectConfigurations, containsString("offset.storage.topic=my-other-offset-topic"));
        assertThat(connectConfigurations, containsString("status.storage.topic=my-other-status-topic"));
        assertThat(connectConfigurations, containsString("key.converter=org.apache.kafka.connect.json.JsonConverter"));
        assertThat(connectConfigurations, containsString("value.converter=org.apache.kafka.connect.json.JsonConverter"));
        assertThat(connectConfigurations, containsString("foo=bar"));
    }

    // This should stop working after we use v1 API only and the new options in `.spec` are required
    @Test
    public void testConnectConfigurationOldStylePriority() {
        KafkaConnect oldStyleConnect = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withConfig(Map.of(
                            "group.id", "my-other-group",
                            "config.storage.topic", "my-other-config-topic",
                            "offset.storage.topic", "my-other-offset-topic",
                            "status.storage.topic", "my-other-status-topic",
                            "foo", "bar"))
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldStyleConnect, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.groupId, is("my-group"));
        assertThat(kc.configStorageTopic, is("my-config-topic"));
        assertThat(kc.offsetStorageTopic, is("my-offset-topic"));
        assertThat(kc.statusStorageTopic, is("my-status-topic"));
        assertThat(kc.configuration.getConfigOption("group.id"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("config.storage.topic"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("offset.storage.topic"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("status.storage.topic"), is(nullValue()));

        String connectConfigurations = kc.generateConnectConfigMap(new MetricsAndLogging(null, null)).getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("bootstrap.servers=my-kafka:9092"));
        assertThat(connectConfigurations, containsString("group.id=my-group"));
        assertThat(connectConfigurations, containsString("config.storage.topic=my-config-topic"));
        assertThat(connectConfigurations, containsString("offset.storage.topic=my-offset-topic"));
        assertThat(connectConfigurations, containsString("status.storage.topic=my-status-topic"));
        assertThat(connectConfigurations, containsString("key.converter=org.apache.kafka.connect.json.JsonConverter"));
        assertThat(connectConfigurations, containsString("value.converter=org.apache.kafka.connect.json.JsonConverter"));
        assertThat(connectConfigurations, containsString("foo=bar"));
    }

    // This should stop working after we use v1 API only and the new options in `.spec` are required
    @Test
    public void testConnectConfigurationOldDefaultConfiguration() {
        KafkaConnect oldStyleConnect = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withGroupId(null)
                    .withConfigStorageTopic(null)
                    .withOffsetStorageTopic(null)
                    .withStatusStorageTopic(null)
                    .withConfig(Map.of("foo", "bar"))
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldStyleConnect, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.groupId, is("connect-cluster"));
        assertThat(kc.configStorageTopic, is("connect-cluster-configs"));
        assertThat(kc.offsetStorageTopic, is("connect-cluster-offsets"));
        assertThat(kc.statusStorageTopic, is("connect-cluster-status"));
        assertThat(kc.configuration.getConfigOption("group.id"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("config.storage.topic"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("offset.storage.topic"), is(nullValue()));
        assertThat(kc.configuration.getConfigOption("status.storage.topic"), is(nullValue()));

        String connectConfigurations = kc.generateConnectConfigMap(new MetricsAndLogging(null, null)).getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("bootstrap.servers=my-kafka:9092"));
        assertThat(connectConfigurations, containsString("group.id=connect-cluster"));
        assertThat(connectConfigurations, containsString("config.storage.topic=connect-cluster-configs"));
        assertThat(connectConfigurations, containsString("offset.storage.topic=connect-cluster-offsets"));
        assertThat(connectConfigurations, containsString("status.storage.topic=connect-cluster-status"));
        assertThat(connectConfigurations, containsString("key.converter=org.apache.kafka.connect.json.JsonConverter"));
        assertThat(connectConfigurations, containsString("value.converter=org.apache.kafka.connect.json.JsonConverter"));
        assertThat(connectConfigurations, containsString("foo=bar"));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.modifiableMap(Labels.STRIMZI_CLUSTER_LABEL, NAME,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaConnectCluster.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectCluster.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, NAME,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + NAME,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaConnectResources.componentName(NAME));
    }

    @Test
    public void testDefaultValues() {
        KafkaConnect connect = new KafkaConnectBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withBootstrapServers("my-kafka:9092")
                .withGroupId("my-group")
                .withConfigStorageTopic("my-config-topic")
                .withOffsetStorageTopic("my-offset-topic")
                .withStatusStorageTopic("my-status-topic")
            .endSpec()
            .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, connect, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.image, is(KafkaVersionTestUtils.DEFAULT_KAFKA_CONNECT_IMAGE));
        assertThat(kc.getReplicas(), is(3)); // Once only v1 API is supported, there should be no default replicas number as it is a required field in v1
        assertThat(kc.readinessProbeOptions.getInitialDelaySeconds(), is(60));
        assertThat(kc.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(kc.livenessProbeOptions.getInitialDelaySeconds(), is(60));
        assertThat(kc.livenessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(kc.configuration.asOrderedProperties().asMap(), is(Map.of("key.converter", "org.apache.kafka.connect.json.JsonConverter", "value.converter", "org.apache.kafka.connect.json.JsonConverter")));
    }

    @Test
    public void testFromCrd() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
            .editSpec()
                .withImage("my-image:latest")
                .withReadinessProbe(new Probe(200, 20))
                .withLivenessProbe(new Probe(100, 10))
            .endSpec()
            .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.getReplicas(), is(2));
        assertThat(kc.image, is("my-image:latest"));
        assertThat(kc.readinessProbeOptions.getInitialDelaySeconds(), is(200));
        assertThat(kc.readinessProbeOptions.getTimeoutSeconds(), is(20));
        assertThat(kc.livenessProbeOptions.getInitialDelaySeconds(), is(100));
        assertThat(kc.livenessProbeOptions.getTimeoutSeconds(), is(10));
        assertThat(kc.configuration.asOrderedProperties().asMap(), is(Map.of("key.converter", "org.apache.kafka.connect.json.JsonConverter", "value.converter", "org.apache.kafka.connect.json.JsonConverter", "foo", "bar")));
        assertThat(kc.bootstrapServers, is("my-kafka:9092"));
    }

    @Test
    public void testGenerateService()   {
        Service svc = KC.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(KC.getComponentName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getMetadata().getAnnotations().size(), is(0));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));

        io.strimzi.operator.cluster.TestUtils.checkOwnerReference(svc, RESOURCE);
    }

    @Test
    public void testGenerateHeadlessService()   {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewHeadlessService()
                            .withNewMetadata()
                                .withLabels(Map.of("label1", "label-value1"))
                                .withAnnotations(Map.of("anno1", "anno-value1"))
                            .endMetadata()
                        .endHeadlessService()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        Service svc = kc.generateHeadlessService();

        Map<String, String> expectedLabels = expectedLabels(kc.getComponentName());
        expectedLabels.put("label1", "label-value1");

        assertThat(svc.getMetadata().getLabels(), is(expectedLabels));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of("anno1", "anno-value1")));
        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getClusterIP(), is("None"));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        io.strimzi.operator.cluster.TestUtils.checkOwnerReference(svc, resource);
    }

    @Test
    public void testPodSetWithRack() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editOrNewSpec()
                    .withNewRack()
                        .withTopologyKey("topology-key")
                    .endRack()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            // check that pod spec contains the init Kafka container
            List<Container> initContainers = pod.getSpec().getInitContainers();
            assertThat(initContainers, is(notNullValue()));
            assertThat(!initContainers.isEmpty(), is(true));
            assertThat(initContainers.stream().anyMatch(container -> container.getName().equals(KafkaConnectCluster.INIT_NAME)), is(true));
        });
    }

    @Test
    public void testWithAffinityAndToleration() {
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

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(affinity)
                            .withTolerations(toleration)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet ps = kc.generatePodSet(3, null, null, false, null, null, null);

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods)  {
            assertThat(pod.getSpec().getAffinity(), is(affinity));
            assertThat(pod.getSpec().getTolerations(), is(toleration));
        }
    }

    @Test
    public void testPodSetWithTls() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                    .endTls()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("ssl.truststore.certificates=${strimzisecrets:namespace/" + KafkaConnectResources.internalTlsTrustedCertsSecretName(NAME) + ":ca.crt}"));
        assertThat(connectConfigurations, containsString("ssl.truststore.type=PEM"));
        assertThat(connectConfigurations, containsString("security.protocol=SSL"));
        assertThat(connectConfigurations, not(containsString("ssl.keystore.")));
    }

    @Test
    public void testPodSetWithTlsAuth() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("user-secret")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("ssl.keystore.certificate.chain=${strimzisecrets:namespace/user-secret:user.crt}"));
        assertThat(connectConfigurations, containsString("ssl.keystore.type=PEM"));
        assertThat(connectConfigurations, containsString("security.protocol=SSL"));
        assertThat(connectConfigurations, containsString("ssl.truststore.certificates=${strimzisecrets:namespace/" + KafkaConnectResources.internalTlsTrustedCertsSecretName(NAME) + ":ca.crt}"));
        assertThat(connectConfigurations, containsString("ssl.truststore.type=PEM"));
    }

    @Test
    public void testPodSetWithTlsSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("my-secret")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            // 1 temp volume + 1 volume from logging/metrics + just 1 from the above certs Secret
            assertThat(pod.getSpec().getVolumes().size(), is(3));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
        });
    }

    @Test
    public void testPodSetWithScramSha512Auth() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("user1-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-512"));
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/user1-secret:password}\";"));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            List<Container> containers = pod.getSpec().getContainers();
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testPodSetWithScramSha512AuthAndTLSSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
            .editSpec()
                .editOrNewTls()
                    .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("user1.password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
            .endSpec()
            .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-512"));
        assertThat(connectConfigurations, containsString("security.protocol=SASL_SSL"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-secret:user1.password}\";"));
        assertThat(connectConfigurations, containsString("ssl.truststore.certificates=${strimzisecrets:namespace/" + KafkaConnectResources.internalTlsTrustedCertsSecretName(NAME) + ":ca.crt}"));
        assertThat(connectConfigurations, containsString("ssl.truststore.type=PEM"));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(3));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));

            List<Container> containers = pod.getSpec().getContainers();

            assertThat(containers.get(0).getVolumeMounts().size(), is(3));
            assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
        });
    }

    @Test
    public void testPodSetWithScramSha256Auth() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewKafkaClientAuthenticationScramSha256()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("user1-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha256()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-256"));
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAIN"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/user1-secret:password}\""));
        assertThat(connectConfigurations, not(containsString("ssl.truststore.")));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            List<Container> containers = pod.getSpec().getContainers();
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testPodSetWithScramSha256AuthAndTLSSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                    .endTls()
                    .withNewKafkaClientAuthenticationScramSha256()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("my-secret")
                            .withPassword("user1.password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha256()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-256"));
        assertThat(connectConfigurations, containsString("security.protocol=SASL_SSL"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-secret:user1.password}\";"));
        assertThat(connectConfigurations, containsString("ssl.truststore.certificates=${strimzisecrets:namespace/" + KafkaConnectResources.internalTlsTrustedCertsSecretName(NAME) + ":ca.crt}"));
        assertThat(connectConfigurations, containsString("ssl.truststore.type=PEM"));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(3));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));

            List<Container> containers = pod.getSpec().getContainers();

            assertThat(containers.get(0).getVolumeMounts().size(), is(3));
            assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
        });
    }

    @Test
    public void testPodSetWithPlainAuth() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("user1-secret")
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
            .endSpec()
            .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=PLAIN"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/user1-secret:password}\";"));
        assertThat(connectConfigurations, not(containsString("ssl.truststore.")));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            List<Container> containers = pod.getSpec().getContainers();
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for plain client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testPodSetWithPlainAuthAndTLSSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
            .editSpec()
                .editOrNewTls()
                    .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("user1.password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
            .endSpec()
            .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_SSL"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=PLAIN"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-secret:user1.password}\";"));
        assertThat(connectConfigurations, containsString("ssl.truststore.certificates=${strimzisecrets:namespace/" + KafkaConnectResources.internalTlsTrustedCertsSecretName(NAME) + ":ca.crt}"));
        assertThat(connectConfigurations, containsString("ssl.truststore.type=PEM"));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(3));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));

            List<Container> containers = pod.getSpec().getContainers();

            assertThat(containers.get(0).getVolumeMounts().size(), is(3));
            assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
        });
    }

    @Test
    public void testPodSet()   {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withImage("my-image:latest")
                    .withReadinessProbe(new Probe(200, 20))
                    .withLivenessProbe(new Probe(100, 10))
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("metrics-cm").withKey("metrics-config.yml").withOptional(true).build())
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(Map.of("label1", "label-value1"))
                                .withAnnotations(Map.of("anno1", "anno-value1"))
                            .endMetadata()
                        .endPodSet()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet ps = kc.generatePodSet(3, Map.of("anno2", "anno-value2"), Map.of("anno3", "anno-value3"), false, null, null, null);

        assertThat(ps.getMetadata().getName(), is(KafkaConnectResources.componentName(NAME)));
        assertThat(ps.getMetadata().getLabels().entrySet().containsAll(kc.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(ps.getMetadata().getAnnotations(), is(Map.of("anno1", "anno-value1", "anno2", "anno-value2")));
        io.strimzi.operator.cluster.TestUtils.checkOwnerReference(ps, resource);
        assertThat(ps.getSpec().getSelector().getMatchLabels(), is(kc.getSelectorLabels().withStrimziPodSetController(KafkaConnectResources.componentName(NAME)).toMap()));
        assertThat(ps.getSpec().getPods().size(), is(3));

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(kc.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(kc.getComponentName()).toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(2));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("anno3"), is("anno-value3"));

            assertThat(pod.getSpec().getHostname(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getSubdomain(), is(KafkaConnectResources.componentName(NAME)));
            assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
            assertThat(pod.getSpec().getVolumes().stream()
                    .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                    .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getName(), is(KafkaConnectResources.componentName(NAME)));
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-image:latest"));
            assertThat(pod.getSpec().getContainers().get(0).getEnv(), is(List.of(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_JMX_EXPORTER_ENABLED).withValue(String.valueOf(true)).build(),
                    new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build(),
                    new EnvVarBuilder().withName(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS).withValue("-Xms" + JvmOptionUtils.DEFAULT_JVM_XMS).build(),
                    new EnvVarBuilder().withName("NO_PROXY").withValue("127.0.0.1").build())));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(10));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(100));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(20));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(200));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().size(), is(2));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getContainerPort(), is(KafkaConnectCluster.REST_API_PORT));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getProtocol(), is("TCP"));
        }
    }

    @Test
    @SuppressWarnings({"checkstyle:methodlength"})
    public void testTemplate() {
        Map<String, String> spsLabels = Map.of("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(spsLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> spsAnnos = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnnos = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> svcAnnos = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> pdbLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> pdbAnnos = Map.of("a7", "v7", "a8", "v8");

        Map<String, String> crbLabels = Map.of("l9", "v9", "l10", "v10");
        Map<String, String> crbAnnos = Map.of("a9", "v9", "a10", "v10");

        Map<String, String> saLabels = Map.of("l11", "v11", "l12", "v12");
        Map<String, String> saAnnos = Map.of("a11", "v11", "a12", "v12");

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

        DnsPolicy dnsPolicy = DnsPolicy.NONE;
        PodDNSConfig dnsConfig = new PodDNSConfigBuilder()
            .withNameservers("192.0.2.1")
            .withSearches("ns1.svc.cluster-domain.example", "my.dns.search.suffix")
            .withOptions(
                new PodDNSConfigOptionBuilder()
                        .withName("ndots")
                        .withValue("2")
                        .build(),
                new PodDNSConfigOptionBuilder()
                        .withName("edns0")
                        .build()
            )
            .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
                .build();

        ConfigMapVolumeSource configMap = new ConfigMapVolumeSourceBuilder()
                .withName("configMap1")
                .build();

        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();

        EmptyDirVolume emptyDir = new EmptyDirVolumeBuilder()
                .withMedium(EmptyDirMedium.MEMORY)
                .build();

        AdditionalVolume additionalVolumeConfigMap = new AdditionalVolumeBuilder()
                .withName("config-map-volume-name")
                .withConfigMap(configMap)
                .build();

        AdditionalVolume additionalVolumeSecret = new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build();

        AdditionalVolume additionalVolumeEmptyDir = new AdditionalVolumeBuilder()
                .withName("empty-dir-volume-name")
                .withEmptyDir(emptyDir)
                .build();

        VolumeMount additionalVolumeMountConfigMap = new VolumeMountBuilder()
                .withName("config-map-volume-name")
                .withMountPath("/mnt/config")
                .withSubPath("def")
                .build();

        VolumeMount additionalVolumeMountSecret = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret")
                .withSubPath("abc")
                .build();

        VolumeMount additionalVolumeMountEmptyDir = new VolumeMountBuilder()
                .withName("empty-dir-volume-name")
                .withMountPath("/mnt/empty-dir")
                .withSubPath("def")
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewRack("my-topology-key")
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(spsLabels)
                                .withAnnotations(spsAnnos)
                            .endMetadata()
                        .endPodSet()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(podLabels)
                                .withAnnotations(podAnnos)
                            .endMetadata()
                            .withPriorityClassName("top-priority")
                            .withSchedulerName("my-scheduler")
                            .withHostAliases(hostAlias1, hostAlias2)
                            .withDnsPolicy(dnsPolicy)
                            .withDnsConfig(dnsConfig)
                            .withTopologySpreadConstraints(tsc1, tsc2)
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                            .withVolumes(additionalVolumeSecret, additionalVolumeEmptyDir, additionalVolumeConfigMap)
                        .endPod()
                        .withNewConnectContainer()
                            .withVolumeMounts(additionalVolumeMountSecret, additionalVolumeMountEmptyDir)
                        .endConnectContainer()
                        .withNewInitContainer()
                            .withVolumeMounts(additionalVolumeMountConfigMap)
                        .endInitContainer()
                        .withNewApiService()
                            .withNewMetadata()
                                .withLabels(svcLabels)
                                .withAnnotations(svcAnnos)
                            .endMetadata()
                            .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                            .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                        .endApiService()
                        .withNewPodDisruptionBudget()
                            .withNewMetadata()
                                .withLabels(pdbLabels)
                                .withAnnotations(pdbAnnos)
                            .endMetadata()
                        .endPodDisruptionBudget()
                        .withNewClusterRoleBinding()
                            .withNewMetadata()
                                .withLabels(crbLabels)
                                .withAnnotations(crbAnnos)
                            .endMetadata()
                        .endClusterRoleBinding()
                        .withNewServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnnos)
                            .endMetadata()
                        .endServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
            assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnos.entrySet()), is(true));

            // Check Pods
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnnos.entrySet()), is(true));
            assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
            assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
            assertThat(pod.getSpec().getDnsPolicy(), is(DnsPolicy.NONE.toValue()));
            assertThat(pod.getSpec().getDnsConfig(), is(dnsConfig));
            assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
            assertThat(pod.getSpec().getEnableServiceLinks(), is(false));


            assertThat(pod.getSpec().getVolumes().size(), is(6));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(1).getConfigMap().getName(), is("foo-connect-config"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("1Mi")));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("secret-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is("secret1"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("empty-dir-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir().getMedium(), is("Memory"));
            assertThat(pod.getSpec().getVolumes().get(5).getName(), is("config-map-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is("configMap1"));

            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().size(), is(2));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(0).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(1).getName(), is("config-map-volume-name"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/mnt/config"));

            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is("/opt/kafka/init"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("secret-volume-name"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/mnt/secret"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("empty-dir-volume-name"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/mnt/empty-dir"));
        });

        // Check Service
        Service svc = kc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnnos.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnos.entrySet()), is(true));

        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kc.generateClusterRoleBinding();
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnnos.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnos.entrySet()), is(true));
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationSecretEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);

        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
            assertThat(selected.get(0).getValueFrom().getSecretKeyRef(), is(env.getValueFrom().getSecretKeyRef()));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationConfigEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);

        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
            assertThat(selected.get(0).getValueFrom().getConfigMapKeyRef(), is(env.getValueFrom().getConfigMapKeyRef()));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationSecretVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationSecretVolumesWithDots() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my.volume")
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my.secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my.volume"));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationConfigVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationConfigVolumesWithDots() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my.volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my.map").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my.volume"));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationInvalidVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(0));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(0));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testNoExternalConfigurationVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(0));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(0));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testInvalidExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(0));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testNoExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(0));
        });
    }

    @Test
    public void testGracePeriod() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
        });
    }

    @Test
    public void testDefaultGracePeriod() {
        // Check PodSet
        StrimziPodSet podSet = KC.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        });
    }

    @Test
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        });
    }

    @Test
    public void testImagePullSecretsCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        // Check PodSet
        StrimziPodSet podSet = KC.generatePodSet(3, Map.of(), Map.of(), false, null, List.of(secret1, secret2), null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        });
    }

    @Test
    public void testImagePullSecretsBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, List.of(secret1), null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        });
    }

    @Test
    public void testDefaultImagePullSecrets() {
        // Check PodSet
        StrimziPodSet podSet = KC.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets(), is(List.of()));
        });
    }

    @Test
    public void testSecurityContext() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
            assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(123L));
            assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(456L));
            assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(789L));
        });
    }

    @Test
    public void testDefaultSecurityContext() {
        // Check PodSet
        StrimziPodSet podSet = KC.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
        });
    }

    @Test
    public void testRestrictedSecurityContext() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, RESOURCE, VERSIONS, SHARED_ENV_PROVIDER);
        kc.securityProvider = new RestrictedPodSecurityProvider();
        kc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
        });
    }

    @Test
    public void testPodDisruptionBudget() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(0)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
    }

    @Test
    public void testDefaultPodDisruptionBudget() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(1)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
    }

    @Test
    public void testImagePullPolicy() {
        // Check PodSet
        StrimziPodSet podSet = KC.generatePodSet(3, Map.of(), Map.of(), false, ImagePullPolicy.ALWAYS, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        });

        podSet = KC.generatePodSet(3, Map.of(), Map.of(), false, ImagePullPolicy.IFNOTPRESENT, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        });
    }

    @Test
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getResources().getLimits(), is(limits));
            assertThat(cont.getResources().getRequests(), is(requests));
        });
    }

    @Test
    public void testJvmOptions() {
        Map<String, String> xx = new HashMap<>(2);
        xx.put("UseG1GC", "true");
        xx.put("MaxGCPauseMillis", "20");

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewJvmOptions()
                        .withXms("512m")
                        .withXmx("1024m")
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"), is(true));
        });
    }

    @Test
    public void testKafkaConnectContainerEnvVars() {
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
        ContainerTemplate kafkaConnectContainer = new ContainerTemplate();
        kafkaConnectContainer.setEnv(testEnvs);

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaConnectContainer)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @Test
    public void testKafkaConnectContainerSecurityContext() {
        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editOrNewTemplate()
                        .withNewConnectContainer()
                            .withSecurityContext(securityContext)
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaConnectCluster kcc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kcc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getContainers(),
                    hasItem(allOf(
                            hasProperty("name", equalTo(NAME + "-connect")),
                            hasProperty("securityContext", equalTo(securityContext))
                    )));
        });
    }

    @Test
    public void testOpenTelemetryTracing() {
        KafkaConnectBuilder builder = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewOpenTelemetryTracing()
                    .endOpenTelemetryTracing()
                .endSpec();
        KafkaConnect resource = builder.build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("consumer.interceptor.classes=" + OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME));
        assertThat(connectConfigurations, containsString("producer.interceptor.classes=" + OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(env -> KafkaConnectCluster.ENV_VAR_STRIMZI_TRACING.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals(OpenTelemetryTracing.TYPE_OPENTELEMETRY), is(true));
        });
    }

    @Test
    public void testPodSetWithOAuthWithAccessToken() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withNewAccessToken()
                                        .withSecretName("my-token-secret")
                                        .withKey("my-token-key")
                                    .endAccessToken()
                                    .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token=\"${strimzidir:/opt/kafka/oauth/my-token-secret:my-token-key}\";"));
        assertThat(connectConfigurations, containsString("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));

    }

    @Test
    public void testPodSetWithOAuthWithRefreshToken() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withConnectTimeoutSeconds(15)
                                .withReadTimeoutSeconds(15)
                                .withHttpRetries(2)
                                .withHttpRetryPauseMs(500)
                                .withEnableMetrics(true)
                                .withIncludeAcceptHeader(false)
                                .withNewRefreshToken()
                                    .withSecretName("my-token-secret")
                                    .withKey("my-token-key")
                                .endRefreshToken()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.connect.timeout.seconds=\"15\" " +
                "oauth.read.timeout.seconds=\"15\" " +
                "oauth.http.retries=\"2\" " +
                "oauth.http.retry.pause.millis=\"500\" " +
                "oauth.enable.metrics=\"true\" " +
                "oauth.include.accept.header=\"false\" " +
                "oauth.refresh.token=\"${strimzidir:/opt/kafka/oauth/my-token-secret:my-token-key}\";"));
        assertThat(connectConfigurations, containsString("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
    }

    @Test
    public void testPodSetWithOAuthWithClientSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withAudience("kafka")
                                .withScope("all")
                                .withGrantType("custom-client-credentials")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"my-client-id\" oauth.token.endpoint.uri=\"http://my-oauth-server\" oauth.client.credentials.grant.type=\"custom-client-credentials\" oauth.scope=\"all\" oauth.audience=\"kafka\" oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\";"));
        assertThat(connectConfigurations, containsString("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
    }

    @Test
    public void testPodSetWithOAuthWithClientSecretAndSaslExtensions() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withAudience("kafka")
                                .withScope("all")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .withSaslExtensions(new TreeMap<>(Map.of("key1", "value1", "key2", "value2")))
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.scope=\"all\" oauth.audience=\"kafka\" " +
                "oauth.sasl.extension.key1=\"value1\" " +
                "oauth.sasl.extension.key2=\"value2\" " +
                "oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\";"));
        assertThat(connectConfigurations, containsString("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));

    }

    @Test
    public void testPodSetWithOAuthWithClientAssertion() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withAudience("kafka")
                                .withScope("all")
                                .withNewClientAssertion()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientAssertion()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.scope=\"all\" " +
                "oauth.audience=\"kafka\" " +
                "oauth.client.assertion=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\";"));
        assertThat(connectConfigurations, containsString("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
    }

    @Test
    public void testPodSetWithOAuthWithUsernameAndPassword() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withClientId("my-client-id")
                                .withUsername("user1")
                                .withNewPasswordSecret()
                                .withSecretName("my-password-secret")
                                .withPassword("user1.password")
                                .endPasswordSecret()
                                .withNewClientSecret()
                                .withSecretName("my-secret-secret")
                                .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.client.id=\"my-client-id\" " +
                "oauth.password.grant.username=\"user1\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\" " +
                "oauth.password.grant.password=\"${strimzidir:/opt/kafka/oauth/my-password-secret:user1.password}\";"));
        assertThat(connectConfigurations, containsString("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
    }

    @Test
    public void testPodSetWithOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                    .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withTokenEndpointUri("http://my-oauth-server")
                                    .build())
                    .endSpec()
                    .build();

            KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        });
    }

    @Test
    public void testPodSetWithOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                    .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                    .endClientSecret()
                                    .build())
                    .endSpec()
                    .build();

            KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        });
    }

    @Test
    public void testPodSetWithOAuthWithTls() {
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

        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .withDisableTlsHostnameVerification(true)
                                .withTlsTrustedCertificates(cert1, cert2, cert3)
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        String oauthSecret = KafkaConnectResources.internalOauthTrustedCertsSecretName(NAME);
        // Check config map
        ConfigMap configMap = kc.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.ssl.endpoint.identification.algorithm=\"\" " +
                "oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\" " +
                "oauth.ssl.truststore.location=\"/opt/kafka/oauth-certs/" + oauthSecret + "/ca.crt\" " +
                "oauth.ssl.truststore.type=\"PEM\";"));
        assertThat(connectConfigurations, containsString("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);

            // Volume mounts
            assertThat(cont.getVolumeMounts().stream().filter(mount -> oauthSecret.equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + oauthSecret));
            // Volumes
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> oauthSecret.equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
        });
    }

    @Test
    public void testNetworkPolicyWithConnectorOperator() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("metrics-cm").withKey("metrics-config.yml").withOptional(true).build())
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(true, "operator-namespace", null);

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Map.of()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @Test
    public void testNetworkPolicyWithConnectorOperatorSameNamespace() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("metrics-cm").withKey("metrics-config.yml").withOptional(true).build())
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(true, NAMESPACE, null);

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @Test
    public void testNetworkPolicyWithConnectorOperatorWithNamespaceLabels() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("metrics-cm").withKey("metrics-config.yml").withOptional(true).build())
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(true, "operator-namespace", Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @Test
    public void testNetworkPolicyWithoutConnectorOperator() {
        assertThat(KC.generateNetworkPolicy(false, null, null), is(nullValue()));
    }

    @Test
    public void testClusterRoleBindingRack() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                    .editOrNewSpec()
                        .withNewRack("topology-key")
                    .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = kc.generateClusterRoleBinding();

        assertThat(crb.getMetadata().getName(), is(KafkaConnectResources.initContainerClusterRoleBindingName(NAME, NAMESPACE)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(NAMESPACE));
        assertThat(crb.getSubjects().get(0).getName(), is(kc.componentName));
    }

    @Test
    public void testNullClusterRoleBindingWithoutRackAwareness() {
        ClusterRoleBinding crb = KC.generateClusterRoleBinding();
        assertThat(crb, is(nullValue()));
    }

    @Test
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.metrics(), is(notNullValue()));
        assertThat(((JmxPrometheusExporterModel) kc.metrics()).getConfigMapName(), is("my-metrics-configuration"));
        assertThat(((JmxPrometheusExporterModel) kc.metrics()).getConfigMapKey(), is("config.yaml"));
    }

    @Test
    public void testMetricsParsingNoMetrics() {
        assertThat(KC.metrics(), is(nullValue()));
    }

    @Test
    public void testStrimziMetricsReporterConfig() {
        MetricsConfig metrics = new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList("kafka_connect_connector_metrics.*" + "kafka_connect_connector_task_metrics.*")
                .endValues().build();

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.metrics(), is(notNullValue()));
        assertThat(((StrimziMetricsReporterModel) kc.metrics()).getAllowList(), is("kafka_connect_connector_metrics.*" + "kafka_connect_connector_task_metrics.*"));

        NetworkPolicy np = kc.generateNetworkPolicy(true, null, null);
        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream()
                .filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(MetricsModel.METRICS_PORT)))
                .toList();

        assertThat(rules.size(), is(1));
    }

    @Test
    public void testJmxSecretCustomLabelsAndAnnotations() {
        Map<String, String> customLabels = new HashMap<>(2);
        customLabels.put("label1", "value1");
        customLabels.put("label2", "value2");

        Map<String, String> customAnnotations = new HashMap<>(2);
        customAnnotations.put("anno1", "value3");
        customAnnotations.put("anno2", "value4");

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
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
                .endSpec()
                .build();

        KafkaConnectCluster kafkaConnectCluster = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS, SHARED_ENV_PROVIDER);

        Secret jmxSecret = kafkaConnectCluster.jmx().jmxSecret(null);

        for (Map.Entry<String, String> entry : customAnnotations.entrySet()) {
            assertThat(jmxSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry : customLabels.entrySet()) {
            assertThat(jmxSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    @Test
    public void testKafkaConnectInitContainerSectionIsConfigurable() {
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

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(RESOURCE)
            .editSpec()
                .withResources(resourceReq)
                .withNewRack()
                    .withTopologyKey("rack-key")
                .endRack()
            .endSpec()
            .build();

        KafkaConnectCluster kcc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS, SHARED_ENV_PROVIDER);

        ResourceRequirements initContainersResources = kcc.createInitContainer(ImagePullPolicy.IFNOTPRESENT).getResources();
        assertThat(initContainersResources.getRequests(), is(requirements));
        assertThat(initContainersResources.getLimits(), is(limits));
    }

    @Test
    public void testLoggingWithLog4j2() {
        // Check config map
        ConfigMap cm = KC.generateConnectConfigMap(new MetricsAndLogging(null, null));
        assertThat(cm.getData().get(LoggingModel.LOG4J2_CONFIG_MAP_KEY), is(notNullValue()));
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testOciConnectorPlugins() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
            .editSpec()
                .withPlugins(
                        new MountedPluginBuilder()
                                .withName("first-connector")
                                .withArtifacts(new ImageArtifactBuilder().withReference("first-artifact:latest").build(),
                                        new ImageArtifactBuilder().withReference("second-artifact:0.2.0").withPullPolicy("Never").build())
                                .build(),
                        new MountedPluginBuilder()
                                .withName("second-connector")
                                .withArtifacts(new ImageArtifactBuilder().withReference("third-artifact:latest").withPullPolicy("IfNotPresent").build())
                                .build()
                )
            .endSpec()
            .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(5));
            // Default volumes used for /tmp and for Connect configuration Config Map
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));

            // Connector plugin volumes
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("plugin-first-connector-0a4dc47f"));
            assertThat(pod.getSpec().getVolumes().get(2).getImage().getReference(), is("first-artifact:latest"));
            assertThat(pod.getSpec().getVolumes().get(2).getImage().getPullPolicy(), is(nullValue()));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("plugin-first-connector-73d69cbd"));
            assertThat(pod.getSpec().getVolumes().get(3).getImage().getReference(), is("second-artifact:0.2.0"));
            assertThat(pod.getSpec().getVolumes().get(3).getImage().getPullPolicy(), is("Never"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("plugin-second-connector-695ab9d6"));
            assertThat(pod.getSpec().getVolumes().get(4).getImage().getReference(), is("third-artifact:latest"));
            assertThat(pod.getSpec().getVolumes().get(4).getImage().getPullPolicy(), is("IfNotPresent"));

            List<Container> containers = pod.getSpec().getContainers();

            assertThat(containers.get(0).getVolumeMounts().size(), is(5));
            // Default volume mounts used for /tmp and for Connect configuration Config Map
            assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));

            // Connector plugin volume mounts
            assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("plugin-first-connector-0a4dc47f"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is("/opt/kafka/plugins/first-connector/0a4dc47f"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is("plugin-first-connector-73d69cbd"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is("/opt/kafka/plugins/first-connector/73d69cbd"));
            assertThat(containers.get(0).getVolumeMounts().get(4).getName(), is("plugin-second-connector-695ab9d6"));
            assertThat(containers.get(0).getVolumeMounts().get(4).getMountPath(), is("/opt/kafka/plugins/second-connector/695ab9d6"));
        });
    }

    @Test
    public void testRoleAbdRoleBindingNoSecrets() {
        assertThat(KC.generateRole(), is(nullValue()));
        assertThat(KC.generateRoleBindingForRole(), is(nullValue()));
    }

    @Test
    public void testRoleAbdRoleBindingWithSecrets() {
        KafkaConnect resource = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTls()
                        .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("ca.crt").build())
                    .endTls()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        Role role = kc.generateRole();
        assertThat(role.getMetadata().getName(), is(kc.componentName));
        assertThat(role.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(role.getRules().size(), is(1));
        assertThat(role.getRules().get(0).getApiGroups(), is(List.of("")));
        assertThat(role.getRules().get(0).getResources(), is(List.of("secrets")));
        assertThat(role.getRules().get(0).getVerbs(), is(List.of("get")));
        assertThat(role.getRules().get(0).getResourceNames(), is(List.of(kc.componentName + "-tls-trusted-certs")));

        RoleBinding rb = kc.generateRoleBindingForRole();
        assertThat(rb.getMetadata().getName(), is(KafkaConnectResources.connectRoleBindingName(NAME)));
        assertThat(rb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(rb.getSubjects().size(), is(1));
        assertThat(rb.getSubjects().get(0).getKind(), is("ServiceAccount"));
        assertThat(rb.getSubjects().get(0).getNamespace(), is(NAMESPACE));
        assertThat(rb.getSubjects().get(0).getName(), is(kc.componentName));
        assertThat(rb.getRoleRef().getKind(), is("Role"));
        assertThat(rb.getRoleRef().getName(), is(kc.componentName));
    }
}
