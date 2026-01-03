/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.PodDNSConfigBuilder;
import io.fabric8.kubernetes.api.model.PodDNSConfigOptionBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
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
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.TestUtils;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class KafkaMirrorMaker2ClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "test";
    private static final String NAME = "foo";
    private static final ConfigMap METRICS_CONFIG = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName("mm2-metrics-config")
                .endMetadata()
                .withData(singletonMap("metrics-config.yml", "{\"animal\":\"wombat\"}"))
                .build();
    private static final KafkaMirrorMaker2 RESOURCE = new KafkaMirrorMaker2Builder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Map.of("my-user-label", "cromulent"))
            .endMetadata()
            .withNewSpec()
                .withNewTarget()
                    .withAlias("target")
                    .withGroupId("my-mm2-group")
                    .withConfigStorageTopic("my-mm2-config")
                    .withOffsetStorageTopic("my-mm2-offset")
                    .withStatusStorageTopic("my-mm2-status")
                    .withBootstrapServers("target:9092")
                .endTarget()
                .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withNewSource()
                            .withAlias("source")
                            .withBootstrapServers("source:9092")
                        .endSource()
                        .withNewSourceConnector()
                            .withTasksMax(5)
                            .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                        .endSourceConnector()
                        .withNewCheckpointConnector()
                            .withTasksMax(3)
                            .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                        .endCheckpointConnector()
                        .withNewHeartbeatConnector()
                            .withTasksMax(1)
                        .endHeartbeatConnector()
                        .withTopicsPattern("my-topic-.*")
                        .withTopicsExcludePattern("exclude-topic-.*")
                        .withGroupsPattern("my-group-.*")
                        .withGroupsExcludePattern("exclude-group-.*")
                        .build())
            .endSpec()
            .build();
    private static final KafkaMirrorMaker2Cluster KMM2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, RESOURCE, VERSIONS, SHARED_ENV_PROVIDER);

    private final OrderedProperties defaultConfiguration = new OrderedProperties()
            .addPair("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .addPair("header.converter", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .addPair("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    private Map<String, String> expectedLabels(String name)    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaMirrorMaker2Cluster.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, KafkaMirrorMaker2Cluster.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, NAME,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + NAME,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaMirrorMaker2Resources.componentName(NAME));
    }

    @Test
    public void testDefaultValues() {
        assertThat(KMM2.image, is(KafkaVersionTestUtils.DEFAULT_KAFKA_CONNECT_IMAGE));
        assertThat(KMM2.getReplicas(), is(3));
        assertThat(KMM2.readinessProbeOptions.getInitialDelaySeconds(), is(60));
        assertThat(KMM2.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(KMM2.livenessProbeOptions.getInitialDelaySeconds(), is(60));
        assertThat(KMM2.livenessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(KMM2.configuration.asOrderedProperties(), is(defaultConfiguration));
        assertThat(KMM2.groupId, is("my-mm2-group"));
        assertThat(KMM2.configStorageTopic, is("my-mm2-config"));
        assertThat(KMM2.offsetStorageTopic, is("my-mm2-offset"));
        assertThat(KMM2.statusStorageTopic, is("my-mm2-status"));
        assertThat(KMM2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("group.id=")));
        assertThat(KMM2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("config.storage.topic=")));
        assertThat(KMM2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("offset.storage.topic=")));
        assertThat(KMM2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("status.storage.topic=")));
    }

    @Test
    public void testConfigurationConfigMap() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                        .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                                .withName("mm2-metrics-config")
                                .withKey("metrics-config.yml")
                                .build())
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        ConfigMap cm = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));

        assertThat(cm.getData().get(JmxPrometheusExporterModel.CONFIG_MAP_KEY), is("{\"animal\":\"wombat\"}"));

        String connectConfigurations = cm.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("bootstrap.servers=target:9092"));
        assertThat(connectConfigurations, containsString("group.id=my-mm2-group"));
        assertThat(connectConfigurations, containsString("config.storage.topic=my-mm2-config"));
        assertThat(connectConfigurations, containsString("offset.storage.topic=my-mm2-offset"));
        assertThat(connectConfigurations, containsString("status.storage.topic=my-mm2-status"));
        assertThat(connectConfigurations, containsString(defaultConfiguration.asPairs()));
        // MirrorMaker relies on env and file config providers to be configured by default
        assertThat(connectConfigurations, containsString("config.providers=strimzienv,strimzifile"));
        assertThat(connectConfigurations, containsString("config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider"));
        assertThat(connectConfigurations, containsString("config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider"));
    }

    @Test
    public void testConfigurationConfigMapWithOldCustomConfig() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName("my-mm2")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withConnectCluster("target")
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                                    .withAlias("source")
                                    .withBootstrapServers("source:9092")
                                    .build(),
                            new KafkaMirrorMaker2ClusterSpecBuilder()
                                    .withAlias("target")
                                    .withBootstrapServers("target:9092")
                                    .withConfig(Map.of(
                                            "group.id", "my-other-mm2-group",
                                            "config.storage.topic", "my-other-mm2-config",
                                            "offset.storage.topic", "my-other-mm2-offset",
                                            "status.storage.topic", "my-other-mm2-status"
                                    ))
                                    .build())
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withSourceCluster("source")
                            .withTargetCluster("target")
                            .withNewSourceConnector()
                                .withTasksMax(5)
                                .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                            .endSourceConnector()
                            .withNewCheckpointConnector()
                                .withTasksMax(3)
                                .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                            .endCheckpointConnector()
                            .withNewHeartbeatConnector()
                                .withTasksMax(1)
                            .endHeartbeatConnector()
                            .withTopicsPattern("my-topic-.*")
                            .withTopicsExcludePattern("exclude-topic-.*")
                            .withGroupsPattern("my-group-.*")
                            .withGroupsExcludePattern("exclude-group-.*")
                            .build())
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kmm2.groupId, is("my-other-mm2-group"));
        assertThat(kmm2.configStorageTopic, is("my-other-mm2-config"));
        assertThat(kmm2.offsetStorageTopic, is("my-other-mm2-offset"));
        assertThat(kmm2.statusStorageTopic, is("my-other-mm2-status"));
        assertThat(kmm2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("group.id=")));
        assertThat(kmm2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("config.storage.topic=")));
        assertThat(kmm2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("offset.storage.topic=")));
        assertThat(kmm2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("status.storage.topic=")));

        ConfigMap cm = kmm2.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = cm.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("bootstrap.servers=target:9092"));
        assertThat(connectConfigurations, containsString("group.id=my-other-mm2-group"));
        assertThat(connectConfigurations, containsString("config.storage.topic=my-other-mm2-config"));
        assertThat(connectConfigurations, containsString("offset.storage.topic=my-other-mm2-offset"));
        assertThat(connectConfigurations, containsString("status.storage.topic=my-other-mm2-status"));
        assertThat(connectConfigurations, containsString(defaultConfiguration.asPairs()));
        // MirrorMaker relies on env and file config providers to be configured by default
        assertThat(connectConfigurations, containsString("config.providers=strimzienv,strimzifile"));
        assertThat(connectConfigurations, containsString("config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider"));
        assertThat(connectConfigurations, containsString("config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider"));
    }

    @Test
    public void testConfigurationConfigMapWithOldDefaults() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName("my-mm2")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withConnectCluster("target")
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                                    .withAlias("source")
                                    .withBootstrapServers("source:9092")
                                    .build(),
                            new KafkaMirrorMaker2ClusterSpecBuilder()
                                    .withAlias("target")
                                    .withBootstrapServers("target:9092")
                                    .build())
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withSourceCluster("source")
                            .withTargetCluster("target")
                            .withNewSourceConnector()
                                .withTasksMax(5)
                                .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                            .endSourceConnector()
                            .withNewCheckpointConnector()
                                .withTasksMax(3)
                                .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                            .endCheckpointConnector()
                            .withNewHeartbeatConnector()
                                .withTasksMax(1)
                            .endHeartbeatConnector()
                            .withTopicsPattern("my-topic-.*")
                            .withTopicsExcludePattern("exclude-topic-.*")
                            .withGroupsPattern("my-group-.*")
                            .withGroupsExcludePattern("exclude-group-.*")
                            .build())
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kmm2.groupId, is("mirrormaker2-cluster"));
        assertThat(kmm2.configStorageTopic, is("mirrormaker2-cluster-configs"));
        assertThat(kmm2.offsetStorageTopic, is("mirrormaker2-cluster-offsets"));
        assertThat(kmm2.statusStorageTopic, is("mirrormaker2-cluster-status"));
        assertThat(kmm2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("group.id=")));
        assertThat(kmm2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("config.storage.topic=")));
        assertThat(kmm2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("offset.storage.topic=")));
        assertThat(kmm2.configuration.asOrderedProperties().asPairs(), not(Matchers.containsString("status.storage.topic=")));

        ConfigMap cm = kmm2.generateConnectConfigMap(new MetricsAndLogging(null, null));
        String connectConfigurations = cm.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("bootstrap.servers=target:9092"));
        assertThat(connectConfigurations, containsString("group.id=mirrormaker2-cluster"));
        assertThat(connectConfigurations, containsString("config.storage.topic=mirrormaker2-cluster-configs"));
        assertThat(connectConfigurations, containsString("offset.storage.topic=mirrormaker2-cluster-offsets"));
        assertThat(connectConfigurations, containsString("status.storage.topic=mirrormaker2-cluster-status"));
        assertThat(connectConfigurations, containsString(defaultConfiguration.asPairs()));
        // MirrorMaker relies on env and file config providers to be configured by default
        assertThat(connectConfigurations, containsString("config.providers=strimzienv,strimzifile"));
        assertThat(connectConfigurations, containsString("config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider"));
        assertThat(connectConfigurations, containsString("config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider"));
    }

    @Test
    public void testGenerateService()   {
        Service svc = KMM2.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(KMM2.getComponentName())));
        assertThat(svc.getMetadata().getAnnotations().size(), is(0));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaMirrorMaker2Cluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaMirrorMaker2Cluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));

        io.strimzi.operator.cluster.TestUtils.checkOwnerReference(svc, RESOURCE);
    }

    @Test
    public void testPodSet()   {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withImage("my-image:latest")
                    .withReadinessProbe(new Probe(123, 456))
                    .withLivenessProbe(new Probe(321, 654))
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                        .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                                .withName("mm2-metrics-config")
                                .withKey("metrics-config.yml")
                                .build())
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
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet ps = kmm2.generatePodSet(3, Map.of("anno2", "anno-value2"), Map.of("anno3", "anno-value3"), false, null, null, null);

        assertThat(ps.getMetadata().getName(), is(KafkaMirrorMaker2Resources.componentName(NAME)));
        assertThat(ps.getMetadata().getLabels().entrySet().containsAll(kmm2.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(ps.getMetadata().getAnnotations(), is(Map.of("anno1", "anno-value1", "anno2", "anno-value2")));
        TestUtils.checkOwnerReference(ps, resource);
        assertThat(ps.getSpec().getSelector().getMatchLabels(), is(kmm2.getSelectorLabels().withStrimziPodSetController(KafkaMirrorMaker2Resources.componentName(NAME)).toMap()));
        assertThat(ps.getSpec().getPods().size(), is(3));

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(kmm2.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(kmm2.getComponentName()).toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(2));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("anno3"), is("anno-value3"));

            assertThat(pod.getSpec().getHostname(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getSubdomain(), is(KafkaMirrorMaker2Resources.componentName(NAME)));
            assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
            assertThat(pod.getSpec().getVolumes().stream()
                    .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                    .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getName(), is(KafkaMirrorMaker2Resources.componentName(NAME)));
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-image:latest"));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().size(), is(4));
            assertThat(pod.getSpec().getContainers().get(0).getEnv(), hasItem(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_JMX_EXPORTER_ENABLED).withValue(String.valueOf(true)).build()));
            assertThat(pod.getSpec().getContainers().get(0).getEnv(), hasItem(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build()));
            assertThat(pod.getSpec().getContainers().get(0).getEnv(), hasItem(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_HEAP_OPTS).withValue("-Xms" + JvmOptionUtils.DEFAULT_JVM_XMS).build()));
            assertThat(pod.getSpec().getContainers().get(0).getEnv(), hasItem(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS).withValue("target;source").build()));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(654));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(321));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(456));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(123));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().size(), is(2));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getContainerPort(), is(KafkaConnectCluster.REST_API_PORT));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getProtocol(), is("TCP"));
        }
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(affinity)
                            .withTolerations(toleration)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet ps = kmm2.generatePodSet(3, null, null, false, null, null, null);

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods)  {
            assertThat(pod.getSpec().getAffinity(), is(affinity));
            assertThat(pod.getSpec().getTolerations(), is(toleration));
        }
    }

    @Test
    public void testPodSetWithTls() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewTls()
                            .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                            .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                            .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                        .endTls()
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("target-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-another-secret"));

            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-another-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS),
                    is("target=my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
        });
    }

    @Test
    public void testPodSetWithTlsWithoutCerts() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewTls()
                        .endTls()
                    .endTarget()
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS),
                    is(nullValue()));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS),
                    is(nullValue()));
        });
    }

    @Test
    public void testPodSetWithTlsAuth() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .endTls()
                        .withAuthentication(
                                new KafkaClientAuthenticationTlsBuilder()
                                        .withNewCertificateAndKey()
                                        .withSecretName("tuser-secret")
                                        .withCertificate("user.crt")
                                        .withKey("user.key")
                                        .endCertificateAndKey()
                                        .build())
                    .endTarget()
                    .editFirstMirror()
                        .editSource()
                            .withNewTls()
                                .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                            .endTls()
                            .withAuthentication(
                                    new KafkaClientAuthenticationTlsBuilder()
                                            .withNewCertificateAndKey()
                                            .withSecretName("suser-secret")
                                            .withCertificate("user.crt")
                                            .withKey("user.key")
                                            .endCertificateAndKey()
                                            .build())
                        .endSource()
                    .endMirror()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(7));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("tuser-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("target-tuser-secret"));
            assertThat(pod.getSpec().getVolumes().get(5).getName(), is("source-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(6).getName(), is("source-suser-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(7));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("tuser-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "tuser-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-tuser-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/tuser-secret"));
            assertThat(cont.getVolumeMounts().get(5).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getName(), is("source-suser-secret"));
            assertThat(cont.getVolumeMounts().get(6).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/suser-secret"));
        });
    }

    @Test
    public void testPodSetWithTlsSameSecret() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .endTls()
                        .withAuthentication(
                                new KafkaClientAuthenticationTlsBuilder()
                                        .withNewCertificateAndKey()
                                        .withSecretName("my-secret")
                                        .withCertificate("user.crt")
                                        .withKey("user.key")
                                        .endCertificateAndKey()
                                        .build())
                    .endTarget()
                    .editFirstMirror()
                        .editSource()
                            .withNewTls()
                                .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                            .endTls()
                            .withAuthentication(
                                    new KafkaClientAuthenticationTlsBuilder()
                                            .withNewCertificateAndKey()
                                            .withSecretName("my-secret")
                                            .withCertificate("user.crt")
                                            .withKey("user.key")
                                            .endCertificateAndKey()
                                            .build())
                        .endSource()
                    .endMirror()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("source-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(5));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
        });
    }

    @Test
    public void testPodSetWithScramSha512Auth() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewKafkaClientAuthenticationScramSha512()
                            .withUsername("user1")
                            .withNewPasswordSecret()
                                .withSecretName("user1-secret")
                                .withPassword("password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationScramSha512()
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAIN"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-512"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/user1-secret:password}\";"));
        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testPodSetWithScramSha512AuthAndTLSSameSecret() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
            .editSpec()
                .editTarget()
                    .withNewTls()
                        .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                    .endTls()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("my-secret")
                            .withPassword("user1.password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endTarget()
            .endSpec()
            .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_SSL"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-512"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-secret:user1.password}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(4));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(5));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "target/my-secret"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication for multiple clusters.
     * It checks that the volumes and volume mounts that reference the secret are correctly created and that each volume name and volume mount path is only
     * created once - duplicate volume names and duplicate volume mount paths will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testPodSetWithMultipleClustersScramSha512AuthAndTLSSameSecret() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
            .editSpec()
                .editTarget()
                    .withNewTls()
                        .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                    .endTls()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("my-secret")
                            .withPassword("user1.password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endTarget()
                .editFirstMirror()
                    .editSource()
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .endTls()
                        .withNewKafkaClientAuthenticationScramSha512()
                            .withUsername("user1")
                            .withNewPasswordSecret()
                                .withSecretName("my-secret")
                                .withPassword("user1.password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationScramSha512()
                    .endSource()
                .endMirror()
            .endSpec()
            .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_SSL"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-512"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-secret:user1.password}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("source-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(7));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "source/my-secret"));
        });
    }

    @Test
    public void testPodSetWithScramSha256Auth() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewKafkaClientAuthenticationScramSha256()
                            .withUsername("user1")
                            .withNewPasswordSecret()
                                .withSecretName("user1-secret")
                                .withPassword("password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationScramSha256()
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-256"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/user1-secret:password}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 256 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testPodSetWithScramSha256AuthAndTLSSameSecret() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .endTls()
                        .withNewKafkaClientAuthenticationScramSha256()
                            .withUsername("user1")
                            .withNewPasswordSecret()
                                .withSecretName("my-secret")
                                .withPassword("user1.password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationScramSha256()
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_SSL"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-256"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-secret:user1.password}\";"));
        
        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(4));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(5));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "target/my-secret"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 256 client authentication for multiple clusters.
     * It checks that the volumes and volume mounts that reference the secret are correctly created and that each volume name and volume mount path is only
     * created once - duplicate volume names and duplicate volume mount paths will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testPodSetWithMultipleClustersScramSha256AuthAndTLSSameSecret() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .endTls()
                        .withNewKafkaClientAuthenticationScramSha256()
                            .withUsername("user1")
                            .withNewPasswordSecret()
                                .withSecretName("my-secret")
                                .withPassword("user1.password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationScramSha256()
                    .endTarget()
                    .editFirstMirror()
                        .editSource()
                            .withNewTls()
                                .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                            .endTls()
                            .withNewKafkaClientAuthenticationScramSha256()
                                .withUsername("user1")
                                .withNewPasswordSecret()
                                    .withSecretName("my-secret")
                                    .withPassword("user1.password")
                                .endPasswordSecret()
                            .endKafkaClientAuthenticationScramSha256()
                        .endSource()
                    .endMirror()
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_SSL"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=SCRAM-SHA-256"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-secret:user1.password}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("source-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(7));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "source/my-secret"));
        });
    }

    @Test
    public void testPodSetWithPlainAuth() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewKafkaClientAuthenticationPlain()
                            .withUsername("user1")
                            .withNewPasswordSecret()
                                .withSecretName("user1-secret")
                                .withPassword("password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationPlain()
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAIN"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=PLAIN"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/user1-secret:password}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
        });
    }


    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for plain client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @Test
    public void testPodSetWithPlainAuthAndTLSSameSecret() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .endTls()
                        .withNewKafkaClientAuthenticationPlain()
                            .withUsername("user1")
                            .withNewPasswordSecret()
                                .withSecretName("my-secret")
                                .withPassword("user1.password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationPlain()
                    .endTarget()
                .endSpec()
            .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_SSL"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=PLAIN"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-secret:user1.password}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(4));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(5));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "target/my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "target/my-secret"));
        });
    }

    @Test
    @SuppressWarnings({"checkstyle:methodlength"})
    public void testTemplate() {
        Map<String, String> podSetLabels = Map.of("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedPodSetLabels = new HashMap<>(podSetLabels);
        expectedPodSetLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> podSetAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> svcAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> pdbLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> pdbAnnotations = Map.of("a7", "v7", "a8", "v8");

        Map<String, String> crbLabels = Map.of("l9", "v9", "l10", "v10");
        Map<String, String> crbAnnotations = Map.of("a9", "v9", "a10", "v10");

        Map<String, String> saLabels = Map.of("l11", "v11", "l12", "v12");
        Map<String, String> saAnnotations = Map.of("a11", "v11", "a12", "v12");

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
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

        ConfigMapVolumeSource configMap = new ConfigMapVolumeSourceBuilder()
                .withName("configMap1")
                .build();

        PersistentVolumeClaimVolumeSource pvc = new PersistentVolumeClaimVolumeSourceBuilder()
                .withClaimName("pvc-name")
                .withReadOnly(true)
                .build();

        AdditionalVolume additionalVolumeConfigMap = new AdditionalVolumeBuilder()
                .withName("config-map-volume-name")
                .withConfigMap(configMap)
                .build();

        AdditionalVolume additionalVolumePvc = new AdditionalVolumeBuilder()
                .withName("pvc-volume-name")
                .withPersistentVolumeClaim(pvc)
                .build();

        VolumeMount additionalVolumeMountConfigMap = new VolumeMountBuilder()
                .withName("config-map-volume-name")
                .withMountPath("/mnt/myconfigmap")
                .withSubPath("def")
                .build();

        VolumeMount additionalVolumeMountPvc = new VolumeMountBuilder()
                .withName("pvc-volume-name")
                .withMountPath("/mnt/mypvc")
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewRack("my-topology-key")
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(podSetLabels)
                                .withAnnotations(podSetAnnotations)
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
                            .withDnsPolicy(dnsPolicy)
                            .withDnsConfig(dnsConfig)
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                            .withVolumes(additionalVolumeConfigMap, additionalVolumePvc)
                        .endPod()
                        .withNewInitContainer()
                            .withVolumeMounts(additionalVolumeMountPvc)
                        .endInitContainer()
                        .withNewConnectContainer()
                            .withVolumeMounts(additionalVolumeMountConfigMap)
                        .endConnectContainer()
                        .withNewApiService()
                            .withNewMetadata()
                                .withLabels(svcLabels)
                                .withAnnotations(svcAnnotations)
                            .endMetadata()
                            .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                            .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                        .endApiService()
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
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(expectedPodSetLabels.entrySet()), is(true));
        assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(podSetAnnotations.entrySet()), is(true));

        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            // Check Pods
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(true));
            assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
            assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
            assertThat(pod.getSpec().getDnsPolicy(), is(DnsPolicy.NONE.toValue()));
            assertThat(pod.getSpec().getDnsConfig(), is(dnsConfig));
            assertThat(pod.getSpec().getEnableServiceLinks(), is(false));

            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(1).getConfigMap().getName(), is("foo-mirrormaker2-config"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("1Mi")));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("config-map-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is("configMap1"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("pvc-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(4).getPersistentVolumeClaim().getClaimName(), is("pvc-name"));

            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().size(), is(2));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(0).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(1).getName(), is("pvc-volume-name"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/mnt/mypvc"));

            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(4));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is("/opt/kafka/init"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("config-map-volume-name"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/mnt/myconfigmap"));
        });

        // Check Service
        Service svc = kmm2.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kmm2.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));

        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kmm2.generateClusterRoleBinding();
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnnotations.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kmm2.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnotations.entrySet()), is(true));
    }

    @SuppressWarnings("deprecation") // External configuration is deprecated
    @Test
    public void testExternalConfigurationSecretEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
            assertThat(selected.get(0).getValueFrom().getSecretKeyRef(), is(env.getValueFrom().getSecretKeyRef()));
        });
    }

    @SuppressWarnings("deprecation") // External configuration is deprecated
    @Test
    public void testExternalConfigurationConfigEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationConfigVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(0));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(0));
        });
    }

    @Test
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testNoExternalConfigurationVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(0));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(0));
        });
    }

    @SuppressWarnings("deprecation") // External configuration is deprecated
    @Test
    public void testInvalidExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(0));
        });
    }

    @SuppressWarnings("deprecation") // External configuration is deprecated
    @Test
    public void testNoExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withEnv(env)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(0));
        });
    }

    @Test
    public void testGracePeriod() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
        });
    }

    @Test
    public void testDefaultGracePeriod() {
        // Check PodSet
        StrimziPodSet podSet = KMM2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        });
    }

    @Test
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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
        StrimziPodSet podSet = KMM2.generatePodSet(3, Map.of(), Map.of(), false, null, List.of(secret1, secret2), null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, List.of(secret1), null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        });
    }

    @Test
    public void testDefaultImagePullSecrets() {
        // Check PodSet
        StrimziPodSet podSet = KMM2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets(), is(List.of()));
        });
    }

    @Test
    public void testSecurityContext() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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
        StrimziPodSet podSet = KMM2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
        });
    }

    @Test
    public void testRestrictedSecurityContext() {
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, RESOURCE, VERSIONS, SHARED_ENV_PROVIDER);
        kmm2.securityProvider = new RestrictedPodSecurityProvider();
        kmm2.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withReplicas(2)
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kmm2.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(0)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
    }

    @Test
    public void testDefaultPodDisruptionBudget() {
        PodDisruptionBudget pdb = KMM2.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(2)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
    }

    @Test
    public void testImagePullPolicy() {
        // Check PodSet
        StrimziPodSet podSet = KMM2.generatePodSet(3, Map.of(), Map.of(), false, ImagePullPolicy.ALWAYS, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        });

        // Check PodSet
        podSet = KMM2.generatePodSet(3, Map.of(), Map.of(), false, ImagePullPolicy.IFNOTPRESENT, null, null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewJvmOptions()
                        .withXms("512m")
                        .withXmx("1024m")
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"), is(true));
        });
    }

    @Test
    public void testKafkaMirrorMaker2ContainerEnvVars() {
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
        ContainerTemplate kafkaMirrorMaker2Container = new ContainerTemplate();
        kafkaMirrorMaker2Container.setEnv(testEnvs);

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaMirrorMaker2Container)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @Test
    public void testOpenTelemetryTracing() {
        KafkaMirrorMaker2Builder builder = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewOpenTelemetryTracing()
                    .endOpenTelemetryTracing()
                .endSpec();
        KafkaMirrorMaker2 resource = builder.build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("consumer.interceptor.classes=" + OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME));
        assertThat(connectConfigurations, containsString("producer.interceptor.classes=" + OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(env -> KafkaMirrorMaker2Cluster.ENV_VAR_STRIMZI_TRACING.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals(OpenTelemetryTracing.TYPE_OPENTELEMETRY), is(true));
        });
    }

    @Test
    public void testPodSetWithOAuthWithAccessToken() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withAuthentication(new KafkaClientAuthenticationOAuthBuilder()
                                .withNewAccessToken()
                                .withSecretName("my-token-secret")
                                .withKey("my-token-key")
                                .endAccessToken()
                                .build())
                    .endTarget()
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-token-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @Test
    public void testPodSetWithAccessTokenLocation() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withAuthentication(new KafkaClientAuthenticationOAuthBuilder()
                                .withAccessTokenLocation("/var/run/secrets/kubernetes.io/serviceaccount/token")
                                .build())
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token.location=\"/var/run/secrets/kubernetes.io/serviceaccount/token\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().stream().filter(var -> var.getName().endsWith("token-secret")).count(), is(0L));
        });
    }

    @Test
    public void testPodSetWithOAuthWithRefreshToken() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withAuthentication(new KafkaClientAuthenticationOAuthBuilder()
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
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.connect.timeout.seconds=\"15\" " +
                "oauth.read.timeout.seconds=\"15\" " +
                "oauth.http.retries=\"2\" " +
                "oauth.http.retry.pause.millis=\"500\" " +
                "oauth.enable.metrics=\"true\" " +
                "oauth.include.accept.header=\"false\" " +
                "oauth.refresh.token=\"${strimzidir:/opt/kafka/oauth/my-token-secret:my-token-key}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-token-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @Test
    public void testPodSetWithOAuthWithClientSecret() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withScope("all")
                                        .withGrantType("custom_client_credentials")
                                        .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.client.credentials.grant.type=\"custom_client_credentials\" " +
                "oauth.scope=\"all\" " +
                "oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-secret-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @Test
    public void testPodSetWithOAuthWithClientSecretAndSaslExtensions() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withSaslExtensions(new TreeMap<>(Map.of("key1", "value1", "key2", "value2")))
                                        .build())
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.sasl.extension.key1=\"value1\" " +
                "oauth.sasl.extension.key2=\"value2\" " +
                "oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-secret-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @Test
    public void testPodSetWithOAuthWithClientAssertion() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientAssertion()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientAssertion()
                                        .build())
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.client.assertion=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\";"));
    }

    @Test
    public void testPodSetWithOAuthWithUsernameAndPassword() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
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
                    .endTarget()
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.client.id=\"my-client-id\" " +
                "oauth.password.grant.username=\"user1\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\" " +
                "oauth.password.grant.password=\"${strimzidir:/opt/kafka/oauth/my-password-secret:user1.password}\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-password-secret".equals(var.getName())).count(), is(1L));
            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-secret-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @Test
    public void testPodSetWithOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                    .editSpec()
                        .editTarget()
                            .withAuthentication(
                                    new KafkaClientAuthenticationOAuthBuilder()
                                            .withClientId("my-client-id")
                                            .withTokenEndpointUri("http://my-oauth-server")
                                            .build())
                        .endTarget()
                    .endSpec()
                    .build();

            KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        });
    }

    @Test
    public void testPodSetWithOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                    .editSpec()
                        .editTarget()
                            .withAuthentication(
                                    new KafkaClientAuthenticationOAuthBuilder()
                                            .withClientId("my-client-id")
                                            .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                            .endClientSecret()
                                            .build())
                        .endTarget()
                    .endSpec()
                    .build();

            KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
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
                    .endTarget()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("security.protocol=SASL_PLAINTEXT"));
        assertThat(connectConfigurations, containsString("sasl.mechanism=OAUTHBEARER"));
        assertThat(connectConfigurations, containsString("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.client.id=\"my-client-id\" " +
                "oauth.token.endpoint.uri=\"http://my-oauth-server\" " +
                "oauth.ssl.endpoint.identification.algorithm=\"\" " +
                "oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-secret-secret:my-secret-key}\" " +
                "oauth.ssl.truststore.location=\"/opt/kafka/oauth-certs/foo-connect-oauth-trusted-certs/ca.crt\" " +
                "oauth.ssl.truststore.type=\"PEM\";"));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            String oauthSecret = KafkaConnectResources.internalOauthTrustedCertsSecretName(NAME);

            // Volume mounts
            assertThat(cont.getVolumeMounts().stream().filter(mount -> oauthSecret.equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + oauthSecret));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "target-oauth-certs-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "target/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "target-oauth-certs-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "target/second-certificate"));

            // Volumes
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> oauthSecret.equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> "target-oauth-certs-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> "target-oauth-certs-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS_CLUSTERS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("target=first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
        });
    }

    @Test
    public void testNetworkPolicy() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kmm2.generateNetworkPolicy(true, "operator-namespace", null);

        assertThat(np.getMetadata().getName(), is(kmm2.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kmm2.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));

        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kmm2.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Map.of()));

        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @Test
    public void testNetworkPolicyWithConnectorOperatorSameNamespace() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kmm2.generateNetworkPolicy(true, NAMESPACE, null);

        assertThat(np.getMetadata().getName(), is(kmm2.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kmm2.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kmm2.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @Test
    public void testNetworkPolicyWithConnectorOperatorWithNamespaceLabels() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kmm2.generateNetworkPolicy(true, "operator-namespace", Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));

        assertThat(np.getMetadata().getName(), is(kmm2.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kmm2.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kmm2.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @Test
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 kafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kmm.metrics(), is(notNullValue()));
        assertThat(((JmxPrometheusExporterModel) kmm.metrics()).getConfigMapName(), is("my-metrics-configuration"));
        assertThat(((JmxPrometheusExporterModel) kmm.metrics()).getConfigMapKey(), is("config.yaml"));
    }

    @Test
    public void testJmxSecretCustomLabelsAndAnnotations() {
        Map<String, String> customLabels = new HashMap<>(2);
        customLabels.put("label1", "value1");
        customLabels.put("label2", "value2");

        Map<String, String> customAnnotations = new HashMap<>(2);
        customAnnotations.put("anno1", "value3");
        customAnnotations.put("anno2", "value4");

        KafkaMirrorMaker2 kafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(RESOURCE)
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

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);

        Secret jmxSecret = kmm2.jmx().jmxSecret(null);

        for (Map.Entry<String, String> entry : customAnnotations.entrySet()) {
            assertThat(jmxSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry : customLabels.entrySet()) {
            assertThat(jmxSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    @Test
    public void testMetricsParsingNoMetrics() {
        KafkaMirrorMaker2Cluster kmm = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, RESOURCE, VERSIONS, SHARED_ENV_PROVIDER);
        assertThat(kmm.metrics(), is(nullValue()));
    }

    @Test
    public void testStrimziMetricsReporterConfig() {
        MetricsConfig metrics = new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList("kafka_log.*", "kafka_network.*")
                .endValues().build();

        KafkaMirrorMaker2 kafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kc = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.metrics(), is(notNullValue()));
        assertThat(((StrimziMetricsReporterModel) kc.metrics()).getAllowList(), is("kafka_log.*,kafka_network.*"));

        NetworkPolicy np = kc.generateNetworkPolicy(true, null, null);
        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream()
                .filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(MetricsModel.METRICS_PORT)))
                .toList();

        assertThat(rules.size(), is(1));
    }

    @Test
    public void testPodSetWithRack() {
        String clientRackInitImage = "client-rack-init-image";
        
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editOrNewSpec()
                    .withNewRack()
                        .withTopologyKey("topology-key")
                    .endRack()
                    .withClientRackInitImage(clientRackInitImage)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = cluster.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            // check that pod spec contains the init Kafka container
            List<Container> initContainers = pod.getSpec().getInitContainers();
            assertThat(initContainers, is(notNullValue()));
            assertThat(!initContainers.isEmpty(), is(true));

            Optional<Container> matchedKafkaInitContainer = initContainers.stream().filter(container -> container.getName().equals(KafkaConnectCluster.INIT_NAME)).findAny();
            assertThat(matchedKafkaInitContainer.isPresent(), is(true));
            assertThat(matchedKafkaInitContainer.get().getImage(), is(clientRackInitImage));
        });
    }

    @Test
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";
        String topologyKey = "topology-key";

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editOrNewMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editOrNewSpec()
                    .withNewRack(topologyKey)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb.getMetadata().getName(), is(KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(NAME, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(cluster.componentName));
    }

    @Test
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editOrNewMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .build();

        KafkaMirrorMaker2Cluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb, is(nullValue()));
    }

    @Test
    public void testLoggingWithLog4j2() {
        // Check config map
        ConfigMap cm = KMM2.generateConnectConfigMap(new MetricsAndLogging(METRICS_CONFIG, null));
        assertThat(cm.getData().get(LoggingModel.LOG4J2_CONFIG_MAP_KEY), is(notNullValue()));
    }

    @Test
    public void testRoleAbdRoleBindingNoSecrets() {
        assertThat(KMM2.generateRole(), is(nullValue()));
        assertThat(KMM2.generateRoleBindingForRole(), is(nullValue()));
    }

    @Test
    public void testRoleAbdRoleBindingWithSecrets() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .editTarget()
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("ca.crt").build())
                        .endTls()
                    .endTarget()
                    .editFirstMirror()
                        .editSource()
                            .withNewTls()
                                .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-source-secret").withCertificate("ca.crt").build())
                            .endTls()
                        .endSource()
                    .endMirror()
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        Role role = kmm2.generateRole();
        assertThat(role.getMetadata().getName(), is(kmm2.componentName));
        assertThat(role.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(role.getRules().size(), is(1));
        assertThat(role.getRules().get(0).getApiGroups(), is(List.of("")));
        assertThat(role.getRules().get(0).getResources(), is(List.of("secrets")));
        assertThat(role.getRules().get(0).getVerbs(), is(List.of("get")));
        assertThat(role.getRules().get(0).getResourceNames(), is(List.of(KafkaConnectResources.componentName(NAME) + "-tls-trusted-certs")));

        RoleBinding rb = kmm2.generateRoleBindingForRole();
        assertThat(rb.getMetadata().getName(), is(KafkaMirrorMaker2Resources.mm2RoleBindingName(NAME)));
        assertThat(rb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(rb.getSubjects().size(), is(1));
        assertThat(rb.getSubjects().get(0).getKind(), is("ServiceAccount"));
        assertThat(rb.getSubjects().get(0).getNamespace(), is(NAMESPACE));
        assertThat(rb.getSubjects().get(0).getName(), is(kmm2.componentName));
        assertThat(rb.getRoleRef().getKind(), is("Role"));
        assertThat(rb.getRoleRef().getName(), is(kmm2.componentName));
    }

    @Test
    public void testDefaultMetricsConfigurationIncludesBothConnectAndMM2Metrics() {
        // Test that MirrorMaker 2 default metrics include both Connect and MM2-specific metrics
        MetricsConfig metrics = new StrimziMetricsReporterBuilder().build();

        KafkaMirrorMaker2 kafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(RESOURCE)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kmm2.metrics(), is(notNullValue()));
        String allowList = ((StrimziMetricsReporterModel) kmm2.metrics()).getAllowList();
        
        // Verify MM2 metrics ARE included in MirrorMaker 2 default configuration
        assertThat(allowList, containsString("mirrorcheckpointconnector"));
        assertThat(allowList, containsString("mirrorsourceconnector"));
        
        // Verify Connect metrics ARE also included
        assertThat(allowList, containsString("kafka_connect_connector_metrics"));
        assertThat(allowList, containsString("kafka_connect_connect_worker_metrics_"));
    }
}
