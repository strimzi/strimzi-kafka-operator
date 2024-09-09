/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.strimzi.api.kafka.model.common.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.AfterAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertNull;

@ParallelSuite
public class KafkaExporterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(CLUSTER_NAME)
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("tls")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .build())
                .endKafka()
                .withNewKafkaExporter()
                    .withLogging("debug")
                    .withGroupRegex("my-group-.*")
                    .withTopicRegex("my-topic-.*")
                    .withGroupExcludeRegex("my-group-exclude-.*")
                    .withTopicExcludeRegex("my-topic-exclude-.*")
                    .withImage("my-image:latest")
                    .withEnableSaramaLogging(true)
                    .withShowAllOffsets(false)
                    .withNewTemplate()
                        .withNewPod()
                            .withTmpDirSizeLimit("100Mi")
                        .endPod()
                    .endTemplate()
                .endKafkaExporter()
            .endSpec()
            .build();
    private static final KafkaExporter KE = KafkaExporter.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), KAFKA, VERSIONS, SHARED_ENV_PROVIDER);

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @ParallelTest
    public void testFromConfigMapDefaultConfig() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .build();
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(ke.getImage(), is(KafkaVersionTestUtils.DEFAULT_KAFKA_IMAGE));
        assertThat(ke.exporterLogging, is("info"));
        assertThat(ke.groupRegex, is(".*"));
        assertThat(ke.topicRegex, is(".*"));
        assertNull(ke.groupExcludeRegex);
        assertNull(ke.topicExcludeRegex);
        assertThat(ke.saramaLoggingEnabled, is(false));
        assertThat(ke.showAllOffsets, is(true));
    }

    @ParallelTest
    public void testFromConfigMap() {
        assertThat(KE.namespace, is(NAMESPACE));
        assertThat(KE.cluster, is(CLUSTER_NAME));
        assertThat(KE.getImage(), is("my-image:latest"));
        assertThat(KE.exporterLogging, is("debug"));
        assertThat(KE.groupRegex, is("my-group-.*"));
        assertThat(KE.topicRegex, is("my-topic-.*"));
        assertThat(KE.groupExcludeRegex, is("my-group-exclude-.*"));
        assertThat(KE.topicExcludeRegex, is("my-topic-exclude-.*"));
        assertThat(KE.saramaLoggingEnabled, is(true));
        assertThat(KE.showAllOffsets, is(false));
    }

    @ParallelTest
    public void testGenerateDeployment() {
        Deployment dep = KE.generateDeployment(Map.of(), true, null, null);

        assertThat(dep.getMetadata().getName(), is(KafkaExporterResources.componentName(CLUSTER_NAME)));
        assertThat(dep.getMetadata().getNamespace(), is(NAMESPACE));
        TestUtils.checkOwnerReference(dep, KAFKA);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        assertThat(containers.size(), is(1));

        // checks on the main Exporter container
        assertThat(containers.get(0).getImage(), is(KE.image));
        assertThat(containers.get(0).getEnv(), is(getExpectedEnvVars()));
        assertThat(containers.get(0).getPorts().size(), is(1));
        assertThat(containers.get(0).getPorts().get(0).getName(), is(MetricsModel.METRICS_PORT_NAME));
        assertThat(containers.get(0).getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));

        // Test healthchecks
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getHttpGet().getPath(), is("/healthz"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getHttpGet().getPort(), is(new IntOrString(MetricsModel.METRICS_PORT_NAME)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(KafkaExporter.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getPeriodSeconds(), is(KafkaExporter.DEFAULT_HEALTHCHECK_PERIOD));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(KafkaExporter.DEFAULT_HEALTHCHECK_TIMEOUT));

        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getHttpGet().getPath(), is("/healthz"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getHttpGet().getPort(), is(new IntOrString(MetricsModel.METRICS_PORT_NAME)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(KafkaExporter.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getPeriodSeconds(), is(KafkaExporter.DEFAULT_HEALTHCHECK_PERIOD));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(KafkaExporter.DEFAULT_HEALTHCHECK_TIMEOUT));

        // Test volumes
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes.size(), is(3));

        Volume volume = volumes.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));
        assertThat(volume.getEmptyDir().getSizeLimit(), is(new Quantity("100Mi")));

        volume = volumes.stream().filter(vol -> KafkaExporter.CLUSTER_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)));

        volume = volumes.stream().filter(vol -> KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(KafkaExporterResources.secretName(CLUSTER_NAME)));

        // Test volume mounts
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(volumesMounts.size(), is(3));

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));

        volumeMount = volumesMounts.stream().filter(vol -> KafkaExporter.CLUSTER_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(KafkaExporter.CLUSTER_CA_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_MOUNT));
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(KE.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Deployment dep = KE.generateDeployment(Map.of(), true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = KE.generateDeployment(Map.of(), true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testContainerTemplateEnvVars() {
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

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafkaExporter()
                        .withNewTemplate()
                            .withNewContainer()
                                .withEnv(envVar1, envVar2)
                            .endContainer()
                        .endTemplate()
                    .endKafkaExporter()
                .endSpec()
                .build();
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        List<EnvVar> kafkaEnvVars = ke.getEnvVars();
        assertThat(kafkaEnvVars.stream().filter(var -> testEnvOneKey.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is(testEnvOneValue));
        assertThat(kafkaEnvVars.stream().filter(var -> testEnvTwoKey.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is(testEnvTwoValue));
    }

    @ParallelTest
    public void testContainerTemplateEnvVarsWithKeyConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaExporter.ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX;
        String testEnvTwoValue = "my-special-value";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafkaExporter()
                        .withNewTemplate()
                            .withNewContainer()
                                .withEnv(envVar1, envVar2)
                            .endContainer()
                        .endTemplate()
                    .endKafkaExporter()
                .endSpec()
                .build();
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        List<EnvVar> kafkaEnvVars = ke.getEnvVars();
        assertThat(kafkaEnvVars.stream().filter(var -> testEnvOneKey.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is(testEnvOneValue));
        assertThat(kafkaEnvVars.stream().filter(var -> testEnvTwoKey.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is("my-group-.*"));
    }

    @ParallelTest
    public void testExporterNotDeployed() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withKafkaExporter(null)
                .endSpec()
                .build();
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(ke, is(nullValue()));
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = Map.of("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> depAnots = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> saAnots = Map.of("a5", "v5", "a6", "v6");

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

        List<Toleration> tolerations = singletonList(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

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
        
        ConfigMapVolumeSource configMap = new ConfigMapVolumeSourceBuilder()
                .withName("configMap1")
                .build();
        
        AdditionalVolume additionalVolumeConfigMap = new AdditionalVolumeBuilder()
                .withName("config-map-volume-name")
                .withConfigMap(configMap)
                .build();
        
        VolumeMount additionalVolumeMountConfigMap = new VolumeMountBuilder()
                .withName("config-map-volume-name-2")
                .withMountPath("/mnt/config-map-path")
                .withSubPath("def")
                .build();

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewKafkaExporter()
                        .withNewTemplate()
                            .withNewDeployment()
                                .withNewMetadata()
                                    .withLabels(depLabels)
                                    .withAnnotations(depAnots)
                                .endMetadata()
                            .endDeployment()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnots)
                                .endMetadata()
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withAffinity(affinity)
                                .withTolerations(tolerations)
                                .withTopologySpreadConstraints(tsc1, tsc2)
                                .withEnableServiceLinks(false)
                                .withVolumes(additionalVolumeConfigMap)
                            .endPod()
                            .withNewContainer()
                                .withVolumeMounts(additionalVolumeMountConfigMap)
                            .endContainer()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnots)
                                .endMetadata()
                            .endServiceAccount()
                        .endTemplate()
                    .endKafkaExporter()
                .endSpec()
                .build();
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check Deployment
        Deployment dep = ke.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()), is(true));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));
        assertThat(getVolume(dep.getSpec().getTemplate().getSpec(), additionalVolumeConfigMap.getName()).getConfigMap(), is(configMap));
        assertThat(getVolumeMount(dep.getSpec().getTemplate().getSpec().getContainers().get(0), additionalVolumeMountConfigMap.getName()), is(additionalVolumeMountConfigMap));

        // Check Service Account
        ServiceAccount sa = ke.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithRecreateDeploymentStrategy() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewKafkaExporter()
                        .withNewTemplate()
                            .withNewDeployment()
                                .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                            .endDeployment()
                        .endTemplate()
                    .endKafkaExporter()
                .endSpec()
                .build();
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = ke.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getStrategy().getType(), is("Recreate"));
    }

    @ParallelTest
    public void testNetworkPolicy() {
        NetworkPolicy np = KE.generateNetworkPolicy();
        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(MetricsModel.METRICS_PORT))).findFirst().orElse(null), is(notNullValue()));
    }

    ////////////////////
    // Utility methods
    ////////////////////

    private static List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();

        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_LOGGING).withValue("1").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION).withValue(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX).withValue("my-group-.*").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX).withValue("my-topic-.*").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_GROUP_EXCLUDE_REGEX).withValue("my-group-exclude-.*").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_TOPIC_EXCLUDE_REGEX).withValue("my-topic-exclude-.*").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER).withValue("my-cluster-kafka-bootstrap:" + KafkaCluster.REPLICATION_PORT).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA).withValue("true").build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_OFFSET_SHOW_ALL).withValue("false").build());

        return expected;
    }

    private static Volume getVolume(PodSpec podSpec, String volumeName) {
        return podSpec.getVolumes().stream().filter(volume -> volumeName.equals(volume.getName())).iterator().next();
    }

    private static VolumeMount getVolumeMount(Container container, String volumeName) {
        return container.getVolumeMounts().stream().filter(volumeMount -> volumeName.equals(volumeMount.getName())).iterator().next();
    }
}
