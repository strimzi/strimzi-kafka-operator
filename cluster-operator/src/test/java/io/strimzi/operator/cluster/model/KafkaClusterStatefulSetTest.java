/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTerm;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.RackBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.template.PodManagementPolicy;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasProperty;

@ParallelSuite
public class KafkaClusterStatefulSetTest {
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
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
                            .build())
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build())
                    .endJbodStorage()
                    .withConfig(Map.of("log.message.format.version", "3.0", "inter.broker.protocol.version", "3.0"))
                .endKafka()
            .endSpec()
            .build();
    private final static KafkaCluster KC = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, VERSIONS);

    //////////
    // Utility methods
    //////////

    private Map<String, String> expectedLabels()    {
        return TestUtils.map(
            Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
            Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaStatefulSetName(CLUSTER),
            Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaCluster.COMPONENT_TYPE,
            Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
            Labels.KUBERNETES_NAME_LABEL, KafkaCluster.COMPONENT_TYPE,
            Labels.KUBERNETES_INSTANCE_LABEL, CLUSTER,
            Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + CLUSTER,
            Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private void checkStatefulSet(StatefulSet sts, Kafka kafka) {
        assertThat(sts.getMetadata().getName(), is(KafkaResources.kafkaStatefulSetName(CLUSTER)));
        // ... in the same namespace ...
        assertThat(sts.getMetadata().getNamespace(), is(NAMESPACE));
        // ... with these labels
        assertThat(sts.getMetadata().getLabels(), is(expectedLabels()));
        assertThat(sts.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(kafka.getSpec().getKafka().getStorage())));
        assertThat(sts.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));

        assertThat(sts.getSpec().getTemplate().getSpec().getSchedulerName(), is("default-scheduler"));

        List<Container> containers = sts.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(1));

        // checks on the main Kafka container
        assertThat(sts.getSpec().getReplicas(), is(REPLICAS));
        assertThat(sts.getSpec().getPodManagementPolicy(), is(PodManagementPolicy.PARALLEL.toValue()));
        assertThat(containers.get(0).getImage(), is("strimzi/kafka:latest-kafka-" + VERSIONS.defaultVersion().version()));
        assertThat(containers.get(0).getLivenessProbe().getTimeoutSeconds(), is(5));
        assertThat(containers.get(0).getLivenessProbe().getInitialDelaySeconds(), is(15));
        assertThat(containers.get(0).getReadinessProbe().getTimeoutSeconds(), is(5));
        assertThat(containers.get(0).getReadinessProbe().getInitialDelaySeconds(), is(15));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED), is(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)));
        assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
        assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
        assertThat(containers.get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
        assertThat(containers.get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
        assertThat(containers.get(0).getPorts().get(0).getName(), is(KafkaCluster.CONTROLPLANE_PORT_NAME));
        assertThat(containers.get(0).getPorts().get(0).getContainerPort(), is(KafkaCluster.CONTROLPLANE_PORT));
        assertThat(containers.get(0).getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(containers.get(0).getPorts().get(1).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(containers.get(0).getPorts().get(1).getContainerPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(containers.get(0).getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream()
            .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
            .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

        if (kafka.getSpec().getKafka().getRack() != null) {
            Rack rack = kafka.getSpec().getKafka().getRack();

            // check that the pod spec contains anti-affinity rules with the right topology key
            PodSpec podSpec = sts.getSpec().getTemplate().getSpec();
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

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    //////////
    // Tests
    //////////

    @ParallelTest
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet sts = KC.generateStatefulSet(true, null, null, null);
        checkStatefulSet(sts, KAFKA);
        TestUtils.checkOwnerReference(sts, KAFKA);

        // Check Volumes
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().size(), is(6));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir(), is(notNullValue()));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(1).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(1).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-kafka-brokers"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(3).getSecret().getSecretName(), is("foo-clients-ca-cert"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(4).getName(), is("kafka-metrics-and-logging"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(4).getConfigMap().getName(), is("foo-kafka-config"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(5).getName(), is("ready-files"));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(5).getEmptyDir(), is(notNullValue()));
    }

    @ParallelTest
    public void testGenerateStatefulSetWithSetStorageSelector() {
        Map<String, String> selector = TestUtils.map("foo", "bar");
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage().withSelector(selector).withSize("100Gi").endPersistentClaimStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);
        assertThat(sts.getSpec().getVolumeClaimTemplates().get(0).getSpec().getSelector().getMatchLabels(), is(selector));
    }

    @ParallelTest
    public void testGenerateStatefulSetWithEmptyStorageSelector() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage().withSelector(emptyMap()).withSize("100Gi").endPersistentClaimStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);
        assertThat(sts.getSpec().getVolumeClaimTemplates().get(0).getSpec().getSelector(), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateStatefulSetWithSetSizeLimit() {
        String sizeLimit = "1Gi";
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorage().withSizeLimit(sizeLimit).endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("1", "Gi")));
    }

    @ParallelTest
    public void testGenerateStatefulSetWithEmptySizeLimit() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateStatefulSetWithRack() {
        Kafka editKafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewRack().withTopologyKey("rack-key").endRack()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, editKafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(true, null, null, null);
        checkStatefulSet(sts, editKafkaAssembly);
    }

    @ParallelTest
    public void testGenerateStatefulSetWithInitContainers() {
        Kafka editKafkaAssembly =
                new KafkaBuilder(KAFKA)
                        .editSpec()
                            .editKafka()
                                .withNewPersistentClaimStorage().withSize("1Gi").endPersistentClaimStorage()
                                .withNewRack().withTopologyKey("rack-key").endRack()
                            .endKafka()
                        .endSpec().build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, editKafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);
        checkStatefulSet(sts, editKafkaAssembly);
    }

    @ParallelTest
    public void testGenerateStatefulSetWithPodManagementPolicy() {
        Kafka editKafkaAssembly =
                new KafkaBuilder(KAFKA)
                        .editSpec()
                            .editKafka()
                                .withNewTemplate()
                                    .withNewStatefulset()
                                        .withPodManagementPolicy(PodManagementPolicy.ORDERED_READY)
                                    .endStatefulset()
                                .endTemplate()
                            .endKafka()
                        .endSpec().build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, editKafkaAssembly, VERSIONS);
        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);
        assertThat(sts.getSpec().getPodManagementPolicy(), is(PodManagementPolicy.ORDERED_READY.toValue()));
    }

    @ParallelTest
    public void testCustomLabelsFromCR() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToLabels("foo", "bar")
                .endMetadata()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);
        assertThat(sts.getMetadata().getLabels().get("foo"), is("bar"));
        assertThat(sts.getSpec().getTemplate().getMetadata().getLabels().get("foo"), is("bar"));
    }

    @ParallelTest
    public void testCustomizedStatefulSet() {
        Map<String, String> ssLabels = TestUtils.map("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedStsLabels = new HashMap<>(ssLabels);
        expectedStsLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> ssAnnotations = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = TestUtils.map("a3", "v3", "a4", "v4");

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
                            .withNewStatefulset()
                                .withNewMetadata()
                                    .withLabels(ssLabels)
                                    .withAnnotations(ssAnnotations)
                                .endMetadata()
                            .endStatefulset()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnnotations)
                                .endMetadata()
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withHostAliases(hostAlias1, hostAlias2)
                                .withTopologySpreadConstraints(tsc1, tsc2)
                                .withEnableServiceLinks(false)
                                .withTmpDirSizeLimit("10Mi")
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check StatefulSet
        StatefulSet sts = kc.generateStatefulSet(true, null, null, null);
        assertThat(sts.getMetadata().getLabels().entrySet().containsAll(expectedStsLabels.entrySet()), is(true));
        assertThat(sts.getMetadata().getAnnotations().entrySet().containsAll(ssAnnotations.entrySet()), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));

        // Check Pods
        assertThat(sts.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(sts.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(sts.getSpec().getTemplate().getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
        assertThat(sts.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(sts.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream()
            .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
            .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
    }

    @ParallelTest
    public void testGracePeriod() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
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
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(true, null, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(123L));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(true, null, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(30L));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
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
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(true, null, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(true, null, secrets, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
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
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(true, null, singletonList(secret1), null);
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        StatefulSet sts = KC.generateStatefulSet(true, null, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
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
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(true, null, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(123L));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(456L));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(789L));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        StatefulSet sts = KC.generateStatefulSet(false, null, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(0L));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withRack(new RackBuilder().withTopologyKey("topology-key").build())
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = kc.generateStatefulSet(true, ImagePullPolicy.ALWAYS, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getInitContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        sts = kc.generateStatefulSet(true, ImagePullPolicy.IFNOTPRESENT, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getInitContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsEphemeral()    {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check Storage annotation on STS
        StatefulSet sts = kc.generateStatefulSet(true, ImagePullPolicy.NEVER, null, null);
        assertThat(sts.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(kafkaAssembly.getSpec().getKafka().getStorage())));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream().filter(v -> "data".equals(v.getName())).findFirst().orElseThrow().getEmptyDir(), is(notNullValue()));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(kc.getStorage());
        assertThat(pvcs.size(), is(0));
    }

    @ParallelTest
    public void testKafkaContainerSecurityContext() {
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
                    .editKafka()
                        .withNewTemplate()
                            .withNewKafkaContainer()
                                .withSecurityContext(securityContext)
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);
        assertThat(kc.createContainer(null).getSecurityContext(), is(securityContext));

        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);

        assertThat(sts.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(KafkaCluster.KAFKA_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testRestrictedSecurityContextWithTemplate() {
        PodSecurityContext podSecurityContext = new PodSecurityContextBuilder()
                .withFsGroup(123L)
                .withRunAsGroup(456L)
                .withRunAsUser(789L)
                .build();

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
                    .editKafka()
                        .withNewTemplate()
                            .withNewKafkaContainer()
                                .withSecurityContext(securityContext)
                            .endKafkaContainer()
                            .withNewPod()
                                .withSecurityContext(podSecurityContext)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);
        kc.securityProvider = new RestrictedPodSecurityProvider();
        kc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);

        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext(), is(podSecurityContext));

        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext(), is(securityContext));
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, VERSIONS);
        kc.securityProvider = new RestrictedPodSecurityProvider();
        kc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        StatefulSet sts = kc.generateStatefulSet(false, null, null, null);

        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(0L));

        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));

    }
}
