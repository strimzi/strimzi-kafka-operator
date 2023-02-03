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
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.template.PodManagementPolicy;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.hamcrest.Matchers;

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

@ParallelSuite
public class ZookeeperClusterStatefulSetTest {
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

    private Map<String, String> expectedLabels()    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
            Labels.STRIMZI_NAME_LABEL, KafkaResources.zookeeperStatefulSetName(CLUSTER),
            Labels.STRIMZI_COMPONENT_TYPE_LABEL, ZookeeperCluster.COMPONENT_TYPE,
            Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
            Labels.KUBERNETES_NAME_LABEL, ZookeeperCluster.COMPONENT_TYPE,
            Labels.KUBERNETES_INSTANCE_LABEL, CLUSTER,
            Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + CLUSTER,
            Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private void checkStatefulSet(StatefulSet sts) {
        assertThat(sts.getMetadata().getName(), is(KafkaResources.zookeeperStatefulSetName(CLUSTER)));
        // ... in the same namespace ...
        assertThat(sts.getMetadata().getNamespace(), is(NAMESPACE));
        // ... with these labels
        assertThat(sts.getMetadata().getLabels(), is(expectedLabels()));
        assertThat(sts.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(KAFKA.getSpec().getZookeeper().getStorage())));

        assertThat(sts.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));

        assertThat(sts.getSpec().getTemplate().getSpec().getSchedulerName(), is("default-scheduler"));

        List<Container> containers = sts.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(1));

        // checks on the main Zookeeper container
        assertThat(sts.getSpec().getReplicas(), is(REPLICAS));
        assertThat(sts.getSpec().getPodManagementPolicy(), is(PodManagementPolicy.PARALLEL.toValue()));
        assertThat(containers.get(0).getImage(), is("strimzi/kafka:latest-kafka-" + VERSIONS.defaultVersion().version()));
        assertThat(containers.get(0).getLivenessProbe().getTimeoutSeconds(), is(5));
        assertThat(containers.get(0).getLivenessProbe().getInitialDelaySeconds(), is(15));
        assertThat(containers.get(0).getReadinessProbe().getTimeoutSeconds(), is(5));
        assertThat(containers.get(0).getReadinessProbe().getInitialDelaySeconds(), is(15));
        OrderedProperties expectedConfig = new OrderedProperties().addMapPairs(ZookeeperConfiguration.DEFAULTS).addPair("foo", "bar");
        OrderedProperties actual = new OrderedProperties()
                .addStringPairs(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ZookeeperCluster.ENV_VAR_ZOOKEEPER_CONFIGURATION));
        assertThat(actual, is(expectedConfig));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ZookeeperCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED), is(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)));
        assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is(ZookeeperCluster.ZOOKEEPER_NODE_CERTIFICATES_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(ZookeeperCluster.ZOOKEEPER_NODE_CERTIFICATES_VOLUME_MOUNT));
        assertThat(containers.get(0).getVolumeMounts().get(4).getName(), is(ZookeeperCluster.ZOOKEEPER_CLUSTER_CA_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(4).getMountPath(), is(ZookeeperCluster.ZOOKEEPER_CLUSTER_CA_VOLUME_MOUNT));
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().stream()
                .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));
    }

    //////////
    // Tests
    //////////

    @ParallelTest
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet sts = ZC.generateStatefulSet(true, null, null);
        checkStatefulSet(sts);
        TestUtils.checkOwnerReference(sts, KAFKA);
    }

    @ParallelTest
    public void testGenerateStatefulSetWithPodManagementPolicy() {
        Kafka editZooAssembly = new KafkaBuilder(KAFKA)
                        .editSpec()
                            .editZookeeper()
                                .withNewTemplate()
                                    .withNewStatefulset()
                                        .withPodManagementPolicy(PodManagementPolicy.ORDERED_READY)
                                    .endStatefulset()
                                .endTemplate()
                            .endZookeeper()
                        .endSpec().build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, editZooAssembly, VERSIONS);
        StatefulSet sts = zc.generateStatefulSet(false, null, null);
        assertThat(sts.getSpec().getPodManagementPolicy(), is(PodManagementPolicy.ORDERED_READY.toValue()));
    }

    @ParallelTest
    public void testTemplate() {
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
                    .editZookeeper()
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
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check StatefulSet
        StatefulSet sts = zc.generateStatefulSet(true, null, null);
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
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withTerminationGracePeriodSeconds(123)
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(123L));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        StatefulSet sts = ZC.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(30L));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
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
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
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

        StatefulSet sts = ZC.generateStatefulSet(true, null, secrets);
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
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret2)
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, singletonList(secret1));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        StatefulSet sts = ZC.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(123L));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(456L));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(789L));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        StatefulSet sts = ZC.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(0L));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext(), is(Matchers.nullValue()));
    }

    @ParallelTest
    public void testGenerateSTSWithPersistentVolumeEphemeral()    {
        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(false, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateSTSWithPersistentVolumeEphemeralWithSizeLimit()    {
        String sizeLimit = "1Gi";
        Kafka ka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editZookeeper()
                        .withNewEphemeralStorage().withSizeLimit(sizeLimit).endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(false, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("1", "Gi")));
    }

    @ParallelTest
    public void testRestrictedContainerSecurityContext() {
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, VERSIONS);
        zc.securityProvider = new RestrictedPodSecurityProvider();
        zc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        StatefulSet sts = zc.generateStatefulSet(false, null, null);

        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(0L));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
    }

    @ParallelTest
    public void testCustomLabelsFromCR() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToLabels("foo", "bar")
                .endMetadata()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(false, null, null);
        assertThat(sts.getMetadata().getLabels().get("foo"), is("bar"));
        assertThat(sts.getSpec().getTemplate().getMetadata().getLabels().get("foo"), is("bar"));
    }

}
