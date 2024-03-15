/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpecBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
@ParallelSuite
public class EntityOperatorTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    static Map<String, String> volumeMounts(List<VolumeMount> mounts) {
        return mounts.stream().collect(Collectors.toMap(VolumeMount::getName, VolumeMount::getMountPath));
    }

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;

    private final EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
            .build();
    private final EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTopicOperator(entityTopicOperatorSpec)
            .withUserOperator(entityUserOperatorSpec)
            .withNewTemplate()
                .withNewPod()
                    .withTmpDirSizeLimit("100Mi")
                .endPod()
            .endTemplate()
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);
    
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testGenerateDeployment(boolean cruiseControlEnabled) {
        entityOperator.cruiseControlEnabled = cruiseControlEnabled;
        Deployment dep = entityOperator.generateDeployment(true, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(dep.getMetadata().getName(), is(KafkaResources.entityOperatorDeploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        assertThat(dep.getSpec().getReplicas(), is(1));
        TestUtils.checkOwnerReference(dep, resource);

        assertThat(containers.size(), is(2));
        // just check names of topic and user operators (their containers are tested in the related unit test classes)
        assertThat(containers.get(0).getName(), is(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME));
        assertThat(containers.get(1).getName(), is(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME));

        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes.stream().filter(volume -> volume.getName().equals(EntityUserOperator.USER_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME)).findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity("100", "Mi")));
        assertThat(volumes.stream().filter(volume -> volume.getName().equals(EntityTopicOperator.TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME)).findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity("100", "Mi")));
        assertThat(volumes.stream().filter(volume -> volume.getName().equals(EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME)).findFirst().isEmpty(), is(false));
        
        if (cruiseControlEnabled) {
            assertThat(volumes.stream().filter(volume -> volume.getName().equals(EntityOperator.ETO_CC_API_VOLUME_NAME)).findFirst().isEmpty(), is(false));
        }
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(entityOperator.namespace, is(namespace));
        assertThat(entityOperator.cluster, is(cluster));
        assertThat(entityOperator.zookeeperConnect, is(KafkaResources.zookeeperServiceName(cluster) + ":" + ZookeeperCluster.CLIENT_TLS_PORT));
    }

    @ParallelTest
    public void testFromCrdNoTopicAndUserOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                .endSpec()
                .build();

        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(entityOperator, is(nullValue()));
    }

    @ParallelTest
    public void testFromCrdNoTopicInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withNewUserOperator()
                .endUserOperator()
                .build();
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                .endSpec()
                .build();

        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(entityOperator.topicOperator(), is(nullValue()));
        assertThat(entityOperator.userOperator(), is(notNullValue()));
    }

    @ParallelTest
    public void testFromCrdNoUserOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .build();
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                .endSpec()
                .build();

        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(entityOperator.topicOperator(), is(notNullValue()));
        assertThat(entityOperator.userOperator(), is(nullValue()));
    }

    @ParallelTest
    public void withAffinityAndTolerations() throws IOException {
        ResourceTester<Kafka, EntityOperator> helper = new ResourceTester<>(Kafka.class, VERSIONS, (kAssembly, versions) -> EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), kAssembly, versions, SHARED_ENV_PROVIDER), this.getClass().getSimpleName() + ".withAffinityAndTolerations");
        helper.assertDesiredModel("-DeploymentAffinity.yaml", zc -> zc.generateDeployment(true, null, null).getSpec().getTemplate().getSpec().getAffinity());
        helper.assertDesiredModel("-DeploymentTolerations.yaml", zc -> zc.generateDeployment(true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> depAnnotations = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> saAnnotations = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> rLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> rAnots = TestUtils.map("a7", "v7", "a8", "v8");

        Map<String, String> rbLabels = TestUtils.map("l9", "v9", "l10", "v10");
        Map<String, String> rbAnots = TestUtils.map("a9", "v9", "a10", "v10");

        Toleration toleration = new TolerationBuilder()
                .withEffect("NoSchedule")
                .withValue("")
                .build();

        Toleration assertToleration = new TolerationBuilder()
                .withEffect("NoSchedule")
                .withValue(null)
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

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .withNewEntityOperator()
                                .withTopicOperator(entityTopicOperatorSpec)
                                .withUserOperator(entityUserOperatorSpec)
                                .withNewTemplate()
                                    .withNewDeployment()
                                        .withNewMetadata()
                                            .withLabels(depLabels)
                                            .withAnnotations(depAnnotations)
                                        .endMetadata()
                                    .endDeployment()
                                    .withNewPod()
                                        .withNewMetadata()
                                            .withLabels(podLabels)
                                            .withAnnotations(podAnnotations)
                                        .endMetadata()
                                        .withPriorityClassName("top-priority")
                                        .withSchedulerName("my-scheduler")
                                        .withTolerations(singletonList(toleration))
                                        .withTopologySpreadConstraints(tsc1, tsc2)
                                        .withEnableServiceLinks(false)
                                    .endPod()
                                    .withNewEntityOperatorRole()
                                        .withNewMetadata()
                                            .withLabels(rLabels)
                                            .withAnnotations(rAnots)
                                        .endMetadata()
                                    .endEntityOperatorRole()
                                    .withNewServiceAccount()
                                        .withNewMetadata()
                                            .withLabels(saLabels)
                                            .withAnnotations(saAnnotations)
                                        .endMetadata()
                                    .endServiceAccount()
                                .endTemplate()
                            .endEntityOperator()
                        .endSpec()
                        .build();

        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check Deployment
        Deployment dep = entityOperator.generateDeployment(true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnnotations.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(singletonList(assertToleration)));

        // Generate Role metadata
        Role crb = entityOperator.generateRole(null, namespace);
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(rLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(rAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = entityOperator.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnotations.entrySet()), is(true));
    }

    @ParallelTest
    public void testGracePeriod() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                    .withTopicOperator(entityTopicOperatorSpec)
                    .withUserOperator(entityUserOperatorSpec)
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(123L));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(30L));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                    .withTopicOperator(entityTopicOperatorSpec)
                    .withUserOperator(entityUserOperatorSpec)
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromCo() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, null, secrets);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .withNewEntityOperator()
                .withTopicOperator(entityTopicOperatorSpec)
                .withUserOperator(entityUserOperatorSpec)
                .withNewTemplate()
                .withNewPod()
                .withImagePullSecrets(secret2)
                .endPod()
                .endTemplate()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, null, singletonList(secret1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(123L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(456L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(789L));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        Deployment dep = eo.generateDeployment(true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = eo.generateDeployment(true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @ParallelTest
    public void testTopicOperatorContainerEnvVars() {

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

        ContainerTemplate topicOperatorContainer = new ContainerTemplate();
        topicOperatorContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withTopicOperatorContainer(topicOperatorContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER).topicOperator().getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));


    }

    @ParallelTest
    public void testTopicOperatorContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = EntityTopicOperator.ENV_VAR_RESOURCE_LABELS;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = EntityTopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate topicOperatorContainer = new ContainerTemplate();
        topicOperatorContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withTopicOperatorContainer(topicOperatorContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER).topicOperator().getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));

    }

    @ParallelTest
    public void testUserOperatorContainerEnvVars() {

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

        ContainerTemplate userOperatorContainer = new ContainerTemplate();
        userOperatorContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withUserOperatorContainer(userOperatorContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER).userOperator().getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));

    }

    @ParallelTest
    public void testUserOperatorContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = EntityUserOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = EntityUserOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate userOperatorContainer = new ContainerTemplate();
        userOperatorContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withUserOperatorContainer(userOperatorContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER).userOperator().getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));
    }

    @ParallelTest
    public void testUserOperatorContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editOrNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .editOrNewTemplate()
                            .editOrNewUserOperatorContainer()
                                .withSecurityContext(securityContext)
                            .endUserOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);
        Deployment deployment = eo.generateDeployment(false, null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testTopicOperatorContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                .addToDrop("ALL")
                .endCapabilities()
                .build();

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editOrNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .editOrNewTemplate()
                            .editOrNewTopicOperatorContainer()
                                .withSecurityContext(securityContext)
                            .endTopicOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);
        Deployment deployment = eo.generateDeployment(false, null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }
    
    @ParallelTest
    public void testRole() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editOrNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);
        Role role = eo.generateRole(namespace, namespace);

        assertThat(role.getMetadata().getName(), is("foo-entity-operator"));
        assertThat(role.getMetadata().getNamespace(), is(namespace));

        List<PolicyRule> rules = new ArrayList<>();
        rules.add(new PolicyRuleBuilder()
                .addToResources("kafkatopics")
                .addToVerbs("get", "list", "watch", "create", "patch", "update", "delete")
                .addToApiGroups(Constants.RESOURCE_GROUP_NAME)
                .build());
        rules.add(new PolicyRuleBuilder()
                .addToResources("kafkausers")
                .addToVerbs("get", "list", "watch", "create", "patch", "update")
                .addToApiGroups(Constants.RESOURCE_GROUP_NAME)
                .build());
        rules.add(new PolicyRuleBuilder()
                .addToResources("kafkatopics/status", "kafkausers/status")
                .addToVerbs("get", "patch", "update")
                .addToApiGroups(Constants.RESOURCE_GROUP_NAME)
                .build());
        rules.add(new PolicyRuleBuilder()
                .addToResources("events")
                .addToVerbs("create")
                .addToApiGroups("")
                .build());
        rules.add(new PolicyRuleBuilder()
                .addToResources("secrets")
                .addToVerbs("get", "list", "watch", "create", "delete", "patch", "update")
                .addToApiGroups("")
                .build());
        assertThat(role.getRules(), is(rules));
    }

    @ParallelTest
    public void testRoleInDifferentNamespace() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .editOrNewEntityOperator()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);
        Role role = eo.generateRole(namespace, namespace);

        TestUtils.checkOwnerReference(role, resource);

        role = eo.generateRole(namespace, "some-other-namespace");
        assertThat(role.getMetadata().getOwnerReferences().size(), is(0));
    }
    @ParallelTest
    public void testTopicOperatorNetworkPolicy() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .editOrNewEntityOperator()
                .withNewTopicOperator()
                .endTopicOperator()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = eo.generateNetworkPolicy();

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(EntityTopicOperator.HEALTHCHECK_PORT))).findFirst().orElse(null), is(notNullValue()));
        assertThat(np.getSpec().getIngress().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort(), is(new IntOrString(EntityTopicOperator.HEALTHCHECK_PORT)));
        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(EntityTopicOperator.HEALTHCHECK_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(0));

    }
    @ParallelTest
    public void testUserOperatorNetworkPolicy() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .editOrNewEntityOperator()
                .withNewUserOperator()
                .endUserOperator()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = eo.generateNetworkPolicy();

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(EntityUserOperator.HEALTHCHECK_PORT))).findFirst().orElse(null), is(notNullValue()));
        assertThat(np.getSpec().getIngress().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort(), is(new IntOrString(EntityUserOperator.HEALTHCHECK_PORT)));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(EntityUserOperator.HEALTHCHECK_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(0));

    }

    @ParallelTest
    public void testUserOperatorAndTopicOperatorNetworkPolicy() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .editOrNewEntityOperator()
                .withNewUserOperator()
                .endUserOperator()
                .withNewTopicOperator()
                .endTopicOperator()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = eo.generateNetworkPolicy();

        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort(), is(new IntOrString(EntityTopicOperator.HEALTHCHECK_PORT)));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort(), is(new IntOrString(EntityUserOperator.HEALTHCHECK_PORT)));

    }
}


