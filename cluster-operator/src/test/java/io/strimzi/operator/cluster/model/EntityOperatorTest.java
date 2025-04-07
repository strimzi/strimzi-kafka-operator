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
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
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
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Annotations;
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
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(CLUSTER_NAME)
                .withLabels(Map.of("my-user-label", "cromulent"))
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
            .withNewEntityOperator()
                .withNewUserOperator()
                .endUserOperator()
                .withNewTopicOperator()
                .endTopicOperator()
                .withNewTemplate()
                    .withNewPod()
                        .withTmpDirSizeLimit("100Mi")
                    .endPod()
                .endTemplate()
            .endEntityOperator()
            .endSpec()
            .build();
    private static final EntityOperator ENTITY_OPERATOR = EntityOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), KAFKA, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testGenerateDeployment(boolean cruiseControlEnabled) {
        ENTITY_OPERATOR.cruiseControlEnabled = cruiseControlEnabled;
        Deployment dep = ENTITY_OPERATOR.generateDeployment(Map.of(), true, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(dep.getMetadata().getName(), is(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)));
        assertThat(dep.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(dep.getSpec().getReplicas(), is(1));
        TestUtils.checkOwnerReference(dep, KAFKA);

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
        assertThat(ENTITY_OPERATOR.namespace, is(NAMESPACE));
        assertThat(ENTITY_OPERATOR.cluster, is(CLUSTER_NAME));
    }

    @ParallelTest
    public void testFromCrdNoTopicAndUserOperatorInEntityOperator() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityOperator, is(nullValue()));
    }

    @ParallelTest
    public void testFromCrdNoTopicInEntityOperator() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityOperator.topicOperator(), is(nullValue()));
        assertThat(entityOperator.userOperator(), is(notNullValue()));
    }

    @ParallelTest
    public void testFromCrdNoUserOperatorInEntityOperator() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        assertThat(entityOperator.topicOperator(), is(notNullValue()));
        assertThat(entityOperator.userOperator(), is(nullValue()));
    }

    @ParallelTest
    public void withAffinityAndTolerations() throws IOException {
        ResourceTester<Kafka, EntityOperator> helper = new ResourceTester<>(Kafka.class, VERSIONS, (kAssembly, versions) -> EntityOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), kAssembly, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig()), this.getClass().getSimpleName() + ".withAffinityAndTolerations");
        helper.assertDesiredModel("-DeploymentAffinity.yaml", zc -> zc.generateDeployment(Map.of(), true, null, null).getSpec().getTemplate().getSpec().getAffinity());
        helper.assertDesiredModel("-DeploymentTolerations.yaml", zc -> zc.generateDeployment(Map.of(), true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = Map.of("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> depAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> saAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> rLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> rAnnotations = Map.of("a7", "v7", "a8", "v8");

        Toleration toleration = new TolerationBuilder()
                .withEffect("NoSchedule")
                .withValue("")
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
        
        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();
        
        List<AdditionalVolume> additionalVolumes  = singletonList(new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build());
        
        List<VolumeMount> additionalVolumeMounts = singletonList(new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret")
                .withSubPath("def")
                .build());

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
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
                                .withVolumes(additionalVolumes)
                            .endPod()
                            .withNewTopicOperatorContainer()
                                .withVolumeMounts(additionalVolumeMounts)
                            .endTopicOperatorContainer()
                            .withNewUserOperatorContainer()
                                .withVolumeMounts(additionalVolumeMounts)
                            .endUserOperatorContainer()
                            .withNewEntityOperatorRole()
                                .withNewMetadata()
                                    .withLabels(rLabels)
                                    .withAnnotations(rAnnotations)
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

        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        // Check Deployment
        Deployment dep = entityOperator.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnnotations.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(singletonList(toleration)));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(volume -> "secret-volume-name".equals(volume.getName())).iterator().next().getSecret(), is(secret));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().stream().filter(volumeMount -> "secret-volume-name".equals(volumeMount.getName())).iterator().next(), is(additionalVolumeMounts.get(0)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getVolumeMounts().stream().filter(volumeMount -> "secret-volume-name".equals(volumeMount.getName())).iterator().next(), is(additionalVolumeMounts.get(0)));

        // Generate Role metadata
        Role crb = entityOperator.generateRole(null, NAMESPACE);
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(rLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(rAnnotations.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = entityOperator.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnotations.entrySet()), is(true));
    }

    @ParallelTest
    public void testGracePeriod() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewPod()
                                .withTerminationGracePeriodSeconds(123)
                            .endPod()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        Deployment dep = eo.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(123L));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        Deployment dep = ENTITY_OPERATOR.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(30L));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret1, secret2)
                            .endPod()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        Deployment dep = eo.generateDeployment(Map.of(), true, null, null);
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

        Deployment dep = ENTITY_OPERATOR.generateDeployment(Map.of(), true, null, secrets);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret2)
                            .endPod()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        Deployment dep = eo.generateDeployment(Map.of(), true, null, singletonList(secret1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        Deployment dep = ENTITY_OPERATOR.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        Deployment dep = eo.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(123L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(456L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(789L));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        Deployment dep = ENTITY_OPERATOR.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Deployment dep = ENTITY_OPERATOR.generateDeployment(Map.of(), true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = ENTITY_OPERATOR.generateDeployment(Map.of(), true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
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

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withTopicOperatorContainer(topicOperatorContainer)
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig()).topicOperator().getEnvVars();

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

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withTopicOperatorContainer(topicOperatorContainer)
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig()).topicOperator().getEnvVars();

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

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withUserOperatorContainer(userOperatorContainer)
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig()).userOperator().getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testUserOperatorContainerEnvVarsConflict() {
        ContainerEnvVar envVar = new ContainerEnvVar();
        String testEnvTwoKey = EntityUserOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS;
        String testEnvTwoValue = "test.env.two";
        envVar.setName(testEnvTwoKey);
        envVar.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar);
        ContainerTemplate userOperatorContainer = new ContainerTemplate();
        userOperatorContainer.setEnv(testEnvs);

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withUserOperatorContainer(userOperatorContainer)
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), 
            resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig()).userOperator().getEnvVars();
        
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

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewUserOperatorContainer()
                                .withSecurityContext(securityContext)
                            .endUserOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());
        Deployment deployment = eo.generateDeployment(Map.of(), false, null, null);

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

        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewTopicOperatorContainer()
                                .withSecurityContext(securityContext)
                            .endTopicOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());
        Deployment deployment = eo.generateDeployment(Map.of(), false, null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }
    
    @ParallelTest
    public void testRole() {
        Role role = ENTITY_OPERATOR.generateRole(NAMESPACE, NAMESPACE);

        assertThat(role.getMetadata().getName(), is("my-cluster-entity-operator"));
        assertThat(role.getMetadata().getNamespace(), is(NAMESPACE));

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
                .addToResources("secrets")
                .addToVerbs("get", "list", "watch", "create", "delete", "patch", "update")
                .addToApiGroups("")
                .build());
        assertThat(role.getRules(), is(rules));
    }

    @ParallelTest
    public void testRoleInDifferentNamespace() {
        Role role = ENTITY_OPERATOR.generateRole(NAMESPACE, NAMESPACE);
        TestUtils.checkOwnerReference(role, KAFKA);

        role = ENTITY_OPERATOR.generateRole(NAMESPACE, "some-other-namespace");
        assertThat(role.getMetadata().getOwnerReferences().size(), is(0));
    }

    @ParallelTest
    public void testTopicOperatorNetworkPolicy() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        NetworkPolicy np = eo.generateNetworkPolicy();
        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(EntityTopicOperator.HEALTHCHECK_PORT))).findFirst().orElse(null), is(notNullValue()));
        assertThat(np.getSpec().getIngress().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort(), is(new IntOrString(EntityTopicOperator.HEALTHCHECK_PORT)));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(EntityTopicOperator.HEALTHCHECK_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);
        assertThat(rules.size(), is(0));

    }

    @ParallelTest
    public void testUserOperatorNetworkPolicy() {
        Kafka resource = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());

        NetworkPolicy np = eo.generateNetworkPolicy();
        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(EntityUserOperator.HEALTHCHECK_PORT))).findFirst().orElse(null), is(notNullValue()));
        assertThat(np.getSpec().getIngress().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort(), is(new IntOrString(EntityUserOperator.HEALTHCHECK_PORT)));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(EntityUserOperator.HEALTHCHECK_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);
        assertThat(rules.size(), is(0));
    }

    @ParallelTest
    public void testUserOperatorAndTopicOperatorNetworkPolicy() {
        NetworkPolicy np = ENTITY_OPERATOR.generateNetworkPolicy();
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort(), is(new IntOrString(EntityTopicOperator.HEALTHCHECK_PORT)));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort(), is(new IntOrString(EntityUserOperator.HEALTHCHECK_PORT)));
    }

    @ParallelTest
    public void testFeatureGateEnvVars() {
        ClusterOperatorConfig config = new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), VERSIONS)
                .with(ClusterOperatorConfig.FEATURE_GATES.key(), "+DummyFeatureGate")
                .build();

        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", KAFKA.getKind(), KAFKA.getMetadata().getNamespace(), KAFKA.getMetadata().getName()), KAFKA, SHARED_ENV_PROVIDER, config);
        Deployment dep = eo.generateDeployment(Map.of(), false, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().filter(env -> "STRIMZI_FEATURE_GATES".equals(env.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is("+DummyFeatureGate"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getEnv().stream().filter(env -> "STRIMZI_FEATURE_GATES".equals(env.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is("+DummyFeatureGate"));
    }

    ////////////////////
    // Utility methods
    ////////////////////

    /**
     * Converts list of volume mounts to a map. This is used also from the EUO and ETO test classes.
     *
     * @param mounts    List of volume mounts
     *
     * @return  Map with volume mounts where the mount name is used as a key
     */
    static Map<String, String> volumeMounts(List<VolumeMount> mounts) {
        return mounts.stream().collect(Collectors.toMap(VolumeMount::getName, VolumeMount::getMountPath));
    }
}