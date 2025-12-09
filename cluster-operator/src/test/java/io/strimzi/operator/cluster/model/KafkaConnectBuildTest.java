/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.openshift.api.model.BuildConfig;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.TestUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaConnectBuildTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final List<String> EXPECTED_DEFAULT_KANIKO_OPTIONS = List.of("--dockerfile=/dockerfile/Dockerfile",
            "--image-name-with-digest-file=/dev/termination-log",
            "--destination=my-image:latest");
    private static final String EXPECTED_DEFAULT_BUILDAH_BUILD_ARGS = "--file=/dockerfile/Dockerfile --tag=my-image:latest --storage-driver=vfs";
    private static final String EXPECTED_DEFAULT_BUILDAH_PUSH_ARGS = "--storage-driver=vfs --digestfile=/tmp/digest";

    private static final String NAMESPACE = "my-ns";
    private static final String NAME = "my-connect";
    private static final KafkaConnect RESOURCE = new KafkaConnectBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withImage("my-source-image:latest")
                .withBootstrapServers("my-kafka:9092")
                .withGroupId("my-group")
                .withConfigStorageTopic("my-config-topic")
                .withOffsetStorageTopic("my-offset-topic")
                .withStatusStorageTopic("my-status-topic")
            .withNewBuild()
                .withNewDockerOutput()
                    .withImage("my-image:latest")
                    .withPushSecret("my-docker-credentials")
                .endDockerOutput()
                .withPlugins(
                        new PluginBuilder().withName("my-connector").withArtifacts(new JarArtifactBuilder().withUrl("https://mydomain.tld/my.jar").build()).build(),
                        new PluginBuilder().withName("my-connector2").withArtifacts(new JarArtifactBuilder().withUrl("https://mydomain.tld/my2.jar").withSha512sum("sha-512-checksum").build()).build())
            .endBuild()
            .endSpec()
            .build();

    @Test
    public void testFromCrd()   {
        assertDoesNotThrow(() -> {
            KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), RESOURCE, VERSIONS, SHARED_ENV_PROVIDER, false);
        });

    }

    @Test
    public void testValidationPluginsExist()   {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewBuild()
                        .withPlugins()
                    .endBuild()
                .endSpec()
                .build();

        InvalidResourceException thrown = assertThrows(InvalidResourceException.class, () ->
            KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false)
        );
        assertThat(thrown.getMessage(), is("List of connector plugins is required when Kafka Connect Build is used."));
    }

    @Test
    public void testValidationArtifactsExist()   {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withPlugins(new PluginBuilder().withName("my-connector").build())
                    .endBuild()
                .endSpec()
                .build();

        InvalidResourceException thrown = assertThrows(InvalidResourceException.class, () ->
            KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false)
        );
        assertThat(thrown.getMessage(), is("Each connector plugin needs to have a list of artifacts."));
    }

    @Test
    public void testValidationUniqueNames()   {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withPlugins(
                                new PluginBuilder().withName("my-connector").withArtifacts(new JarArtifactBuilder().withUrl("https://mydomain.tld/my.jar").build()).build(),
                                new PluginBuilder().withName("my-connector").withArtifacts(new JarArtifactBuilder().withUrl("https://mydomain.tld/my2.jar").withSha512sum("sha-512-checksum").build()).build()
                        )
                    .endBuild()
                .endSpec()
                .build();

        InvalidResourceException thrown = assertThrows(InvalidResourceException.class, () ->
            KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false)
        );
        assertThat(thrown.getMessage(), is("Connector plugins names have to be unique within a single KafkaConnect resource."));
    }

    @Test
    public void testKanikoDeployment()   {
        Map<String, Quantity> limit = new HashMap<>();
        limit.put("cpu", new Quantity("500m"));
        limit.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> request = new HashMap<>();
        request.put("cpu", new Quantity("1000m"));
        request.put("memory", new Quantity("1Gi"));

        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withResources(new ResourceRequirementsBuilder().withLimits(limit).withRequests(request).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false);

        assertThat(build.baseImage, is("my-source-image:latest"));

        Pod pod = build.generateBuilderPod(true, false, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        assertThat(pod.getMetadata().getName(), is(KafkaConnectResources.buildPodName(NAME)));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));

        Map<String, String> expectedDeploymentLabels = Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME,
                Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.buildPodName(NAME),
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaConnectBuild.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectBuild.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, NAME,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + NAME,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        assertThat(pod.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(pod.getSpec().getServiceAccountName(), is(KafkaConnectResources.buildServiceAccountName(NAME)));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getArgs(), is(EXPECTED_DEFAULT_KANIKO_OPTIONS));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is(KafkaConnectResources.buildPodName(NAME)));
        // TODO: use configuration from the `ClusterOperatorConfig` rather than from env variables directly - https://github.com/strimzi/strimzi-kafka-operator/issues/11981
        assertThat(pod.getSpec().getContainers().get(0).getImage(), is(KafkaConnectBuild.DEFAULT_KANIKO_EXECUTOR_IMAGE));
        assertThat(pod.getSpec().getContainers().get(0).getPorts(), is(nullValue()));
        assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(limit));
        assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(request));
        assertThat(pod.getSpec().getVolumes().size(), is(2));
        assertThat(pod.getSpec().getVolumes().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getName(), is(KafkaConnectResources.dockerFileConfigMapName(NAME)));
        assertThat(pod.getSpec().getVolumes().get(1).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getSecretName(), is("my-docker-credentials"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(2));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/kaniko/.docker"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().size(), is(0));
        io.strimzi.operator.cluster.TestUtils.checkOwnerReference(pod, kc);
    }

    @Test
    public void testBuildahDeployment() {
        Map<String, Quantity> limit = new HashMap<>();
        limit.put("cpu", new Quantity("500m"));
        limit.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> request = new HashMap<>();
        request.put("cpu", new Quantity("1000m"));
        request.put("memory", new Quantity("1Gi"));

        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withResources(new ResourceRequirementsBuilder().withLimits(limit).withRequests(request).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, true);

        assertThat(build.baseImage, is("my-source-image:latest"));

        Pod pod = build.generateBuilderPod(true, true, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        assertThat(pod.getMetadata().getName(), is(KafkaConnectResources.buildPodName(NAME)));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));

        Map<String, String> expectedDeploymentLabels = Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME,
            Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.buildPodName(NAME),
            Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
            Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaConnectBuild.COMPONENT_TYPE,
            Labels.KUBERNETES_NAME_LABEL, KafkaConnectBuild.COMPONENT_TYPE,
            Labels.KUBERNETES_INSTANCE_LABEL, NAME,
            Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + NAME,
            Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        String[] commands = pod.getSpec().getContainers().get(0).getArgs().get(2).split("\n");

        assertThat(pod.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(pod.getSpec().getServiceAccountName(), is(KafkaConnectResources.buildServiceAccountName(NAME)));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(commands[0], containsString(EXPECTED_DEFAULT_BUILDAH_BUILD_ARGS));
        assertThat(commands[1], containsString(EXPECTED_DEFAULT_BUILDAH_PUSH_ARGS));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is(KafkaConnectResources.buildPodName(NAME)));
        // TODO: use configuration from the `ClusterOperatorConfig` rather than from env variables directly - https://github.com/strimzi/strimzi-kafka-operator/issues/11981
        assertThat(pod.getSpec().getContainers().get(0).getImage(), is(KafkaConnectBuild.DEFAULT_BUILDAH_IMAGE));
        assertThat(pod.getSpec().getContainers().get(0).getPorts(), is(nullValue()));
        assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(limit));
        assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(request));
        assertThat(pod.getSpec().getVolumes().size(), is(2));
        assertThat(pod.getSpec().getVolumes().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getName(), is(KafkaConnectResources.dockerFileConfigMapName(NAME)));
        assertThat(pod.getSpec().getVolumes().get(1).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getSecretName(), is("my-docker-credentials"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(2));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/build/.docker"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().get(0).getName(), is("REGISTRY_AUTH_FILE"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().get(0).getValue(), is("/build/.docker/config.json"));
        io.strimzi.operator.cluster.TestUtils.checkOwnerReference(pod, kc);
    }

    @Test
    public void testKanikoDeploymentWithoutPushSecret()   {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false);

        Pod pod = build.generateBuilderPod(true, false, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        assertThat(pod.getSpec().getVolumes().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getArgs(), is(EXPECTED_DEFAULT_KANIKO_OPTIONS));
        assertThat(pod.getSpec().getVolumes().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getName(), is(KafkaConnectResources.dockerFileConfigMapName(NAME)));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().size(), is(0));
    }

    @Test
    public void testBuildahDeploymentWithoutPushSecret()   {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, true);

        Pod pod = build.generateBuilderPod(true, true, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        String[] commands = pod.getSpec().getContainers().get(0).getArgs().get(2).split("\n");

        assertThat(pod.getSpec().getVolumes().size(), is(1));
        assertThat(commands[0], containsString(EXPECTED_DEFAULT_BUILDAH_BUILD_ARGS));
        assertThat(commands[1], containsString(EXPECTED_DEFAULT_BUILDAH_PUSH_ARGS));
        assertThat(pod.getSpec().getVolumes().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getName(), is(KafkaConnectResources.dockerFileConfigMapName(NAME)));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().size(), is(0));
    }

    @Test
    public void testConfigMap()   {
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), RESOURCE, VERSIONS, SHARED_ENV_PROVIDER, false);

        KafkaConnectDockerfile dockerfile = new KafkaConnectDockerfile("my-image:latest", RESOURCE.getSpec().getBuild(), SHARED_ENV_PROVIDER);
        ConfigMap cm = build.generateDockerfileConfigMap(dockerfile);

        assertThat(cm.getMetadata().getName(), is(KafkaConnectResources.dockerFileConfigMapName(NAME)));
        assertThat(cm.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(cm.getData().get("Dockerfile"), is(dockerfile.getDockerfile()));
        io.strimzi.operator.cluster.TestUtils.checkOwnerReference(cm, RESOURCE);
    }

    @Test
    public void testBuildconfigWithDockerOutput()   {
        Map<String, Quantity> limit = new HashMap<>();
        limit.put("cpu", new Quantity("500m"));
        limit.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> request = new HashMap<>();
        request.put("cpu", new Quantity("1000m"));
        request.put("memory", new Quantity("1Gi"));

        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withResources(new ResourceRequirementsBuilder().withLimits(limit).withRequests(request).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false);

        KafkaConnectDockerfile dockerfile = new KafkaConnectDockerfile("my-image:latest", kc.getSpec().getBuild(), SHARED_ENV_PROVIDER);
        BuildConfig bc = build.generateBuildConfig(dockerfile);
        assertThat(bc.getMetadata().getName(), is(KafkaConnectResources.buildConfigName(NAME)));
        assertThat(bc.getMetadata().getNamespace(), is(NAMESPACE));

        Map<String, String> expectedDeploymentLabels = Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME,
                Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.buildPodName(NAME),
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaConnectBuild.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectBuild.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, NAME,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + NAME,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        assertThat(bc.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(bc.getSpec().getSource().getDockerfile(), is(dockerfile.getDockerfile()));
        assertThat(bc.getSpec().getOutput().getTo().getKind(), is("DockerImage"));
        assertThat(bc.getSpec().getOutput().getTo().getName(), is("my-image:latest"));
        assertThat(bc.getSpec().getOutput().getPushSecret().getName(), is("my-docker-credentials"));
        assertThat(bc.getSpec().getStrategy().getDockerStrategy(), is(notNullValue()));
        assertThat(bc.getSpec().getResources().getLimits(), is(limit));
        assertThat(bc.getSpec().getResources().getRequests(), is(request));
        io.strimzi.operator.cluster.TestUtils.checkOwnerReference(bc, kc);
    }

    // Test to validate that .spec.image and spec.build.output.image in Kafka Connect are not pointing to the same image
    @Test
    public void testKafkaConnectBuildWithSpecImageSameAsDockerOutput() {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withImage("my-image:latest")
                .endSpec()
                .build();

        InvalidResourceException thrown = assertThrows(InvalidResourceException.class, () -> KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false), "InvalidResourceException was expected");
        assertThat(thrown.getMessage(), is("KafkaConnect .spec.image cannot be the same as .spec.build.output.image"));
    }

    @Test
    public void testBuildconfigWithImageStreamOutput()   {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewImageStreamOutput()
                            .withImage("my-image:latest")
                        .endImageStreamOutput()
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false);

        KafkaConnectDockerfile dockerfile = new KafkaConnectDockerfile("my-image:latest", kc.getSpec().getBuild(), SHARED_ENV_PROVIDER);
        BuildConfig bc = build.generateBuildConfig(dockerfile);
        assertThat(bc.getMetadata().getName(), is(KafkaConnectResources.buildConfigName(NAME)));
        assertThat(bc.getMetadata().getNamespace(), is(NAMESPACE));

        Map<String, String> expectedDeploymentLabels = Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME,
                Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.buildPodName(NAME),
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaConnectBuild.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectBuild.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, NAME,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + NAME,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        assertThat(bc.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(bc.getSpec().getSource().getDockerfile(), is(dockerfile.getDockerfile()));
        assertThat(bc.getSpec().getOutput().getTo().getKind(), is("ImageStreamTag"));
        assertThat(bc.getSpec().getOutput().getTo().getName(), is("my-image:latest"));
        assertThat(bc.getSpec().getStrategy().getDockerStrategy(), is(notNullValue()));
        TestUtils.checkOwnerReference(bc, kc);
    }

    @Test
    public void testTemplate()   {
        Map<String, String> buildPodLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> buildPodAnnos = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> buildConfigLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> buildConfigAnnos = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> saAnots = Map.of("a5", "v5", "a6", "v6");

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

        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .withNewTemplate()
                        .withNewBuildPod()
                            .withNewMetadata()
                                .withLabels(buildPodLabels)
                                .withAnnotations(buildPodAnnos)
                            .endMetadata()
                            .withPriorityClassName("top-priority")
                            .withSchedulerName("my-scheduler")
                            .withEnableServiceLinks(false)
                            .withVolumes(additionalVolume)
                        .endBuildPod()
                        .withNewBuildContainer()
                            .withEnv(new ContainerEnvVarBuilder().withName("TEST_ENV_VAR").withValue("testValue").build())
                            .withVolumeMounts(additionalVolumeMount)
                        .endBuildContainer()
                        .withNewBuildConfig()
                            .withNewMetadata()
                                .withLabels(buildConfigLabels)
                                .withAnnotations(buildConfigAnnos)
                            .endMetadata()
                            .withPullSecret("my-pull-secret")
                        .endBuildConfig()
                        .withNewBuildServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnots)
                            .endMetadata()
                        .endBuildServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false);

        Pod pod = build.generateBuilderPod(true, false, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        assertThat(pod.getMetadata().getLabels().entrySet().containsAll(buildPodLabels.entrySet()), is(true));
        assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(buildPodAnnos.entrySet()), is(true));
        assertThat(pod.getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(env -> "TEST_ENV_VAR".equals(env.getName())).findFirst().orElseThrow().getValue(), is("testValue"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().stream().filter(volumeMount -> "secret-volume-name".equals(volumeMount.getName())).iterator().next(), is(additionalVolumeMount));
        assertThat(pod.getSpec().getVolumes().stream().filter(volume -> "secret-volume-name".equals(volume.getName())).iterator().next().getSecret(), is(secret));

        KafkaConnectDockerfile dockerfile = new KafkaConnectDockerfile("my-image:latest", kc.getSpec().getBuild(), SHARED_ENV_PROVIDER);
        BuildConfig bc = build.generateBuildConfig(dockerfile);
        assertThat(bc.getMetadata().getLabels().entrySet().containsAll(buildConfigLabels.entrySet()), is(true));
        assertThat(bc.getMetadata().getAnnotations().entrySet().containsAll(buildConfigAnnos.entrySet()), is(true));
        assertThat(bc.getSpec().getStrategy().getDockerStrategy().getPullSecret().getName(), is("my-pull-secret"));

        // Check Service Account
        ServiceAccount sa = build.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @Test
    public void testValidKanikoOptions()   {
        List<String> expectedArgs = new ArrayList<>(EXPECTED_DEFAULT_KANIKO_OPTIONS);
        expectedArgs.add("--reproducible");
        expectedArgs.add("--single-snapshot");
        expectedArgs.add("--log-format=json");

        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withPushSecret("my-docker-credentials")
                            .withAdditionalKanikoOptions("--reproducible", "--single-snapshot", "--log-format=json")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false);

        Pod pod = build.generateBuilderPod(true, false, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        assertThat(pod.getSpec().getContainers().get(0).getArgs(), is(expectedArgs));
    }

    @Test
    public void testInvalidKanikoOptions()   {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withPushSecret("my-docker-credentials")
                            .withAdditionalKanikoOptions("--reproducible", "--reproducible-something", "--build-arg", "--single-snapshot", "--digest-file=/dev/null", "--log-format=json")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () ->
            KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false)
        );

        assertThat(e.getMessage(), containsString(".spec.build.output.additionalKanikoOptions contains forbidden options: [--reproducible-something, --build-arg, --digest-file]"));
    }

    @Test
    public void testValidBuildahBuildOptions() {
        String expectedBuildOptions = EXPECTED_DEFAULT_BUILDAH_BUILD_ARGS + " --retry=3";
        String expectedPushOptions = EXPECTED_DEFAULT_BUILDAH_PUSH_ARGS + " --quiet";

        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withPushSecret("my-docker-credentials")
                            .withAdditionalBuildOptions("--retry=3")
                            .withAdditionalPushOptions("--quiet")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, true);

        Pod pod = build.generateBuilderPod(true, true, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        String[] commands = pod.getSpec().getContainers().get(0).getArgs().get(2).split("\n");
        assertThat(commands[0], containsString(expectedBuildOptions));
        assertThat(commands[1], containsString(expectedPushOptions));
    }

    @Test
    public void testInvalidBuildahBuildOptions() {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withPushSecret("my-docker-credentials")
                            .withAdditionalBuildOptions("--logfile=my-file", "--file=/docker/Another", "--storage-driver=random")
                            .withAdditionalPushOptions("--quiet")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () ->
            KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, true)
        );

        assertThat(e.getMessage(), containsString(".spec.build.output.additionalBuildOptions contains forbidden options: [--logfile, --file, --storage-driver]"));
    }

    @Test
    public void testInvalidBuildahPushOptions() {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withPushSecret("my-docker-credentials")
                            .withAdditionalBuildOptions("--retry=3")
                            .withAdditionalPushOptions("--digestfile=/tmp/random", "--sign-by=different", "--format=xy")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () ->
            KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, true)
        );

        assertThat(e.getMessage(), containsString(".spec.build.output.additionalPushOptions contains forbidden options: [--digestfile, --sign-by, --format]"));
    }

    @Test
    public void testKanikoAdditionalOptionsConfiguredOnBothPlaces() {
        List<String> expectedArgs = new ArrayList<>(EXPECTED_DEFAULT_KANIKO_OPTIONS);
        expectedArgs.add("--single-snapshot");
        expectedArgs.add("--log-format=json");

        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withPushSecret("my-docker-credentials")
                            .withAdditionalKanikoOptions("--reproducible")
                            .withAdditionalBuildOptions("--single-snapshot", "--log-format=json")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false);

        Pod pod = build.generateBuilderPod(true, false, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        assertThat(pod.getSpec().getContainers().get(0).getArgs(), is(expectedArgs));
        assertThat(pod.getSpec().getContainers().get(0).getArgs().contains("--reproducible"), is(false));
    }

    @Test
    public void testInvalidKanikoOptionsInAdditionalBuildOptions() {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editBuild()
                        .withNewDockerOutput()
                           .withImage("my-image:latest")
                           .withPushSecret("my-docker-credentials")
                           .withAdditionalBuildOptions("--reproducible", "--reproducible-something", "--build-arg", "--single-snapshot", "--digest-file=/dev/null", "--log-format=json")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () ->
            KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false)
        );

        assertThat(e.getMessage(), containsString(".spec.build.output.additionalBuildOptions contains forbidden options: [--reproducible-something, --build-arg, --digest-file]"));
    }

    @Test
    public void testKanikoWithAdditionalVolumesVolumeMountsAndEnvVariables() {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editOrNewTemplate()
                        .withNewBuildContainer()
                            .addToVolumeMounts(
                                new VolumeMountBuilder()
                                    .withName("volume")
                                    .withMountPath("/mnt/my/path")
                                    .build()
                            )
                            .addNewEnv()
                                .withName("MY_ENV")
                                .withValue("value")
                            .endEnv()
                        .endBuildContainer()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, false);

        Pod pod = build.generateBuilderPod(true, false, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        assertThat(pod.getSpec().getVolumes().size(), is(2));
        assertThat(pod.getSpec().getVolumes().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getName(), is(KafkaConnectResources.dockerFileConfigMapName(NAME)));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getItems().size(), is(1));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getItems().get(0).getKey(), is("Dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getItems().get(0).getPath(), is("Dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(1).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getSecretName(), is("my-docker-credentials"));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getItems().size(), is(1));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getItems().get(0).getKey(), is(".dockerconfigjson"));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getItems().get(0).getPath(), is("config.json"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(3));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/kaniko/.docker"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is("volume"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is("/mnt/my/path"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().get(0).getName(), is("MY_ENV"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().get(0).getValue(), is("value"));
    }

    @Test
    public void testBuildahWithAdditionalVolumesVolumeMountsAndEnvVariables() {
        KafkaConnect kc = new KafkaConnectBuilder(RESOURCE)
                .editSpec()
                    .editOrNewTemplate()
                        .withNewBuildContainer()
                            .addToVolumeMounts(
                                new VolumeMountBuilder()
                                    .withName("volume")
                                    .withMountPath("/mnt/my/path")
                                    .build()
                            )
                            .addNewEnv()
                                .withName("MY_ENV")
                                .withValue("value")
                            .endEnv()
                        .endBuildContainer()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME), kc, VERSIONS, SHARED_ENV_PROVIDER, true);

        Pod pod = build.generateBuilderPod(true, true, ImagePullPolicy.IFNOTPRESENT, null, "cf065b80ede090aa");
        assertThat(pod.getSpec().getVolumes().size(), is(2));
        assertThat(pod.getSpec().getVolumes().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getName(), is(KafkaConnectResources.dockerFileConfigMapName(NAME)));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getItems().size(), is(1));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getItems().get(0).getKey(), is("Dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(0).getConfigMap().getItems().get(0).getPath(), is("Dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(1).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getSecretName(), is("my-docker-credentials"));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getItems().size(), is(1));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getItems().get(0).getKey(), is(".dockerconfigjson"));
        assertThat(pod.getSpec().getVolumes().get(1).getSecret().getItems().get(0).getPath(), is("config.json"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(3));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/build/.docker"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is("volume"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is("/mnt/my/path"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().size(), is(2));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().get(0).getName(), is("REGISTRY_AUTH_FILE"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().get(0).getValue(), is("/build/.docker/config.json"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().get(1).getName(), is("MY_ENV"));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().get(1).getValue(), is("value"));
    }
}
