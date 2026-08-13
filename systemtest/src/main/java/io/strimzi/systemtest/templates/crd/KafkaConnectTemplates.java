/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.skodjob.kubetest4j.utils.KubeTestUtils;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.build.DockerOutput;
import io.strimzi.api.kafka.model.connect.build.DockerOutputBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class KafkaConnectTemplates {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectTemplates.class);

    private static final String METRICS_CONNECT_CONFIG_MAP_SUFFIX = "-connect-metrics";
    private static final String CONFIG_MAP_KEY = "metrics-config.yml";
    private static final String PATH_TO_CONNECT_BUILD_YAML = TestUtils.USER_PATH + "/../systemtest/src/test/resources/connect-build/connect-build-template.yaml";

    private KafkaConnectTemplates() {}

    public static KafkaConnectBuilder kafkaConnect(
        final String namespaceName,
        final String kafkaConnectClusterName,
        final String kafkaClusterName,
        final int kafkaConnectReplicas
    ) {
        return defaultKafkaConnect(namespaceName, kafkaConnectClusterName, kafkaClusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnect(
        final String namespaceName,
        final String kafkaClusterName,
        final int kafkaConnectReplicas
    ) {
        return kafkaConnect(namespaceName, kafkaClusterName, kafkaClusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetricsAndFileSinkPlugin(
        final String namespaceName,
        final String kafkaConnectClusterName,
        final String kafkaClusterName,
        final int replicas
    ) {
        return kafkaConnectWithFilePlugin(namespaceName, kafkaConnectClusterName, kafkaClusterName, replicas)
            .editOrNewSpec()
            .withNewJmxPrometheusExporterMetricsConfig()
                .withNewValueFrom()
                    .withNewConfigMapKeyRef(CONFIG_MAP_KEY, getConfigMapName(kafkaConnectClusterName), false)
                .endValueFrom()
            .endJmxPrometheusExporterMetricsConfig()
            .endSpec();
    }

    public static ConfigMap connectMetricsConfigMap(String namespaceName, String kafkaConnectClusterName) {
        return new ConfigMapBuilder(FileUtils.extractConfigMapFromYAMLWithResources(TestConstants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG, "connect-metrics"))
            .editOrNewMetadata()
                .withNamespace(namespaceName)
                .withName(getConfigMapName(kafkaConnectClusterName))
            .endMetadata()
            .build();
    }

    private static String getConfigMapName(String kafkaConnectClusterName) {
        return kafkaConnectClusterName + METRICS_CONNECT_CONFIG_MAP_SUFFIX;
    }

    @SuppressFBWarnings("DMI_RANDOM_USED_ONLY_ONCE")
    public static KafkaConnectBuilder kafkaConnectBuild(
        final String namespaceName,
        String kafkaConnectClusterName,
        String kafkaClusterName,
        int kafkaConnectReplicas
    ) {
        final String imageName = Environment.getImageOutputRegistry(namespaceName, TestConstants.ST_CONNECT_BUILD_IMAGE_NAME, String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));
        return kafkaConnectBuild(namespaceName, kafkaConnectClusterName, kafkaClusterName, kafkaConnectReplicas, imageName);
    }

    public static KafkaConnectBuilder kafkaConnectBuild(
        final String namespaceName,
        String kafkaConnectClusterName,
        String kafkaClusterName,
        int kafkaConnectReplicas,
        String imageName
    ) {
        try {
            String connectFromResourcesYaml = Files.readString(Paths.get(PATH_TO_CONNECT_BUILD_YAML));
            KafkaConnect connectFromResources = KubeTestUtils.configFromYaml(connectFromResourcesYaml, KafkaConnect.class);
            DockerOutputBuilder dockerOutputBuilder = new DockerOutputBuilder();

            if (connectFromResources.getSpec() != null
                && connectFromResources.getSpec().getBuild() != null
                && connectFromResources.getSpec().getBuild().getOutput() instanceof DockerOutput dockerOutput
            ) {
                dockerOutputBuilder = new DockerOutputBuilder(dockerOutput);
            }

            return configureKafkaConnectBuilderWithDefaults(namespaceName, kafkaConnectClusterName, kafkaClusterName, kafkaConnectReplicas, new KafkaConnectBuilder(connectFromResources))
                .editSpec()
                    .editOrNewBuild()
                        .withOutput(dockerOutput(imageName, dockerOutputBuilder))
                    .endBuild()
                .endSpec();
        } catch (Exception e) {
            LOGGER.error("Failed to read Connect Build template from: {}, due to: {}", PATH_TO_CONNECT_BUILD_YAML, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static KafkaConnectBuilder defaultKafkaConnect(
        final String namespaceName,
        String kafkaConnectClusterName,
        String kafkaClusterName,
        int kafkaConnectReplicas
    ) {
        return configureKafkaConnectBuilderWithDefaults(namespaceName, kafkaConnectClusterName, kafkaClusterName, kafkaConnectReplicas, new KafkaConnectBuilder());
    }

    private static KafkaConnectBuilder configureKafkaConnectBuilderWithDefaults(
        final String namespaceName,
        String kafkaConnectClusterName,
        String kafkaClusterName,
        int kafkaConnectReplicas,
        KafkaConnectBuilder connectBuilder
    ) {
        return connectBuilder
            .editOrNewMetadata()
                .withName(kafkaConnectClusterName)
                .withNamespace(namespaceName)
            .endMetadata()
            .editOrNewSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterName))
                .withGroupId(KafkaConnectResources.componentName(kafkaConnectClusterName))
                .withConfigStorageTopic(KafkaConnectResources.configMapName(kafkaConnectClusterName))
                .withOffsetStorageTopic(KafkaConnectResources.configStorageTopicOffsets(kafkaConnectClusterName))
                .withStatusStorageTopic(KafkaConnectResources.configStorageTopicStatus(kafkaConnectClusterName))
                .withReplicas(kafkaConnectReplicas)
                .withNewTls()
                    .withTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterName))
                            .withCertificate("ca.crt")
                            .build()
                    )
                .endTls()
                .addToConfig("config.storage.replication.factor", "-1")
                .addToConfig("offset.storage.replication.factor", "-1")
                .addToConfig("status.storage.replication.factor", "-1")
                .withNewInlineLogging()
                    .addToLoggers("rootLogger.level", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String namespaceName, String kafkaClusterName, int replicas) {
        return kafkaConnectWithFilePlugin(namespaceName, kafkaClusterName, kafkaClusterName, replicas);
    }

    /**
     * Method for creating the KafkaConnect builder with File plugin - using the KafkaConnect build feature.
     * @param namespaceName namespace, where the KafkaConnect resource will be deployed
     * @param kafkaConnectClusterName Name for the KafkaConnect resource
     * @param kafkaClusterName name of the Kafka cluster
     * @param replicas number of KafkaConnect replicas
     * @return KafkaConnect builder with File plugin
     */
    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String namespaceName, String kafkaConnectClusterName, String kafkaClusterName, int replicas) {
        return addFileSinkPluginOrImage(kafkaConnectBuild(namespaceName, kafkaConnectClusterName, kafkaClusterName, replicas));
    }

    /**
     * Method for adding Connect Build with file-sink plugin to the Connect spec or set Connect's image in case that
     * the image is set in `CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN` env. variable
     * @param kafkaConnectBuilder builder of the Connect resource
     * @return updated Connect resource in builder
     */
    public static KafkaConnectBuilder addFileSinkPluginOrImage(KafkaConnectBuilder kafkaConnectBuilder) {
        if (!KubeClusterResource.getInstance().isMicroShift() && Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN.isEmpty()) {
            final Plugin fileSinkPlugin = new PluginBuilder()
                .withName("file-plugin")
                .withArtifacts(
                    new JarArtifactBuilder()
                        .withUrl(Environment.ST_FILE_PLUGIN_URL)
                        .build()
                )
                .build();


            return kafkaConnectBuilder
                .editOrNewSpec()
                    .editOrNewBuild()
                        .withPlugins(fileSinkPlugin)
                    .endBuild()
                .endSpec();
        } else {
            if (KubeClusterResource.getInstance().isMicroShift()) {
                LOGGER.warn("Using MicroShift cluster - you should have created your own Connect image with file-sink plugin and pass the image into {} env variable", Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN_ENV);
            }

            LOGGER.info("Using {} image from {} env variable", Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN, Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN_ENV);

            return kafkaConnectBuilder
                .editOrNewSpec()
                    .withBuild(null)
                    .withImage(Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN)
                .endSpec();
        }
    }

    public static DockerOutput dockerOutput(String imageName, DockerOutputBuilder dockerOutputBuilder) {
        dockerOutputBuilder.withImage(imageName);

        if (Environment.CONNECT_BUILD_REGISTRY_SECRET != null && !Environment.CONNECT_BUILD_REGISTRY_SECRET.isEmpty()) {
            dockerOutputBuilder.withPushSecret(Environment.CONNECT_BUILD_REGISTRY_SECRET);
        }

        if (Environment.isConnectBuildWithBuildahEnabled() && !KubeClusterResource.getInstance().isOpenShiftLikeCluster()) {
            if (dockerOutputBuilder.getAdditionalBuildOptions() == null || !dockerOutputBuilder.getAdditionalBuildOptions().contains("--tls-verify=false")) {
                // for Buildah on minikube or Kind, we need to add `--tls-verify=false` in order to push via HTTP
                dockerOutputBuilder.addToAdditionalBuildOptions("--tls-verify=false");
            }
            if (dockerOutputBuilder.getAdditionalPushOptions() == null || !dockerOutputBuilder.getAdditionalPushOptions().contains("--tls-verify=false")) {
                dockerOutputBuilder.addToAdditionalPushOptions("--tls-verify=false");
            }
        } else if (!Environment.isConnectBuildWithBuildahEnabled() && KubeClusterResource.getInstance().isKind()) {
            // if we use Kind we add insecure option
            dockerOutputBuilder.addToAdditionalBuildOptions(
                // --insecure for PUSH via HTTP instead of HTTPS
                "--insecure");
        }

        return dockerOutputBuilder.build();
    }
}
