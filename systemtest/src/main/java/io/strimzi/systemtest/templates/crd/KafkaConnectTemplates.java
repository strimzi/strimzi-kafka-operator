/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.connect.build.DockerOutput;
import io.strimzi.api.kafka.model.connect.build.DockerOutputBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectTemplates {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectTemplates.class);

    private KafkaConnectTemplates() {}

    public static KafkaConnectBuilder kafkaConnect(String name, final String namespaceName, String clusterName, int kafkaConnectReplicas, String pathToConnectConfig) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(pathToConnectConfig);
        return defaultKafkaConnect(kafkaConnect, namespaceName, name, clusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnect(String name, final String namespaceName, String clusterName, int kafkaConnectReplicas) {
        return kafkaConnect(name, namespaceName, clusterName, kafkaConnectReplicas, Constants.PATH_TO_KAFKA_CONNECT_CONFIG);
    }

    public static KafkaConnectBuilder kafkaConnect(String name, final String namespaceName, int kafkaConnectReplicas) {
        return kafkaConnect(name, namespaceName, name, kafkaConnectReplicas, Constants.PATH_TO_KAFKA_CONNECT_CONFIG);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetrics(String name, String namespaceName, int kafkaConnectReplicas) {
        return kafkaConnectWithMetrics(name, namespaceName, name, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetrics(String name, String namespaceName, String clusterName, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(Constants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG);
        createOrReplaceConnectMetrics(namespaceName);
        return defaultKafkaConnect(kafkaConnect, namespaceName, name, clusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetricsAndFileSinkPlugin(String name, String namespaceName, String clusterName, int replicas) {
        createOrReplaceConnectMetrics(namespaceName);
        return kafkaConnectWithFilePlugin(name, namespaceName, clusterName, replicas, Constants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG);
    }

    private static void createOrReplaceConnectMetrics(String namespaceName) {
        ConfigMap metricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG, "connect-metrics");
        kubeClient().createConfigMapInNamespace(namespaceName, metricsCm);
    }

    private static KafkaConnectBuilder defaultKafkaConnect(KafkaConnect kafkaConnect, final String namespaceName, String name, String kafkaClusterName, int kafkaConnectReplicas) {
        return new KafkaConnectBuilder(kafkaConnect)
            .withNewMetadata()
                .withName(name)
                .withNamespace(namespaceName)
            .endMetadata()
            .editOrNewSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterName))
                .withReplicas(kafkaConnectReplicas)
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName(kafkaClusterName + "-cluster-ca-cert").withCertificate("ca.crt").build())
                .endTls()
                .addToConfig("group.id", KafkaConnectResources.deploymentName(name))
                .addToConfig("offset.storage.topic", KafkaConnectResources.configStorageTopicOffsets(name))
                .addToConfig("config.storage.topic", KafkaConnectResources.metricsAndLogConfigMapName(name))
                .addToConfig("status.storage.topic", KafkaConnectResources.configStorageTopicStatus(name))
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String clusterName, String namespaceName, int replicas) {
        return kafkaConnectWithFilePlugin(clusterName, namespaceName, clusterName, replicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String name, String namespaceName, String clusterName, int replicas) {
        return kafkaConnectWithFilePlugin(name, namespaceName, clusterName, replicas, Constants.PATH_TO_KAFKA_CONNECT_CONFIG);
    }

    /**
     * Method for creating the KafkaConnect builder with File plugin - using the KafkaConnect build feature.
     * @param name Name for the KafkaConnect resource
     * @param namespaceName namespace, where the KafkaConnect resource will be deployed
     * @param clusterName name of the Kafka cluster
     * @param replicas number of KafkaConnect replicas
     * @return KafkaConnect builder with File plugin
     */
    @SuppressFBWarnings("DMI_RANDOM_USED_ONLY_ONCE")
    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String name, String namespaceName, String clusterName, int replicas, String pathToConnectConfig) {
        final Plugin fileSinkPlugin = new PluginBuilder()
            .withName("file-plugin")
            .withArtifacts(
                new JarArtifactBuilder()
                    .withUrl(Environment.ST_FILE_PLUGIN_URL)
                    .build()
            )
            .build();

        final String imageFullPath = Environment.getImageOutputRegistry(namespaceName, Constants.ST_CONNECT_BUILD_IMAGE_NAME, String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));

        KafkaConnectBuilder kafkaConnectBuilder = kafkaConnect(name, namespaceName, clusterName, replicas, pathToConnectConfig)
            .editOrNewSpec()
                .editOrNewBuild()
                    .withPlugins(fileSinkPlugin)
                    .withOutput(dockerOutput(imageFullPath))
                .endBuild()
            .endSpec();

        return kafkaConnectBuilder;
    }

    public static DockerOutput dockerOutput(String imageName) {
        DockerOutputBuilder dockerOutputBuilder = new DockerOutputBuilder().withImage(imageName);
        if (Environment.CONNECT_BUILD_REGISTRY_SECRET != null && !Environment.CONNECT_BUILD_REGISTRY_SECRET.isEmpty()) {
            dockerOutputBuilder.withPushSecret(Environment.CONNECT_BUILD_REGISTRY_SECRET);
        }

        // if we use Kind we add insecure option
        if (KubeClusterResource.getInstance().isKind()) {
            dockerOutputBuilder.withAdditionalKanikoOptions(
                // --insecure for PUSH via HTTP instead of HTTPS
                "--insecure");
        }

        return dockerOutputBuilder.build();
    }

    private static KafkaConnect getKafkaConnectFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnect.class);
    }
}
