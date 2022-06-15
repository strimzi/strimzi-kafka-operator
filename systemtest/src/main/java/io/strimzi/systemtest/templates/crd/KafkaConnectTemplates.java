/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;

import java.util.Random;

import static io.strimzi.operator.common.Util.hashStub;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class KafkaConnectTemplates {

    private KafkaConnectTemplates() {}

    public static KafkaConnectBuilder kafkaConnect(String name, final String namespaceName, String clusterName, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(Constants.PATH_TO_KAFKA_CONNECT_CONFIG);
        return defaultKafkaConnect(kafkaConnect, namespaceName, name, clusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnect(String name, int kafkaConnectReplicas) {
        return kafkaConnect(name, ResourceManager.kubeClient().getNamespace(), name, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnect(String name, String clusterName, int kafkaConnectReplicas) {
        return kafkaConnect(name, ResourceManager.kubeClient().getNamespace(), clusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetrics(String name, int kafkaConnectReplicas) {
        return kafkaConnectWithMetrics(name, name, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetrics(String name, String clusterName, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(Constants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG);
        ConfigMap metricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG, "connect-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).resource(metricsCm).createOrReplace();
        return defaultKafkaConnect(kafkaConnect, ResourceManager.kubeClient().getNamespace(), name, clusterName, kafkaConnectReplicas);
    }

    private static KafkaConnectBuilder defaultKafkaConnect(KafkaConnect kafkaConnect, final String namespaceName, String name, String kafkaClusterName, int kafkaConnectReplicas) {
        return new KafkaConnectBuilder(kafkaConnect)
            .withNewMetadata()
                .withName(name)
                .withNamespace(namespaceName)
                .withClusterName(kafkaClusterName)
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

    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String namespaceName, String clusterName, int replicas) {
        return kafkaConnectWithFilePlugin(clusterName, namespaceName, clusterName, replicas);
    }

    /**
     * Method for creating the KafkaConnect builder with File plugin - using the KafkaConnect build feature.
     * @param name Name for the KafkaConnect resource
     * @param namespaceName namespace, where the KafkaConnect resource will be deployed
     * @param clusterName name of the Kafka cluster
     * @param replicas number of KafkaConnect replicas
     * @return KafkaConnect builder with File plugin
     */
    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String name, String namespaceName, String clusterName, int replicas) {
        final Plugin fileSinkPlugin = new PluginBuilder()
            .withName("file-plugin")
            .withArtifacts(
                new JarArtifactBuilder()
                    .withUrl(Environment.ST_FILE_PLUGIN_URL)
                    .build()
            )
            .build();

        final String imageName = Environment.getImageOutputRegistry() + "/" + namespaceName + "/connect-" + hashStub(String.valueOf(new Random().nextInt(Integer.MAX_VALUE))) + ":latest";

        return kafkaConnect(name, namespaceName, clusterName, replicas)
            .editOrNewSpec()
                .editOrNewBuild()
                    .withPlugins(fileSinkPlugin)
                    .withNewDockerOutput()
                        .withImage(imageName)
                    .endDockerOutput()
                .endBuild()
            .endSpec();
    }

    private static KafkaConnect getKafkaConnectFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnect.class);
    }
}
