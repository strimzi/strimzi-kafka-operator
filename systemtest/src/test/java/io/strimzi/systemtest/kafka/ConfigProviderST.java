/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
public class ConfigProviderST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConfigProviderST.class);

    @ParallelNamespaceTest
    void testConnectWithConnectorUsingConfigAndEnvProvider(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String clusterName = testStorage.getClusterName();
        final String topicName = testStorage.getTopicName();
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String producerName = "producer-" + ClientUtils.generateRandomConsumerGroup();
        final String customFileSinkPath = "/tmp/my-own-path.txt";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        Map<String, String> configData = new HashMap<>();
        configData.put("topics", topicName);
        configData.put("file", customFileSinkPath);
        configData.put("key", "org.apache.kafka.connect.storage.StringConverter");
        configData.put("value", "org.apache.kafka.connect.storage.StringConverter");

        String cmName = "connector-config";
        String configRoleName = "connector-config-role";

        ConfigMap connectorConfig = new ConfigMapBuilder()
            .editOrNewMetadata()
                .withName(cmName)
            .endMetadata()
            .withData(configData)
            .build();

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(connectorConfig).create();

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(clusterName, namespaceName, 1)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("config.providers", "configmaps,env")
                .addToConfig("config.providers.configmaps.class", "io.strimzi.kafka.KubernetesConfigMapConfigProvider")
                .addToConfig("config.providers.env.class", "org.apache.kafka.common.config.provider.EnvVarConfigProvider")
                .editOrNewExternalConfiguration()
                    .addNewEnv()
                .withName("FILE_SINK_FILE")
                .withNewValueFrom()
                    .withNewConfigMapKeyRef("file", cmName, false)
                .endValueFrom()
                    .endEnv()
                .endExternalConfiguration()
            .endSpec()
            .build());

        LOGGER.info("Creating needed RoleBinding and Role for Kubernetes Config Provider");

        ResourceManager.getInstance().createResourceWithWait(extensionContext,
            new RoleBindingBuilder()
                .editOrNewMetadata()
                    .withName("connector-config-rb")
                    .withNamespace(namespaceName)
                .endMetadata()
                .withSubjects(
                    new SubjectBuilder()
                        .withKind("ServiceAccount")
                        .withName(clusterName + "-connect")
                        .withNamespace(namespaceName)
                        .build()
                )
                .withRoleRef(
                    new RoleRefBuilder()
                        .withKind("Role")
                        .withName(configRoleName)
                        .withApiGroup("rbac.authorization.k8s.io")
                        .build())
                .build());

        // create a role
        Role configRole = new RoleBuilder()
            .editOrNewMetadata()
                .withName(configRoleName)
                .withNamespace(namespaceName)
            .endMetadata()
            .addNewRule()
                .withApiGroups("")
                .withResources("configmaps")
                .withResourceNames(cmName)
                .withVerbs("get")
            .endRule()
            .build();

        kubeClient().namespace(namespaceName).createOrUpdateRole(configRole);

        String configPrefix = "configmaps:" + namespaceName + "/connector-config:";

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", "${env:FILE_SINK_FILE}")
                .addToConfig("key.converter", "${" + configPrefix + "key}")
                .addToConfig("value.converter", "${" + configPrefix + "value}")
                .addToConfig("topics", "${" + configPrefix + "topics}")
            .endSpec()
            .build());

        KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(0)
            .withNamespaceName(namespaceName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaBasicClientJob.producerStrimzi());

        String kafkaConnectPodName = kubeClient().listPods(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(namespaceName, kafkaConnectPodName, customFileSinkPath, "Hello-world - 99");
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation(extensionContext)
                .createInstallation()
                .runInstallation();
    }
}
