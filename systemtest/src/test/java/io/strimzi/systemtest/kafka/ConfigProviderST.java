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
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
public class ConfigProviderST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConfigProviderST.class);
    private static final String NAMESPACE = "config-provider-test";

    @ParallelNamespaceTest
    void testConnectWithConnectorUsingConfigProvider(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        final String producerName = "producer-" + ClientUtils.generateRandomConsumerGroup();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("config.providers", "configmaps")
                .addToConfig("config.providers.configmaps.class", "io.strimzi.kafka.KubernetesConfigMapConfigProvider")
            .endSpec()
            .build());

        Map<String, String> configData = new HashMap<>();
        configData.put("topics", topicName);
        configData.put("file", Constants.DEFAULT_SINK_FILE_PATH);
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

        kubeClient().getClient().configMaps().inNamespace(namespaceName).create(connectorConfig);

        LOGGER.info("Creating needed RoleBinding and Role for Kubernetes Config Provider");

        RoleBindingResource.createRoleBinding(
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
                .build(), namespaceName);

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

        kubeClient().getClient().resource(configRole).createOrReplace();

        String configPrefix = "configmaps:" + namespaceName + "/connector-config:";

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", "${" + configPrefix + "file}")
                .addToConfig("key.converter", "${" + configPrefix + "key}")
                .addToConfig("value.converter", "${" + configPrefix + "value}")
                .addToConfig("topics", "${" + configPrefix + "topics}")
            .endSpec()
            .build());

        KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBasicExampleClients.Builder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(0)
            .withNamespaceName(namespaceName)
            .build();

        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerStrimzi().build());

        String kafkaConnectPodName = kubeClient().listPods(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(namespaceName, kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "Hello-world - 99");
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .createInstallation()
            .runInstallation();
    }
}
