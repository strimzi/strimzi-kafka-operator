/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
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

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false)
            .withNewSpec()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(clusterName))
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("config.providers", "secrets,configmap")
                .addToConfig("config.providers.secrets.class", "io.strimzi.kafka.KubernetesSecretConfigProvider")
                .addToConfig("config.providers.secrets.class", "io.strimzi.kafka.KubernetesSecretConfigProvider")
            .endSpec()
            .build());

        Map<String, String> configData = new HashMap<>();
        configData.put("topics", topicName);
        configData.put("file", Constants.DEFAULT_SINK_FILE_PATH);
        configData.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configData.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        ConfigMap connectorConfig = new ConfigMapBuilder()
            .editOrNewMetadata()
                .withName("connector-config")
            .endMetadata()
            .withData(configData)
            .build();

        kubeClient().getClient().configMaps().inNamespace(namespaceName).create(connectorConfig);

        RoleBindingResource.createRoleBinding(
            RoleBindingResource.roleBinding(namespaceName)
                .editOrNewMetadata()
                    .withName("connector-config-rb")
                .endMetadata()
                .editFirstSubject()
                    .withKind("ServiceAccount")
                    .withName(KafkaConnectResources.serviceName(clusterName))
                    .withNamespace(namespaceName)
                .endSubject()
                .withRoleRef(
                    new RoleRefBuilder()
                        .withKind("Role")
                        .withName("connector-config-role")
                        .withApiGroup("rbac.authorization.k8s.io")
                        .build())
                .build(), namespaceName);

        // create a role
        Role configRole = new RoleBuilder()
            .editOrNewMetadata()
                .withName("connector-config-role")
                .withNamespace(namespaceName)
            .endMetadata()
            .editFirstRule()
                .withApiGroups("")
                .withResources("configmaps")
                .withResourceNames("connector-configuration")
                .withVerbs("get")
            .endRule()
            .build();

        kubeClient().getClient().resource(configRole).createOrReplace();
        LOGGER.info("asd");
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
