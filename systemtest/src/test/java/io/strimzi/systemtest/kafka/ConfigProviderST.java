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
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("This test suite verifies KafkaConnect using ConfigMap and EnvVar configuration."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator across all namespaces, with custom configuration.", expected = "Cluster Operator is deployed.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class ConfigProviderST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConfigProviderST.class);

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test to ensure Kafka Connect functions correctly using ConfigMap and EnvVar configuration."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Resources are created and are in ready state."),
            @Step(value = "Create Kafka cluster.", expected = "Kafka cluster is ready"),
            @Step(value = "Create ConfigMap for connector configuration.", expected = "ConfigMap with connector configuration is created."),
            @Step(value = "Deploy Kafka Connect with external configuration from ConfigMap.", expected = "KafkaConnect is deployed with proper configuration."),
            @Step(value = "Create necessary Role and RoleBinding for connector.", expected = "Role and RoleBinding are created and applied."),
            @Step(value = "Deploy KafkaConnector.", expected = "KafkaConnector is successfully deployed."),
            @Step(value = "Deploy Kafka clients.", expected = "Kafka clients are deployed and ready."),
            @Step(value = "Send messages and verify they are written to sink file.", expected = "Messages are successfully written to the specified sink file.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testConnectWithConnectorUsingConfigAndEnvProvider() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String customFileSinkPath = "/tmp/my-own-path.txt";

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, String> configData = new HashMap<>();
        configData.put("topics", testStorage.getTopicName());
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

        kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).resource(connectorConfig).create();

        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
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
                .editOrNewTemplate()
                    .editOrNewConnectContainer()
                        .addNewEnv()
                            .withName("FILE_SINK_TOPICS")
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("topics", cmName, false)
                            .endValueFrom()
                        .endEnv()
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .build());

        LOGGER.info("Creating needed RoleBinding and Role for Kubernetes Config Provider");

        KubeResourceManager.get().createResourceWithWait(
            new RoleBindingBuilder()
                .editOrNewMetadata()
                    .withName("connector-config-rb")
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withSubjects(
                    new SubjectBuilder()
                        .withKind("ServiceAccount")
                        .withName(testStorage.getClusterName() + "-connect")
                        .withNamespace(testStorage.getNamespaceName())
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
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addNewRule()
                .withApiGroups("")
                .withResources("configmaps")
                .withResourceNames(cmName)
                .withVerbs("get")
            .endRule()
            .build();

        kubeClient().namespace(testStorage.getNamespaceName()).createOrUpdateRole(configRole);

        String configPrefix = "configmaps:" + testStorage.getNamespaceName() + "/connector-config:";

        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", "${env:FILE_SINK_FILE}")
                .addToConfig("key.converter", "${" + configPrefix + "key}")
                .addToConfig("value.converter", "${" + configPrefix + "value}")
                .addToConfig("topics", "${env:FILE_SINK_TOPICS}")
            .endSpec()
            .build());

        final KafkaClients kafkaBasicClientJob = ClientUtils.getInstantPlainClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(kafkaBasicClientJob.producerStrimzi());

        String kafkaConnectPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, customFileSinkPath, testStorage.getMessageCount());
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
