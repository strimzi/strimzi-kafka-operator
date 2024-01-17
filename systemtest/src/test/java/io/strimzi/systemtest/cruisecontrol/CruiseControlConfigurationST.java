/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpecBuilder;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.KRaftWithoutUTONotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
public class CruiseControlConfigurationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlConfigurationST.class);

    @ParallelNamespaceTest
    void testDeployAndUnDeployCruiseControl(ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaComponentName(clusterName));

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Removing CruiseControl from Kafka");
            kafka.getSpec().setCruiseControl(null);
        }, namespaceName);

        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);

        LOGGER.info("Verifying that in {} is not present in the Kafka cluster", TestConstants.CRUISE_CONTROL_NAME);
        assertThat(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getCruiseControl(), nullValue());

        LOGGER.info("Verifying that {} Pod is not present", clusterName + "-cruise-control-");
        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, clusterName + "-cruise-control-", 0);

        LOGGER.info("Verifying that there is no configuration to CruiseControl metric reporter in Kafka ConfigMap");
        assertThrows(WaitException.class, () -> CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(namespaceName, clusterName)));

        // https://github.com/strimzi/strimzi-kafka-operator/issues/8864
        if (!Environment.isKRaftModeEnabled() && !Environment.isUnidirectionalTopicOperatorEnabled()) {
            LOGGER.info("Cruise Control Topics will not be deleted and will stay in the Kafka cluster");
            CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Adding CruiseControl to the classic Kafka");
            kafka.getSpec().setCruiseControl(new CruiseControlSpec());
        }, namespaceName);

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);

        LOGGER.info("Verifying that configuration of CruiseControl metric reporter is present in Kafka ConfigMap");
        CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(namespaceName, clusterName));

        // https://github.com/strimzi/strimzi-kafka-operator/issues/8864
        if (!Environment.isKRaftModeEnabled() && !Environment.isUnidirectionalTopicOperatorEnabled()) {
            LOGGER.info("Verifying that {} Topics are created after CC is instantiated", TestConstants.CRUISE_CONTROL_NAME);

            CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);
        }
    }

    @ParallelNamespaceTest
    @KRaftWithoutUTONotSupported
    void testConfigurationDiskChangeDoNotTriggersRollingUpdateOfKafkaPods(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaComponentName(clusterName));

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Map<String, String> kafkaSnapShot = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> cruiseControlSnapShot = DeploymentUtils.depSnapshot(namespaceName, CruiseControlResources.componentName(clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {

            LOGGER.info("Changing the Broker capacity of the CruiseControl");

            CruiseControlSpec cruiseControl = new CruiseControlSpecBuilder()
                .withNewBrokerCapacity()
                    .withOutboundNetwork("20KB/s")
                .endBrokerCapacity()
                .build();

            kafka.getSpec().setCruiseControl(cruiseControl);
        }, namespaceName);

        LOGGER.info("Verifying that CC Pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(namespaceName, CruiseControlResources.componentName(clusterName), 1, cruiseControlSnapShot);

        LOGGER.info("Verifying that Kafka Pods did not roll");
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaSnapShot);

        LOGGER.info("Verifying new configuration in the Kafka CR");

        assertThat(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec()
            .getCruiseControl().getBrokerCapacity().getOutboundNetwork(), is("20KB/s"));

        // https://github.com/strimzi/strimzi-kafka-operator/issues/8864
        if (!Environment.isUnidirectionalTopicOperatorEnabled()) {
            CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);
        }
    }

    @ParallelNamespaceTest
    void testConfigurationReflection(ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Pod cruiseControlPod = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0);

        String cruiseControlPodName = cruiseControlPod.getMetadata().getName();

        String configurationFileContent = cmdKubeClient(namespaceName).execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

        InputStream configurationFileStream = new ByteArrayInputStream(configurationFileContent.getBytes(StandardCharsets.UTF_8));

        Properties fileConfiguration = new Properties();
        fileConfiguration.load(configurationFileStream);

        Container cruiseControlContainer = null;

        for (Container container : cruiseControlPod.getSpec().getContainers()) {
            if (container.getName().equals("cruise-control")) {
                cruiseControlContainer = container;
            }
        }

        EnvVar cruiseControlConfiguration = null;

        for (EnvVar envVar : Objects.requireNonNull(cruiseControlContainer).getEnv()) {
            if (envVar.getName().equals(TestConstants.CRUISE_CONTROL_CONFIGURATION_ENV)) {
                cruiseControlConfiguration = envVar;
            }
        }

        InputStream configurationContainerStream = new ByteArrayInputStream(Objects.requireNonNull(cruiseControlConfiguration).getValue().getBytes(StandardCharsets.UTF_8));

        Properties containerConfiguration = new Properties();
        containerConfiguration.load(configurationContainerStream);

        LOGGER.info("Verifying that all configuration in the CruiseControl container matching the CruiseControl file {} properties", TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH);
        List<String> checkCCProperties = Arrays.asList(
                CruiseControlConfigurationParameters.PARTITION_METRICS_WINDOW_NUM_CONFIG_KEY.getValue(),
                CruiseControlConfigurationParameters.PARTITION_METRICS_WINDOW_MS_CONFIG_KEY.getValue(),
                CruiseControlConfigurationParameters.BROKER_METRICS_WINDOW_NUM_CONFIG_KEY.getValue(),
                CruiseControlConfigurationParameters.BROKER_METRICS_WINDOW_MS_CONFIG_KEY.getValue(),
                CruiseControlConfigurationParameters.COMPLETED_USER_TASK_RETENTION_MS_CONFIG_KEY.getValue(),
                "goals", "default.goals");

        for (String propertyName : checkCCProperties) {
            assertThat(containerConfiguration.stringPropertyNames(), hasItem(propertyName));
            assertThat(containerConfiguration.getProperty(propertyName), is(fileConfiguration.getProperty(propertyName)));
        }
    }

    @ParallelNamespaceTest
    void testConfigurationFileIsCreated(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        String cruiseControlPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0).getMetadata().getName();

        String cruiseControlConfigurationFileContent = cmdKubeClient(namespaceName).execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

        assertThat(cruiseControlConfigurationFileContent, not(nullValue()));
    }

    @ParallelNamespaceTest
    void testConfigurationPerformanceOptions(ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaComponentName(clusterName));

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Container cruiseControlContainer;
        EnvVar cruiseControlConfiguration;

        Map<String, String> kafkaSnapShot = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> cruiseControlSnapShot = DeploymentUtils.depSnapshot(namespaceName, CruiseControlResources.componentName(clusterName));
        Map<String, Object> performanceTuningOpts = new HashMap<String, Object>() {{
                put(CruiseControlConfigurationParameters.CONCURRENT_INTRA_PARTITION_MOVEMENTS.getValue(), 2);
                put(CruiseControlConfigurationParameters.CONCURRENT_PARTITION_MOVEMENTS.getValue(), 5);
                put(CruiseControlConfigurationParameters.CONCURRENT_LEADER_MOVEMENTS.getValue(), 1000);
                put(CruiseControlConfigurationParameters.REPLICATION_THROTTLE.getValue(), -1);
            }};

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Changing CruiseControl performance tuning options");
            kafka.getSpec().setCruiseControl(new CruiseControlSpecBuilder()
                    .addToConfig(performanceTuningOpts)
                    .build());
        }, namespaceName);

        LOGGER.info("Verifying that CC Pod is rolling, after changing options");
        DeploymentUtils.waitTillDepHasRolled(namespaceName, CruiseControlResources.componentName(clusterName), 1, cruiseControlSnapShot);

        LOGGER.info("Verifying that Kafka Pods did not roll");
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaSnapShot);

        LOGGER.info("Verifying new configuration in the Kafka CR");
        Pod cruiseControlPod = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0);

        // Get CruiseControl resource properties
        cruiseControlContainer = cruiseControlPod.getSpec().getContainers().stream()
                .filter(container -> container.getName().equals(TestConstants.CRUISE_CONTROL_CONTAINER_NAME))
                .findFirst().orElse(null);

        cruiseControlConfiguration = Objects.requireNonNull(cruiseControlContainer).getEnv().stream()
                .filter(envVar -> envVar.getName().equals(TestConstants.CRUISE_CONTROL_CONFIGURATION_ENV))
                .findFirst().orElse(null);

        InputStream configurationContainerStream = new ByteArrayInputStream(
                Objects.requireNonNull(cruiseControlConfiguration).getValue().getBytes(StandardCharsets.UTF_8));
        Properties containerConfiguration = new Properties();
        containerConfiguration.load(configurationContainerStream);

        LOGGER.info("Verifying CruiseControl performance options are set in Kafka CR");
        performanceTuningOpts.forEach((key, value) ->
                assertThat(containerConfiguration, hasEntry(key, value.toString())));
    }

    @BeforeAll
    void setUp(final ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation(extensionContext)
                .createInstallation()
                .runInstallation();
    }
}
