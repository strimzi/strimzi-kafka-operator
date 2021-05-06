/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@ParallelSuite
public class CruiseControlConfigurationST extends CruiseControlBaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlConfigurationST.class);
    private static final String NAMESPACE = "cruise-control-configuration-test";

    @ParallelNamespaceTest
    void testCapacityFile(ExtensionContext extensionContext) {
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        String cruiseControlPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0).getMetadata().getName();

        JsonObject cruiseControlCapacityFileContent =
            new JsonObject(cmdKubeClient(namespaceName).execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + Constants.CRUISE_CONTROL_CAPACITY_FILE_PATH).out());

        assertThat(cruiseControlCapacityFileContent.getJsonArray("brokerCapacities"), not(nullValue()));

        LOGGER.info("We got only one configuration of broker-capacities");
        assertThat(cruiseControlCapacityFileContent.getJsonArray("brokerCapacities").size(), is(1));

        LOGGER.info("Verifying cruise control configuration.");

        JsonObject cruiseControlFirstConfiguration = cruiseControlCapacityFileContent.getJsonArray("brokerCapacities").getJsonObject(0);

        assertThat(cruiseControlFirstConfiguration.getString("brokerId"), is("-1"));
        assertThat(cruiseControlFirstConfiguration.getString("doc"), not(nullValue()));

        JsonObject cruiseControlConfigurationOfBrokerCapacity = cruiseControlFirstConfiguration.getJsonObject("capacity");

        LOGGER.info("Verifying default cruise control capacities");

        assertThat(cruiseControlConfigurationOfBrokerCapacity.getString("DISK"), is("100000.0"));
        assertThat(cruiseControlConfigurationOfBrokerCapacity.getString("CPU"), is("100"));
        assertThat(cruiseControlConfigurationOfBrokerCapacity.getString("NW_IN"), is("10000.0"));
        assertThat(cruiseControlConfigurationOfBrokerCapacity.getString("NW_OUT"), is("10000.0"));
    }

    @ParallelNamespaceTest
    void testDeployAndUnDeployCruiseControl(ExtensionContext extensionContext) throws IOException {
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Removing Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(null);
        }, namespaceName);

        StatefulSetUtils.waitTillSsHasRolled(namespaceName, kafkaStatefulSetName(clusterName), 3, kafkaPods);

        LOGGER.info("Verifying that in {} is not present in the Kafka cluster", Constants.CRUISE_CONTROL_NAME);
        assertThat(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getCruiseControl(), nullValue());

        LOGGER.info("Verifying that {} pod is not present", clusterName + "-cruise-control-");
        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, clusterName + "-cruise-control-", 0);

        LOGGER.info("Verifying that in Kafka config map there is no configuration to cruise control metric reporter");
        assertThrows(WaitException.class, () -> CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(namespaceName, clusterName)));

        LOGGER.info("Cruise Control topics will not be deleted and will stay in the Kafka cluster");
        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);

        kafkaPods = StatefulSetUtils.ssSnapshot(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Adding Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(new CruiseControlSpec());
        }, namespaceName);

        StatefulSetUtils.waitTillSsHasRolled(namespaceName, kafkaStatefulSetName(clusterName), 3, kafkaPods);

        LOGGER.info("Verifying that in Kafka config map there is configuration to cruise control metric reporter");
        CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(namespaceName, clusterName));

        LOGGER.info("Verifying that {} topics are created after CC is instantiated.", Constants.CRUISE_CONTROL_NAME);

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);
    }

    @ParallelNamespaceTest
    void testConfigurationDiskChangeDoNotTriggersRollingUpdateOfKafkaPods(ExtensionContext extensionContext) {
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Map<String, String> kafkaSnapShot = StatefulSetUtils.ssSnapshot(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName));
        Map<String, String> cruiseControlSnapShot = DeploymentUtils.depSnapshot(namespaceName, CruiseControlResources.deploymentName(clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {

            LOGGER.info("Changing the broker capacity of the cruise control");

            CruiseControlSpec cruiseControl = new CruiseControlSpecBuilder()
                .withNewBrokerCapacity()
                    .withNewDisk("200M")
                .endBrokerCapacity()
                .build();

            kafka.getSpec().setCruiseControl(cruiseControl);
        }, namespaceName);

        LOGGER.info("Verifying that CC pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(namespaceName, CruiseControlResources.deploymentName(clusterName), 1, cruiseControlSnapShot);

        LOGGER.info("Verifying that Kafka pods did not roll");
        StatefulSetUtils.waitForNoRollingUpdate(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), kafkaSnapShot);

        LOGGER.info("Verifying new configuration in the Kafka CR");

        assertThat(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec()
            .getCruiseControl().getBrokerCapacity().getDisk(), is("200M"));

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);
    }

    @ParallelNamespaceTest
    void testConfigurationReflection(ExtensionContext extensionContext) throws IOException {
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Pod cruiseControlPod = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0);

        String cruiseControlPodName = cruiseControlPod.getMetadata().getName();

        String configurationFileContent = cmdKubeClient(namespaceName).execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

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
            if (envVar.getName().equals(Constants.CRUISE_CONTROL_CONFIGURATION_ENV)) {
                cruiseControlConfiguration = envVar;
            }
        }

        InputStream configurationContainerStream = new ByteArrayInputStream(Objects.requireNonNull(cruiseControlConfiguration).getValue().getBytes(StandardCharsets.UTF_8));

        Properties containerConfiguration = new Properties();
        containerConfiguration.load(configurationContainerStream);

        LOGGER.info("Verifying that all configuration in the cruise control container matching the cruise control file {} properties", Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH);
        List<String> checkCCProperties = Arrays.asList(
                CruiseControlConfigurationParameters.PARTITION_METRICS_WINDOWS.getValue(),
                CruiseControlConfigurationParameters.PARTITION_METRICS_WINDOW_MS.getValue(),
                CruiseControlConfigurationParameters.BROKER_METRICS_WINDOWS.getValue(),
                CruiseControlConfigurationParameters.BROKER_METRICS_WINDOW_MS.getValue(),
                CruiseControlConfigurationParameters.COMPLETED_USER_TASK_RETENTION_MS.getValue(),
                "goals", "default.goals");

        for (String propertyName : checkCCProperties) {
            assertThat(containerConfiguration.stringPropertyNames(), hasItem(propertyName));
            assertThat(containerConfiguration.getProperty(propertyName), is(fileConfiguration.getProperty(propertyName)));
        }
    }

    @ParallelNamespaceTest
    void testConfigurationFileIsCreated(ExtensionContext extensionContext) {
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        String cruiseControlPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0).getMetadata().getName();

        String cruiseControlConfigurationFileContent = cmdKubeClient(namespaceName).execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

        assertThat(cruiseControlConfigurationFileContent, not(nullValue()));
    }

    @ParallelNamespaceTest
    void testConfigurationPerformanceOptions(ExtensionContext extensionContext) throws IOException {
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Container cruiseControlContainer;
        EnvVar cruiseControlConfiguration;

        Map<String, String> kafkaSnapShot = StatefulSetUtils.ssSnapshot(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName));
        Map<String, String> cruiseControlSnapShot = DeploymentUtils.depSnapshot(namespaceName, CruiseControlResources.deploymentName(clusterName));
        Map<String, Object> performanceTuningOpts = new HashMap<String, Object>() {{
                put(CruiseControlConfigurationParameters.CONCURRENT_INTRA_PARTITION_MOVEMENTS.getValue(), 2);
                put(CruiseControlConfigurationParameters.CONCURRENT_PARTITION_MOVEMENTS.getValue(), 5);
                put(CruiseControlConfigurationParameters.CONCURRENT_LEADER_MOVEMENTS.getValue(), 1000);
                put(CruiseControlConfigurationParameters.REPLICATION_THROTTLE.getValue(), -1);
            }};

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Changing cruise control performance tuning options");
            kafka.getSpec().setCruiseControl(new CruiseControlSpecBuilder()
                    .addToConfig(performanceTuningOpts)
                    .build());
        }, namespaceName);

        LOGGER.info("Verifying that CC pod is rolling, after changing options");
        DeploymentUtils.waitTillDepHasRolled(namespaceName, CruiseControlResources.deploymentName(clusterName), 1, cruiseControlSnapShot);

        LOGGER.info("Verifying that Kafka pods did not roll");
        StatefulSetUtils.waitForNoRollingUpdate(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), kafkaSnapShot);

        LOGGER.info("Verifying new configuration in the Kafka CR");
        Pod cruiseControlPod = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0);

        // Get CruiseControl resource properties
        cruiseControlContainer = cruiseControlPod.getSpec().getContainers().stream()
                .filter(container -> container.getName().equals(Constants.CRUISE_CONTROL_CONTAINER_NAME))
                .findFirst().orElse(null);

        cruiseControlConfiguration = Objects.requireNonNull(cruiseControlContainer).getEnv().stream()
                .filter(envVar -> envVar.getName().equals(Constants.CRUISE_CONTROL_CONFIGURATION_ENV))
                .findFirst().orElse(null);

        InputStream configurationContainerStream = new ByteArrayInputStream(
                Objects.requireNonNull(cruiseControlConfiguration).getValue().getBytes(StandardCharsets.UTF_8));
        Properties containerConfiguration = new Properties();
        containerConfiguration.load(configurationContainerStream);

        LOGGER.info("Verifying Cruise control performance options are set in Kafka CR");
        performanceTuningOpts.forEach((key, value) ->
                assertThat(containerConfiguration, hasEntry(key, value.toString())));
    }
}
