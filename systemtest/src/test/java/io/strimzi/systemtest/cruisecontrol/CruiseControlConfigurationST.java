/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpecBuilder;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@SuiteDoc(
    description = @Desc("This test suite, verify configuration of the Cruise Control component."),
    beforeTestSteps = {
        @Step(value = "Set up the Cluster Operator", expected = "Cluster Operator is installed and running")
    },
    labels = {
        @Label(value = TestDocsLabels.CRUISE_CONTROL)
    }
)
public class CruiseControlConfigurationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlConfigurationST.class);

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Deploy and subsequently remove Cruise Control from Kafka cluster to verify system stability and correctness of configuration management."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Deploy Kafka with Cruise Control", expected = "Kafka cluster with Cruise Control is deployed"),
            @Step(value = "Take a snapshot of broker pods", expected = "Snapshot of the current broker pods is taken"),
            @Step(value = "Remove Cruise Control from Kafka", expected = "Cruise Control is removed from Kafka and configuration is updated"),
            @Step(value = "Verify Cruise Control is removed", expected = "No Cruise Control related pods or configurations are found"),
            @Step(value = "Create Admin client to verify Cruise Control topics", expected = "Admin client is created and Cruise Control topics are verified to exist"),
            @Step(value = "Re-add Cruise Control to Kafka", expected = "Cruise Control is added back to Kafka"),
            @Step(value = "Verify Cruise Control and related configurations", expected = "Cruise Control and its configurations are verified to be present")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testDeployAndUnDeployCruiseControl() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // number of brokers to be created and also number of default replica count for each topic created
        final int defaultBrokerReplicaCount = 3;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), defaultBrokerReplicaCount).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), defaultBrokerReplicaCount).build()
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), defaultBrokerReplicaCount)
                .editSpec()
                    .editOrNewKafka()
                        .addToConfig(Map.of("default.replication.factor", defaultBrokerReplicaCount))
                    .endKafka()
                .endSpec()
            .build()
        );

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            LOGGER.info("Removing CruiseControl from Kafka");
            kafka.getSpec().setCruiseControl(null);
        });

        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), defaultBrokerReplicaCount, brokerPods);

        LOGGER.info("Verifying that in {} is not present in the Kafka cluster", TestConstants.CRUISE_CONTROL_NAME);
        assertThat(KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getCruiseControl(), nullValue());

        LOGGER.info("Verifying that {} Pod is not present", testStorage.getClusterName() + "-cruise-control-");
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cruise-control-", 0);

        LOGGER.info("Verifying that there is no configuration to CruiseControl metric reporter in Kafka ConfigMap");
        assertThrows(WaitException.class, () -> CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName())));

        resourceManager.createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(testStorage.getClusterName())
            ).build()
        );
        final AdminClient adminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());

        LOGGER.info("Cruise Control Topics will not be deleted and will stay in the Kafka cluster");
        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(adminClient, defaultBrokerReplicaCount);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            LOGGER.info("Adding CruiseControl to the classic Kafka");
            kafka.getSpec().setCruiseControl(new CruiseControlSpec());
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), defaultBrokerReplicaCount, brokerPods);

        LOGGER.info("Verifying that configuration of CruiseControl metric reporter is present in Kafka ConfigMap");
        CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName()));

        LOGGER.info("Verifying that {} Topics are created after CC is instantiated", TestConstants.CRUISE_CONTROL_NAME);
        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(adminClient, defaultBrokerReplicaCount);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying configuration update for Cruise Control and ensuring Kafka Pods did not roll unnecessarily."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create and wait for Kafka with Cruise Control", expected = "Kafka and Cruise Control are deployed successfully"),
            @Step(value = "Take initial snapshots of Kafka and Cruise Control deployments", expected = "Snapshots of current deployments are stored"),
            @Step(value = "Update Cruise Control configuration with new performance tuning options", expected = "Configuration update initiated"),
            @Step(value = "Verify Cruise Control Pod rolls after configuration change", expected = "Cruise Control Pod restarts to apply new configurations"),
            @Step(value = "Verify Kafka Pods did not roll after configuration change", expected = "Kafka Pods remain unchanged"),
            @Step(value = "Verify new configurations are applied to Cruise Control in Kafka CR", expected = "New configurations are correctly applied")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testConfigurationUpdate() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, String> kafkaSnapShot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> cruiseControlSnapShot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));
        Map<String, Object> performanceTuningOpts = new HashMap<>() {{
                put(CruiseControlConfigurationParameters.CONCURRENT_INTRA_PARTITION_MOVEMENTS.getValue(), 2);
                put(CruiseControlConfigurationParameters.CONCURRENT_PARTITION_MOVEMENTS.getValue(), 5);
                put(CruiseControlConfigurationParameters.CONCURRENT_LEADER_MOVEMENTS.getValue(), 1000);
                put(CruiseControlConfigurationParameters.REPLICATION_THROTTLE.getValue(), -1);
            }};

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            LOGGER.info("Changing CruiseControl performance tuning options");
            kafka.getSpec().setCruiseControl(new CruiseControlSpecBuilder()
                    .addToConfig(performanceTuningOpts)
                    .build());
        });

        LOGGER.info("Verifying that CC Pod is rolling, after changing options");
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, cruiseControlSnapShot);

        LOGGER.info("Verifying that Kafka Pods did not roll");
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), kafkaSnapShot);

        LOGGER.info("Verifying new configuration in the Kafka CR");
        ConfigMap configMap = kubeClient(testStorage.getNamespaceName()).getConfigMap(testStorage.getNamespaceName(), CruiseControlResources.configMapName(testStorage.getClusterName()));

        InputStream configurationContainerStream = new ByteArrayInputStream(
                Objects.requireNonNull(configMap.getData().get("cruisecontrol.properties")).getBytes(StandardCharsets.UTF_8));
        Properties containerConfiguration = new Properties();
        containerConfiguration.load(configurationContainerStream);

        LOGGER.info("Verifying CruiseControl performance options are set in Kafka CR");
        performanceTuningOpts.forEach((key, value) ->
                assertThat(containerConfiguration, hasEntry(key, value.toString())));
    }

    @BeforeAll
    void setUp() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
