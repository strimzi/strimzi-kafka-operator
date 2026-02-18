/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.connect.build.TgzArtifactBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Feature Gates should give us additional options on
 * how to control and mature different behaviors in the operators.
 * https://github.com/strimzi/proposals/blob/main/022-feature-gates.md
 */
@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Feature Gates test suite verifying that feature gates provide additional options to control operator behavior, specifically testing Server Side Apply functionality."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with configurable feature gates.", expected = "Cluster Operator is deployed with feature gate support.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class FeatureGatesST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(FeatureGatesST.class);
    private static final String SERVER_SIDE_APPLY_PHASE_1_ENABLED = "+ServerSideApplyPhase1";
    private static final String SERVER_SIDE_APPLY_PHASE_1_DISABLED = "-ServerSideApplyPhase1";
    private static final String USE_CONNECT_BUILD_WITH_BUILDAH_ENABLED = "+UseConnectBuildWithBuildah";

    @IsolatedTest("Creates ClusterOperator with Server Side Apply FG enabled")
    @TestDoc(
        description = @Desc("This test verifies that Server Side Apply Phase 1 feature gate works correctly by testing annotation preservation behavior. When SSA is disabled, manual annotations are removed during reconciliation. When SSA is enabled, manual annotations are preserved."),
        steps = {
            @Step(value = "Deploy Cluster Operator with Server Side Apply Phase 1 disabled.", expected = "Cluster Operator is deployed without SSA feature gate."),
            @Step(value = "Create Kafka cluster with broker and controller node pools.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Add manual annotations to Kafka resources and verify they are removed.", expected = "Manual annotations are removed during reconciliation when SSA is disabled."),
            @Step(value = "Enable Server Side Apply Phase 1 feature gate.", expected = "Cluster Operator is reconfigured with SSA enabled and redeployed."),
            @Step(value = "Add manual annotations to Kafka resources and verify they are preserved.", expected = "Manual annotations are preserved during reconciliation when SSA is enabled."),
            @Step(value = "Disable Server Side Apply Phase 1 feature gate.", expected = "Cluster Operator is reconfigured with SSA disabled and rolled."),
            @Step(value = "Add manual annotations to Kafka resources and verify they are removed again.", expected = "Manual annotations are removed during reconciliation when SSA is disabled again.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testServerSideApply() {
        TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        LOGGER.info("Deploying CO with SSA Phase 1 disabled");

        // Firstly deploy CO without SSA enabled to check that changes to the resources will be re-written
        setupClusterOperatorWithFeatureGate(SERVER_SIDE_APPLY_PHASE_1_DISABLED);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        annotateResourcesAndCheckIfPresent(testStorage, false);

        LOGGER.info("Enabling Server Side Apply Phase 1");
        changeFeatureGatesAndWaitForCoRollingUpdate("");

        annotateResourcesAndCheckIfPresent(testStorage, true);

        LOGGER.info("Finally, changing back to SSA disabled");

        changeFeatureGatesAndWaitForCoRollingUpdate(SERVER_SIDE_APPLY_PHASE_1_DISABLED);

        annotateResourcesAndCheckIfPresent(testStorage, false);
    }

    @IsolatedTest("Enables UseConnectBuildWithBuildah feature gate in CO")
    void testUseConnectBuildWithBuildah() {
        // Buildah is used only on Kubernetes, so running this test on OCP doesn't add much value
        assumeFalse(KubeClusterResource.getInstance().isOpenShiftLikeCluster());

        TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String camelChecksum = "6d1f9311fe10521a5de3262574ad7c21073cd45089fc67245f04a303562b0ea54869c8cd0375ee76de8b5c24d454a0545688018ccdae0563bd5f254aceb98b5e";
        final String camelConnectorUrl = "https://repo.maven.apache.org/maven2/org/apache/camel/kafkaconnector/camel-timer-kafka-connector/0.9.0/camel-timer-kafka-connector-0.9.0-package.tar.gz";
        int randomNum = new Random().nextInt(Integer.MAX_VALUE);
        final String imageName = Environment.getImageOutputRegistry(testStorage.getNamespaceName(), testStorage.getNamespaceName(), String.valueOf(randomNum));

        LOGGER.info("Deploying CO with UseConnectBuildWithBuildah disabled");

        setupClusterOperatorWithFeatureGate(USE_CONNECT_BUILD_WITH_BUILDAH_ENABLED);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        KubeResourceManager.get().createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build(),
            KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getClusterName(), 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editOrNewSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewBuild()
                        .addNewPlugin()
                            .withName("camel-connector")
                            .withArtifacts(
                                new TgzArtifactBuilder()
                                    .withUrl(camelConnectorUrl)
                                    .withSha512sum(camelChecksum)
                                    .build()
                            )
                        .endPlugin()
                        .withNewDockerOutputLike(KafkaConnectTemplates.dockerOutput(imageName))
                            .withAdditionalPushOptions("--tls-verify=false")
                            .withAdditionalBuildOptions("--tls-verify=false")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build());

        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", testStorage.getTopicName());
        connectorConfig.put("camel.source.path.timerName", "timer");

        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getClusterName())
            .editOrNewSpec()
                .withClassName("org.apache.camel.kafkaconnector.timer.CamelTimerSourceConnector")
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        KafkaClients kafkaClient = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        KubeResourceManager.get().createResourceWithWait(kafkaClient.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    private static void annotateResourcesAndCheckIfPresent(TestStorage testStorage, boolean shouldBePresent) {
        final String annotationKey = "my-annotation";
        final String annotationValue = "my-value";
        final String annotationFull = String.format("%s=%s", annotationKey, annotationValue);

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        String controllerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getControllerSelector()).get(0).getMetadata().getName();

        String bootstrapService = KafkaResources.bootstrapServiceName(testStorage.getClusterName());
        String controllerPvc = String.format("data-%s", controllerPodName);
        String kafkaServiceAccount = KafkaResources.kafkaComponentName(testStorage.getClusterName());

        KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName())
            .exec("annotate", "configmap", brokerPodName, annotationFull);
        KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName())
            .exec("annotate", "service", bootstrapService, annotationFull);
        KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName())
            .exec("annotate", "pvc", controllerPvc, annotationFull);
        KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName())
            .exec("annotate", "serviceaccount", kafkaServiceAccount, annotationFull);

        LOGGER.info("Waiting for {} for reconciliation in order to see if the annotation will stay or not.", TestConstants.RECONCILIATION_INTERVAL);

        // Wait for one reconciliation interval to happen
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(TestConstants.RECONCILIATION_INTERVAL));

        Map<String, String> currentAnnotations = KubeResourceManager.get().kubeClient().getClient().configMaps()
            .inNamespace(testStorage.getNamespaceName()).withName(brokerPodName).get().getMetadata().getAnnotations();
        assertThat(currentAnnotations.containsKey(annotationKey), is(shouldBePresent));

        currentAnnotations = KubeResourceManager.get().kubeClient().getClient().services()
            .inNamespace(testStorage.getNamespaceName()).withName(bootstrapService).get().getMetadata().getAnnotations();
        assertThat(currentAnnotations.containsKey(annotationKey), is(shouldBePresent));

        currentAnnotations = KubeResourceManager.get().kubeClient().getClient().persistentVolumeClaims()
            .inNamespace(testStorage.getNamespaceName()).withName(controllerPvc).get().getMetadata().getAnnotations();
        assertThat(currentAnnotations.containsKey(annotationKey), is(shouldBePresent));

        currentAnnotations = KubeResourceManager.get().kubeClient().getClient().serviceAccounts()
            .inNamespace(testStorage.getNamespaceName()).withName(kafkaServiceAccount).get().getMetadata().getAnnotations();
        assertThat(currentAnnotations.containsKey(annotationKey), is(shouldBePresent));
    }

    /**
     * Changes the feature gate value in `STRIMZI_FEATURE_GATES` env variable to {@param featureGatesValue}.
     *
     * @param featureGatesValue     value of FG that should be set in CO's `STRIMZI_FEATURE_GATES` env variable
     */
    private void changeFeatureGatesAndWaitForCoRollingUpdate(String featureGatesValue) {
        LOGGER.info("Changing STRIMZI_FEATURE_GATES to {}", featureGatesValue);

        Map<String, String> coPod = DeploymentUtils.depSnapshot(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName());

        KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace())
            .withName(SetupClusterOperator.getInstance().getOperatorDeploymentName()).edit(dep -> new DeploymentBuilder(dep)
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .addToEnv(new EnvVar("STRIMZI_FEATURE_GATES", featureGatesValue, null))
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build());

        DeploymentUtils.waitTillDepHasRolled(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName(), 1, coPod);
    }

    /**
     * Sets up a Cluster Operator with specified feature gates.
     *
     * @param extraFeatureGates A String representing additional feature gates (comma separated) to be
     *                          enabled or disabled for the Cluster Operator.
     */
    private void setupClusterOperatorWithFeatureGate(String extraFeatureGates) {
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withFeatureGates(extraFeatureGates)
                // configuring it explicitly here to be sure that we will use the correct reconciliation interval
                .withReconciliationInterval(TestConstants.RECONCILIATION_INTERVAL)
                .build()
            )
            .install();
    }
}
