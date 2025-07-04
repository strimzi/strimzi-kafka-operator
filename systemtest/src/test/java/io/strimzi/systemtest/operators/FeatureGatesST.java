/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Feature Gates should give us additional options on
 * how to control and mature different behaviors in the operators.
 * https://github.com/strimzi/proposals/blob/main/022-feature-gates.md
 */
@Tag(REGRESSION)
public class FeatureGatesST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(FeatureGatesST.class);
    private static final String USE_SERVER_SIDE_APPLY = "+UseServerSideApply";

    @IsolatedTest("Creates ClusterOperator with Server Side Apply FG enabled")
    void testServerSideApply() {
        TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // Firstly deploy CO without SSA enabled to check that changes to the resources will be re-written
        setupClusterOperatorWithFeatureGate("");

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        annotateResourcesAndCheckIfPresent(testStorage, false);

        LOGGER.info("Change STRIMZI_FEATURE_GATES to enable SSA");
        final Map<String, String> coPod = DeploymentUtils.depSnapshot(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName());

        KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace())
            .withName(SetupClusterOperator.getInstance().getOperatorDeploymentName()).edit(dep -> new DeploymentBuilder(dep)
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .addToEnv(new EnvVar("STRIMZI_FEATURE_GATES", USE_SERVER_SIDE_APPLY, null))
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build());

        DeploymentUtils.waitTillDepHasRolled(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName(), 1, coPod);

        annotateResourcesAndCheckIfPresent(testStorage, true);
    }

    private void annotateResourcesAndCheckIfPresent(TestStorage testStorage, boolean shouldBePresent) {
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
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(30));

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
