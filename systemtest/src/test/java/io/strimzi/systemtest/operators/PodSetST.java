/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.ProbeBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for StrimziPodSet related functionality and features, which verifies pod set reconciliation behavior.")
)
public class PodSetST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(PodSetST.class);

    @IsolatedTest("We are changing CO env variables in this test")
    @TestDoc(
        description = @Desc("Test verifies that when STRIMZI_POD_SET_RECONCILIATION_ONLY environment variable is enabled, only StrimziPodSet resources are reconciled while Kafka configuration changes do not trigger pod rolling updates."),
        steps = {
            @Step(value = "Deploy Kafka cluster with 3 replicas and configure topics for resilience.", expected = "Kafka cluster with node pools and topics is deployed successfully."),
            @Step(value = "Start continuous producer and consumer clients.", expected = "Clients are producing and consuming messages."),
            @Step(value = "Enable STRIMZI_POD_SET_RECONCILIATION_ONLY environment variable in Cluster Operator.", expected = "Cluster Operator rolls out with the new environment variable."),
            @Step(value = "Change Kafka readiness probe timeout.", expected = "No pod rolling update occurs despite configuration change."),
            @Step(value = "Delete one Kafka pod.", expected = "Pod is recreated by StrimziPodSet controller."),
            @Step(value = "Remove STRIMZI_POD_SET_RECONCILIATION_ONLY environment variable from Cluster Operator.", expected = "Cluster Operator rolls out again."),
            @Step(value = "Verify pod rolling update occurs.", expected = "Kafka pods are rolled due to the pending configuration change."),
            @Step(value = "Verify StrimziPodSet status.", expected = "All StrimziPodSets are ready with matching pod counts."),
            @Step(value = "Verify message continuity.", expected = "Continuous clients successfully produced and consumed all messages.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testPodSetOnlyReconciliation() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final Map<String, String> coPod = DeploymentUtils.depSnapshot(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName());
        final int replicas = 3;
        final int probeTimeoutSeconds = 6;

        EnvVar reconciliationEnv = new EnvVar(Environment.STRIMZI_POD_SET_RECONCILIATION_ONLY_ENV, "true", null);
        List<EnvVar> envVars = KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).withName(SetupClusterOperator.getInstance().getOperatorDeploymentName()).get().getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        envVars.add(reconciliationEnv);

        LOGGER.info("Deploy Kafka configured to create topics more resilient against data loss or unavailability");
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), replicas).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), replicas).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), replicas)
            .editSpec()
                .editOrNewKafka()
                    .addToConfig("default.replication.factor", 3)
                    .addToConfig("min.insync.replicas", 2)
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(
            KafkaTopicTemplates.continuousTopic(testStorage)
                .editOrNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build()
        );

        final KafkaClients clients = ClientUtils.getContinuousPlainClientBuilder(testStorage).build();

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        KubeResourceManager.get().createResourceWithWait(
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );

        LOGGER.info("Changing {} to 'true', so only SPS will be reconciled", Environment.STRIMZI_POD_SET_RECONCILIATION_ONLY_ENV);

        DeploymentUtils.replace(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName(), coDep -> coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envVars));

        DeploymentUtils.waitTillDepHasRolled(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName(), 1, coPod);

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        LOGGER.info("Changing Kafka resource configuration, the Pods should not be rolled");

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(probeTimeoutSeconds).build());
        });

        RollingUpdateUtils.waitForNoKafkaRollingUpdate(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerPods);

        LOGGER.info("Deleting one Kafka pod, the should be recreated");
        String kafkaPodName = PodUtils.listPodNames(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0);
        KubeResourceManager.get().deleteResourceWithoutWait(KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withName(kafkaPodName).get());
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), replicas, true);

        brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        LOGGER.info("Removing {} env from CO", Environment.STRIMZI_POD_SET_RECONCILIATION_ONLY_ENV);

        envVars.remove(reconciliationEnv);
        DeploymentUtils.replace(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName(), coDep -> coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envVars));

        DeploymentUtils.waitTillDepHasRolled(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName(), 1, coPod);

        LOGGER.info("Because the configuration was changed, Pods should be rolled");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), replicas, brokerPods);

        LOGGER.info("Wait till all StrimziPodSet {}/{} status match number of ready pods", testStorage.getNamespaceName(), testStorage.getBrokerComponentName());
        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getBrokerComponentName(), 3);

        ClientUtils.waitForContinuousClientSuccess(testStorage);
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
