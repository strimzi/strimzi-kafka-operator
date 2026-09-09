/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceConfigurationBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.GoalViolationInfo;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.operator.resource.kubernetes.CrdOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaAutoRebalanceImbalanceDetectorTest {

    private static final String NAMESPACE = "test-ns";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

    private ResourceOperatorSupplier supplier;
    private CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> rebalanceOperator;

    @BeforeEach
    public void setup() {
        supplier = ResourceUtils.supplierWithMocks(false);
        rebalanceOperator = supplier.kafkaRebalanceOperator;
    }


    @Test
    public void testHasNoActiveRebalance() {
        when(rebalanceOperator.listAsync(eq(NAMESPACE), any(Labels.class)))
                .thenReturn(CompletableFuture.completedFuture(List.of()));

        boolean result = detector(buildKafka(null, null)).hasActiveRebalance().toCompletableFuture().join();

        assertThat(result, is(false));
    }

    @Test
    public void testHasActiveRebalanceWithRebalancingState() {
        KafkaRebalance rebalancing = buildRebalance("kr1", KafkaRebalanceState.Rebalancing);
        when(rebalanceOperator.listAsync(eq(NAMESPACE), any(Labels.class)))
                .thenReturn(CompletableFuture.completedFuture(List.of(rebalancing)));

        boolean result = detector(buildKafka(null, null)).hasActiveRebalance().toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testHasRebalanceButIgnoresNonActiveStates() {
        // New/PendingProposal/ProposalReady are ignored — not considered active
        KafkaRebalance newRebalance = buildRebalance("kr1", KafkaRebalanceState.New);
        KafkaRebalance pendingRebalance = buildRebalance("kr2", KafkaRebalanceState.PendingProposal);
        when(rebalanceOperator.listAsync(eq(NAMESPACE), any(Labels.class)))
                .thenReturn(CompletableFuture.completedFuture(List.of(newRebalance, pendingRebalance)));

        boolean result = detector(buildKafka(null, null)).hasActiveRebalance().toCompletableFuture().join();

        assertThat(result, is(false));
    }

    @Test
    public void testShouldTriggerRebalanceNoTracker() {
        when(supplier.configMapOperations.getAsync(eq(NAMESPACE), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        boolean result = detector(buildKafka(null, null))
                .shouldTriggerRebalance(Instant.now()).toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testShouldTriggerRebalanceNewerViolation() {
        Instant lastCompletion = Instant.parse("2024-01-01T10:00:00Z");
        Instant detectionDate = Instant.parse("2024-01-01T11:00:00Z");
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata().withName("x").endMetadata()
                .withData(Map.of("lastRebalanceCompletionTime", lastCompletion.toString()))
                .build();
        when(supplier.configMapOperations.getAsync(eq(NAMESPACE), any()))
                .thenReturn(CompletableFuture.completedFuture(cm));

        boolean result = detector(buildKafka(null, null))
                .shouldTriggerRebalance(detectionDate).toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testShouldNotTriggerRebalanceOlderViolation() {
        Instant lastCompletion = Instant.parse("2024-01-01T12:00:00Z");
        Instant detectionDate = Instant.parse("2024-01-01T11:00:00Z");
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata().withName("x").endMetadata()
                .withData(Map.of("lastRebalanceCompletionTime", lastCompletion.toString()))
                .build();
        when(supplier.configMapOperations.getAsync(eq(NAMESPACE), any()))
                .thenReturn(CompletableFuture.completedFuture(cm));

        boolean result = detector(buildKafka(null, null))
                .shouldTriggerRebalance(detectionDate).toCompletableFuture().join();

        assertThat(result, is(false));
    }


    @Test
    public void testIsInMaintenanceWindowWithNoWindows() {
        Kafka kafka = new KafkaBuilder(buildKafka(null, null))
                .editSpec()
                    .withMaintenanceTimeWindows((List<String>) null)
                .endSpec()
                .build();

        assertThat(detector(kafka).isInMaintenanceWindow(), is(true));
    }

    @Test
    public void testValidateTemplateGoalsWithNoImbalanceConfig() {
        // Kafka with only REMOVE_BROKERS config — no IMBALANCE config
        Kafka kafka = buildKafkaWithoutImbalanceConfig();

        boolean result = detector(kafka).validateTemplateGoals().toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testValidateTemplateGoalsWithNoTemplateReference() {
        // IMBALANCE config exists but has no template reference
        Kafka kafka = buildKafka(null, null);

        boolean result = detector(kafka).validateTemplateGoals().toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testValidateTemplateGoalsWithMissingTemplate() {
        Kafka kafka = buildKafka(null, "my-template");
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq("my-template")))
                .thenReturn(CompletableFuture.completedFuture(null));

        boolean result = detector(kafka).validateTemplateGoals().toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testValidateTemplateGoalsWithNoGoalsInTemplate() {
        Kafka kafka = buildKafka(null, "my-template");
        KafkaRebalance template = new KafkaRebalanceBuilder()
                .withNewMetadata().withName("my-template").withNamespace(NAMESPACE).endMetadata()
                .withNewSpec().endSpec()
                .build();
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq("my-template")))
                .thenReturn(CompletableFuture.completedFuture(template));

        boolean result = detector(kafka).validateTemplateGoals().toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testValidateTemplateGoalsTemplateGoalsAreSupersetOfAnomalyGoalsReturnsTrue() {
        // Template includes all default anomaly detection goals plus extras
        Kafka kafka = buildKafka(null, "my-template");
        KafkaRebalance template = new KafkaRebalanceBuilder()
                .withNewMetadata().withName("my-template").withNamespace(NAMESPACE).endMetadata()
                .withNewSpec()
                    .withGoals(
                        "RackAwareGoal",
                        "MinTopicLeadersPerBrokerGoal",
                        "ReplicaCapacityGoal",
                        "DiskCapacityGoal",
                        "CpuCapacityGoal")
                .endSpec()
                .build();
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq("my-template")))
                .thenReturn(CompletableFuture.completedFuture(template));

        boolean result = detector(kafka).validateTemplateGoals().toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testValidateTemplateGoalsWithMissingAnomalyGoals() {
        Kafka kafka = buildKafka(null, "my-template");
        // Template missing some default anomaly detection goals
        KafkaRebalance template = new KafkaRebalanceBuilder()
                .withNewMetadata().withName("my-template").withNamespace(NAMESPACE).endMetadata()
                .withNewSpec()
                    .withGoals("RackAwareGoal", "CpuCapacityGoal") // missing ReplicaCapacityGoal, DiskCapacityGoal, etc.
                .endSpec()
                .build();
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq("my-template")))
                .thenReturn(CompletableFuture.completedFuture(template));

        boolean result = detector(kafka).validateTemplateGoals().toCompletableFuture().join();

        assertThat(result, is(false));
    }

    @Test
    public void testValidateTemplateGoalsFullClassNameGoalsInTemplateMatchedByShortName() {
        // Template goals specified as fully qualified class names — extractor should normalize them
        Kafka kafka = buildKafka(null, "my-template");
        KafkaRebalance template = new KafkaRebalanceBuilder()
                .withNewMetadata().withName("my-template").withNamespace(NAMESPACE).endMetadata()
                .withNewSpec()
                    .withGoals(
                        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal",
                        "com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal",
                        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal",
                        "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal")
                .endSpec()
                .build();
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq("my-template")))
                .thenReturn(CompletableFuture.completedFuture(template));

        boolean result = detector(kafka).validateTemplateGoals().toCompletableFuture().join();

        assertThat(result, is(true));
    }

    @Test
    public void testValidateTemplateGoalsCustomAnomalyGoalsInCCConfigTemplateValidatedAgainstCustomGoals() {
        // Kafka CR has custom anomaly.detection.goals — template must cover those, not the defaults
        Kafka kafka = buildKafka(
                Map.of(CruiseControlConfigurationParameters.ANOMALY_DETECTION_CONFIG_KEY.toString(), "RackAwareGoal,CpuCapacityGoal"),
                "my-template");
        KafkaRebalance template = new KafkaRebalanceBuilder()
                .withNewMetadata().withName("my-template").withNamespace(NAMESPACE).endMetadata()
                .withNewSpec()
                    .withGoals("RackAwareGoal", "CpuCapacityGoal", "DiskCapacityGoal")
                .endSpec()
                .build();
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq("my-template")))
                .thenReturn(CompletableFuture.completedFuture(template));

        boolean result = detector(kafka).validateTemplateGoals().toCompletableFuture().join();

        assertThat(result, is(true));
    }


    @Test
    public void testCheckForGoalViolationsWithNoImbalanceConfig() {
        Kafka kafka = buildKafkaWithoutImbalanceConfig();

        GoalViolationInfo result = detector(kafka).checkForGoalViolations().toCompletableFuture().join();

        assertThat(result, is(nullValue()));
    }

    @Test
    public void testCheckForGoalViolationsWhenSecretNotFound() {
        Kafka kafka = buildKafka(null, null);
        when(supplier.secretOperations.getAsync(eq(NAMESPACE), eq(CruiseControlResources.secretName(CLUSTER_NAME))))
                .thenReturn(CompletableFuture.completedFuture(null));

        GoalViolationInfo result = detector(kafka).checkForGoalViolations().toCompletableFuture().join();

        assertThat(result, is(nullValue()));
    }

    @Test
    public void testCheckForGoalViolationsWhenViolationsDetected() {
        Kafka kafka = buildKafka(null, null);
        Secret ccSecret = new SecretBuilder().withNewMetadata().withName("cc-secret").endMetadata().build();
        Secret ccApiSecret = new SecretBuilder().withNewMetadata().withName("cc-api-secret").endMetadata().build();
        GoalViolationInfo expectedInfo = new GoalViolationInfo(Instant.now(), GoalViolationInfo.Fixability.FIXABLE);
        CruiseControlApi mockApi = mock(CruiseControlApi.class);

        when(supplier.secretOperations.getAsync(eq(NAMESPACE), eq(CruiseControlResources.secretName(CLUSTER_NAME))))
                .thenReturn(CompletableFuture.completedFuture(ccSecret));
        when(supplier.secretOperations.getAsync(eq(NAMESPACE), eq(CruiseControlResources.apiSecretName(CLUSTER_NAME))))
                .thenReturn(CompletableFuture.completedFuture(ccApiSecret));
        when(mockApi.getGoalViolations(any(), any(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(expectedInfo));

        GoalViolationInfo result = detectorWithApi(kafka, mockApi)
                .checkForGoalViolations().toCompletableFuture().join();

        assertThat(result, is(notNullValue()));
        assertThat(result.fixability(), is(GoalViolationInfo.Fixability.FIXABLE));
    }

    @Test
    public void testCheckForGoalViolationsWhenApiThrows() {
        Kafka kafka = buildKafka(null, null);
        Secret ccSecret = new SecretBuilder().withNewMetadata().withName("cc-secret").endMetadata().build();
        Secret ccApiSecret = new SecretBuilder().withNewMetadata().withName("cc-api-secret").endMetadata().build();
        CruiseControlApi mockApi = mock(CruiseControlApi.class);

        when(supplier.secretOperations.getAsync(eq(NAMESPACE), eq(CruiseControlResources.secretName(CLUSTER_NAME))))
                .thenReturn(CompletableFuture.completedFuture(ccSecret));
        when(supplier.secretOperations.getAsync(eq(NAMESPACE), eq(CruiseControlResources.apiSecretName(CLUSTER_NAME))))
                .thenReturn(CompletableFuture.completedFuture(ccApiSecret));
        when(mockApi.getGoalViolations(any(), any(), anyInt()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("CC not ready")));

        GoalViolationInfo result = detectorWithApi(kafka, mockApi)
                .checkForGoalViolations().toCompletableFuture().join();

        assertThat(result, is(nullValue()));
    }


    private KafkaAutoRebalanceImbalanceDetector detector(Kafka kafka) {
        return new KafkaAutoRebalanceImbalanceDetector(RECONCILIATION, kafka, supplier) {
            @Override
            protected String cruiseControlHost(String clusterName, String clusterNamespace) {
                return "localhost";
            }

            @Override
            protected int cruiseControlPort() {
                return 9090;
            }
        };
    }

    private KafkaAutoRebalanceImbalanceDetector detectorWithApi(Kafka kafka, CruiseControlApi api) {
        return new KafkaAutoRebalanceImbalanceDetector(RECONCILIATION, kafka, supplier) {
            @Override
            protected CruiseControlApi cruiseControlClientProvider(Secret ccSecret, Secret ccApiSecret,
                                                                   boolean apiAuthEnabled, boolean apiSslEnabled) {
                return api;
            }

            @Override
            protected String cruiseControlHost(String clusterName, String clusterNamespace) {
                return "localhost";
            }

            @Override
            protected int cruiseControlPort() {
                return 9090;
            }
        };
    }

    private Kafka buildKafka(Map<String, Object> ccConfig, String templateName) {
        KafkaAutoRebalanceConfigurationBuilder imbalanceConfig = new KafkaAutoRebalanceConfigurationBuilder()
                .withMode(KafkaAutoRebalanceMode.IMBALANCE);
        if (templateName != null) {
            imbalanceConfig.withNewTemplate(templateName);
        }

        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewCruiseControl()
                        .withConfig(ccConfig)
                        .withAutoRebalance(imbalanceConfig.build())
                    .endCruiseControl()
                .endSpec()
                .build();
    }

    private Kafka buildKafkaWithoutImbalanceConfig() {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewCruiseControl()
                        .withAutoRebalance(
                            new KafkaAutoRebalanceConfigurationBuilder()
                                    .withMode(KafkaAutoRebalanceMode.REMOVE_BROKERS)
                                    .build())
                    .endCruiseControl()
                .endSpec()
                .build();
    }

    private KafkaRebalance buildRebalance(String name, KafkaRebalanceState state) {
        return new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewStatus()
                    .withConditions(new ConditionBuilder()
                            .withType(state.toString())
                            .withStatus("True")
                            .build())
                .endStatus()
                .build();
    }
}
