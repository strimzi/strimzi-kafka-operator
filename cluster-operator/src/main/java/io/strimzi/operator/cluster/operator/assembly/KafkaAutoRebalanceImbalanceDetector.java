/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceConfiguration;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.cruisecontrol.CruiseControlConfiguration;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.GoalViolationInfo;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.kubernetes.CrdOperator;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Handles imbalance detection and Cruise Control querying for the auto-rebalancing feature.
 * This includes checking for goal violations, validating template goals against anomaly detection goals,
 * checking for active rebalances, and evaluating maintenance windows.
 */
public class KafkaAutoRebalanceImbalanceDetector {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAutoRebalanceImbalanceDetector.class.getName());

    private final Reconciliation reconciliation;
    private final Kafka kafkaCr;
    private final List<KafkaAutoRebalanceConfiguration> kafkaAutoRebalanceConfigurations;
    private final CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> kafkaRebalanceOperator;
    private final ConfigMapOperator configMapOperator;
    private final ResourceOperatorSupplier supplier;

    /**
     * Constructs the imbalance detector
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaCr           The Kafka custom resource
     * @param supplier          Supplies the operators for different resources
     */
    public KafkaAutoRebalanceImbalanceDetector(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            ResourceOperatorSupplier supplier) {
        this.reconciliation = reconciliation;
        this.kafkaCr = kafkaCr;
        this.supplier = supplier;
        this.kafkaAutoRebalanceConfigurations = kafkaCr.getSpec().getCruiseControl().getAutoRebalance();
        this.kafkaRebalanceOperator = supplier.kafkaRebalanceOperator;
        this.configMapOperator = supplier.configMapOperations;
    }

    /**
     * Creates an instance of the Cruise Control API client.
     * Overriding this method can be used in tests to inject a mock client.
     *
     * @param ccSecret          Cruise Control secret
     * @param ccApiSecret       Cruise Control API secret
     * @param apiAuthEnabled    Whether API authentication is enabled
     * @param apiSslEnabled     Whether API SSL is enabled
     *
     * @return  Cruise Control API client instance
     */
    protected CruiseControlApi cruiseControlClientProvider(Secret ccSecret, Secret ccApiSecret,
                                                           boolean apiAuthEnabled, boolean apiSslEnabled) {
        return new CruiseControlApiImpl(60, ccSecret, ccApiSecret, apiAuthEnabled, apiSslEnabled);
    }

    /**
     * Returns the hostname for connecting to Cruise Control.
     * Overriding this method can be used in tests to redirect to a mock server.
     *
     * @param clusterName       Name of the Kafka cluster
     * @param clusterNamespace  Namespace of the Kafka cluster
     *
     * @return  Cruise Control hostname
     */
    protected String cruiseControlHost(String clusterName, String clusterNamespace) {
        return CruiseControlResources.qualifiedServiceName(clusterName, clusterNamespace);
    }

    /**
     * Returns the port for connecting to Cruise Control.
     * Overriding this method can be used in tests to redirect to a mock server.
     *
     * @return  Cruise Control port
     */
    protected int cruiseControlPort() {
        return CruiseControl.REST_API_PORT;
    }

    /**
     * Checks if there is an actively executing rebalance (manual or auto-generated) that should block auto-rebalance on imbalance
     *
     * @return Future with boolean - true if auto-rebalance should be blocked, false otherwise
     */
    public CompletionStage<Boolean> hasActiveRebalance() {
        return kafkaRebalanceOperator.listAsync(reconciliation.namespace(),
                Labels.fromMap(Map.of(Labels.STRIMZI_CLUSTER_LABEL, reconciliation.name())))
                .thenCompose(rebalanceList -> {
                    for (KafkaRebalance rebalance : rebalanceList) {
                        KafkaRebalanceState state = KafkaRebalanceUtils.rebalanceState(rebalance.getStatus());

                        if (state == KafkaRebalanceState.Rebalancing) {
                            LOGGER.infoCr(reconciliation, "KafkaRebalance {}/{} is actively rebalancing. Auto-rebalance on imbalance will be skipped.",
                                    rebalance.getMetadata().getNamespace(), rebalance.getMetadata().getName());
                            return CompletableFuture.completedFuture(true);
                        } else if (state == KafkaRebalanceState.New ||
                                   state == KafkaRebalanceState.PendingProposal ||
                                   state == KafkaRebalanceState.ProposalReady) {
                            LOGGER.infoCr(reconciliation, "KafkaRebalance {}/{} is in {} state and will be ignored. Auto-rebalance on imbalance will proceed.",
                                    rebalance.getMetadata().getNamespace(), rebalance.getMetadata().getName(), state);
                        }
                    }
                    return CompletableFuture.completedFuture(false);
                });
    }

    /**
     * Checks for goal violations by querying Cruise Control
     *
     * @return Future with GoalViolationInfo if violations detected, null otherwise
     */
    public CompletionStage<GoalViolationInfo> checkForGoalViolations() {
        if (kafkaCr.getSpec() == null || kafkaCr.getSpec().getCruiseControl() == null) {
            return CompletableFuture.completedFuture(null);
        }

        Optional<KafkaAutoRebalanceConfiguration> imbalanceConfig = kafkaAutoRebalanceConfigurations.stream()
                .filter(c -> c.getMode().equals(KafkaAutoRebalanceMode.IMBALANCE))
                .findFirst();

        if (imbalanceConfig.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return supplier.secretOperations.getAsync(reconciliation.namespace(), CruiseControlResources.secretName(reconciliation.name()))
                .thenCompose(ccSecret -> {
                    if (ccSecret == null) {
                        LOGGER.warnCr(reconciliation, "Cruise Control secret not found, skipping anomaly detection");
                        return CompletableFuture.completedFuture(null);
                    }

                    return supplier.secretOperations.getAsync(reconciliation.namespace(), CruiseControlResources.apiSecretName(reconciliation.name()))
                            .thenCompose(ccApiSecret -> {
                                Map<String, Object> ccConfigMap = kafkaCr.getSpec().getCruiseControl().getConfig();
                                CruiseControlConfiguration ccConfig = new CruiseControlConfiguration(
                                        reconciliation,
                                        ccConfigMap != null ? ccConfigMap.entrySet() : Map.<String, Object>of().entrySet(),
                                        Map.of());
                                boolean apiAuthEnabled = ccConfig.isApiAuthEnabled();
                                boolean apiSslEnabled = KafkaClusterSecurityContext.fromCrd(kafkaCr).isTlsEncryption();

                                CruiseControlApi ccApi = cruiseControlClientProvider(ccSecret, ccApiSecret, apiAuthEnabled, apiSslEnabled);

                                String ccHost = cruiseControlHost(reconciliation.name(), reconciliation.namespace());
                                int ccPort = cruiseControlPort();

                                return ccApi.getGoalViolations(reconciliation, ccHost, ccPort)
                                        .exceptionally(error -> {
                                            LOGGER.debugCr(reconciliation, "Unable to query Cruise Control for goal violations (pod may not be ready yet): {}", error.getMessage());
                                            return null;
                                        });
                            });
                });
    }

    /**
     * Checks if the detected anomaly should trigger a rebalance by comparing timestamps
     *
     * @param detectionDate When the anomaly was detected
     * @return Future with boolean indicating if rebalance should be triggered
     */
    public CompletionStage<Boolean> shouldTriggerRebalance(Instant detectionDate) {
        String configMapName = reconciliation.name() + KafkaAutoRebalancingReconciler.AUTO_REBALANCE_IMBALANCE_TRACKER_SUFFIX;

        return configMapOperator.getAsync(reconciliation.namespace(), configMapName)
                .thenCompose(configMap -> {
                    if (configMap == null || configMap.getData() == null) {
                        return CompletableFuture.completedFuture(true);
                    }

                    String lastCompletionTimeStr = configMap.getData().get("lastRebalanceCompletionTime");
                    if (lastCompletionTimeStr == null) {
                        return CompletableFuture.completedFuture(true);
                    }

                    try {
                        Instant lastCompletionTime = Instant.parse(lastCompletionTimeStr);
                        return CompletableFuture.completedFuture(detectionDate.isAfter(lastCompletionTime));
                    } catch (Exception e) {
                        LOGGER.warnCr(reconciliation, "Failed to parse lastRebalanceCompletionTime: {}", e.getMessage());
                        return CompletableFuture.completedFuture(true);
                    }
                });
    }

    /**
     * Checks if current time is within maintenance windows
     *
     * @return true if within maintenance window or no windows configured, false otherwise
     */
    public boolean isInMaintenanceWindow() {
        List<String> maintenanceWindows = kafkaCr.getSpec().getMaintenanceTimeWindows();
        if (maintenanceWindows == null || maintenanceWindows.isEmpty()) {
            return true;
        }

        return Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, Instant.now());
    }

    /**
     * Validates that the template goals are a superset of the anomaly detection goals.
     * Every anomaly detection goal must be present in the template so that the resulting
     * rebalance proposal actually addresses the detected violations.
     *
     * @return Future with boolean value - true if validation passes or no validation needed, false if validation fails
     */
    public CompletionStage<Boolean> validateTemplateGoals() {
        Optional<KafkaAutoRebalanceConfiguration> imbalanceConfig = kafkaAutoRebalanceConfigurations.stream()
                .filter(c -> c.getMode().equals(KafkaAutoRebalanceMode.IMBALANCE))
                .findFirst();

        if (imbalanceConfig.isEmpty() || imbalanceConfig.get().getTemplate() == null) {
            return CompletableFuture.completedFuture(true);
        }

        String templateName = imbalanceConfig.get().getTemplate().getName();

        return kafkaRebalanceOperator.getAsync(reconciliation.namespace(), templateName)
                .thenCompose(template -> {
                    if (template == null) {
                        LOGGER.warnCr(reconciliation, "KafkaRebalance template {} not found", templateName);
                        return CompletableFuture.completedFuture(true);
                    }

                    if (template.getSpec() == null || template.getSpec().getGoals() == null || template.getSpec().getGoals().isEmpty()) {
                        return CompletableFuture.completedFuture(true);
                    }

                    List<String> anomalyDetectionGoals = getAnomalyDetectionGoals();

                    Set<String> templateGoals = template.getSpec().getGoals().stream()
                            .map(this::extractGoalShortName)
                            .collect(Collectors.toSet());

                    List<String> missingGoals = anomalyDetectionGoals.stream()
                            .filter(goal -> !templateGoals.contains(goal))
                            .collect(Collectors.toList());

                    if (!missingGoals.isEmpty()) {
                        String message = String.format(
                                "Anomaly detection goals %s are missing from template '%s'. " +
                                "The template must include all anomaly detection goals so that the rebalance addresses detected violations. " +
                                "Template goals: %s. Add missing goals to the template.",
                                missingGoals, templateName, templateGoals);
                        LOGGER.warnCr(reconciliation, message);
                        return CompletableFuture.completedFuture(false);
                    }

                    return CompletableFuture.completedFuture(true);
                });
    }

    /**
     * Gets the anomaly detection goals from Cruise Control configuration or returns defaults
     *
     * @return List of anomaly detection goal names
     */
    private List<String> getAnomalyDetectionGoals() {
        List<String> defaultGoals = List.of(
                "RackAwareGoal",
                "MinTopicLeadersPerBrokerGoal",
                "ReplicaCapacityGoal",
                "DiskCapacityGoal"
        );

        if (kafkaCr.getSpec() == null || kafkaCr.getSpec().getCruiseControl() == null ||
                kafkaCr.getSpec().getCruiseControl().getConfig() == null) {
            return defaultGoals;
        }

        Map<String, Object> ccConfig = kafkaCr.getSpec().getCruiseControl().getConfig();
        Object goalsConfig = ccConfig.get("anomaly.detection.goals");

        if (goalsConfig == null) {
            return defaultGoals;
        }

        String goalsString = goalsConfig.toString();
        if (goalsString.isEmpty()) {
            return defaultGoals;
        }

        return List.of(goalsString.split(","))
                .stream()
                .map(String::trim)
                .map(this::extractGoalShortName)
                .collect(Collectors.toList());
    }

    /**
     * Extracts the short name from a goal (handles both full class names and short names)
     * E.g., "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal" -> "RackAwareGoal"
     *       "RackAwareGoal" -> "RackAwareGoal"
     *
     * @param goal The goal name (full or short)
     * @return The short goal name
     */
    private String extractGoalShortName(String goal) {
        if (goal.contains(".")) {
            int lastDot = goal.lastIndexOf('.');
            return goal.substring(lastDot + 1);
        }
        return goal;
    }
}
