/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
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
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.operator.resource.kubernetes.CrdOperator;

import java.time.Instant;
import java.util.List;
import java.util.Map;
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
     * @return CompletionStage with boolean - true if auto-rebalance should be blocked, false otherwise
     */
    public CompletionStage<Boolean> hasActiveRebalance() {
        return kafkaRebalanceOperator.listAsync(reconciliation.namespace(),
                Labels.fromMap(Map.of(Labels.STRIMZI_CLUSTER_LABEL, reconciliation.name())))
                .thenCompose(rebalanceList -> {
                    for (KafkaRebalance rebalance : rebalanceList) {
                        KafkaRebalanceState state = KafkaRebalanceUtils.rebalanceState(rebalance.getStatus());

                        if (state == KafkaRebalanceState.Rebalancing) {
                            LOGGER.debugCr(reconciliation, "KafkaRebalance {}/{} is actively rebalancing: Auto-rebalance on imbalance will be skipped until it completes",
                                    rebalance.getMetadata().getNamespace(), rebalance.getMetadata().getName());
                            return CompletableFuture.completedFuture(true);
                        } else if (state == KafkaRebalanceState.New ||
                                   state == KafkaRebalanceState.PendingProposal ||
                                   state == KafkaRebalanceState.ProposalReady) {
                            LOGGER.debugCr(reconciliation, "KafkaRebalance {}/{} is in {} state (not yet executing): Auto-rebalance on imbalance will proceed",
                                    rebalance.getMetadata().getNamespace(), rebalance.getMetadata().getName(), state);
                        }
                    }
                    return CompletableFuture.completedFuture(false);
                });
    }

    /**
     * Checks for goal violations by querying Cruise Control
     *
     * @return CompletionStage with GoalViolationInfo if violations detected, null otherwise
     */
    public CompletionStage<GoalViolationInfo> checkForGoalViolations() {
        return supplier.secretOperations.getAsync(reconciliation.namespace(), CruiseControlResources.secretName(reconciliation.name()))
                .thenCompose(ccSecret -> supplier.secretOperations.getAsync(reconciliation.namespace(), CruiseControlResources.apiSecretName(reconciliation.name()))
                .thenCompose(ccApiSecret -> {
                    if (ccSecret == null) {
                        LOGGER.warnCr(reconciliation, "Cruise Control secret not found: Skipping goal violation detection this reconciliation");
                        return CompletableFuture.completedFuture(null);
                    }
                    if (ccApiSecret == null) {
                        LOGGER.warnCr(reconciliation, "Cruise Control API secret not found: Skipping goal violation detection this reconciliation");
                        return CompletableFuture.completedFuture(null);
                    }

                    Map<String, Object> ccConfigMap = kafkaCr.getSpec().getCruiseControl().getConfig();
                    CruiseControlConfiguration ccConfig = new CruiseControlConfiguration(
                            reconciliation,
                            ccConfigMap != null ? ccConfigMap.entrySet() : Map.<String, Object>of().entrySet(),
                            Map.of());
                    boolean apiAuthEnabled = ccConfig.isApiAuthEnabled();
                    boolean apiSslEnabled = KafkaClusterSecurityContext.fromCrd(kafkaCr).encryption() instanceof TlsEncryptionConfiguration;

                    CruiseControlApi ccApi = cruiseControlClientProvider(ccSecret, ccApiSecret, apiAuthEnabled, apiSslEnabled);

                    String ccHost = cruiseControlHost(reconciliation.name(), reconciliation.namespace());
                    int ccPort = cruiseControlPort();

                    return ccApi.getGoalViolations(reconciliation, ccHost, ccPort)
                            .exceptionally(error -> {
                                LOGGER.debugCr(reconciliation, "Unable to query Cruise Control for goal violations: Pod may not be ready yet", error);
                                return null;
                            });
                }));
    }

    /**
     * Checks if the detected anomaly should trigger a rebalance by comparing timestamps
     *
     * @param detectionTime When the anomaly was detected
     * @return CompletionStage with boolean indicating if rebalance should be triggered
     */
    public CompletionStage<Boolean> shouldTriggerRebalance(Instant detectionTime) {
        return configMapOperator.getAsync(reconciliation.namespace(), reconciliation.name() + KafkaAutoRebalancingReconciler.AUTO_REBALANCE_IMBALANCE_TRACKER_SUFFIX)
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
                        return CompletableFuture.completedFuture(detectionTime.isAfter(lastCompletionTime));
                    } catch (Exception e) {
                        LOGGER.warnCr(reconciliation, "Failed to parse lastRebalanceCompletionTime '{}': Treating as no previous rebalance and allowing trigger", lastCompletionTimeStr);
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
     * @param templateName  Name of the KafkaRebalance template to validate
     * @return CompletionStage with boolean value - true if validation passes, false if validation fails
     */
    public CompletionStage<Boolean> validateTemplateGoals(String templateName) {
        return kafkaRebalanceOperator.getAsync(reconciliation.namespace(), templateName)
                .thenCompose(template -> {
                    if (template == null) {
                        LOGGER.warnCr(reconciliation, "KafkaRebalance template '{}' not found: Skipping template goal validation and proceeding with detection", templateName);
                        return CompletableFuture.completedFuture(true);
                    }

                    if (template.getSpec().getGoals() == null || template.getSpec().getGoals().isEmpty()) {
                        return CompletableFuture.completedFuture(true);
                    }

                    List<String> anomalyDetectionGoals = getAnomalyDetectionGoals();

                    Set<String> templateGoals = template.getSpec().getGoals().stream()
                            .map(this::extractGoalShortName)
                            .collect(Collectors.toSet());

                    List<String> missingGoals = anomalyDetectionGoals.stream()
                            .filter(goal -> !templateGoals.contains(goal))
                            .toList();

                    if (!missingGoals.isEmpty()) {
                        LOGGER.warnCr(reconciliation,
                                "Anomaly detection goals {} are missing from template '{}'. " +
                                "The template must include all anomaly detection goals so that the rebalance addresses detected violations. " +
                                "Template goals: {}. Add the missing goals to the template.",
                                missingGoals, templateName, templateGoals);
                        return CompletableFuture.completedFuture(false);
                    }

                    return CompletableFuture.completedFuture(true);
                }).toCompletableFuture();
    }

    /**
     * Gets the anomaly detection goals from Cruise Control configuration or returns defaults
     *
     * @return List of anomaly detection goal names
     */
    private List<String> getAnomalyDetectionGoals() {
        List<String> defaultGoals = List.of(CruiseControlConfiguration.CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS.split(","))
                .stream()
                .map(String::trim)
                .map(this::extractGoalShortName)
                .toList();

        Map<String, Object> ccConfig = kafkaCr.getSpec().getCruiseControl().getConfig();
        if (ccConfig == null) {
            return defaultGoals;
        }
        Object goalsConfig = ccConfig.get(CruiseControlConfigurationParameters.ANOMALY_DETECTION_CONFIG_KEY.toString());

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
                .toList();
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
