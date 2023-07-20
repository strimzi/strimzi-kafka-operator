/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectSpec;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;

import java.util.Optional;
import java.util.function.Consumer;

import static io.strimzi.operator.cluster.operator.assembly.AbstractConnectOperator.isUseResources;
import static io.strimzi.operator.common.AbstractOperator.LOCK_TIMEOUT_MS;

/**
 * Watcher implementation for watching node pools and triggering Kafka reconciliations based on changes to node pool
 * configurations.
 */
public class KafkaConnectorWatcher implements Watcher<KafkaConnector> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaNodePoolWatcher.class);

    private final Optional<LabelSelector> selector;
    private final AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, Resource<KafkaConnect>, KafkaConnectSpec, KafkaConnectStatus> connectOperator;
    private final Consumer<WatcherException> onClose;
    private final Labels selectorLabels;

    /**
     * Constructs the KafkaNodePool watcher
     *
     * @param namespace                 Namespace to watch (or asterisk for all namespaces)
     * @param selector                  The selector to check if the Kafka cluster is operated by this instance of the CLuster Operator
     * @param connectOperator           Kafka operator for managing the Kafka custom resources
     * @param onClose                   On-close method which is called when the watch is closed
     * @param selectorLabels            Selector labels for filtering the custom resources
     */
    public KafkaConnectorWatcher(String namespace, Optional<LabelSelector> selector,
                                 AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, Resource<KafkaConnect>, KafkaConnectSpec, KafkaConnectStatus> connectOperator,
                                 Consumer<WatcherException> onClose, Labels selectorLabels) {
        this.selector = selector;
        this.connectOperator = connectOperator;
        this.onClose = onClose;
        this.selectorLabels = selectorLabels;
    }

    /**
     * Handles the event received from the watch
     *
     * @param action    Action describing the event
     * @param kafkaConnector  KafkaConnectorPool resource to which the event happened
     */
    public void eventReceived(Action action, KafkaConnector kafkaConnector) {
        String connectorName = kafkaConnector.getMetadata().getName();
        String connectorNamespace = kafkaConnector.getMetadata().getNamespace();
        String connectorKind = kafkaConnector.getKind();
        String connectName = kafkaConnector.getMetadata().getLabels() == null ? null : kafkaConnector.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String connectNamespace = connectorNamespace;

        switch (action) {
            case ADDED:
            case DELETED:
            case MODIFIED:
                if (connectName != null) {
                    // Check whether a KafkaConnect exists
                    connectOperator.resourceOperator.getAsync(connectNamespace, connectName)
                            .compose(connect -> {
                                KafkaConnectApi apiClient = connectOperator.connectClientProvider.apply(connectOperator.vertx);
                                if (connect == null) {
                                    Reconciliation r = new Reconciliation("connector-watch", connectOperator.kind(),
                                            kafkaConnector.getMetadata().getNamespace(), connectName);
                                    updateStatus(r, connectOperator.noConnectCluster(connectNamespace, connectName), kafkaConnector, connectOperator.connectorOperator);
                                    LOGGER.infoCr(r, "{} {} in namespace {} was {}, but Connect cluster {} does not exist", connectorKind, connectorName, connectorNamespace, action, connectName);
                                    return Future.succeededFuture();
                                } else {
                                    // grab the lock and call reconcileConnectors()
                                    // (i.e. short circuit doing a whole KafkaConnect reconciliation).
                                    Reconciliation reconciliation = new Reconciliation("connector-watch", connectOperator.kind(),
                                            kafkaConnector.getMetadata().getNamespace(), connectName);

                                    if (!Util.matchesSelector(selector, connect))   {
                                        LOGGER.debugCr(reconciliation, "{} {} in namespace {} was {}, but Connect cluster {} does not match label selector {} and will be ignored", connectorKind, connectorName, connectorNamespace, action, connectName, selectorLabels);
                                        return Future.succeededFuture();
                                    } else if (connect.getSpec() != null && connect.getSpec().getReplicas() == 0)  {
                                        LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}, but Connect cluster {} has 0 replicas", connectorKind, connectorName, connectorNamespace, action, connectName);
                                        updateStatus(reconciliation, connectOperator.zeroReplicas(connectNamespace, connectName), kafkaConnector, connectOperator.connectorOperator);
                                        return Future.succeededFuture();
                                    } else {
                                        LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}", connectorKind, connectorName, connectorNamespace, action);

                                        return connectOperator.withLock(reconciliation, LOCK_TIMEOUT_MS,
                                                () -> connectOperator.reconcileConnectorAndHandleResult(reconciliation,
                                                        KafkaConnectResources.qualifiedServiceName(connectName, connectNamespace), apiClient,
                                                        isUseResources(connect),
                                                        kafkaConnector.getMetadata().getName(), action == Action.DELETED ? null : kafkaConnector)
                                                        .compose(reconcileResult -> {
                                                            LOGGER.infoCr(reconciliation, "reconciled");
                                                            return Future.succeededFuture(reconcileResult);
                                                        }));
                                    }
                                }
                            });
                } else {
                    updateStatus(new Reconciliation("connector-watch", connectOperator.kind(),
                                    kafkaConnector.getMetadata().getNamespace(), null),
                            new InvalidResourceException("Resource lacks label '"
                                    + Labels.STRIMZI_CLUSTER_LABEL
                                    + "': No connect cluster in which to create this connector."),
                            kafkaConnector, connectOperator.connectorOperator);
                }

                break;
            case ERROR:
                LOGGER.errorCr(new Reconciliation("connector-watch", connectorKind, connectName, connectorNamespace), "Failed {} {} in namespace {} ", connectorKind, connectorName, connectorNamespace);
                break;
            default:
                LOGGER.errorCr(new Reconciliation("connector-watch", connectorKind, connectName, connectorNamespace), "Unknown action: {} {} in namespace {}", connectorKind, connectorName, connectorNamespace);
        }
    }

    @Override
    public void onClose(WatcherException e) {
        onClose.accept(e);
    }

    /**
     * Update the status of the Kafka Connector custom resource
     *
     * @param reconciliation        Reconciliation marker
     * @param error                 Throwable indicating if any errors occurred during the reconciliation
     * @param kafkaConnector2       Latest version of the KafkaConnector resource where the status should be updated
     * @param connectorOperations   The KafkaConnector operations for updating the status
     */
    public static void updateStatus(Reconciliation reconciliation, Throwable error, KafkaConnector kafkaConnector2, CrdOperator<?, KafkaConnector, ?> connectorOperations) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnector2, status, error);
        StatusDiff diff = new StatusDiff(kafkaConnector2.getStatus(), status);
        if (!diff.isEmpty()) {
            KafkaConnector copy = new KafkaConnectorBuilder(kafkaConnector2).build();
            copy.setStatus(status);
            connectorOperations.updateStatusAsync(reconciliation, copy);
        }
    }
}
