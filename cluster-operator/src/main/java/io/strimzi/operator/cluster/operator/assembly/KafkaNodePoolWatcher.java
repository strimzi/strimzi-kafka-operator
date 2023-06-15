/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Watcher implementation for watching node pools and triggering Kafka reconciliations based on changes to node pool
 * configurations.
 */
public class KafkaNodePoolWatcher implements Watcher<KafkaNodePool> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaNodePoolWatcher.class);

    private final String namespace;
    private final Optional<LabelSelector> selector;
    private final KafkaAssemblyOperator kafkaAssemblyOperator;
    private final CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
    private final Consumer<WatcherException> onClose;

    /**
     * Constructs the KafkaNodePool watcher
     *
     * @param namespace                 Namespace to watch (or asterisk for all namespaces)
     * @param selector                  The selector to check if the Kafka cluster is operated by this instance of the CLuster Operator
     * @param kafkaAssemblyOperator     Kafka assembly operator fpr operating Kafka clusters
     * @param kafkaOperator             Kafka operator for managing the Kafka custom resources
     * @param onClose                   On-close method which is called when the watch is closed
     */
    public KafkaNodePoolWatcher(String namespace, Optional<LabelSelector> selector, KafkaAssemblyOperator kafkaAssemblyOperator, CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator, Consumer<WatcherException> onClose) {
        this.namespace = namespace;
        this.selector = selector;
        this.kafkaAssemblyOperator = kafkaAssemblyOperator;
        this.kafkaOperator = kafkaOperator;
        this.onClose = onClose;
    }

    /**
     * Handles the event received from the watch
     *
     * @param action    Action describing the event
     * @param resource  Resource to which the event happened
     */
    @Override
    public void eventReceived(Action action, KafkaNodePool resource) {
        switch (action) {
            case ADDED:
            case DELETED:
            case MODIFIED:
                maybeEnqueueReconciliation(action, resource);
                break;
            case ERROR:
                LOGGER.errorCr(new Reconciliation("watch", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), "Error action: {} {} in namespace {} ", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName());
                kafkaAssemblyOperator.reconcileAll("watch error", this.namespace, ignored -> { });
                break;
            default:
                LOGGER.errorCr(new Reconciliation("watch", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), "Unknown action: {} in namespace {}", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName());
                kafkaAssemblyOperator.reconcileAll("watch unknown", this.namespace, ignored -> { });
        }
    }

    /**
     * Checks the KafkaNodePool resource and decides if a reconciliation should be triggered. This decision is based on
     * whether there is a matching Kafka resource, if it matches the seelctor etc.
     *
     * @param action    Action describing the event
     * @param resource  KafkaNodePool resource to which the event happened
     */
    private void maybeEnqueueReconciliation(Action action, KafkaNodePool resource) {
        if (resource.getMetadata().getLabels() != null
                && resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL) != null)    {
            String kafkaName = resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
            Kafka kafka = kafkaOperator.get(resource.getMetadata().getNamespace(), kafkaName);

            if (kafka != null
                    && Util.matchesSelector(selector, kafka)) {
                if (ReconcilerUtils.nodePoolsEnabled(kafka)) {
                    Reconciliation reconciliation = new Reconciliation("watch", kafkaAssemblyOperator.kind(), kafka.getMetadata().getNamespace(), kafkaName);
                    LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action);
                    kafkaAssemblyOperator.reconcile(reconciliation);
                } else {
                    LOGGER.warnOp("{} {} in namespace {} was {}, but the Kafka cluster {} to which it belongs does not have {} support enabled", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action, kafkaName, resource.getKind());
                }
            } else if (kafka == null) {
                LOGGER.warnOp("{} {} in namespace {} was {}, but the Kafka cluster {} to which it belongs does not exist", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action, kafkaName);
            } else {
                LOGGER.debugOp("{} {} in namespace {} was {}, but the Kafka cluster {} to which it belongs is not managed by this operator instance", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action, kafkaName);
            }
        } else {
            LOGGER.warnOp("{} {} in namespace {} was {}, but does not contain {} label", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action, Labels.STRIMZI_CLUSTER_LABEL);
        }
    }

    @Override
    public void onClose(WatcherException e) {
        onClose.accept(e);
    }
}
