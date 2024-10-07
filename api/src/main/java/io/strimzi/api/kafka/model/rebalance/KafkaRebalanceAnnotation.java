/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

public enum KafkaRebalanceAnnotation {
    /**
     * No annotation set on the rebalance resource.
     */
    none,
    /**
     * Used to approve a rebalance proposal and trigger the actual rebalancing.
     * This value should only be used when in the {@code ProposalReady} state.
     */
    approve,
    /**
     * Used to stop a request for an actual ongoing rebalancing.
     * This value should only be used when in the {@code Rebalancing} state.
     */
    stop,
    /**
     * Used to refresh a ready rebalance proposal or to restart a stopped request for getting a rebalance proposal.
     * This value should only be used when in the {@code ProposalReady} or {@code Stopped} states.
     */
    refresh,
    /**
     * Used to represent a KafkaRebalance custom resource that represents a configuration template.
     * This is used for auto-rebalancing on scaling to define the configuration template to be used for a specific
     * rebalancing mode, as add-brokers or remove-brokers.
     * When this annotation is applied to a KafkaRebalance custom resource, it doesn't trigger any actual auto-rebalance,
     * instead the resource is just ignored.
     */
    template,
    /**
     * Any other unsupported/unknown annotation value.
     */
    unknown
}
