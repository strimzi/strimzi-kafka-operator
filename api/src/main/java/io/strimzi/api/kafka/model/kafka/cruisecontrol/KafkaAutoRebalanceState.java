/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

public enum KafkaAutoRebalanceState {

    /**
     * Initial state with a new auto-rebalancing initiated when scaling down/up operations were requested.
     * This is also the ending state after an auto-rebalancing completed successfully or failed.
     * Transitions to:
     * <dl>
     *     <dt>RebalanceOnScaleDown</dt>
     *     <dd>
     *          if a scale down operation was requested. This transition happens even if a scale up was requested at the same time
     *          but the rebalancing on scaling down has the precedence. The rebalancing on scale up is queued. They will run sequentially.
     *     </dd>
     *     <dt>RebalanceOnScaleUp</dt><dd>if only a scale up operation was requested. There was no scale down operation requested.</dd>
     * </dl>
     */
    Idle,

    /**
     * A rebalancing related to a scale down operation is running.
     * Transitions to:
     * <dl>
     *     <dt>RebalanceOnScaleDown</dt><dd>if a rebalancing on scale down is still running or another one was requested while the first one ended.</dd>
     *     <dt>RebalanceOnScaleUp</dt>
     *     <dd>
     *          if a scale down operation was requested together with a scale up and, because they run sequentially,
     *          the rebalance on scale down had the precedence, was executed first and completed successfully.
     *          We can now move on with rebalancing for the scale up.
     *     </dd>
     *     <dt>Idle</dt><dd>if only a scale down operation was requested, it was executed and completed successfully or failed.</dd>
     * </dl>
     */
    RebalanceOnScaleDown,

    /**
     * A rebalancing related to a scale up operation is running.
     * Transitions to:
     * <dl>
     *     <dt>RebalanceOnScaleUp</dt><dd>if a rebalancing on scale up is still running or another one was requested while the first one ended.</dd>
     *     <dt>RebalanceOnScaleDown</dt>
     *     <dd>
     *          if a scale down operation was requested, so the current rebalancing scale up is stopped (and queued) and
     *          a new rebalancing scale down is started. The rebalancing scale up will be postponed.
     *     </dd>
     *     <dt>Idle</dt><dd>if a scale up operation was requested, it was executed and completed successfully or failed.</dd>
     * </dl>
     */
    RebalanceOnScaleUp
}
