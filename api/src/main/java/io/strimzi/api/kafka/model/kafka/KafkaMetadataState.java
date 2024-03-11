/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents where metadata are stored for the current cluster (ZooKeeper or KRaft)
 * or if a migration from ZooKeeper to KRaft is in progress and in which phase
 */
public enum KafkaMetadataState {

    /**
     * The metadata are stored in ZooKeeper.
     * The strimzi.io/kraft: disabled annotation is set on the Kafka resource.
     * Waiting for the user to create the controllers pool and start the migration.
     * Transitions to:
     * <dl>
     *     <dt>ZooKeeper</dt><dd>If the user hasn't created the controller node pool yet or set the strimzi.io/kraft: migration annotation, or set it to invalid value (e.g. enabled or rollback)</dd>
     *     <dt>KRaftMigration</dt><dd>If user has created the controller node pool and set the strimzi.io/kraft: migration annotation. The controllers are deployed with ZooKeeper migration enabled and connection to it.</dd>
     * </dl>
     */
    ZooKeeper,

    /**
     * Pre-migration phase is running and migration is going to start.
     * The KRaft controllers were rolled and are now running with ZooKeeper migration enabled, and they are connected to it.
     * Next expected step is to roll brokers with ZooKeeper migration enabled and connection information to controllers configured. Unless user wants to rollback.
     * After rolling brokers, the metadata migration starts. This state represents when it is in progress as well.
     * A first reconcile on this state would roll the brokers, next reconciliations will be used to check the migration status on corresponding metrics.
     * Transitions to:
     * <dl>
     *     <dt>KRaftMigration</dt><dd>If the user keeps the strimzi.io/kraft: migration annotation. The brokers are rolled with ZooKeeper migration enabled and connection information to controllers configured. It is also in this phase, when the migration is still ongoing or has invalid annotation value (enabled and rollback)</dd>
     *     <dt>ZooKeeper</dt><dd>If the user wants to rollback and set strimzi.io/kraft: disabled annotation. The brokers are rolled back with ZooKeeper migration disabled and no connection to controllers.</dd>
     *     <dt>KRaftDualWriting</dt><dd>Metadata migration finished. The cluster is in "dual write" mode. Metadata are written on both ZooKeeper and KRaft controllers.</dd>
     * </dl>
     */
    KRaftMigration,

    /**
     * The cluster is working in "dual write" mode.
     * Metadata are written on both ZooKeeper and KRaft controllers.
     * Next expected step is to finalize the migration and disable ZooKeeper.
     * Transitions to:
     * <dl>
     *     <dt>KRaftDualWriting</dt><dd>If user applies any invalid values for this state on the strimzi.io/kraft annotation (enabled or rollback)</dd>
     *     <dt>ZooKeeper</dt><dd>If the user wants to rollback and sets strimzi.io/kraft: disabled annotation. The brokers are rolled back with ZooKeeper migration disabled and no connection to controllers.</dd>
     *     <dt>KRaftPostMigration</dt><dd>The user keeps the strimzi.io/kraft: migration annotation. The brokers are rolled with ZooKeeper migration disabled and without connection to it anymore.</dd>
     * </dl>
     */
    KRaftDualWriting,

    /**
     * There is a post-migration phase running.
     * The brokers were rolled and are now running with ZooKeeper migration disabled and without any connection to it anymore.
     * Next expected step is to finalize the migration and disable ZooKeeper on controllers as well.
     * Transitions to:
     * <dl>
     *     <dt>KraftDualWriting</dt><dd>If user applies the strimzi.io/kraft: rollback annotation because they want to rollback to a ZooKeeper-based cluster.</dd>
     *     <dt>KRaftPostMigration</dt><dd>If user applies any invalid values for this state on the strimzi.io/kraft annotation.</dd>
     *     <dt>PreKRaft</dt><dd>The strimzi.io/kraft: enabled is still in place, and after brokers, the operator has rolled controllers with ZooKeeper migration disabled and no connection to it anymore. ZooKeeper pods are still running.</dd>
     * </dl>
     */
    KRaftPostMigration,

    /**
     * The matadata are stored in KRaft.
     * The strimzi.io/kraft: enabled annotation is set on the Kafka resource.
     * ZooKeeper pods are still running and they will be removed during migration finalization when moving to KRaft.
     * Transitions to:
     * <dl>
     *     <dt>KRaft</dt><dd>The strimzi.io/kraft: enabled is still in place, and after brokers, the operator has rolled controllers with ZooKeeper migration disabled and no connection to it anymore. ZooKeeper pods are also deleted.</dd>
     * </dl>
     */
    PreKRaft,

    /**
     * The metadata are stored in KRaft.
     * The strimzi.io/kraft: enabled annotation is set on the Kafka resource.
     * Transitions to:
     * <dl>
     *     <dt>KRaft</dt><dd>If the user sets strimzi.io/kraft: migration annotation but, of course, it's not possible because the cluster is already KRaft-based.</dd>
     *     <dt>KRaft</dt><dd>If the user sets strimzi.io/kraft: rollback or disabled annotation which can't be used to rollback to be ZooKeeper-based from this state.</dd>
     * </dl>
     */
    KRaft;

    @JsonCreator
    public static KafkaMetadataState forValue(String value) {
        switch (value) {
            case "ZooKeeper":
                return ZooKeeper;
            case "KRaftMigration":
                return KRaftMigration;
            case "KRaftDualWriting":
                return KRaftDualWriting;
            case "KRaftPostMigration":
                return KRaftPostMigration;
            case "PreKRaft":
                return PreKRaft;
            case "KRaft":
                return KRaft;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case ZooKeeper:
                return "ZooKeeper";
            case KRaftMigration:
                return "KRaftMigration";
            case KRaftDualWriting:
                return "KRaftDualWriting";
            case KRaftPostMigration:
                return "KRaftPostMigration";
            case PreKRaft:
                return "PreKRaft";
            case KRaft:
                return "KRaft";
            default:
                return null;
        }
    }
}
