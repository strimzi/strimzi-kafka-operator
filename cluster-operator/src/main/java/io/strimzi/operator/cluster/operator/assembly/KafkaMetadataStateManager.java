/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.cluster.model.KafkaMetadataConfigurationState;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.StatusUtils;

import java.util.Locale;

/**
 * Class used to reconcile the metadata state which represents where metadata are stored (ZooKeeper or KRaft)
 * It is also used to compute metadata state changes during the migration process from ZooKeeper to KRaft (or rollback)
 */
public class KafkaMetadataStateManager {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaMetadataStateManager.class.getName());

    /**
     * The annotation value which indicates that the KRaft mode is enabled
     */
    public static final String ENABLED_VALUE_STRIMZI_IO_KRAFT = "enabled";

    /**
     * The annotation value which indicates that the ZooKeeper to KRaft migration is enabled
     */
    public static final String MIGRATION_VALUE_STRIMZI_IO_KRAFT = "migration";

    /**
     * The annotation value which indicates that the ZooKeeper mode is enabled
     */
    public static final String DISABLED_VALUE_STRIMZI_IO_KRAFT = "disabled";

    /**
     * The annotation value which indicates that the KRaft to ZooKeeper rollback is enabled
     */
    public static final String ROLLBACK_VALUE_STRIMZI_IO_KRAFT = "rollback";

    private final Reconciliation reconciliation;

    private KafkaMetadataState metadataState;

    private final String kraftAnno;

    private boolean isMigrationDone = false;

    /**
     * Constructor
     *
     * @param reconciliation Reconciliation information
     * @param kafkaCr instance of the Kafka CR
     * @param useKRaftFeatureGateEnabled if the UseKRaft feature gate is enabled on the operator
     */
    public KafkaMetadataStateManager(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            boolean useKRaftFeatureGateEnabled) {
        this.reconciliation = reconciliation;
        this.kraftAnno = kraftAnnotation(kafkaCr);
        KafkaMetadataState metadataStateFromKafkaCr = kafkaCr.getStatus() != null ? kafkaCr.getStatus().getKafkaMetadataState() : null;
        // missing metadata state means reconciling an already existing Kafka resource with newer operator supporting metadata state or first reconcile
        if (metadataStateFromKafkaCr == null) {
            this.metadataState = isKRaftAnnoEnabled() ? KafkaMetadataState.KRaft : KafkaMetadataState.ZooKeeper;
        } else {
            this.metadataState = metadataStateFromKafkaCr;
        }
        if (!useKRaftFeatureGateEnabled && (isKRaftAnnoEnabled() || isKRaftAnnoMigration())) {
            LOGGER.errorCr(reconciliation, "Trying to reconcile a KRaft enabled cluster or migrating to KRaft without the useKRaft feature gate enabled");
            throw new IllegalArgumentException("Failed to reconcile a KRaft enabled cluster or migration to KRaft because useKRaft feature gate is disabled");
        }
        LOGGER.debugCr(reconciliation, "Loaded metadata state from the Kafka CR [{}] and strimzi.io/kraft annotation [{}]", this.metadataState, this.kraftAnno);
    }

    /**
     * Computes the next state in the metadata migration Finite State Machine (FSM)
     * based on the current state and the strimzi.io/kraft annotation value at the
     * beginning of the reconciliation when this instance is created
     *
     * @param kafkaStatus Status of the Kafka custom resource where warnings about any issues with metadata state will be added
     *
     * @return the next FSM state
     */
    public KafkaMetadataState computeNextMetadataState(KafkaStatus kafkaStatus) {
        KafkaMetadataState currentState = metadataState;
        metadataState = switch (currentState) {
            case KRaft -> onKRaft(kafkaStatus);
            case ZooKeeper -> onZooKeeper(kafkaStatus);
            case KRaftMigration -> onKRaftMigration(kafkaStatus);
            case KRaftDualWriting -> onKRaftDualWriting(kafkaStatus);
            case KRaftPostMigration -> onKRaftPostMigration(kafkaStatus);
            case PreKRaft -> onPreKRaft(kafkaStatus);
        };
        if (metadataState != currentState) {
            LOGGER.infoCr(reconciliation, "Transitioning metadata state from [{}] to [{}] with strimzi.io/kraft annotation [{}]", currentState, metadataState, kraftAnno);
        } else {
            LOGGER.debugCr(reconciliation, "Metadata state [{}] with strimzi.io/kraft annotation [{}]", metadataState, kraftAnno);
        }
        return metadataState;
    }

    /**
     * Get the next desired nodes configuration related state based on the current internal FSM state.
     * Starting from the current internal FSM state and taking into account the strimzi.io/kraft annotation value,
     * it returns a state representing the desired configuration for nodes (brokers and/or controllers)
     *
     * @return the next desired nodes configuration related state
     */
    public KafkaMetadataConfigurationState getMetadataConfigurationState() {
        switch (metadataState) {
            case ZooKeeper -> {
                // cluster is still using ZooKeeper, but controllers need to be deployed with ZooKeeper and migration enabled
                if (isKRaftAnnoMigration()) {
                    return KafkaMetadataConfigurationState.PRE_MIGRATION;
                } else {
                    return KafkaMetadataConfigurationState.ZK;
                }
            }
            case KRaftMigration -> {
                if (isKRaftAnnoMigration()) {
                    // ZooKeeper configured and migration enabled on both controllers and brokers
                    return KafkaMetadataConfigurationState.MIGRATION;
                } else {
                    // ZooKeeper rolled back on brokers
                    return KafkaMetadataConfigurationState.ZK;
                }
            }
            case KRaftDualWriting -> {
                if (isKRaftAnnoDisabled()) {
                    // ZooKeeper rolled back on brokers
                    return KafkaMetadataConfigurationState.ZK;
                } else if (isKRaftAnnoMigration()) {
                    // ZooKeeper still configured on controllers, removed on brokers
                    return KafkaMetadataConfigurationState.POST_MIGRATION;
                } else if (isKRaftAnnoRollback()) {
                    // a rollback is going on, back in dual writing with ZooKeeper configured on both controllers and brokers
                    return KafkaMetadataConfigurationState.MIGRATION;
                }
            }
            case KRaftPostMigration -> {
                if (isKRaftAnnoEnabled()) {
                    return KafkaMetadataConfigurationState.KRAFT;
                // rollback
                } else if (isKRaftAnnoRollback()) {
                    // ZooKeeper configured and migration enabled on both controllers and brokers
                    return KafkaMetadataConfigurationState.MIGRATION;
                } else {
                    // ZooKeeper still configured on controllers, removed on brokers
                    return KafkaMetadataConfigurationState.POST_MIGRATION;
                }
            }
            case PreKRaft, KRaft -> {
                return KafkaMetadataConfigurationState.KRAFT;
            }
        }
        // this should never happen
        throw new IllegalArgumentException("Invalid internal Kafka metadata state [" + this.metadataState + "] with strimzi.io/kraft annotation [" + this.kraftAnno + "]");
    }

    /**
     * @return true if the ZooKeeper ensemble has to be deleted because KRaft migration is done. False otherwise.
     */
    public boolean shouldDestroyZooKeeperNodes() {
        return this.metadataState.equals(KafkaMetadataState.PreKRaft) && isKRaftAnnoEnabled();
    }

    /**
     * Handles the transition from the {@code Kraft} state
     *
     * @param kafkaStatus Status of the Kafka custom resource where warnings about any issues with metadata state will be added
     *
     * @return next state
     */
    private KafkaMetadataState onKRaft(KafkaStatus kafkaStatus) {
        if (isKRaftAnnoMigration() || isKRaftAnnoRollback() || isKRaftAnnoDisabled()) {
            // set warning condition on Kafka CR status that strimzi.io/kraft: migration|rollback|disabled are not allowed in this state
            addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'migration', 'rollback' or 'disabled' because the cluster is already KRaft.");
        }
        return KafkaMetadataState.KRaft;
    }

    /**
     * Handles the transition from the {@code ZooKeeper} state
     *
     * @param kafkaStatus Status of the Kafka custom resource where warnings about any issues with metadata state will be added
     *
     * @return next state
     */
    private KafkaMetadataState onZooKeeper(KafkaStatus kafkaStatus) {
        if (!isKRaftAnnoMigration()) {
            if (isKRaftAnnoEnabled()) {
                // set warning condition on Kafka CR status that strimzi.io/kraft: enabled is not allowed in this state
                addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'enabled' because the cluster is ZooKeeper-based. " +
                        "If you want to migrate it to be KRaft-based apply the 'migration' value instead.");
            } else if (isKRaftAnnoRollback()) {
                // set warning condition on Kafka CR status that strimzi.io/kraft: rollback is not allowed in this state
                addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'rollback' because the cluster is already ZooKeeper-based. " +
                        "There is no migration ongoing to rollback. If you want to migrate it to be KRaft-based apply the 'migration' value instead.");
            }
            return KafkaMetadataState.ZooKeeper;
        }
        return KafkaMetadataState.KRaftMigration;
    }

    /**
     * Handles the transition from the {@code KRaftMigration} state
     *
     * @param kafkaStatus Status of the Kafka custom resource where warnings about any issues with metadata state will be added
     *
     * @return next state
     */
    private KafkaMetadataState onKRaftMigration(KafkaStatus kafkaStatus) {
        if (isKRaftAnnoMigration()) {
            // migration completed
            if (isMigrationDone()) {
                return KafkaMetadataState.KRaftDualWriting;
            } else {
                return KafkaMetadataState.KRaftMigration;
            }
        }
        // rollback
        if (isKRaftAnnoDisabled()) {
            return KafkaMetadataState.ZooKeeper;
        }
        if (isKRaftAnnoEnabled()) {
            // set warning condition on Kafka CR status that strimzi.io/kraft: enabled is not allowed in this state
            addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'enabled' during a migration process. " +
                    "It has to be used in post migration to finalize it and move definitely to KRaft.");
        }
        if (isKRaftAnnoRollback()) {
            // set warning condition on Kafka CR status that strimzi.io/kraft: rollback is not allowed in this state
            addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'rollback' during a migration process. " +
                    "It can be used in post migration to start rollback process.");
        }
        return KafkaMetadataState.KRaftMigration;
    }

    /**
     * Handles the transition from the {@code KRaftDualWriting} state
     *
     * @param kafkaStatus Status of the Kafka custom resource where warnings about any issues with metadata state will be added
     *
     * @return next state
     */
    private KafkaMetadataState onKRaftDualWriting(KafkaStatus kafkaStatus) {
        if (isKRaftAnnoMigration()) {
            return KafkaMetadataState.KRaftPostMigration;
        }
        // rollback
        if (isKRaftAnnoDisabled()) {
            return KafkaMetadataState.ZooKeeper;
        }
        if (isKRaftAnnoEnabled()) {
            // set warning condition on Kafka CR status that strimzi.io/kraft: enabled is not allowed in this state
            addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'enabled' during a migration process. " +
                    "It has to be used in post migration to finalize it and move definitely to KRaft.");
        }
        if (isKRaftAnnoRollback()) {
            // set warning condition on Kafka CR status that strimzi.io/kraft: rollback is not allowed in this state
            addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'rollback' during dual writing. " +
                    "It can be used in post migration to start rollback process.");
        }
        return KafkaMetadataState.KRaftDualWriting;
    }

    /**
     * Handles the transition from the {@code KRaftPostMigration} state
     *
     * @param kafkaStatus Status of the Kafka custom resource where warnings about any issues with metadata state will be added
     *
     * @return next state
     */
    private KafkaMetadataState onKRaftPostMigration(KafkaStatus kafkaStatus) {
        if (isKRaftAnnoEnabled()) {
            return KafkaMetadataState.PreKRaft;
        }
        // rollback
        if (isKRaftAnnoRollback()) {
            return KafkaMetadataState.KRaftDualWriting;
        }
        // set warning condition on Kafka CR status that strimzi.io/kraft: migration|disabled are not allowed in this state
        addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'migration' or 'disabled' in the post-migration. " +
                "You can use 'rollback' value to come back to ZooKeeper. Use the 'enabled' value to finalize migration instead.");
        return KafkaMetadataState.KRaftPostMigration;
    }

    /**
     * Handles the transition from the {@code PreKRaft} state
     *
     * @param kafkaStatus Status of the Kafka custom resource where warnings about any issues with metadata state will be added
     *
     * @return next state
     */
    private KafkaMetadataState onPreKRaft(KafkaStatus kafkaStatus) {
        if (isKRaftAnnoEnabled()) {
            return KafkaMetadataState.KRaft;
        }
        // set warning condition on Kafka CR status that strimzi.io/kraft: migration|disabled|rollback are not allowed in this state
        addConditionAndLog(kafkaStatus, "The strimzi.io/kraft annotation can't be set to 'migration', 'disabled' or 'rollback' in the pre-kraft. " +
                "Use the 'enabled' value to finalize migration and removing ZooKeeper.");
        return KafkaMetadataState.PreKRaft;
    }

    /**
     * @return if the metadata migration finished based on corresponding metrics
     */
    private boolean isMigrationDone() {
        return this.isMigrationDone;
    }

    /**
     * Set if the migration was done or not
     *
     * @param migrationDone if the migration was done or not
     */
    public void setMigrationDone(boolean migrationDone) {
        isMigrationDone = migrationDone;
    }

    /**
     * @return if the metadata migration rollback is going on from dual-write to ZooKeeper
     */
    public boolean isRollingBack() {
        return metadataState.equals(KafkaMetadataState.KRaftDualWriting) && isKRaftAnnoDisabled();
    }

    /**
     * Gets the value of strimzi.io/kraft annotation on the provided Kafka CR
     *
     * @param kafkaCr Kafka CR from which getting the value of strimzi.io/kraft annotation
     * @return the value of strimzi.io/kraft annotation on the provided Kafka CR
     */
    private String kraftAnnotation(Kafka kafkaCr) {
        return Annotations.stringAnnotation(kafkaCr, Annotations.ANNO_STRIMZI_IO_KRAFT, DISABLED_VALUE_STRIMZI_IO_KRAFT).toLowerCase(Locale.ENGLISH);
    }

    /**
     * @return if strimzi.io/kraft is "migration", as per stored annotation in this metadata state manager instance
     */
    private boolean isKRaftAnnoMigration() {
        return MIGRATION_VALUE_STRIMZI_IO_KRAFT.equals(kraftAnno);
    }

    /**
     * @return if strimzi.io/kraft is "enabled", as per stored annotation in this metadata state manager instance
     */
    private boolean isKRaftAnnoEnabled() {
        return ENABLED_VALUE_STRIMZI_IO_KRAFT.equals(kraftAnno);
    }

    /**
     * @return if strimzi.io/kraft is "disabled", as per stored annotation in this metadata state manager instance
     */
    private boolean isKRaftAnnoDisabled() {
        return DISABLED_VALUE_STRIMZI_IO_KRAFT.equals(kraftAnno);
    }

    /**
     * @return if strimzi.io/kraft is "rollback", as per stored annotation in this metadata state manager instance
     */
    private boolean isKRaftAnnoRollback() {
        return ROLLBACK_VALUE_STRIMZI_IO_KRAFT.equals(kraftAnno);
    }

    /**
     * Add a warning condition to the KafkaStatus instance and log a warning message as well
     *
     * @param kafkaStatus   KafkaStatus instance to be updated with the warning condition
     * @param message   Message to be added on the warning condition and logged
     */
    private void addConditionAndLog(KafkaStatus kafkaStatus, String message) {
        kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning", message));
        LOGGER.warnCr(reconciliation, message);
    }
}
