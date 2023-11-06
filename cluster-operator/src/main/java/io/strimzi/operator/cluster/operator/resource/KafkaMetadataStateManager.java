/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.StatusUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    private String kraftAnno;

    private KafkaAgentClient kafkaAgentClient;
    private boolean isMigrationDone = false;
    private final boolean useKRaftFeatureGateEnabled;

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
        this.useKRaftFeatureGateEnabled = useKRaftFeatureGateEnabled;
        String metadataStateFromKafkaCr = kafkaCr.getStatus() != null ? kafkaCr.getStatus().getKafkaMetadataState() : null;
        // missing metadata state means reconciling an already existing Kafka resource with newer operator supporting metadata state or first reconcile
        if (metadataStateFromKafkaCr == null) {
            this.metadataState = isKRaftAnnoEnabled() ? KafkaMetadataState.KRaft : KafkaMetadataState.ZooKeeper;
        } else {
            this.metadataState = KafkaMetadataState.valueOf(metadataStateFromKafkaCr);
        }
        if (!useKRaftFeatureGateEnabled && (isKRaftAnnoEnabled() || isKRaftAnnoMigration())) {
            LOGGER.errorCr(reconciliation, "Trying to reconcile a KRaft enabled cluster or migrating to KRaft without the useKRaft feature gate enabled");
            throw new IllegalArgumentException("Failed to reconcile a KRaft enabled cluster or migration to KRaft because useKRaft feature gate is disabled");
        }
        LOGGER.infoCr(reconciliation, "Loaded metadata state from the Kafka CR [{}] and strimzi.io/kraft [{}]", this.metadataState, this.kraftAnno);
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
        };
        LOGGER.infoCr(reconciliation, "from [{}] to [{}] with strimzi.io/kraft: [{}]", currentState, metadataState, kraftAnno);
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
            case KRaft -> {
                return KafkaMetadataConfigurationState.KRAFT;
            }
        }
        // this should never happen
        throw new IllegalArgumentException("Invalid internal Kafka metadata state: " + this.metadataState);
    }

    /**
     * Check for the status of the Kafka metadata migration
     *
     * @param reconciliation    Reconciliation information
     * @param clusterCaCertSecret   Secret with the Cluster CA public key
     * @param coKeySecret   Secret with the Cluster CA private key
     * @param controllerPodName Name of the quorum controller leader pod
     */
    public void checkMigrationInProgress(Reconciliation reconciliation, Secret clusterCaCertSecret, Secret coKeySecret, String controllerPodName) {
        if (this.kafkaAgentClient == null) {
            this.kafkaAgentClient = new KafkaAgentClient(reconciliation, reconciliation.name(), reconciliation.namespace(), clusterCaCertSecret, coKeySecret);
        }
        KRaftMigrationState kraftMigrationState = this.kafkaAgentClient.getKRaftMigrationState(controllerPodName);
        LOGGER.infoCr(reconciliation, "ZooKeeper to KRaft migration state {} checked on controller {}", kraftMigrationState.state(), controllerPodName);
        if (kraftMigrationState.state() == -1) {
            throw new RuntimeException("Failed to get the ZooKeeper to KRaft migration state");
        }
        this.isMigrationDone = kraftMigrationState.isMigrationDone();
    }

    /**
     * If the KRaft migration process is in the rollback phase, this method deletes the /controller znode from ZooKeeper
     * to allow the brokers, which are now in ZooKeeper mode again, to elect a new controller among them taking the
     * KRaft controllers out of the picture
     *
     * @param reconciliation    Reconciliation information
     * @param clusterCaCertSecret   Secret with the Cluster CA public key
     * @param coKeySecret   Secret with the Cluster CA private key
     * @param operationTimeoutMs    Timeout to be set on the ZooKeeper request configuration
     * @param zkConnectionString    Connection string to the ZooKeeper ensemble to connect to
     */
    public void maybeDeleteZooKeeperControllerZnode(Reconciliation reconciliation, Secret clusterCaCertSecret, Secret coKeySecret, long operationTimeoutMs, String zkConnectionString) {
        // rollback process ongoing
        if (metadataState.equals(KafkaMetadataState.KRaftDualWriting) && isKRaftAnnoDisabled()) {
            LOGGER.infoCr(reconciliation, "KRaft migration rollback ... going to delete /controller znode");
            // Setup truststore from PEM file in cluster CA secret
            // We cannot use P12 because of custom CAs which for simplicity provide only PEM
            PasswordGenerator pg = new PasswordGenerator(12);
            String trustStorePassword = pg.generate();
            File trustStoreFile = Util.createFileTrustStore(getClass().getName(), "p12", Ca.certs(clusterCaCertSecret), trustStorePassword.toCharArray());

            // Setup keystore from PKCS12 in cluster-operator secret
            String keyStorePassword = new String(Util.decodeFromSecret(coKeySecret, "cluster-operator.password"), StandardCharsets.US_ASCII);
            File keyStoreFile = Util.createFileStore(getClass().getName(), "p12", Util.decodeFromSecret(coKeySecret, "cluster-operator.p12"));
            try {

                ZKClientConfig clientConfig = new ZKClientConfig();

                clientConfig.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
                clientConfig.setProperty("zookeeper.client.secure", "true");
                clientConfig.setProperty("zookeeper.sasl.client", "false");
                clientConfig.setProperty("zookeeper.ssl.trustStore.location", trustStoreFile.getAbsolutePath());
                clientConfig.setProperty("zookeeper.ssl.trustStore.password", trustStorePassword);
                clientConfig.setProperty("zookeeper.ssl.trustStore.type", "PKCS12");
                clientConfig.setProperty("zookeeper.ssl.keyStore.location", keyStoreFile.getAbsolutePath());
                clientConfig.setProperty("zookeeper.ssl.keyStore.password", keyStorePassword);
                clientConfig.setProperty("zookeeper.ssl.keyStore.type", "PKCS12");
                clientConfig.setProperty("zookeeper.request.timeout", String.valueOf(operationTimeoutMs));

                ZooKeeperAdmin admin = new ZooKeeperAdmin(
                        zkConnectionString,
                        10000,
                        watchedEvent -> LOGGER.debugCr(reconciliation, "Received event {} from ZooKeeperAdmin client connected to {}", watchedEvent, zkConnectionString),
                        clientConfig);

                admin.delete("/controller", -1);
                admin.close();
                LOGGER.infoCr(reconciliation, "KRaft migration rollback ... /controller znode deleted");
            } catch (IOException | InterruptedException | KeeperException ex) {
                LOGGER.warnCr(reconciliation, "Failed to delete /controller znode", ex);
            } finally {
                if (trustStoreFile != null) {
                    if (!trustStoreFile.delete())   {
                        LOGGER.warnCr(reconciliation, "Failed to delete file {}", trustStoreFile);
                    }
                }
                if (keyStoreFile != null)   {
                    if (!keyStoreFile.delete())   {
                        LOGGER.warnCr(reconciliation, "Failed to delete file {}", keyStoreFile);
                    }
                }
            }
        } else {
            // TODO: to be removed, just for monitoring/testing
            LOGGER.infoCr(reconciliation, "No KRaft migration rollback ongoing ... no need to delete /controller znode");
        }
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
            kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning",
                    "The strimzi.io/kraft annotation can't be set to migration, rollback or disabled values because the cluster is already KRaft."));
            LOGGER.warnCr(reconciliation, "Warning strimzi.io/kraft: migration|rollback|disabled is not allowed in this state");
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
                kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning",
                        "The strimzi.io/kraft annotation can't be set to enabled because the cluster is ZooKeeper-based." +
                                "If you want to migrate it to be KRaft-based apply the migration value instead."));
                LOGGER.warnCr(reconciliation, "Warning strimzi.io/kraft: enabled is not allowed in this state");
            } else if (isKRaftAnnoRollback()) {
                // set warning condition on Kafka CR status that strimzi.io/kraft: rollback is not allowed in this state
                kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning",
                        "The strimzi.io/kraft annotation can't be set to rollback because the cluster is already ZooKeeper-based." +
                                "There is no migration ongoing to rollback." +
                                "If you want to migrate it to be KRaft-based apply the migration value instead."));
                LOGGER.warnCr(reconciliation, "Warning strimzi.io/kraft: enabled is not allowed in this state");
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
            kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning",
                    "The strimzi.io/kraft annotation can't be set to enabled during a migration process." +
                            "It has to be used in post migration to finalize it and move definitely to KRaft."));
            LOGGER.warnCr(reconciliation, "Warning strimzi.io/kraft: enabled is not allowed in this state");
        }
        if (isKRaftAnnoRollback()) {
            // set warning condition on Kafka CR status that strimzi.io/kraft: rollback is not allowed in this state
            kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning",
                    "The strimzi.io/kraft annotation can't be set to rollback during a migration process." +
                            "It can be used in post migration to start rollback process."));
            LOGGER.warnCr(reconciliation, "Warning strimzi.io/kraft: rollback is not allowed in this state");
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
            kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning",
                    "The strimzi.io/kraft annotation can't be set to enabled during a migration process." +
                            "It has to be used in post migration to finalize it and move definitely to KRaft."));
            LOGGER.warnCr(reconciliation, "Warning strimzi.io/kraft: enabled is not allowed in this state");
        }
        if (isKRaftAnnoRollback()) {
            // set warning condition on Kafka CR status that strimzi.io/kraft: rollback is not allowed in this state
            kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning",
                    "The strimzi.io/kraft annotation can't be set to rollback during dual writing." +
                            "It can be used in post migration to start rollback process."));
            LOGGER.warnCr(reconciliation, "Warning strimzi.io/kraft: rollback is not allowed in this state");
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
            return KafkaMetadataState.KRaft;
        }
        // rollback
        if (isKRaftAnnoRollback()) {
            return KafkaMetadataState.KRaftDualWriting;
        }
        // set warning condition on Kafka CR status that strimzi.io/kraft: migration|disabled are not allowed in this state?
        kafkaStatus.addCondition(StatusUtils.buildWarningCondition("KafkaMetadataStateWarning",
                "The strimzi.io/kraft annotation can't be set to migration or disabled in the post-migration." +
                        "You can use rollback value to come back to ZooKeeper. Use the enabled value to finalize migration instead."));
        LOGGER.warnCr(reconciliation, "Warning strimzi.io/kraft: migration|disabled is not allowed in this state");
        return KafkaMetadataState.KRaftPostMigration;
    }

    /**
     * @return if the metadata migration finished based on corresponding metrics
     */
    private boolean isMigrationDone() {
        return this.isMigrationDone;
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
         *     <dt>ZooKeeper</dt><dd>If the user doesn't create the controllers node poll or doesn't set the strimzi.io/kraft: migration annotation, or he sets any other invalid value for it (enabled or rollback)</dd>
         *     <dt>KRaftMigration</dt><dd>If the user creates the controllers node poll and set the strimzi.io/kraft: migration annotation. The controllers are deployed with ZooKeeper migration enabled and connection to it.</dd>
         * </dl>
         */
        ZooKeeper,

        /**
         * Pre-migration phase is running and migration is going to start.
         * The KRaft controllers were rolled and are now running with ZooKeeper migration enabled, and they are connected to it.
         * Next expected step is rolling brokers with ZooKeeper migration enabled and connection information to controllers configured. Unless user wants to rollback.
         * After rolling brokers, the metadata migration starts. This state represents when it is in progress as well.
         * A first reconcile on this state would roll the brokers, next reconciliations will be used to check the migration status on corresponding metrics.
         * Transitions to:
         * <dl>
         *     <dt>KRaftMigration</dt><dd>If the user keeps the strimzi.io/kraft: migration annotation. The brokers are rolled with ZooKeeper migration enabled and connection to it. It also happens when migration is ongoing or invalid annotation value (enabled and rollback)</dd>
         *     <dt>ZooKeeper</dt><dd>If the user wants to rollback and set strimzi.io/kraft: disabled annotation. The brokers are rolled back with ZooKeeper migration disabled and no connection to controllers.</dd>
         *     <dt>KRaftDualWriting</dt><dd>Metadata migration finished. The cluster is in "dual write" mode. Metadata are written on both ZooKeeper and KRaft controllers.</dd>
         * </dl>
         */
        KRaftMigration,

        /**
         * The cluster is working in "dual write" mode.
         * Metadata are written on both ZooKeeper and KRaft controllers.
         * Next expected step it to finalize the migration and disabling ZooKeeper.
         * Transitions to:
         * <dl>
         *     <dt>KRaftDualWriting</dt><dd>If user applies any invalid values for this state on the strimzi.io/kraft annotation (enabled or rollback)</dd>
         *     <dt>ZooKeeper</dt><dd>If the user wants to rollback and sets strimzi.io/kraft: disabled annotation. The brokers are rolled back with ZooKeeper migration disabled and no connection to controllers.</dd>
         *     <dt>KRaftPostMigration</dt><dd>The user sets the strimzi.io/kraft: enabled annotation. The brokers are rolled with ZooKeeper migration disabled and without connection to it anymore.</dd>
         * </dl>
         */
        KRaftDualWriting,

        /**
         * There is a post-migration phase running.
         * The brokers were rolled and are now running with ZooKeeper migration disabled and without any connection to it anymore.
         * Next expected step is to finalize the migration and disabling ZooKeeper on controllers as well.
         * Transitions to:
         * <dl>
         *     <dt>KraftDualWriting</dt><dd>If user applies the strimzi.io/kraft: rollback annotation because they want to rollback to a ZooKeeper-based cluster.</dd>
         *     <dt>KRaftPostMigration</dt><dd>If user applies any invalid values for this state on the strimzi.io/kraft annotation.</dd>
         *     <dt>KRaft</dt><dd>The strimzi.io/kraft: enabled is still in place, and after brokers, the operator has rolled controllers with ZooKeeper migration disabled and no connection to it anymore. ZooKeeper pods are also deleted if spec.zookeeper was removed.</dd>
         * </dl>
         */
        KRaftPostMigration,

        /**
         * The metadata are stored in KRaft.
         * The strimzi.io/kraft: enabled annotation is set on the Kafka resource.
         * Transitions to:
         * <dl>
         *     <dt>KRaft</dt><dd>If the user sets strimzi.io/kraft: migration annotation but, of course, it's not possible because the cluster is already KRaft-based.</dd>
         *     <dt>KRaft</dt><dd>If the user sets strimzi.io/kraft: rollback or disabled annotation which can't be used to rollback to be ZooKeeper-based from this state.</dd>
         * </dl>
         */
        KRaft
    }
}
