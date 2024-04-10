/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.cluster.operator.resource.DefaultZooKeeperAdminProvider;
import io.strimzi.operator.cluster.operator.resource.KRaftMigrationState;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.admin.ZooKeeperAdmin;

import java.io.File;
import java.io.IOException;

/**
 * Utility class for ZooKeeper to KRaft migration purposes
 */
public class KRaftMigrationUtils {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KRaftMigrationUtils.class.getName());

    /**
     * This method deletes the /controller znode from ZooKeeper to allow the brokers, which are now in ZooKeeper mode again,
     * to elect a new controller among them taking the KRaft controllers out of the picture.
     *
     * @param reconciliation        Reconciliation information
     * @param coTlsPemIdentity      Trust set and identity for TLS client authentication for connecting to ZooKeeper
     * @param operationTimeoutMs    Timeout to be set on the ZooKeeper request configuration
     * @param zkConnectionString    Connection string to the ZooKeeper ensemble to connect to
     */
    public static void deleteZooKeeperControllerZnode(Reconciliation reconciliation, TlsPemIdentity coTlsPemIdentity, long operationTimeoutMs, String zkConnectionString) {
        // Setup truststore from PEM file in cluster CA secret
        File trustStoreFile = Util.createFileStore(KRaftMigrationUtils.class.getName(), PemTrustSet.CERT_SUFFIX, coTlsPemIdentity.pemTrustSet().trustedCertificatesPemBytes());

        // Setup keystore from PEM in cluster-operator secret
        File keyStoreFile = Util.createFileStore(KRaftMigrationUtils.class.getName(), PemAuthIdentity.PEM_SUFFIX, coTlsPemIdentity.pemAuthIdentity().pemKeyStore());
        try {
            ZooKeeperAdmin admin = new DefaultZooKeeperAdminProvider().createZookeeperAdmin(
                    zkConnectionString,
                    10000,
                    watchedEvent -> LOGGER.debugCr(reconciliation, "Received event {} from ZooKeeperAdmin client connected to {}", watchedEvent, zkConnectionString),
                    operationTimeoutMs,
                    trustStoreFile.getAbsolutePath(),
                    keyStoreFile.getAbsolutePath()
                    );
            admin.delete("/controller", -1);
            admin.close();
            LOGGER.infoCr(reconciliation, "Deleted the '/controller' znode as part of the KRaft migration rollback");
        } catch (IOException | InterruptedException | KeeperException ex) {
            LOGGER.warnCr(reconciliation, "Failed to delete '/controller' znode", ex);
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
    }

    /**
     * Check for the status of the Kafka metadata migration
     *
     * @param reconciliation    Reconciliation information
     * @param kafkaAgentClient  KafkaAgentClient instance to query the agent endpoint for migration state metric
     * @param controllerPodName Name of the quorum controller leader pod
     *
     * @return true if the migration is done, false otherwise
     */
    public static boolean checkMigrationInProgress(Reconciliation reconciliation, KafkaAgentClient kafkaAgentClient, String controllerPodName) {
        KRaftMigrationState kraftMigrationState = kafkaAgentClient.getKRaftMigrationState(controllerPodName);
        LOGGER.debugCr(reconciliation, "ZooKeeper to KRaft migration state {} checked on controller {}", kraftMigrationState.state(), controllerPodName);
        if (kraftMigrationState.state() == KRaftMigrationState.UNKNOWN) {
            throw new RuntimeException("Failed to get the ZooKeeper to KRaft migration state");
        }
        return kraftMigrationState.isMigrationDone();
    }
}
