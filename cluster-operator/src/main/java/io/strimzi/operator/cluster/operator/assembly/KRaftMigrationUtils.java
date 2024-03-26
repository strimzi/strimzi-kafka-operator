/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.cluster.operator.resource.KRaftMigrationState;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.TlsPkcs12Identity;
import io.strimzi.operator.common.model.PasswordGenerator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;

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
    public static void deleteZooKeeperControllerZnode(Reconciliation reconciliation, TlsPkcs12Identity coTlsPemIdentity, long operationTimeoutMs, String zkConnectionString) {
        PasswordGenerator pg = new PasswordGenerator(12);
        // Setup truststore from PEM file in cluster CA secret
        // We cannot use P12 because of custom CAs which for simplicity provide only PEM
        String trustStorePassword = pg.generate();
        File trustStoreFile = Util.createFileTrustStore(KRaftMigrationUtils.class.getName(), "p12", coTlsPemIdentity.pemTrustSet().trustedCertificates(), trustStorePassword.toCharArray());

        // Setup keystore from PKCS12 in cluster-operator secret
        String keyStorePassword = coTlsPemIdentity.pkcs12AuthIdentity().password();
        File keyStoreFile = Util.createFileStore(KRaftMigrationUtils.class.getName(), "p12", coTlsPemIdentity.pkcs12AuthIdentity().keystore());
        try {
            ZooKeeperAdmin admin = createZooKeeperAdminClient(reconciliation, zkConnectionString, operationTimeoutMs,
                    trustStoreFile, trustStorePassword, keyStoreFile, keyStorePassword);
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

    /**
     * Create a ZooKeeperAdmin client instance to interact with the ZooKeeper ensamble for migration rollback purposes
     *
     * @param reconciliation    Reconciliation information
     * @param zkConnectionString    Connection information to ZooKeeper
     * @param operationTimeoutMs    Timeout for ZooKeeper requests
     * @param trustStoreFile    File hosting the truststore with TLS certificates to use to connect to ZooKeeper
     * @param trustStorePassword    Password for accessing the truststore
     * @param keyStoreFile  File hosting the keystore with TLS private keys to use to connect to ZooKeeper
     * @param keyStorePassword  Password for accessing the keystore
     * @return  A ZooKeeperAdmin instance
     * @throws IOException
     */
    private static ZooKeeperAdmin createZooKeeperAdminClient(Reconciliation reconciliation, String zkConnectionString, long operationTimeoutMs,
                                                             File trustStoreFile, String trustStorePassword, File keyStoreFile, String keyStorePassword) throws IOException {
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

        return new ZooKeeperAdmin(
                zkConnectionString,
                10000,
                watchedEvent -> LOGGER.debugCr(reconciliation, "Received event {} from ZooKeeperAdmin client connected to {}", watchedEvent, zkConnectionString),
                clientConfig);
    }
}
