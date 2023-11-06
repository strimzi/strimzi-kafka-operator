/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.PasswordGenerator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Utility class for ZooKeeper to KRaft migration purposes
 */
public class KRaftMigrationUtils {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KRaftMigrationUtils.class.getName());

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
    public static void deleteZooKeeperControllerZnode(Reconciliation reconciliation, Secret clusterCaCertSecret, Secret coKeySecret, long operationTimeoutMs, String zkConnectionString) {
        // Setup truststore from PEM file in cluster CA secret
        // We cannot use P12 because of custom CAs which for simplicity provide only PEM
        PasswordGenerator pg = new PasswordGenerator(12);
        String trustStorePassword = pg.generate();
        File trustStoreFile = Util.createFileTrustStore(KRaftMigrationUtils.class.getName(), "p12", Ca.certs(clusterCaCertSecret), trustStorePassword.toCharArray());

        // Setup keystore from PKCS12 in cluster-operator secret
        String keyStorePassword = new String(Util.decodeFromSecret(coKeySecret, "cluster-operator.password"), StandardCharsets.US_ASCII);
        File keyStoreFile = Util.createFileStore(KRaftMigrationUtils.class.getName(), "p12", Util.decodeFromSecret(coKeySecret, "cluster-operator.p12"));
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
    }
}
