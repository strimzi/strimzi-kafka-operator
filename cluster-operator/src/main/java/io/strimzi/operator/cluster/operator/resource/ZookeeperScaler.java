/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Class for scaling Zookeeper 3.5 using the ZookeeperAdmin client
 */
public class ZookeeperScaler implements AutoCloseable {
    private static final Logger log = LogManager.getLogger(ZookeeperScaler.class);

    private final Vertx vertx;
    private final ZooKeeperAdminProvider zooAdminProvider;

    private final String zookeeperConnectionString;

    private final Function<Integer, String> zkNodeAddress;

    private final long operationTimeoutMs;

    private final String trustStorePassword;
    private final File trustStoreFile;

    private final String keyStorePassword;
    private final File keyStoreFile;

    /**
     * ZookeeperScaler constructor
     *
     * @param vertx                         Vertx instance
     * @param zookeeperConnectionString     Connection string to connect to the right Zookeeper
     * @param zkNodeAddress                 Function for generating the Zookeeper node addresses
     * @param clusterCaCertSecret           Secret with Kafka cluster CA public key
     * @param coKeySecret                   Secret with Cluster Operator public and private key
     * @param operationTimeoutMs            Operation timeout
     *
     * @return  ZookeeperScaler instance
     */
    protected ZookeeperScaler(Vertx vertx, ZooKeeperAdminProvider zooAdminProvider, String zookeeperConnectionString, Function<Integer, String> zkNodeAddress, Secret clusterCaCertSecret, Secret coKeySecret, long operationTimeoutMs) {
        log.debug("Creating Zookeeper Scaler for cluster {}", zookeeperConnectionString);

        this.vertx = vertx;
        this.zooAdminProvider = zooAdminProvider;
        this.zookeeperConnectionString = zookeeperConnectionString;
        this.zkNodeAddress = zkNodeAddress;
        this.operationTimeoutMs = operationTimeoutMs;

        // Setup truststore from PEM file in cluster CA secret
        // We cannot use P12 because of custom CAs which for simplicity provide only PEM
        PasswordGenerator pg = new PasswordGenerator(12);
        trustStorePassword = pg.generate();
        trustStoreFile = Util.createFileTrustStore(getClass().getName(), "p12", Ca.cert(clusterCaCertSecret, Ca.CA_CRT), trustStorePassword.toCharArray());

        // Setup keystore from PKCS12 in cluster-operator secret
        keyStorePassword = new String(Util.decodeFromSecret(coKeySecret, "cluster-operator.password"), StandardCharsets.US_ASCII);
        keyStoreFile = Util.createFileStore(getClass().getName(), "p12", Util.decodeFromSecret(coKeySecret, "cluster-operator.p12"));
    }

    /**
     * Scales Zookeeper to defined number of instances.
     * It generates new configuration according to the desired number of nodes and updates Zookeeper configuration.
     *
     * @param scaleTo   Number of Zookeeper nodes which should be used by the cluster
     *
     * @return          Future which succeeds / fails when the scaling is finished
     */
    public Future<Void> scale(int scaleTo) {
        return connect()
                .compose(zkAdmin -> {
                    Promise<Void> scalePromise = Promise.promise();

                    getCurrentConfig(zkAdmin)
                            .compose(servers -> scaleTo(zkAdmin, servers, scaleTo))
                            .onComplete(res -> {
                                closeConnection(zkAdmin);

                                if (res.succeeded())    {
                                    scalePromise.complete();
                                } else {
                                    scalePromise.fail(res.cause());
                                }
                            });

                    return scalePromise.future();
                });
    }

    /**
     * Close the ZookeeperScaler instance. This deletes the certificate files.
     */
    @Override
    public void close() {
        if (trustStoreFile != null) {
            if (!trustStoreFile.delete())   {
                log.debug("Failed to delete file {}", trustStoreFile);
            }
        }

        if (keyStoreFile != null)   {
            if (!keyStoreFile.delete())   {
                log.debug("Failed to delete file {}", keyStoreFile);
            }
        }
    }

    /**
     * Internal method used to create the Zookeeper Admin client and connect it to Zookeeper
     *
     * @return      Future indicating success or failure
     */
    private Future<ZooKeeperAdmin> connect()    {
        Promise<ZooKeeperAdmin> connected = Promise.promise();

        try {
            ZooKeeperAdmin zkAdmin = zooAdminProvider.createZookeeperAdmin(
                this.zookeeperConnectionString,
                10_000,
                watchedEvent -> log.debug("Received event {} from ZooKeeperAdmin client connected to {}", watchedEvent, zookeeperConnectionString),
                getClientConfig());

            Util.waitFor(vertx,
                String.format("ZooKeeperAdmin connection to %s", zookeeperConnectionString),
                "connected",
                1_000,
                operationTimeoutMs,
                () -> zkAdmin.getState().isAlive() && zkAdmin.getState().isConnected())
                .onComplete(res -> {
                    if (res.succeeded())  {
                        connected.complete(zkAdmin);
                    } else {
                        closeConnection(zkAdmin);
                        log.warn("Failed to connect to Zookeeper {}. Connection was not ready in {} ms.", zookeeperConnectionString, operationTimeoutMs);
                        connected.fail(new ZookeeperScalingException("Failed to connect to Zookeeper " + zookeeperConnectionString + ". Connection was not ready in " + operationTimeoutMs + " ms.", res.cause()));
                    }
                });

        } catch (IOException e)   {
            log.warn("Failed to connect to {} to scale Zookeeper", zookeeperConnectionString, e);
            connected.fail(new ZookeeperScalingException("Failed to connect to Zookeeper " + zookeeperConnectionString, e));
        }

        return connected.future();
    }

    /**
     * Internal method to scale Zookeeper up or down or check configuration. It will:
     *     1) Compare the current configuration with the desired configuration
     *     2) Update the configuration if needed
     *
     * @param currentServers    Current list of servers from Zookeeper cluster
     * @param scaleTo           Desired scale
     * @return                  Future indicating success or failure
     */
    private Future<Void> scaleTo(ZooKeeperAdmin zkAdmin, Map<String, String> currentServers, int scaleTo) {
        Map<String, String> desiredServers = generateConfig(scaleTo, zkNodeAddress);

        if (isDifferent(currentServers, desiredServers))    {
            log.debug("The Zookeeper server configuration needs to be updated");
            return updateConfig(zkAdmin, desiredServers).map((Void) null);
        } else {
            log.debug("The Zookeeper server configuration is already up to date");
            return Future.succeededFuture();
        }
    }

    /**
     * Gets the current configuration from Zookeeper.
     *
     * @return  Future containing Map with the current Zookeeper configuration
     */
    private Future<Map<String, String>> getCurrentConfig(ZooKeeperAdmin zkAdmin)    {
        Promise<Map<String, String>> configPromise = Promise.promise();

        vertx.executeBlocking(promise -> {
            try {
                byte[] config = zkAdmin.getConfig(false, null);
                Map<String, String> servers = parseConfig(config);
                log.debug("Current Zookeeper configuration is {}", servers);
                promise.complete(servers);
            } catch (KeeperException | InterruptedException e)    {
                log.warn("Failed to get current Zookeeper server configuration", e);
                promise.fail(new ZookeeperScalingException("Failed to get current Zookeeper server configuration", e));
            }
        }, false, configPromise);

        return configPromise.future();
    }

    /**
     * Updates the configuration in the Zookeeper cluster
     *
     * @param newServers    New configuration which will be used for the update
     * @return              Future with the updated configuration
     */
    private Future<Map<String, String>> updateConfig(ZooKeeperAdmin zkAdmin, Map<String, String> newServers)    {
        Promise<Map<String, String>> configPromise = Promise.promise();

        vertx.executeBlocking(promise -> {
            try {
                log.debug("Updating Zookeeper configuration to {}", newServers);
                byte[] newConfig = zkAdmin.reconfigure(null, null, serversMapToList(newServers), -1, null);
                Map<String, String> servers = parseConfig(newConfig);

                log.debug("New Zookeeper configuration is {}", servers);
                promise.complete(servers);
            } catch (KeeperException | InterruptedException e)    {
                log.warn("Failed to update Zookeeper server configuration", e);
                promise.fail(new ZookeeperScalingException("Failed to update Zookeeper server configuration", e));
            }
        }, false, configPromise);

        return configPromise.future();
    }

    /**
     * Closes the Zookeeper connection
     */
    private void closeConnection(ZooKeeperAdmin zkAdmin) {
        if (zkAdmin != null)    {
            try {
                zkAdmin.close();
            } catch (Exception e) {
                log.debug("Failed to close the ZooKeeperAdmin", e);
            }
        }
    }

    /**
     * Generates the TLS configuration for Zookeeper.
     *
     * @return
     */
    private ZKClientConfig getClientConfig()  {
        ZKClientConfig clientConfig = new ZKClientConfig();

        clientConfig.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
        clientConfig.setProperty("zookeeper.client.secure", "true");
        clientConfig.setProperty("zookeeper.ssl.trustStore.location", trustStoreFile.getAbsolutePath());
        clientConfig.setProperty("zookeeper.ssl.trustStore.password", trustStorePassword);
        clientConfig.setProperty("zookeeper.ssl.trustStore.type", "PKCS12");
        clientConfig.setProperty("zookeeper.ssl.keyStore.location", keyStoreFile.getAbsolutePath());
        clientConfig.setProperty("zookeeper.ssl.keyStore.password", keyStorePassword);
        clientConfig.setProperty("zookeeper.ssl.keyStore.type", "PKCS12");

        return clientConfig;
    }

    /**
     * Converts the map with configuration to List of Strings which is the format in which the ZookeeperAdmin client
     * expects the new configuration.
     *
     * @param servers   Map with Zookeeper configuration
     * @return          List with Zookeeper configuration
     */
    /*test*/ static List<String> serversMapToList(Map<String, String> servers)  {
        List<String> serversList = new ArrayList<String>(servers.size());

        for (Map.Entry entry : servers.entrySet())  {
            serversList.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
        }

        return serversList;
    }

    /**
     * Parse the byte array we get from Zookeeper into a map we use internally. The returned Map will container only
     * the server entries from the Zookeeper configuration. Other entries such as version will be ignored.
     *
     * @param byteConfig    byte[] from Zookeeper client
     * @return              Map with Zookeeper configuration
     */
    /*test*/ static Map<String, String> parseConfig(byte[] byteConfig) {
        String config = new String(byteConfig, StandardCharsets.US_ASCII);

        Map<String, String> configMap = Util.parseMap(config);

        Map<String, String> serverMap = new HashMap<>(configMap.size() - 1);

        for (Map.Entry<String, String> entry : configMap.entrySet())    {
            if (entry.getKey().startsWith("server."))   {
                serverMap.put(entry.getKey(), entry.getValue());
            }
        }

        return serverMap;
    }

    /**
     * Checks whether two Zookeeper configurations are different or not. We will change the configuration only if it
     * differs to minimize the load.
     *
     * @param current   Map with current configuration
     * @param desired   Map with desired configuration
     * @return          True if the configurations differ and should be updated. False otherwise.
     */
    /*test*/ static boolean isDifferent(Map<String, String> current, Map<String, String> desired)    {
        return !current.equals(desired);
    }

    /**
     * Generates a map with Zookeeper configuration
     *
     * @param scale     Number of nodes which the Zookeeper cluster should have
     * @return          Map with configuration
     */
    /*test*/ static Map<String, String> generateConfig(int scale, Function<Integer, String> zkNodeAddress)   {
        Map<String, String> servers = new HashMap<>(scale);

        for (int i = 0; i < scale; i++) {
            // The Zookeeper server IDs starts with 1, but pod index starts from 0
            String key = String.format("server.%d", i + 1);
            String value = String.format("%s:%d:%d:participant;127.0.0.1:%d", zkNodeAddress.apply(i), ZookeeperCluster.CLUSTERING_PORT, ZookeeperCluster.LEADER_ELECTION_PORT, ZookeeperCluster.CLIENT_PLAINTEXT_PORT);

            servers.put(key, value);
        }

        return servers;
    }
}
