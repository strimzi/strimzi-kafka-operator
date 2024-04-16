/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;

import java.io.IOException;

/**
 * Class to provide the real ZooKeeperAdmin which connects to actual Zookeeper
 */
public class DefaultZooKeeperAdminProvider implements ZooKeeperAdminProvider {
    /**
     * Creates an instance of ZooKeeperAdmin
     *
     * @param connectString     Connection String used to connect to Zookeeper
     * @param sessionTimeout    Session timeout
     * @param watcher           Watcher which will be notified about watches and connection changes
     * @param operationTimeoutMs    Timeout for ZooKeeper requests
     * @param trustStoreFile        File hosting the truststore with TLS certificates to use to connect to ZooKeeper
     * @param keyStoreFile          File hosting the keystore with TLS private keys to use to connect to ZooKeeper
     *
     * @return  ZooKeeperAdmin instance
     */
    @Override
    public ZooKeeperAdmin createZookeeperAdmin(String connectString, int sessionTimeout, Watcher watcher,
                                               long operationTimeoutMs, String trustStoreFile, String keyStoreFile) throws IOException {
        ZKClientConfig clientConfig = new ZKClientConfig();
        clientConfig.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
        clientConfig.setProperty("zookeeper.client.secure", "true");
        clientConfig.setProperty("zookeeper.sasl.client", "false");
        clientConfig.setProperty("zookeeper.ssl.trustStore.location", trustStoreFile);
        clientConfig.setProperty("zookeeper.ssl.trustStore.type", "PEM");
        clientConfig.setProperty("zookeeper.ssl.keyStore.location", keyStoreFile);
        clientConfig.setProperty("zookeeper.ssl.keyStore.type", "PEM");
        clientConfig.setProperty("zookeeper.request.timeout", String.valueOf(operationTimeoutMs));

        return new ZooKeeperAdmin(connectString, sessionTimeout, watcher, clientConfig);
    }
}
