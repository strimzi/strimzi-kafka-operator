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
     * @param conf              Zookeeper client configuration
     *
     * @return  ZooKeeperAdmin instance
     */
    @Override
    public ZooKeeperAdmin createZookeeperAdmin(String connectString, int sessionTimeout, Watcher watcher, ZKClientConfig conf) throws IOException {
        return new ZooKeeperAdmin(connectString, sessionTimeout, watcher, conf);
    }
}
