/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.TlsPkcs12Identity;
import io.vertx.core.Vertx;

import java.util.function.Function;

/**
 * Class to provide the real ZookeeperScaler which connects to actual Zookeeper
 */
public class DefaultZookeeperScalerProvider implements ZookeeperScalerProvider {
    private static final ZooKeeperAdminProvider ZOO_ADMIN_PROVIDER = new DefaultZooKeeperAdminProvider();

    /**
     * Creates an instance of ZookeeperScaler
     *
     * @param reconciliation                The reconciliation
     * @param vertx                         Vertx instance
     * @param zookeeperConnectionString     Connection string to connect to the right Zookeeper
     * @param zkNodeAddress                 Function for generating the Zookeeper node addresses
     * @param zkTlsPkcs12Identity           Trust set and identity for TLS client authentication for connecting to ZooKeeper
     * @param operationTimeoutMs            Operation timeout
     *
     * @return  ZookeeperScaler instance
     */
    public ZookeeperScaler createZookeeperScaler(Reconciliation reconciliation, Vertx vertx, String zookeeperConnectionString,
                                                 Function<Integer, String> zkNodeAddress, TlsPkcs12Identity zkTlsPkcs12Identity,
                                                 long operationTimeoutMs, int zkAdminSessionTimeoutMs) {
        return new ZookeeperScaler(reconciliation, vertx, ZOO_ADMIN_PROVIDER, zookeeperConnectionString, zkNodeAddress,
                zkTlsPkcs12Identity, operationTimeoutMs, zkAdminSessionTimeoutMs);
    }
}
