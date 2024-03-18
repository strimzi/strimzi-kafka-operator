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
 * Helper interface to pass different ZookeeperScaler implementations
 */
public interface ZookeeperScalerProvider {
    /**
     * Creates an instance of ZookeeperScaler
     *
     * @param reconciliation                The reconciliation
     * @param vertx                         Vertx instance
     * @param zookeeperConnectionString     Connection string to connect to the right Zookeeper
     * @param zkNodeAddress                 Function for generating the Zookeeper node addresses
     * @param zkTlsPkcs12Identity           Trust set and identity for TLS client authentication for connecting to ZooKeeper
     * @param operationTimeoutMs            Operation timeout
     * @param zkAdminSessionTimeoutMs       Zookeeper Admin client session timeout
     *
     * @return  ZookeeperScaler instance
     */
    ZookeeperScaler createZookeeperScaler(Reconciliation reconciliation, Vertx vertx, String zookeeperConnectionString,
                                          Function<Integer, String> zkNodeAddress, TlsPkcs12Identity zkTlsPkcs12Identity,
                                          long operationTimeoutMs, int zkAdminSessionTimeoutMs);
}
