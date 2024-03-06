/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.model.ClusterOperatorPKCS12AuthIdentity;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.PemTrustSet;
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
     * @param zkCaTrustSet                  Trust set for connecting to Zookeeper
     * @param coAuthIdentity                Cluster Operator identity for TLS client authentication for connecting to Zookeeper
     * @param operationTimeoutMs            Operation timeout
     * @param zkAdminSessionTimeoutMs       Zookeeper Admin client session timeout
     *
     * @return  ZookeeperScaler instance
     */
    ZookeeperScaler createZookeeperScaler(Reconciliation reconciliation, Vertx vertx, String zookeeperConnectionString,
                                          Function<Integer, String> zkNodeAddress, PemTrustSet zkCaTrustSet,
                                          ClusterOperatorPKCS12AuthIdentity coAuthIdentity, long operationTimeoutMs, int zkAdminSessionTimeoutMs);
}
