/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;

import java.util.function.Function;

/**
 * Class to provide the real ZookeeperScaler which connects to actual Zookeeper
 */
public class DefaultZookeeperScalerProvider implements ZookeeperScalerProvider {
    private static ZooKeeperAdminProvider zooAdminProvider = new DefaultZooKeeperAdminProvider();

    /**
     * Creates an instance of ZookeeperScaler
     *
     * @param reconciliation                The reconciliation
     * @param vertx                         Vertx instance
     * @param zookeeperConnectionString     Connection string to connect to the right Zookeeper
     * @param zkNodeAddress                 Function for generating the Zookeeper node addresses
     * @param clusterCaCertSecret           Secret with Kafka cluster CA public key
     * @param coKeySecret                   Secret with Cluster Operator public and private key
     * @param operationTimeoutMs            Operation timeout
     *
     * @return  ZookeeperScaler instance
     */
    public ZookeeperScaler createZookeeperScaler(Reconciliation reconciliation, Vertx vertx, String zookeeperConnectionString,
                                                 Function<Integer, String> zkNodeAddress, Secret clusterCaCertSecret,
                                                 Secret coKeySecret, long operationTimeoutMs, int zkAdminSessionTimeoutMs) {
        return new ZookeeperScaler(reconciliation, vertx, zooAdminProvider, zookeeperConnectionString, zkNodeAddress,
                clusterCaCertSecret, coKeySecret, operationTimeoutMs, zkAdminSessionTimeoutMs);
    }
}
