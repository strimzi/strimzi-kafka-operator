/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Contains utility methods for unregistering KRaft nodes from a Kafka cluster after scale-down
 */
public class KafkaNodeUnregistration {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaNodeUnregistration.class.getName());

    /**
     * Unregisters Kafka broker nodes from a KRaft-based Kafka cluster
     *
     * @param reconciliation        Reconciliation marker
     * @param vertx                 Vert.x instance
     * @param adminClientProvider   Kafka Admin API client provider
     * @param pemTrustSet           Trust set for the admin client to connect to the Kafka cluster
     * @param pemAuthIdentity       Key set  for the admin client to connect to the Kafka cluster
     * @param nodeIdsToUnregister   List of node IDs that should be unregistered
     *
     * @return  Future that completes when all broker nodes are unregistered
     */
    public static Future<Void> unregisterBrokerNodes(
            Reconciliation reconciliation,
            Vertx vertx,
            AdminClientProvider adminClientProvider,
            PemTrustSet pemTrustSet,
            PemAuthIdentity pemAuthIdentity,
            Set<Integer> nodeIdsToUnregister
    ) {
        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, pemTrustSet, pemAuthIdentity);

            List<Future<Void>> futures = new ArrayList<>();
            for (Integer nodeId : nodeIdsToUnregister) {
                futures.add(unregisterBrokerNode(reconciliation, vertx, adminClient, nodeId));
            }

            return Future.all(futures)
                    .eventually(() -> {
                        adminClient.close();
                        return Future.succeededFuture();
                    })
                    .mapEmpty();
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to unregister nodes", e);
            return Future.failedFuture(e);
        }
    }

    /**
     * List registered Kafka broker nodes within a KRaft-based cluster
     *
     * @param reconciliation        Reconciliation marker
     * @param vertx                 Vert.x instance
     * @param adminClientProvider   Kafka Admin API client provider
     * @param pemTrustSet           Trust set for the admin client to connect to the Kafka cluster
     * @param pemAuthIdentity       Key set  for the admin client to connect to the Kafka cluster
     * @param includeFencedBrokers  If listing should include fenced brokers
     *
     * @return  Future that completes when all registered broker nodes are listed
     */
    public static Future<Collection<Node>> listRegisteredBrokerNodes(
            Reconciliation reconciliation,
            Vertx vertx,
            AdminClientProvider adminClientProvider,
            PemTrustSet pemTrustSet,
            PemAuthIdentity pemAuthIdentity,
            boolean includeFencedBrokers) {

        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, pemTrustSet, pemAuthIdentity);

            DescribeClusterOptions option = new DescribeClusterOptions().includeFencedBrokers(includeFencedBrokers);
            return VertxUtil
                    .kafkaFutureToVertxFuture(reconciliation, vertx, adminClient.describeCluster(option).nodes())
                    .compose(nodes -> {
                        LOGGER.debugCr(reconciliation, "Describe cluster: nodes (fenced included) = {}", nodes);
                        adminClient.close();
                        return Future.succeededFuture(nodes);
                    })
                    .recover(throwable -> {
                        adminClient.close();
                        return Future.failedFuture(throwable);
                    });
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to list nodes", e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Unregisters a single Kafka broker node using the Kafka Admin API. In case the failure is caused by the node not being
     * registered, the error will be ignored.
     *
     * @param reconciliation        Reconciliation marker
     * @param vertx                 Vert.x instance
     * @param adminClient           Kafka Admin API client instance
     * @param nodeIdToUnregister    ID of the broker node that should be unregistered
     *
     * @return  Future that completes when the node is unregistered
     */
    private static Future<Void> unregisterBrokerNode(Reconciliation reconciliation, Vertx vertx, Admin adminClient, Integer nodeIdToUnregister) {
        LOGGER.debugCr(reconciliation, "Unregistering node {} from the Kafka cluster", nodeIdToUnregister);

        return VertxUtil
                .kafkaFutureToVertxFuture(reconciliation, vertx, adminClient.unregisterBroker(nodeIdToUnregister).all())
                .onFailure(t -> {
                    LOGGER.warnCr(reconciliation, "Failed to unregister node {} from the Kafka cluster", nodeIdToUnregister, t);
                });
    }
}
