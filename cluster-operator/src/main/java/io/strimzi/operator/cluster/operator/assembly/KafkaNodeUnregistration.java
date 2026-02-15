/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.strimzi.operator.common.Util.unwrap;

/**
 * Contains utility methods for unregistering KRaft nodes from a Kafka cluster after scale-down
 */
public class KafkaNodeUnregistration {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaNodeUnregistration.class.getName());

    private KafkaNodeUnregistration() { }

    /**
     * Unregisters Kafka broker nodes from a KRaft-based Kafka cluster
     *
     * @param reconciliation        Reconciliation marker
     * @param adminClientProvider   Kafka Admin API client provider
     * @param pemTrustSet           Trust set for the admin client to connect to the Kafka cluster
     * @param pemAuthIdentity       Key set  for the admin client to connect to the Kafka cluster
     * @param nodeIdsToUnregister   List of node IDs that should be unregistered
     *
     * @return  CompletableFuture that completes when all broker nodes are unregistered
     */
    public static CompletableFuture<Void> unregisterBrokerNodes(
            Reconciliation reconciliation,
            AdminClientProvider adminClientProvider,
            PemTrustSet pemTrustSet,
            PemAuthIdentity pemAuthIdentity,
            Set<Integer> nodeIdsToUnregister
    ) {
        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, pemTrustSet, pemAuthIdentity);

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Integer nodeId : nodeIdsToUnregister) {
                futures.add(unregisterBrokerNode(reconciliation, adminClient, nodeId));
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((v, t) -> {
                        adminClient.close();
                    });
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to unregister nodes", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * List registered Kafka broker nodes within a KRaft-based cluster
     *
     * @param reconciliation        Reconciliation marker
     * @param adminClientProvider   Kafka Admin API client provider
     * @param pemTrustSet           Trust set for the admin client to connect to the Kafka cluster
     * @param pemAuthIdentity       Key set  for the admin client to connect to the Kafka cluster
     * @param includeFencedBrokers  If listing should include fenced brokers
     *
     * @return  CompletableFuture that completes when all registered broker nodes are listed
     */
    public static CompletableFuture<Collection<Node>> listRegisteredBrokerNodes(
            Reconciliation reconciliation,
            AdminClientProvider adminClientProvider,
            PemTrustSet pemTrustSet,
            PemAuthIdentity pemAuthIdentity,
            boolean includeFencedBrokers) {

        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, pemTrustSet, pemAuthIdentity);

            DescribeClusterOptions option = new DescribeClusterOptions().includeFencedBrokers(includeFencedBrokers);

            return adminClient.describeCluster(option).nodes()
                    .whenComplete((nodes, t) -> {
                        if (t == null) {
                            LOGGER.debugCr(reconciliation, "Describe cluster: nodes (fenced included) = {}", nodes);
                        }
                        adminClient.close();
                    })
                    .toCompletionStage()
                    .toCompletableFuture();
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to list nodes", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Unregisters a single Kafka broker node using the Kafka Admin API. In case the failure is caused by the node not being
     * registered, the error will be ignored.
     *
     * @param reconciliation        Reconciliation marker
     * @param adminClient           Kafka Admin API client instance
     * @param nodeIdToUnregister    ID of the broker node that should be unregistered
     *
     * @return  CompletableFuture that completes when the node is unregistered
     */
    private static CompletableFuture<Void> unregisterBrokerNode(Reconciliation reconciliation, Admin adminClient, Integer nodeIdToUnregister) {
        LOGGER.debugCr(reconciliation, "Unregistering node {} from the Kafka cluster", nodeIdToUnregister);

        return adminClient.unregisterBroker(nodeIdToUnregister).all()
                .whenComplete((v, t) -> {
                    if (t != null) {
                        Throwable cause = unwrap(t);
                        LOGGER.warnCr(reconciliation, "Failed to unregister node {} from the Kafka cluster", nodeIdToUnregister, cause);
                    }
                })
                .toCompletionStage()
                .toCompletableFuture();
    }
}
