/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.AuthIdentity;
import io.strimzi.operator.common.auth.TrustSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.Util.maybeUnwrapCompletionException;

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
     * @param kafkaTrustSet         Trust set for the admin client to connect to the Kafka cluster
     * @param authIdentity          Key set  for the admin client to connect to the Kafka cluster
     * @param nodeIdsToUnregister   List of node IDs that should be unregistered
     *
     * @return  CompletionStage that completes when all broker nodes are unregistered
     */
    public static CompletionStage<Void> unregisterBrokerNodes(
            Reconciliation reconciliation,
            AdminClientProvider adminClientProvider,
            TrustSet kafkaTrustSet,
            AuthIdentity authIdentity,
            Set<Integer> nodeIdsToUnregister
    ) {
        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, kafkaTrustSet, authIdentity);

            List<CompletionStage<Void>> futures = new ArrayList<>();
            for (Integer nodeId : nodeIdsToUnregister) {
                futures.add(unregisterBrokerNode(reconciliation, adminClient, nodeId));
            }
            return CompletableFuture.allOf(futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new))
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
     * @param kafkaTrustSet         Trust set for the admin client to connect to the Kafka cluster
     * @param authIdentity          Key set  for the admin client to connect to the Kafka cluster
     * @param includeFencedBrokers  If listing should include fenced brokers
     *
     * @return  CompletionStage that completes when all registered broker nodes are listed
     */
    public static CompletionStage<Collection<Node>> listRegisteredBrokerNodes(
            Reconciliation reconciliation,
            AdminClientProvider adminClientProvider,
            TrustSet kafkaTrustSet,
            AuthIdentity authIdentity,
            boolean includeFencedBrokers) {

        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, kafkaTrustSet, authIdentity);

            DescribeClusterOptions option = new DescribeClusterOptions().includeFencedBrokers(includeFencedBrokers);

            return adminClient.describeCluster(option).nodes()
                    .whenComplete((nodes, t) -> {
                        if (t == null) {
                            LOGGER.debugCr(reconciliation, "Describe cluster: nodes (fenced included) = {}", nodes);
                        }
                        adminClient.close();
                    })
                    .toCompletionStage();
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to list nodes", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * List registered controller nodes within a KRaft-based cluster.
     *
     * It leverages the describeCluster() API call by querying the controller endpoint.
     * The result reflects the metadata log state: a controller whose RegisterControllerRecord was written
     * but whose UnregisterControllerRecord was never written (e.g. after a scale-down) will still appear
     * here, making it possible to detect and unregister it explicitly.
     *
     * @param reconciliation        Reconciliation marker
     * @param adminClientProvider   Kafka Admin API client provider
     * @param kafkaTrustSet         Trust set for the admin client to connect to the Kafka cluster
     * @param authIdentity          Key set  for the admin client to connect to the Kafka cluster
     * @param controllerNodes       Active controller nodes used to build the controller bootstrap addresses
     *
     * @return  CompletionStage that completes with the registered controller nodes
     */
    public static CompletionStage<Collection<Node>> listRegisteredControllerNodes(
            Reconciliation reconciliation,
            AdminClientProvider adminClientProvider,
            TrustSet kafkaTrustSet,
            AuthIdentity authIdentity,
            Set<NodeRef> controllerNodes) {

        try {
            // Connect to the controller endpoint so that describeCluster() returns the status
            // of registered controllers as they have a RegisterControllerRecord in the metadata log
            // but not a corresponding UnregisterControllerRecord.
            String bootstrapHostnames = controllerNodes.stream()
                    .map(node -> DnsNameGenerator.podDnsNameWithoutClusterDomain(
                            reconciliation.namespace(),
                            KafkaResources.brokersServiceName(reconciliation.name()),
                            node.podName()) + ":" + KafkaCluster.CONTROLPLANE_PORT)
                    .collect(Collectors.joining(","));
            Admin adminClient = adminClientProvider.createControllerAdminClient(bootstrapHostnames, kafkaTrustSet, authIdentity);

            return adminClient.describeCluster().nodes()
                    .whenComplete((nodes, t) -> {
                        if (t == null) {
                            LOGGER.debugCr(reconciliation, "Describe cluster (controller endpoint): controller nodes = {}", nodes);
                        }
                        adminClient.close();
                    })
                    .toCompletionStage();
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to list registered controller nodes", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Unregisters Kafka controller nodes from a KRaft-based Kafka cluster
     *
     * @param reconciliation        Reconciliation marker
     * @param adminClientProvider   Kafka Admin API client provider
     * @param kafkaTrustSet         Trust set for the admin client to connect to the Kafka cluster
     * @param authIdentity          Key set  for the admin client to connect to the Kafka cluster
     * @param nodeIdsToUnregister   List of node IDs that should be unregistered
     *
     * @return  CompletionStage that completes when all controller nodes are unregistered
     */
    public static CompletionStage<Void> unregisterControllerNodes(
            Reconciliation reconciliation,
            AdminClientProvider adminClientProvider,
            TrustSet kafkaTrustSet,
            AuthIdentity authIdentity,
            Set<Integer> nodeIdsToUnregister
    ) {
        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, kafkaTrustSet, authIdentity);

            List<CompletionStage<Void>> futures = new ArrayList<>();
            for (Integer nodeId : nodeIdsToUnregister) {
                futures.add(unregisterControllerNode(reconciliation, adminClient, nodeId));
            }
            return CompletableFuture.allOf(futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new))
                    .whenComplete((v, t) -> {
                        adminClient.close();
                    });
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to unregister nodes", e);
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
     * @return  CompletionStage that completes when the node is unregistered
     */
    private static CompletionStage<Void> unregisterBrokerNode(Reconciliation reconciliation, Admin adminClient, Integer nodeIdToUnregister) {
        LOGGER.debugCr(reconciliation, "Unregistering node {} from the Kafka cluster", nodeIdToUnregister);

        return adminClient.unregisterBroker(nodeIdToUnregister).all()
                .whenComplete((v, t) -> {
                    if (t != null) {
                        Throwable cause = maybeUnwrapCompletionException(t);
                        LOGGER.warnCr(reconciliation, "Failed to unregister node {} from the Kafka cluster", nodeIdToUnregister, cause);
                    }
                })
                .toCompletionStage();
    }

    /**
     * Unregisters a single Kafka controller node using the Kafka Admin API. In case the failure is caused by the node not being
     * registered, the error will be ignored.
     *
     * @param reconciliation        Reconciliation marker
     * @param adminClient           Kafka Admin API client instance
     * @param nodeIdToUnregister    ID of the controller node that should be unregistered
     *
     * @return  CompletionStage that completes when the node is unregistered
     */
    private static CompletionStage<Void> unregisterControllerNode(Reconciliation reconciliation, Admin adminClient, Integer nodeIdToUnregister) {
        LOGGER.debugCr(reconciliation, "Unregistering controller node {} from the Kafka cluster", nodeIdToUnregister);

        return adminClient.unregisterController(nodeIdToUnregister).all()
                .whenComplete((v, t) -> {
                    if (t != null) {
                        Throwable cause = maybeUnwrapCompletionException(t);
                        LOGGER.warnCr(reconciliation, "Failed to unregister node {} from the Kafka cluster", nodeIdToUnregister, cause);
                    }
                })
                .toCompletionStage();
    }
}
