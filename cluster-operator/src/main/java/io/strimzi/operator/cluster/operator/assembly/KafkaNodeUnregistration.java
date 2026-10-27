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
import java.util.HashSet;
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
     * Unregisters Kafka broker nodes that were removed from a KRaft-based Kafka cluster.
     * It lists all registered broker nodes and unregisters those not present in {@code currentBrokerNodes}.
     * Any listing or unregistration failures are logged and swallowed, the returned future always completes successfully.
     *
     * @param reconciliation        Reconciliation marker
     * @param adminClientProvider   Kafka Admin API client provider
     * @param kafkaTrustSet         Trust set for the admin client to connect to the Kafka cluster
     * @param authIdentity          Key set for the admin client to connect to the Kafka cluster
     * @param currentBrokerNodes    Currently active broker nodes after scale-down
     *
     * @return  CompletionStage that always completes successfully when broker unregistration is done
     */
    public static CompletionStage<Void> unregisterBrokerNodes(
            Reconciliation reconciliation,
            AdminClientProvider adminClientProvider,
            TrustSet kafkaTrustSet,
            AuthIdentity authIdentity,
            Set<NodeRef> currentBrokerNodes
    ) {
        List<Integer> currentBrokerIds = currentBrokerNodes.stream().map(NodeRef::nodeId).sorted().toList();
        CompletableFuture<Void> result = new CompletableFuture<>();

        listRegisteredBrokerNodes(reconciliation, adminClientProvider, kafkaTrustSet, authIdentity, true)
                .whenComplete((registeredBrokerNodes, listingFailure) -> {
                    if (listingFailure != null) {
                        // listing broker nodes failed, we will retry on next reconciliation
                        LOGGER.warnCr(reconciliation, "Failed to list Kafka broker nodes from the Kafka cluster", listingFailure);
                        // We complete the future with success even if listing failed as we do not want to fail the reconciliation.
                        result.complete(null);
                        return;
                    }

                    List<Integer> registeredBrokerIds = registeredBrokerNodes.stream().map(Node::id).toList();

                    LOGGER.debugCr(reconciliation, "Broker nodes unregistration: currentBrokerIds = {}, registeredBrokerIds = {}", currentBrokerIds, registeredBrokerIds);

                    Set<Integer> brokerIdsToUnregister = new HashSet<>(registeredBrokerIds);
                    brokerIdsToUnregister.removeAll(currentBrokerIds);

                    if (brokerIdsToUnregister.isEmpty()) {
                        result.complete(null);
                        return;
                    }

                    LOGGER.infoCr(reconciliation, "Kafka broker nodes {} were removed from the Kafka cluster and will be unregistered", brokerIdsToUnregister);

                    try {
                        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
                        Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, kafkaTrustSet, authIdentity);

                        List<CompletionStage<Void>> futures = new ArrayList<>();
                        for (Integer nodeId : brokerIdsToUnregister) {
                            futures.add(unregisterBrokerNode(reconciliation, adminClient, nodeId));
                        }
                        CompletableFuture.allOf(futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new))
                                .whenComplete((v, unregistrationFailure) -> {
                                    adminClient.close();
                                    if (unregistrationFailure == null) {
                                        LOGGER.infoCr(reconciliation, "Kafka broker nodes {} were successfully unregistered from the Kafka cluster", brokerIdsToUnregister);
                                    } else {
                                        // unregistration failed, we will retry on next reconciliation
                                        LOGGER.warnCr(reconciliation, "Failed to unregister Kafka broker nodes {} from the Kafka cluster", brokerIdsToUnregister);
                                    }
                                    // We complete the future with success even if the unregistration failed as we do not want to fail the reconciliation.
                                    result.complete(null);
                                });
                    } catch (KafkaException e) {
                        // unregistration failed, we will retry on next reconciliation
                        LOGGER.warnCr(reconciliation, "Failed to unregister broker nodes", e);
                        // We complete the future with success even if the unregistration failed as we do not want to fail the reconciliation.
                        result.complete(null);
                    }
                });

        return result;
    }

    /* test */ static CompletionStage<Collection<Node>> listRegisteredBrokerNodes(
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
                            LOGGER.debugCr(reconciliation, "Describe cluster: broker nodes (fenced included) = {}", nodes);
                        }
                        adminClient.close();
                    })
                    .toCompletionStage();
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to list broker nodes", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Unregisters Kafka controller nodes that were removed from a KRaft-based Kafka cluster.
     * It lists all registered controller nodes and unregisters those not present in {@code currentControllerNodes}.
     * Any listing or unregistration failures are logged and swallowed, the returned future always completes successfully.
     *
     * @param reconciliation            Reconciliation marker
     * @param adminClientProvider       Kafka Admin API client provider
     * @param kafkaTrustSet             Trust set for the admin client to connect to the Kafka cluster
     * @param authIdentity              Key set for the admin client to connect to the Kafka cluster
     * @param currentControllerNodes    Currently active controller nodes after scale-down
     *
     * @return  CompletionStage that always completes successfully when controller unregistration is done
     */
    public static CompletionStage<Void> unregisterControllerNodes(
            Reconciliation reconciliation,
            AdminClientProvider adminClientProvider,
            TrustSet kafkaTrustSet,
            AuthIdentity authIdentity,
            Set<NodeRef> currentControllerNodes
    ) {
        List<Integer> currentControllerIds = currentControllerNodes.stream().map(NodeRef::nodeId).sorted().toList();
        CompletableFuture<Void> result = new CompletableFuture<>();

        listRegisteredControllerNodes(reconciliation, adminClientProvider, kafkaTrustSet, authIdentity, currentControllerNodes)
                .whenComplete((registeredControllerNodes, listingFailure) -> {
                    if (listingFailure != null) {
                        // listing registered controller nodes failed, we will retry on next reconciliation
                        LOGGER.warnCr(reconciliation, "Failed to list registered Kafka controller nodes from the Kafka cluster", listingFailure);
                        // We complete the future with success even if listing failed as we do not want to fail the reconciliation.
                        result.complete(null);
                        return;
                    }

                    List<Integer> registeredControllerIds = registeredControllerNodes.stream().map(Node::id).toList();

                    LOGGER.debugCr(reconciliation, "Controller nodes unregistration: currentControllerIds = {}, registeredControllerIds = {}", currentControllerIds, registeredControllerIds);

                    Set<Integer> controllerIdsToUnregister = new HashSet<>(registeredControllerIds);
                    controllerIdsToUnregister.removeAll(currentControllerIds);

                    if (controllerIdsToUnregister.isEmpty()) {
                        result.complete(null);
                        return;
                    }

                    LOGGER.infoCr(reconciliation, "Kafka controller nodes {} were removed from the Kafka cluster and will be unregistered", controllerIdsToUnregister);

                    try {
                        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
                        Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, kafkaTrustSet, authIdentity);

                        List<CompletionStage<Void>> futures = new ArrayList<>();
                        for (Integer nodeId : controllerIdsToUnregister) {
                            futures.add(unregisterControllerNode(reconciliation, adminClient, nodeId));
                        }
                        CompletableFuture.allOf(futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new))
                                .whenComplete((v, unregistrationFailure) -> {
                                    adminClient.close();
                                    if (unregistrationFailure == null) {
                                        LOGGER.infoCr(reconciliation, "Kafka controller nodes {} were successfully unregistered from the Kafka cluster", controllerIdsToUnregister);
                                    } else {
                                        // unregistration failed, we will retry on next reconciliation
                                        LOGGER.warnCr(reconciliation, "Failed to unregister Kafka controller nodes {} from the Kafka cluster", controllerIdsToUnregister);
                                    }
                                    // We complete the future with success even if the unregistration failed as we do not want to fail the reconciliation.
                                    result.complete(null);
                                });
                    } catch (KafkaException e) {
                        // unregistration failed, we will retry on next reconciliation
                        LOGGER.warnCr(reconciliation, "Failed to unregister controller nodes", e);
                        // We complete the future with success even if the unregistration failed as we do not want to fail the reconciliation.
                        result.complete(null);
                    }
                });

        return result;
    }

    /* test */ static CompletionStage<Collection<Node>> listRegisteredControllerNodes(
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
