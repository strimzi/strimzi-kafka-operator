/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafka;
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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.BrokerIdNotRegisteredException;

import java.util.ArrayList;
import java.util.List;

/**
 * Class containing methods for handling the configuration around {@link QuotasPluginKafka}
 */
public class KafkaNodeUnregistration {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaNodeUnregistration.class.getName());

    /**
     * Unregisters Kafka nodes from a KRaft-based Kafka cluster
     *
     * @param reconciliation        Reconciliation marker
     * @param vertx                 Vert.x instance
     * @param adminClientProvider   Kafka Admin API client provider
     * @param pemTrustSet           Trust set for the admin client to connect to the Kafka cluster
     * @param pemAuthIdentity       Key set  for the admin client to connect to the Kafka cluster
     * @param nodeIdsToUnregister   List of node IDs that should be unregistered
     *
     * @return  Future that completes when all nodes are unregistered
     */
    public static Future<Void> unregisterNodes(
            Reconciliation reconciliation,
            Vertx vertx,
            AdminClientProvider adminClientProvider,
            PemTrustSet pemTrustSet,
            PemAuthIdentity pemAuthIdentity,
            List<Integer> nodeIdsToUnregister
    ) {
        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin adminClient = adminClientProvider.createAdminClient(bootstrapHostname, pemTrustSet, pemAuthIdentity);

            List<Future<Void>> futures = new ArrayList<>();
            for (Integer nodeId : nodeIdsToUnregister) {
                futures.add(unregisterNode(reconciliation, vertx, adminClient, nodeId));
            }

            return Future.all(futures)
                    .eventually(() -> {
                        adminClient.close();
                        return Future.succeededFuture();
                    })
                    .map((Void) null);
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to unregister nodes", e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Unregisters a single Kafka node using the Kafka Admin API. In case the failure is caused by the node not being
     * registered, the error will be ignored.
     *
     * @param reconciliation        Reconciliation marker
     * @param vertx                 Vert.x instance
     * @param adminClient           Kafka Admin API client instance
     * @param nodeIdToUnregister    ID of the node that should be unregistered
     *
     * @return  Future that completes when the node is unregistered
     */
    private static Future<Void> unregisterNode(Reconciliation reconciliation, Vertx vertx, Admin adminClient, Integer nodeIdToUnregister) {
        LOGGER.debugCr(reconciliation, "Unregistering node {} from the Kafka cluster", nodeIdToUnregister);

        return VertxUtil
                .kafkaFutureToVertxFuture(reconciliation, vertx, adminClient.unregisterBroker(nodeIdToUnregister).all())
                .recover(t -> {
                    if (t instanceof BrokerIdNotRegisteredException)    {
                        // The broker is not registered anymore, we report success
                        LOGGER.warnCr(reconciliation, "Node {} is not registered and cannot be unregistered from the Kafka cluster", nodeIdToUnregister, t);
                        return Future.succeededFuture();
                    } else {
                        LOGGER.warnCr(reconciliation, "Failed to unregister node {} from the Kafka cluster", nodeIdToUnregister, t);
                        return Future.failedFuture(t);
                    }
                });
    }
}
