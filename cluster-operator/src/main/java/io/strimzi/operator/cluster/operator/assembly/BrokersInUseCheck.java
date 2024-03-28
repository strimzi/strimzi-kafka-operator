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
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Class which contains several utility function which check if broker scale down or role change can be done or not.
 */
public class BrokersInUseCheck {
    /**
     * Logger
     */
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(BrokersInUseCheck.class.getName());

    /**
     * Checks if broker contains any partition replicas when scaling down
     *
     * @param reconciliation        Reconciliation marker
     * @param vertx                 Vert.x instance
     * @param coTlsPemIdentity      Trust set and identity for TLS client authentication for connecting to the Kafka cluster
     * @param adminClientProvider   Used to create the Admin client instance
     *
     * @return returns future set of node ids containing partition replicas based on the outcome of the check
     */
    public Future<Set<Integer>> brokersInUse(Reconciliation reconciliation, Vertx vertx, TlsPemIdentity coTlsPemIdentity, AdminClientProvider adminClientProvider) {
        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            LOGGER.debugCr(reconciliation, "Creating AdminClient for Kafka cluster in namespace {}", reconciliation.namespace());
            Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity());

            return topicNames(reconciliation, vertx, kafkaAdmin)
                    .compose(names -> describeTopics(reconciliation, vertx, kafkaAdmin, names))
                    .compose(topicDescriptions -> {
                        Set<Integer> brokersWithPartitionReplicas = new HashSet<>();

                        for (TopicDescription td : topicDescriptions.values()) {
                            for (TopicPartitionInfo pd : td.partitions()) {
                                for (org.apache.kafka.common.Node broker : pd.replicas()) {
                                    brokersWithPartitionReplicas.add(broker.id());
                                }
                            }
                        }

                        kafkaAdmin.close();
                        return Future.succeededFuture(brokersWithPartitionReplicas);
                    }).recover(error -> {
                        LOGGER.warnCr(reconciliation, "Failed to get list of brokers in use", error);
                        kafkaAdmin.close();
                        return Future.failedFuture(error);
                    });
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to check if broker contains any partition replicas", e);
            return Future.failedFuture(e);
        }
    }

    /**
     * This method gets the topic names after interacting with the Admin client
     *
     * @param reconciliation      Reconciliation marker
     * @param vertx               Vert.x instance
     * @param kafkaAdmin          Instance of Kafka Admin
     * @return  a Future with set of topic names
     */
    /* test */ Future<Set<String>> topicNames(Reconciliation reconciliation, Vertx vertx, Admin kafkaAdmin) {
        return VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, kafkaAdmin.listTopics(new ListTopicsOptions().listInternal(true)).names());
    }

    /**
     * Returns a collection of topic descriptions
     *
     * @param reconciliation Reconciliation marker
     * @param vertx          Vert.x instance
     * @param kafkaAdmin     Instance of Admin client
     * @param names          Set of topic names
     * @return a Future with map containing the topic name and description
     */
    /* test */ Future<Map<String, TopicDescription>> describeTopics(Reconciliation reconciliation, Vertx vertx, Admin kafkaAdmin, Set<String> names) {
        return VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, kafkaAdmin.describeTopics(names).allTopicNames());
    }
}
