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
import io.strimzi.operator.common.VertxUtil;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 * Class which contains several utility function which check if broker scale down can be done or not.
 */
public class PreventBrokerScaleDownCheck {
    /**
     * Logger
     */
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(PreventBrokerScaleDownCheck.class.getName());

    /**
     * Checks if broker contains any partition replicas when scaling down
     *
     * @param reconciliation      Reconciliation marker
     * @param vertx               Vert.x instance
     * @param idsToBeRemoved      Ids to be removed
     * @param secretOperator      Secret operator for working with Secrets
     * @param adminClientProvider Used to create the Admin client instance
     * @return returns future set of node ids containing partition replicas based on the outcome of the check
     */
    public Future<Set<Integer>> canScaleDownBrokers(Reconciliation reconciliation, Vertx vertx,
                                                    Set<Integer> idsToBeRemoved, SecretOperator secretOperator, AdminClientProvider adminClientProvider) {

        Promise<Set<Integer>> cannotScaleDown = Promise.promise();
        ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
                .compose(compositeFuture -> {
                    Promise<Void> resultPromise = Promise.promise();

                    final Future<Map<String, TopicDescription>> descriptions;
                    try {
                        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
                        LOGGER.debugCr(reconciliation, "Creating AdminClient for Kafka cluster in namespace {}", reconciliation.namespace());
                        Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, compositeFuture.resultAt(0), compositeFuture.resultAt(1), "cluster-operator");

                        Future<Set<String>> topicNames = topicNames(reconciliation, vertx, kafkaAdmin);
                        descriptions = topicNames.compose(names -> describeTopics(reconciliation, vertx, kafkaAdmin, names));
                        descriptions
                                .compose(topicDescriptions -> {
                                    Set<Integer> idsContainingPartitionReplicas = new HashSet<>();
                                    // Provides the node IDs which the user would like to remove first
                                    for (Integer id: idsToBeRemoved)   {
                                        boolean result =  brokerHasAnyReplicas(reconciliation, topicDescriptions.values(), id);
                                        if (result) {
                                            idsContainingPartitionReplicas.add(id);
                                        }
                                    }
                                    cannotScaleDown.complete(idsContainingPartitionReplicas);
                                    kafkaAdmin.close();
                                    return cannotScaleDown.future();
                                }).recover(error -> {
                                    LOGGER.warnCr(reconciliation, "Failed to get topic descriptions", error);
                                    cannotScaleDown.fail(error);
                                    kafkaAdmin.close();
                                    return Future.failedFuture(error);
                                });

                    } catch (KafkaException e) {
                        LOGGER.warnCr(reconciliation, "Failed to check if broker contains any partition replicas", e.getMessage());
                    }

                    return resultPromise.future();
                });

        return cannotScaleDown.future();
    }

    /**
     * This method gets the topic names after interacting with the Admin client
     *
     * @param reconciliation      Reconciliation marker
     * @param vertx               Vert.x instance
     * @param kafkaAdmin          Instance of Kafka Admin
     * @return  a Future with set of topic names
     */
    protected Future<Set<String>> topicNames(Reconciliation reconciliation, Vertx vertx, Admin kafkaAdmin) {
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
    protected Future<Map<String, TopicDescription>> describeTopics(Reconciliation reconciliation, Vertx vertx, Admin kafkaAdmin, Set<String> names) {
        return VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, kafkaAdmin.describeTopics(names).allTopicNames());
    }

    /**
     * Checks if broker contains any partition replicas
     *
     * @param reconciliation                 Reconciliation marker
     * @param tds                            Collection of Topic description
     * @param podId                          Id of broker
     *
     * @return boolean value based on whether brokers contain any partition replicas or not
     */
    private boolean brokerHasAnyReplicas(Reconciliation reconciliation, Collection<TopicDescription> tds, int podId) {
        for (TopicDescription td : tds) {
            LOGGER.debugCr(reconciliation, td);
            for (TopicPartitionInfo pd : td.partitions()) {
                for (org.apache.kafka.common.Node broker : pd.replicas()) {
                    if (podId == broker.id()) {
                        LOGGER.debugCr(reconciliation, "Partition replicas {} of {} are present on the broker with id {}", pd, td.name(), podId);
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
