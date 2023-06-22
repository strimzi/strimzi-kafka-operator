/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collection;
import java.util.Set;

/**
 * Class which contains several utility function which check if broker scale down can be done or not.
 */
public class PreventBrokerScaleDownUtils {

    /* logger*/
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaReconciler.class.getName());

    /**
     * Checks if broker contains any partition replicas when scaling down
     *
     * @param vertx                Instance of vertx
     * @param reconciliation       Reconciliation marker
     * @param kafka                Instance of Kafka Cluster
     * @param kafkaStatus          The Kafka Status class for adding conditions to it during the reconciliation
     * @param secretOperator       Secret operator for working with Secrets
     * @param adminClientProvider  Used to create the Admin client instance
     * @param currentReplicas      The current number of replicas
     * @return  Future which completes when the check is complete
     */
    public static Future<Void> canScaleDownBrokers(Vertx vertx, Reconciliation reconciliation, KafkaCluster kafka, KafkaStatus kafkaStatus,
                                                   SecretOperator secretOperator, AdminClientProvider adminClientProvider, int currentReplicas) {

        if (currentReplicas != 0 && kafka.getReplicas() != 0 && currentReplicas > kafka.getReplicas()) {

            Future<Boolean> result = canScaleDownBrokerCheck(vertx, reconciliation, kafka, kafkaStatus, secretOperator, adminClientProvider, currentReplicas);

            return result.compose(cannotScaleDown -> {
                if (cannotScaleDown) {
                    kafkaStatus.addCondition(StatusUtils.buildWarningCondition("ScaleDownException", "Can't Scale down since broker contains partition replicas. " +
                            "Ignoring `replicas` setting in Kafka custom resource: " + kafka.getComponentName() + ". Current `spec.replicas` value is " + currentReplicas));
                    kafka.setReplicas(currentReplicas);
                }
                return Future.succeededFuture();
            });
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Checks if broker contains any partition replicas when scaling down
     *
     * @param vertx                Instance of vertx
     * @param reconciliation       Reconciliation marker
     * @param kafka                Instance of Kafka Cluster
     * @param kafkaStatus          The Kafka Status class for adding conditions to it during the reconciliation
     * @param secretOperator       Secret operator for working with Secrets
     * @param adminClientProvider  Used to create the Admin client instance
     * @param currentReplicas      The current number of replicas
     * @return  returns a boolean future based on the outcome of the check
     */
    public static Future<Boolean> canScaleDownBrokerCheck(Vertx vertx, Reconciliation reconciliation, KafkaCluster kafka, KafkaStatus kafkaStatus,
                                                      SecretOperator secretOperator, AdminClientProvider adminClientProvider, int currentReplicas) {

        Promise<Boolean> cannotScaleDown = Promise.promise();
        ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
                .compose(compositeFuture -> {
                    Promise<Void> resultPromise = Promise.promise();
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                            future -> {
                                final Future<Collection<TopicDescription>> descriptions;
                                try {
                                    String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
                                    LOGGER.infoCr(reconciliation, "Creating AdminClient for cluster {}/{}", reconciliation.namespace(), kafka.getCluster());
                                    Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, compositeFuture.resultAt(0), compositeFuture.resultAt(1), "cluster-operator");

                                    Future<Set<String>> topicNames = topicNames(reconciliation, kafkaAdmin);

                                    descriptions = topicNames.compose(names -> {
                                        LOGGER.infoCr(reconciliation, "Topic names {}", names);
                                        return describeTopics(reconciliation, kafkaAdmin, names);
                                    });

                                    descriptions
                                            .compose(topicDescriptions -> {
                                                LOGGER.infoCr(reconciliation, "Got {} topic descriptions", topicDescriptions.size());

                                                for (int i = 0; i < currentReplicas - kafka.getReplicas(); i++) {
                                                    boolean result = brokerHasAnyReplicas(reconciliation, topicDescriptions, currentReplicas - i - 1);
                                                    if (result) {
                                                        cannotScaleDown.complete(true);
                                                        break;
                                                    } else {
                                                        cannotScaleDown.complete(false);
                                                    }
                                                    kafkaAdmin.close();
                                                }
                                                return cannotScaleDown.future();
                                            }).recover(error -> {
                                                LOGGER.warnCr(reconciliation, "failed to get topic descriptions", error);
                                                cannotScaleDown.fail(error);
                                                kafka.setReplicas(currentReplicas);
                                                return Future.failedFuture(error);
                                            });

                                } catch (KafkaException e) {
                                    LOGGER.warnCr(reconciliation, "Kafka exception getting clusterId {}", e.getMessage());
                                }
                                future.complete();
                            }, true, resultPromise);
                    return resultPromise.future();
                });

        return cannotScaleDown.future();
    }

    /**
     * This method gets the topic names after interacting with the Admin client
     *
     * @param reconciliation                Instance of vertx
     * @param kafkaAdmin                    Instance of Kafka Admin
     * @return  return the set of topic names
     */
    protected static Future<Set<String>> topicNames(Reconciliation reconciliation, Admin kafkaAdmin) {
        Promise<Set<String>> namesPromise = Promise.promise();
        kafkaAdmin.listTopics(new ListTopicsOptions().listInternal(true)).names()
                .whenComplete((names, error) -> {
                    if (error != null) {
                        namesPromise.fail(error);
                    } else {
                        LOGGER.debugCr(reconciliation, "Got {} topic names", names.size());
                        namesPromise.complete(names);
                    }
                });
        return namesPromise.future();
    }

    /**
     * Returns a collection of topic descriptions
     *
     * @param reconciliation       Reconciliation marker
     * @param kafkaAdmin           Instance of Admin client
     * @param names                Set of topic names
     * @return  Future which completes when the check is complete
     */
    protected static Future<Collection<TopicDescription>> describeTopics(Reconciliation reconciliation, Admin kafkaAdmin, Set<String> names) {
        Promise<Collection<TopicDescription>> descPromise = Promise.promise();
        kafkaAdmin.describeTopics(names).allTopicNames()
                .whenComplete((tds, error) -> {
                    if (error != null) {
                        descPromise.fail(error);
                    } else {
                        LOGGER.debugCr(reconciliation, "Got topic descriptions for {} topics", tds.size());
                        descPromise.complete(tds.values());
                    }
                });
        return descPromise.future();
    }

    /**
     * Checks if broker contains any partition replicas
     *
     * @param reconciliation       Reconciliation marker
     * @param tds                  Collection of Topic description
     * @return  Future which completes when the check is complete
     */
    private static boolean brokerHasAnyReplicas(Reconciliation reconciliation, Collection<TopicDescription> tds, int podId) {

        for (TopicDescription td : tds) {
            LOGGER.traceCr(reconciliation, td);
            for (TopicPartitionInfo pd : td.partitions()) {
                for (org.apache.kafka.common.Node broker : pd.replicas()) {
                    if (podId == broker.id()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
