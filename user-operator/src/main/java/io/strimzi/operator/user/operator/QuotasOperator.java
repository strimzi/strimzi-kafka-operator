/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.model.QuotaUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * KafkaUserQuotasOperator is responsible for managing quotas in Apache Kafka / Apache Zookeeper.
 */
public class QuotasOperator extends AbstractAdminApiOperator<KafkaUserQuotas, Set<String>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(QuotasOperator.class.getName());

    /**
     * Constructor
     *
     * @param vertx Vertx instance
     * @param adminClient Kafka Admin client instance
     */
    public QuotasOperator(Vertx vertx, Admin adminClient) {
        super(vertx, adminClient);
    }

    /**
     * Reconciles Acl rules for given user
     *
     * @param reconciliation    The reconciliation
     * @param username          User name of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired           The desired quotas configuration
     *
     * @return the Future with reconcile result
     */
    @Override
    public Future<ReconcileResult<KafkaUserQuotas>> reconcile(Reconciliation reconciliation, String username, KafkaUserQuotas desired) {
        return getAsync(reconciliation, username)
                .compose(current -> {
                    if (desired == null) {
                        if (current == null)    {
                            LOGGER.debugCr(reconciliation, "No expected quotas and no existing quotas -> NoOp");
                            return Future.succeededFuture(ReconcileResult.noop(null));
                        } else {
                            LOGGER.debugCr(reconciliation, "No expected quotas, but {} existing quotas -> Deleting quotas", current);
                            return internalDelete(reconciliation, username);
                        }
                    } else {
                        if (current == null)  {
                            LOGGER.debugCr(reconciliation, "{} expected quotas, but no existing quotas -> Adding quotas", desired);
                            return internalAlter(reconciliation, username, desired);
                        } else if (!QuotaUtils.quotasEquals(current, desired)) {
                            LOGGER.debugCr(reconciliation, "{} expected quotas and {} existing quotas differ -> Reconciling quotas", desired, current);
                            return internalAlter(reconciliation, username, desired);
                        } else {
                            LOGGER.debugCr(reconciliation, "{} expected quotas are the same as existing quotas -> NoOp", desired);
                            return Future.succeededFuture(ReconcileResult.noop(desired));
                        }
                    }
                });
    }

    /**
     * Delete the quotas for the given user.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     *
     * @return the Future with reconcile result
     */
    public Future<ReconcileResult<KafkaUserQuotas>> internalDelete(Reconciliation reconciliation, String username) {
        LOGGER.debugCr(reconciliation, "Deleting quotas for user {}", username);

        KafkaUserQuotas emptyQuotas = new KafkaUserQuotas();
        emptyQuotas.setProducerByteRate(null);
        emptyQuotas.setConsumerByteRate(null);
        emptyQuotas.setRequestPercentage(null);
        emptyQuotas.setControllerMutationRate(null);

        return internalAlter(reconciliation, username, emptyQuotas)
                .map(ReconcileResult.deleted());
    }

    /**
     * Set the quotas for the given user.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     * @param desired The desired quotas
     *
     * @return the Future with reconcile result
     */
    protected Future<ReconcileResult<KafkaUserQuotas>> internalAlter(Reconciliation reconciliation, String username, KafkaUserQuotas desired) {
        Set<ClientQuotaAlteration.Op> alterations = QuotaUtils.toClientQuotaAlterationOps(desired);

        ClientQuotaEntity cqe = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, username));
        ClientQuotaAlteration cqa = new ClientQuotaAlteration(cqe, alterations);
        return Util.kafkaFutureToVertxFuture(reconciliation, vertx, adminClient.alterClientQuotas(Collections.singleton(cqa)).all())
                .map(ReconcileResult.patched(desired));
    }

    /**
     * Retrieves the quotas for the given user.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     *
     * @return the Future with reconcile result
     */
    protected Future<KafkaUserQuotas> getAsync(Reconciliation reconciliation, String username) {
        ClientQuotaFilterComponent c = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, username);
        ClientQuotaFilter f =  ClientQuotaFilter.contains(List.of(c));

        return Util.kafkaFutureToVertxFuture(reconciliation, vertx, adminClient.describeClientQuotas(f).entities())
                .compose(quotas -> {
                    ClientQuotaEntity cqe = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, username));
                    KafkaUserQuotas current = null;

                    if (quotas.containsKey(cqe)) {
                        current = QuotaUtils.fromClientQuota(quotas.get(cqe));
                    }

                    return Future.succeededFuture(current);
                });
    }

    /**
     * @return Set with all usernames which have some ACLs set
     */
    @Override
    public Future<Set<String>> getAllUsers() {
        LOGGER.debugOp("Searching for Users with any quotas");

        return Util.kafkaFutureToVertxFuture(vertx, adminClient.describeClientQuotas(ClientQuotaFilter.all()).entities())
                .compose(quotas -> {
                    Set<String> users = new HashSet<>(quotas.size());

                    for (ClientQuotaEntity entity : quotas.keySet()) {
                        if (entity.entries().containsKey(ClientQuotaEntity.USER)) {
                            users.add(entity.entries().get(ClientQuotaEntity.USER));
                        }
                    }

                    return Future.succeededFuture(users);
                });
    }
}
