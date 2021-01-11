/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class KafkaUserQuotasOperator {
    private static final Logger log = LogManager.getLogger(KafkaUserQuotasOperator.class.getName());

    private final Vertx vertx;
    private final Admin adminClient;

    public KafkaUserQuotasOperator(Vertx vertx, Admin adminClient) {
        this.vertx = vertx;
        this.adminClient = adminClient;
    }

    Future<ReconcileResult<KafkaUserQuotas>> reconcile(String username, KafkaUserQuotas quotas) {
        Promise<ReconcileResult<KafkaUserQuotas>> prom = Promise.promise();
        
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    boolean exists = exists(username);
                    if (quotas != null) {
                        createOrUpdate(username, quotas);
                        future.complete(exists ? ReconcileResult.created(quotas) : ReconcileResult.patched(quotas));
                    } else {
                        if (exists) {
                            delete(username);
                            future.complete(ReconcileResult.deleted());
                        } else {
                            future.complete(ReconcileResult.noop(null));
                        }
                    }
                } catch (Throwable t) {
                    prom.fail(t);
                }
            },
            false,
            prom);

        return prom.future();
    }

    /**
     * Create or update the quotas for the given user.
     *
     * @param username The name of the user which should be created or updated
     * @param quotas The desired user quotas
     * @throws Exception when altering quotas fails
     */
    public void createOrUpdate(String username, KafkaUserQuotas quotas) throws Exception {
        KafkaUserQuotas current = describeUserQuotas(username);
        if (current != null) {
            log.debug("Checking quota updates for user {}", username);
            if (!quotasEquals(current, quotas)) {
                log.debug("Updating quotas for user {}", username);
                alterUserQuotas(username, toClientQuotaAlterationOps(quotas));
            } else {
                log.debug("Nothing to update in quotas for user {}", username);
            }
        } else {
            log.debug("Creating quotas for user {}", username);
            alterUserQuotas(username, toClientQuotaAlterationOps(quotas));
        }
    }

    /**
     * Determine whether the given user has quotas.
     *
     * @param username Name of the user
     *
     * @return True if the user exists
     */
    boolean exists(String username) throws Exception {
        return describeUserQuotas(username) != null;
    }

    /**
     * Delete the quotas for the given user.
     * It is not an error if the user doesn't exist, or doesn't currently have any quotas.
     *
     * @param username Name of the user
     * @throws Exception when altering quotas fails
     */
    public void delete(String username) throws Exception {
        KafkaUserQuotas current = describeUserQuotas(username);
        if (current != null) {
            log.debug("Deleting quotas for user {}", username);
            current.setProducerByteRate(null);
            current.setConsumerByteRate(null);
            current.setRequestPercentage(null);
            alterUserQuotas(username, toClientQuotaAlterationOps(current));
        } else {
            log.warn("Quotas for user {} already don't exist", username);
        }
    }

    protected void alterUserQuotas(String username, Set<ClientQuotaAlteration.Op> ops) throws Exception {
        ClientQuotaEntity cqe = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, username));
        ClientQuotaAlteration cqa = new ClientQuotaAlteration(cqe, ops);
        try {
            adminClient.alterClientQuotas(Collections.singleton(cqa)).all().get();
        } catch (Exception e) {
            log.error("Creating/Altering quotas for user {} failed", username, e);
            throw e;
        }
    }

    protected KafkaUserQuotas describeUserQuotas(String username) throws Exception {
        ClientQuotaFilterComponent c = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, username);
        ClientQuotaFilter f =  ClientQuotaFilter.contains(Collections.singleton(c));
        KafkaUserQuotas current = null;
        try {
            ClientQuotaEntity cqe = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, username));
            Map<ClientQuotaEntity, Map<String, Double>> map = adminClient.describeClientQuotas(f).entities().get();
            if (map.containsKey(cqe)) {
                current = fromClientQuota(map.get(cqe));
            }
        } catch (Exception e) {
            log.error("Getting quotas for user {} failed", username, e);
            throw e;
        }
        return current;
    }

    /**
     * Returns a KafkaUserQuotas instance from a map of quotas key-value pairs
     *
     * @param map map of quotas key-value pairs
     * @return KafkaUserQuotas instance
     */
    protected KafkaUserQuotas fromClientQuota(Map<String, Double> map) {
        KafkaUserQuotas kuq = new KafkaUserQuotas();
        if (map.containsKey("producer_byte_rate")) {
            kuq.setProducerByteRate(map.get("producer_byte_rate").intValue());
        }
        if (map.containsKey("consumer_byte_rate")) {
            kuq.setConsumerByteRate(map.get("consumer_byte_rate").intValue());
        }
        if (map.containsKey("request_percentage")) {
            kuq.setRequestPercentage(map.get("request_percentage").intValue());
        }
        return kuq;
    }

    /**
     * Map a KafkaUserQuotas instance to a corresponding set of ClientQuotaAlteration operations for the Admin Client
     *
     * @param quotas KafkaUserQuotas instance to map
     * @return ClientQuotaAlteration operations for the Admin Client
     */
    protected Set<ClientQuotaAlteration.Op> toClientQuotaAlterationOps(KafkaUserQuotas quotas) {
        Set<ClientQuotaAlteration.Op> ops = new HashSet<>(3);
        ops.add(new ClientQuotaAlteration.Op("producer_byte_rate",
                quotas.getProducerByteRate() != null ? Double.valueOf(quotas.getProducerByteRate()) : null));
        ops.add(new ClientQuotaAlteration.Op("consumer_byte_rate",
                quotas.getConsumerByteRate() != null ? Double.valueOf(quotas.getConsumerByteRate()) : null));
        ops.add(new ClientQuotaAlteration.Op("request_percentage",
                quotas.getRequestPercentage() != null ? Double.valueOf(quotas.getRequestPercentage()) : null));
        return ops;
    }

    /**
     * Check if two KafkaUserQuotas instances are equal
     *
     * @param kuq1 first instance to compare
     * @param kuq2 second instance to compare
     * @return true if they are equals, false otherwise
     */
    private boolean quotasEquals(KafkaUserQuotas kuq1, KafkaUserQuotas kuq2) {
        return Objects.equals(kuq1.getProducerByteRate(), kuq2.getProducerByteRate()) &&
                Objects.equals(kuq1.getConsumerByteRate(), kuq2.getConsumerByteRate()) &&
                Objects.equals(kuq1.getRequestPercentage(), kuq2.getRequestPercentage());
    }
}
