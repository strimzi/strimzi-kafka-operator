/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.cache;

import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.operator.user.model.QuotaUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A periodically updated Quotas Cache for keeping the Quotas locally and avoid querying Kafka
 */
public class QuotasCache extends AbstractCache<KafkaUserQuotas> {
    private final static Logger LOGGER = LogManager.getLogger(QuotasCache.class);

    private final Admin adminClient;

    /**
     * Constructs the Quotas cache
     *
     * @param adminClient           Kafka Admin client
     * @param refreshIntervalMs     Interval in which the cache should be refreshed
     */
    public QuotasCache(Admin adminClient, long refreshIntervalMs) {
        super("Quotas", refreshIntervalMs);
        this.adminClient = adminClient;
    }

    /**
     * Loads the Quotas from Kafka for all users
     *
     * @return  ConcurrentHashMap with all users and their Quotas
     */
    @Override
    protected ConcurrentHashMap<String, KafkaUserQuotas> loadCache() {
        KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> futureQuotas = adminClient.describeClientQuotas(ClientQuotaFilter.all()).entities();

        try {
            Map<ClientQuotaEntity, Map<String, Double>> quotas = futureQuotas.get(1, TimeUnit.MINUTES);
            ConcurrentHashMap<String, KafkaUserQuotas> map = new ConcurrentHashMap<>((int) (quotas.size() / 0.75f));

            for (Map.Entry<ClientQuotaEntity, Map<String, Double>> entry : quotas.entrySet()) {
                // We have to check if the ClientQuotaEntity.USER value is not null, because the entries might contain
                // the default user quota for which the key would exist but the value would be null. And that would
                // throw and NPE when we try to insert it into the cache.
                if (entry.getKey().entries().get(ClientQuotaEntity.USER) != null) {
                    map.put(entry.getKey().entries().get(ClientQuotaEntity.USER), QuotaUtils.fromClientQuota(entry.getValue()));
                }
            }

            return map;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.warn("Failed to load Quotas", e);
            throw new RuntimeException("Failed to load Quotas", e);
        }
    }
}
