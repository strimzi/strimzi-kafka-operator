/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import org.apache.kafka.common.quota.ClientQuotaAlteration;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utils for working with Kafka Quotas
 */
public class QuotaUtils {
    /**
     * Returns a KafkaUserQuotas instance from a map of quotas key-value pairs
     *
     * @param map map of quotas key-value pairs
     * @return KafkaUserQuotas instance
     */
    public static KafkaUserQuotas fromClientQuota(Map<String, Double> map) {
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
        if (map.containsKey("controller_mutation_rate")) {
            kuq.setControllerMutationRate(map.get("controller_mutation_rate"));
        }
        return kuq;
    }

    /**
     * Map a KafkaUserQuotas instance to a corresponding set of ClientQuotaAlteration operations for the Admin Client
     *
     * @param quotas KafkaUserQuotas instance to map
     * @return ClientQuotaAlteration operations for the Admin Client
     */
    public static Set<ClientQuotaAlteration.Op> toClientQuotaAlterationOps(KafkaUserQuotas quotas) {
        Set<ClientQuotaAlteration.Op> ops = new HashSet<>(3);
        ops.add(new ClientQuotaAlteration.Op("producer_byte_rate",
                quotas.getProducerByteRate() != null ? Double.valueOf(quotas.getProducerByteRate()) : null));
        ops.add(new ClientQuotaAlteration.Op("consumer_byte_rate",
                quotas.getConsumerByteRate() != null ? Double.valueOf(quotas.getConsumerByteRate()) : null));
        ops.add(new ClientQuotaAlteration.Op("request_percentage",
                quotas.getRequestPercentage() != null ? Double.valueOf(quotas.getRequestPercentage()) : null));
        ops.add(new ClientQuotaAlteration.Op("controller_mutation_rate",
                quotas.getControllerMutationRate() != null ? quotas.getControllerMutationRate() : null));
        return ops;
    }

    /**
     * Check if two KafkaUserQuotas instances are equal
     *
     * @param kuq1 first instance to compare
     * @param kuq2 second instance to compare
     * @return true if they are equals, false otherwise
     */
    public static boolean quotasEquals(KafkaUserQuotas kuq1, KafkaUserQuotas kuq2) {
        return Objects.equals(kuq1.getProducerByteRate(), kuq2.getProducerByteRate()) &&
                Objects.equals(kuq1.getConsumerByteRate(), kuq2.getConsumerByteRate()) &&
                Objects.equals(kuq1.getRequestPercentage(), kuq2.getRequestPercentage()) &&
                Objects.equals(kuq1.getControllerMutationRate(), kuq2.getControllerMutationRate());
    }
}
