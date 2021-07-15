/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.operator.user.model.QuotaUtils;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@ExtendWith(VertxExtension.class)
public class QuotasOperatorIT extends AbstractAdminApiOperatorIT<KafkaUserQuotas, Set<String>> {
    @Override
    AbstractAdminApiOperator<KafkaUserQuotas, Set<String>> operator() {
        return new QuotasOperator(vertx, adminClient);
    }

    @Override
    KafkaUserQuotas getOriginal() {
        KafkaUserQuotas quotas = new KafkaUserQuotas();

        quotas.setProducerByteRate(1024 * 1024);
        quotas.setConsumerByteRate(10 * 1024 * 1024);
        quotas.setRequestPercentage(55);
        quotas.setControllerMutationRate(100.0);

        return quotas;
    }

    @Override
    KafkaUserQuotas getModified() {
        KafkaUserQuotas quotas = new KafkaUserQuotas();

        quotas.setProducerByteRate(2 * 1024 * 1024);
        quotas.setConsumerByteRate(5 * 1024 * 1024);
        quotas.setRequestPercentage(10);

        return quotas;
    }

    @Override
    KafkaUserQuotas get() {
        ClientQuotaFilterComponent c = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, USERNAME);
        ClientQuotaFilter f =  ClientQuotaFilter.contains(List.of(c));

        Map<ClientQuotaEntity, Map<String, Double>> quotas;
        try {
            quotas = adminClient.describeClientQuotas(f).entities().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get quotas", e);
        }

        ClientQuotaEntity cqe = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, USERNAME));

        if (quotas.containsKey(cqe)) {
            return QuotaUtils.fromClientQuota(quotas.get(cqe));
        } else {
            return null;
        }
    }

    @Override
    void assertResources(VertxTestContext context, KafkaUserQuotas expected, KafkaUserQuotas actual) {
        assertThat(QuotaUtils.quotasEquals(expected, actual), is(true));
    }
}
