/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.model.QuotaUtils;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class QuotasOperatorIT extends AdminApiOperatorIT<KafkaUserQuotas, Set<String>> {
    @Override
    AdminApiOperator<KafkaUserQuotas, Set<String>> operator() {
        return new QuotasOperator(adminClient, ResourceUtils.createUserOperatorConfig(), Executors.newSingleThreadExecutor());
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
    KafkaUserQuotas get(String username) {
        final int retries = 3;
        final long delay = 500L;

        for (int i = 0; i < retries; i++) {
            ClientQuotaFilterComponent clientQuotaFilterComponent = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, username);
            ClientQuotaFilter clientQuotaFilter = ClientQuotaFilter.contains(List.of(clientQuotaFilterComponent));

            try {
                Map<ClientQuotaEntity, Map<String, Double>> entities = adminClient.describeClientQuotas(clientQuotaFilter).entities().get();

                for (var entry : entities.entrySet()) {
                    if (username.equals(entry.getKey().entries().get(ClientQuotaEntity.USER))) {
                        return QuotaUtils.fromClientQuota(entry.getValue());
                    }
                }

                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while retrying to get quotas", e);
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch quotas for user " + username, e);
            }
        }
        return null;
    }

    @Override
    void assertResources(KafkaUserQuotas expected, KafkaUserQuotas actual) {
        assertThat(QuotaUtils.quotasEquals(expected, actual), is(true));
    }

    // With quotas, we always patch the credentials regardless whether they exist or not
    // So we override this and return true
    @Override
    public boolean createPatches()    {
        return true;
    }
}
