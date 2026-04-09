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
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

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
    void assertDoesNotExist(String username) {
        final int retries = 10;
        final long delay = 250L;

        for (int i = 0; i < retries; i++) {
            ClientQuotaFilterComponent clientQuotaFilterComponent = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, username);
            ClientQuotaFilter clientQuotaFilter = ClientQuotaFilter.contains(List.of(clientQuotaFilterComponent));

            try {
                Map<ClientQuotaEntity, Map<String, Double>> entities = adminClient.describeClientQuotas(clientQuotaFilter).entities().get();
                KafkaUserQuotas userQuotas = null;

                for (var entry : entities.entrySet()) {
                    if (username.equals(entry.getKey().entries().get(ClientQuotaEntity.USER))) {
                        userQuotas = QuotaUtils.fromClientQuota(entry.getValue());
                    }
                }

                if (userQuotas == null) {
                    return;
                } else {
                    Thread.sleep(delay);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while retrying to get quotas", e);
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch quotas for user " + username, e);
            }
        }

        Assertions.fail("User " + username + " still exists after " + retries + " retries");
    }

    @Override
    void assertResource(String username, KafkaUserQuotas expected) {
        final int retries = 10;
        final long delay = 250L;

        for (int i = 0; i < retries; i++) {
            ClientQuotaFilterComponent clientQuotaFilterComponent = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, username);
            ClientQuotaFilter clientQuotaFilter = ClientQuotaFilter.contains(List.of(clientQuotaFilterComponent));

            try {
                Map<ClientQuotaEntity, Map<String, Double>> entities = adminClient.describeClientQuotas(clientQuotaFilter).entities().get();

                for (var entry : entities.entrySet()) {
                    if (username.equals(entry.getKey().entries().get(ClientQuotaEntity.USER))) {
                        if (QuotaUtils.quotasEquals(expected, QuotaUtils.fromClientQuota(entry.getValue())))    {
                            return;
                        }
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

        Assertions.fail("Quotas for user " + username + " do not match expected values after " + retries + " retries");
    }

    // With quotas, we always patch the credentials regardless whether they exist or not
    // So we override this and return true
    @Override
    public boolean createPatches()    {
        return true;
    }
}
