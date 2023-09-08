/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.cache;

import io.strimzi.operator.user.model.QuotaUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QuotasCacheTest {
    // Tests the cache in the following way:
    //   * Mocks the Admin API call
    //   * Makes the mock return two different results (initial data and updated data) to test the changes
    //   * Lets the initial load happen and tests the initial data
    //   * Flips the switch to move to the updated data
    //   * Tests the updated data
    @Test
    public void testCache() throws InterruptedException, ExecutionException, TimeoutException {
        CountDownLatch initialLoad = new CountDownLatch(2);
        CountDownLatch update = new CountDownLatch(2);
        AtomicBoolean initialData = new AtomicBoolean(true);

        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        @SuppressWarnings("unchecked")
        KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> mockFuture = mock(KafkaFuture.class);

        // For the default user quota, we need to set the user to null value which is not possible with Map.of(). So we use HashMap.
        Map<String, String> defaultUserEntityMap = new HashMap<>();
        defaultUserEntityMap.put(ClientQuotaEntity.USER, null);
        ClientQuotaEntity defaultUserEntity = new ClientQuotaEntity(defaultUserEntityMap);
        Map<String, Double> defaultUserQuotas = Map.of("producer_byte_rate", 1024.0, "consumer_byte_rate", 2048.0);

        ClientQuotaEntity myUserEntity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "my-user"));
        Map<String, Double> myUserQuotas = Map.of("producer_byte_rate", 1024.0, "consumer_byte_rate", 2048.0);

        ClientQuotaEntity myUser2Entity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "my-user2"));
        Map<String, Double> myUser2Quotas = Map.of("producer_byte_rate", 1024000.0, "consumer_byte_rate", 2048000.0);
        Map<String, Double> myUser2QuotasUpdated = Map.of("producer_byte_rate", 4096000.0, "consumer_byte_rate", 4096000.0);

        when(mockFuture.get(anyLong(), any())).thenAnswer(i -> {
            if (initialData.get()) {
                return Map.of(defaultUserEntity, defaultUserQuotas, myUserEntity, myUserQuotas, myUser2Entity, myUser2Quotas);
            } else {
                return Map.of(defaultUserEntity, defaultUserQuotas, myUser2Entity, myUser2QuotasUpdated);
            }
        });

        DescribeClientQuotasResult mockResult = mock(DescribeClientQuotasResult.class);
        when(mockResult.entities()).thenReturn(mockFuture);

        // Mock call
        ArgumentCaptor<ClientQuotaFilter> clientQuotaFilterCaptor = ArgumentCaptor.forClass(ClientQuotaFilter.class);
        when(mockClient.describeClientQuotas(clientQuotaFilterCaptor.capture())).thenAnswer(i -> {
            if (initialData.get())  {
                initialLoad.countDown();
            } else {
                update.countDown();
            }

            return mockResult;
        });

        QuotasCache cache = new QuotasCache(mockClient, 10);

        try {
            cache.start();

            // Wait for the initial data load
            initialLoad.await();

            assertThat(cache.get("my-user"), is(QuotaUtils.fromClientQuota(myUserQuotas)));
            assertThat(cache.get("my-user2"), is(QuotaUtils.fromClientQuota(myUser2Quotas)));

            // Check update data after another call
            initialData.set(false);
            update.await();

            assertThat(cache.get("my-user"), is(nullValue()));
            assertThat(cache.get("my-user2"), is(QuotaUtils.fromClientQuota(myUser2QuotasUpdated)));

            // Check the parameters
            assertThat(clientQuotaFilterCaptor.getAllValues().size(), is(greaterThanOrEqualTo(4)));
            assertThat(clientQuotaFilterCaptor.getValue(), is(ClientQuotaFilter.all()));
        } finally   {
            cache.stop();
        }
    }
}
