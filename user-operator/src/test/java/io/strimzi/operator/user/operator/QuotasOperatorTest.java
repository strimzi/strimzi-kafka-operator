/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.UserOperatorConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QuotasOperatorTest {
    private final static ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    @Test
    public void testGetAllUsers() throws ExecutionException, InterruptedException, TimeoutException {
        QuotasOperator quotasOp = new QuotasOperator(mockQuotasForVariousUsers(), ResourceUtils.createUserOperatorConfig(), EXECUTOR);
        quotasOp.start();

        try {
            Set<String> users = quotasOp.getAllUsers().toCompletableFuture().get();
            assertThat(users, is(Set.of("foo", "bar", "baz")));
        } finally {
            quotasOp.stop();
        }
    }

    @Test
    public void testGetAllUsersCustomIgnoredUsers() throws ExecutionException, InterruptedException, TimeoutException {
        UserOperatorConfig config = new UserOperatorConfig.UserOperatorConfigBuilder(ResourceUtils.createUserOperatorConfig())
                .with(UserOperatorConfig.IGNORED_USERS_PATTERN.key(), "^CN=foo|bar")
                .build();

        QuotasOperator quotasOp = new QuotasOperator(mockQuotasForVariousUsers(), config, EXECUTOR);
        quotasOp.start();

        try {
            Set<String> users = quotasOp.getAllUsers().toCompletableFuture().get();
            assertThat(users, is(Set.of("bar", "baz")));
        } finally {
            quotasOp.stop();
        }
    }

    private static Admin mockQuotasForVariousUsers() throws ExecutionException, InterruptedException, TimeoutException  {
        Admin mockAdminClient = mock(AdminClient.class);

        // Mock result
        @SuppressWarnings("unchecked")
        KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> mockFuture = mock(KafkaFuture.class);

        // For the default user quota, we need to set the user to null value which is not possible with Map.of(). So we use HashMap.
        Map<String, String> defaultUserEntityMap = new HashMap<>();
        defaultUserEntityMap.put(ClientQuotaEntity.USER, null);
        ClientQuotaEntity defaultUserEntity = new ClientQuotaEntity(defaultUserEntityMap);
        Map<String, Double> defaultUserQuotas = Map.of("producer_byte_rate", 1024.0, "consumer_byte_rate", 2048.0);

        ClientQuotaEntity fooEntity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "CN=foo"));
        Map<String, Double> fooQuotas = Map.of("producer_byte_rate", 1024.0, "consumer_byte_rate", 2048.0);

        ClientQuotaEntity barEntity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "CN=bar"));
        Map<String, Double> barQuotas = Map.of("producer_byte_rate", 1024000.0, "consumer_byte_rate", 2048000.0);

        ClientQuotaEntity bazEntity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "baz"));
        Map<String, Double> bazQuotas = Map.of("producer_byte_rate", 1024000.0, "consumer_byte_rate", 2048000.0);

        when(mockFuture.get(anyLong(), any())).thenAnswer(i -> Map.of(defaultUserEntity, defaultUserQuotas, fooEntity, fooQuotas, barEntity, barQuotas, bazEntity, bazQuotas));

        DescribeClientQuotasResult mockResult = mock(DescribeClientQuotasResult.class);
        when(mockResult.entities()).thenReturn(mockFuture);

        ArgumentCaptor<ClientQuotaFilter> clientQuotaFilterCaptor = ArgumentCaptor.forClass(ClientQuotaFilter.class);
        when(mockAdminClient.describeClientQuotas(clientQuotaFilterCaptor.capture())).thenAnswer(i -> mockResult);

        return mockAdminClient;
    }
}