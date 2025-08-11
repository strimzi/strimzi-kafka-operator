/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.UserOperatorConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

import java.util.List;
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

public class ScramCredentialOperatorTest {
    private final static ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    @Test
    public void testGetAllUsers() throws ExecutionException, InterruptedException, TimeoutException {
        ScramCredentialsOperator credentialOp = new ScramCredentialsOperator(mockCredentialsForVariousUsers(), ResourceUtils.createUserOperatorConfig(), EXECUTOR);
        credentialOp.start();

        try {
            Set<String> users = credentialOp.getAllUsers().toCompletableFuture().get();
            assertThat(users, is(Set.of("foo", "bar", "baz")));
        } finally {
            credentialOp.stop();
        }
    }

    @Test
    public void testGetAllUsersCustomIgnoredUsers() throws ExecutionException, InterruptedException, TimeoutException {
        UserOperatorConfig config = new UserOperatorConfig.UserOperatorConfigBuilder(ResourceUtils.createUserOperatorConfig())
                .with(UserOperatorConfig.IGNORED_USERS_PATTERN.key(), "^foo")
                .build();

        ScramCredentialsOperator credentialOp = new ScramCredentialsOperator(mockCredentialsForVariousUsers(), config, EXECUTOR);
        credentialOp.start();

        try {
            Set<String> users = credentialOp.getAllUsers().toCompletableFuture().get();
            assertThat(users, is(Set.of("bar", "baz")));
        } finally {
            credentialOp.stop();
        }
    }

    private static Admin mockCredentialsForVariousUsers() throws ExecutionException, InterruptedException, TimeoutException  {
        Admin mockAdminClient = mock(AdminClient.class);

        // Mock result
        @SuppressWarnings("unchecked")
        KafkaFuture<List<String>> mockFuture = mock(KafkaFuture.class);

        when(mockFuture.get(anyLong(), any())).thenAnswer(i -> List.of("foo", "bar", "baz"));

        DescribeUserScramCredentialsResult mockResult = mock(DescribeUserScramCredentialsResult.class);
        when(mockResult.users()).thenReturn(mockFuture);

        // Mock call
        when(mockAdminClient.describeUserScramCredentials()).thenAnswer(i -> mockResult);

        return mockAdminClient;
    }
}