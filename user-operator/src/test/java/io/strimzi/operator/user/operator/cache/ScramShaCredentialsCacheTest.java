/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.cache;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScramShaCredentialsCacheTest {
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
        KafkaFuture<List<String>> mockFuture = mock(KafkaFuture.class);

        when(mockFuture.get(anyLong(), any())).thenAnswer(i -> {
            if (initialData.get()) {
                return List.of("my-user", "my-user2");
            } else {
                return List.of("my-user2");
            }
        });

        DescribeUserScramCredentialsResult mockResult = mock(DescribeUserScramCredentialsResult.class);
        when(mockResult.users()).thenReturn(mockFuture);

        // Mock call
        when(mockClient.describeUserScramCredentials()).thenAnswer(i -> {
            if (initialData.get())  {
                initialLoad.countDown();
            } else {
                update.countDown();
            }

            return mockResult;
        });

        ScramShaCredentialsCache cache = new ScramShaCredentialsCache(mockClient, 10);

        try {
            cache.start();

            // Wait for the initial data load
            initialLoad.await();

            assertThat(cache.get("my-user"), is(true));
            assertThat(cache.get("my-user2"), is(true));

            // Check update data after another call
            initialData.set(false);
            update.await();

            assertThat(cache.get("my-user"), is(nullValue()));
            assertThat(cache.get("my-user2"), is(true));

            // Check the parameters
            verify(mockClient, atLeast(4)).describeUserScramCredentials();
        } finally   {
            cache.stop();
        }
    }
}
