/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.cache;

import io.strimzi.operator.user.model.acl.SimpleAclRule;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AclCacheTest {
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
        KafkaFuture<Collection<AclBinding>> mockFuture = mock(KafkaFuture.class);

        AclBinding myUserRead = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL),
                new AccessControlEntry("User:my-user", "*", AclOperation.READ, AclPermissionType.ALLOW)
        );
        AclBinding myUserWrite = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL),
                new AccessControlEntry("User:my-user", "*", AclOperation.WRITE, AclPermissionType.ALLOW)
        );
        AclBinding myUser2Read = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL),
                new AccessControlEntry("User:my-user2", "*", AclOperation.READ, AclPermissionType.ALLOW)
        );

        when(mockFuture.get(anyLong(), any())).thenAnswer(i -> {
            if (initialData.get()) {
                return List.of(myUserRead, myUserWrite, myUser2Read);
            } else {
                return List.of(myUser2Read);
            }
        });

        DescribeAclsResult mockResult = mock(DescribeAclsResult.class);
        when(mockResult.values()).thenReturn(mockFuture);

        // Mock call
        ArgumentCaptor<AclBindingFilter> aclBindingsFilterCaptor = ArgumentCaptor.forClass(AclBindingFilter.class);
        when(mockClient.describeAcls(aclBindingsFilterCaptor.capture())).thenAnswer(i -> {
            if (initialData.get())  {
                initialLoad.countDown();
            } else {
                update.countDown();
            }

            return mockResult;
        });

        AclCache cache = new AclCache(mockClient, 10);

        try {
            cache.start();

            // Wait for the initial data load
            initialLoad.await();

            assertThat(cache.get("my-user").size(), is(2));
            assertThat(cache.get("my-user"), hasItems(SimpleAclRule.fromAclBinding(myUserRead), SimpleAclRule.fromAclBinding(myUserWrite)));
            assertThat(cache.get("my-user2").size(), is(1));
            assertThat(cache.get("my-user2"), hasItems(SimpleAclRule.fromAclBinding(myUser2Read)));

            // Check update data after another call
            initialData.set(false);
            update.await();

            assertThat(cache.get("my-user"), is(nullValue()));
            assertThat(cache.get("my-user2").size(), is(1));
            assertThat(cache.get("my-user2"), hasItems(SimpleAclRule.fromAclBinding(myUser2Read)));

            // Check the parameters
            assertThat(aclBindingsFilterCaptor.getAllValues().size(), is(greaterThanOrEqualTo(4)));
            assertThat(aclBindingsFilterCaptor.getValue(), is(AclBindingFilter.ANY));
        } finally   {
            cache.stop();
        }
    }
}
