/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.operator.AdminApiOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AddAclsBatchReconcilerTest {
    // Requests used for testing
    private static final AclBinding MY_USER_READ = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL),
            new AccessControlEntry("User:my-user", "*", AclOperation.READ, AclPermissionType.ALLOW)
    );
    private static final AclBinding MY_USER_WRITE = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL),
            new AccessControlEntry("User:my-user", "*", AclOperation.WRITE, AclPermissionType.ALLOW)
    );
    private static final AclBinding MY_USER_2_READ = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
            new AccessControlEntry("User:my-user2", "*", AclOperation.READ, AclPermissionType.ALLOW)
    );
    private static final AclBinding MY_USER_2_WRITE = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
            new AccessControlEntry("User:my-user2", "*", AclOperation.WRITE, AclPermissionType.ALLOW)
    );
    private static final AclBinding MY_USER_3_READ = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
            new AccessControlEntry("User:my-user3", "*", AclOperation.READ, AclPermissionType.ALLOW)
    );

    @Test
    public void testBatching() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        CreateAclsResult mockResult = mock(CreateAclsResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(
                Map.of(MY_USER_READ, KafkaFuture.completedFuture(null), MY_USER_WRITE, KafkaFuture.completedFuture(null),
                        MY_USER_2_READ, KafkaFuture.completedFuture(null), MY_USER_2_WRITE, KafkaFuture.completedFuture(null),
                        MY_USER_3_READ, KafkaFuture.completedFuture(null))
        );

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<AclBinding>> aclBindingsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.createAcls(aclBindingsCaptor.capture())).thenReturn(mockResult);

        // Test
        AddAclsBatchReconciler reconciler = new AddAclsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<Collection<AclBinding>>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", List.of(MY_USER_READ, MY_USER_WRITE), myUserFuture));

            CompletableFuture<ReconcileResult<Collection<AclBinding>>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", List.of(MY_USER_2_READ, MY_USER_2_WRITE), myUser2Future));

            CompletableFuture<ReconcileResult<Collection<AclBinding>>> myUser3Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user3", List.of(MY_USER_3_READ), myUser3Future));

            // Wait for completion
            ReconcileResult<Collection<AclBinding>> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ReconcileResult<Collection<AclBinding>> myUser2Result = myUser2Future.get(1_000, TimeUnit.MILLISECONDS);
            ReconcileResult<Collection<AclBinding>> myUser3Result = myUser3Future.get(1_000, TimeUnit.MILLISECONDS);

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(instanceOf(ReconcileResult.Created.class)));
            assertThat(myUserResult.resource().size(), is(2));
            assertThat(myUserResult.resource(), hasItems(MY_USER_READ, MY_USER_WRITE));

            assertThat(myUser2Result, is(notNullValue()));
            assertThat(myUser2Result, is(instanceOf(ReconcileResult.Created.class)));
            assertThat(myUser2Result.resource().size(), is(2));
            assertThat(myUser2Result.resource(), hasItems(MY_USER_2_READ, MY_USER_2_WRITE));

            assertThat(myUser3Result, is(notNullValue()));
            assertThat(myUser3Result, is(instanceOf(ReconcileResult.Created.class)));
            assertThat(myUser3Result.resource().size(), is(1));
            assertThat(myUser3Result.resource(), hasItems(MY_USER_3_READ));

            // Test request
            assertThat(aclBindingsCaptor.getAllValues().size(), is(1));
            assertThat(aclBindingsCaptor.getValue().size(), is(5));
            assertThat(aclBindingsCaptor.getValue(), hasItems(MY_USER_READ, MY_USER_WRITE, MY_USER_2_READ, MY_USER_2_WRITE, MY_USER_3_READ));
        } finally {
            reconciler.stop();
        }
    }

    @Test
    public void testCompleteFailurePropagation() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        KafkaFuture<Void> mockFuture = mock(KafkaFuture.class);
        when(mockFuture.toCompletionStage()).thenReturn(CompletableFuture.failedStage(new RuntimeException("Something failed")));

        CreateAclsResult mockResult = mock(CreateAclsResult.class);
        when(mockResult.all()).thenReturn(mockFuture);

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<AclBinding>> aclBindingsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.createAcls(aclBindingsCaptor.capture())).thenReturn(mockResult);

        // Test
        AddAclsBatchReconciler reconciler = new AddAclsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<Collection<AclBinding>>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", List.of(MY_USER_READ, MY_USER_WRITE), myUserFuture));

            CompletableFuture<ReconcileResult<Collection<AclBinding>>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", List.of(MY_USER_2_READ, MY_USER_2_WRITE), myUser2Future));

            // Wait for completion
            ExecutionException myUserException = assertThrows(ExecutionException.class, () -> myUserFuture.get(1_000, TimeUnit.MILLISECONDS));
            ExecutionException myUser2Exception = assertThrows(ExecutionException.class, () -> myUser2Future.get(1_000, TimeUnit.MILLISECONDS));

            // Test results
            assertThat(myUserException.getCause().getMessage(), is("Something failed"));
            assertThat(myUser2Exception.getCause().getMessage(), is("Something failed"));

            // Test request
            assertThat(aclBindingsCaptor.getAllValues().size(), is(1));
            assertThat(aclBindingsCaptor.getValue().size(), is(4));
            assertThat(aclBindingsCaptor.getValue(), hasItems(MY_USER_READ, MY_USER_WRITE, MY_USER_2_READ, MY_USER_2_WRITE));
        } finally {
            reconciler.stop();
        }
    }

    @Test
    public void testPartialFailurePropagation() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        KafkaFuture<Void> mockMyUser2Future = mock(KafkaFuture.class);
        when(mockMyUser2Future.isCompletedExceptionally()).thenReturn(true);
        when(mockMyUser2Future.getNow(any())).thenThrow(new RuntimeException("Something failed"));

        CreateAclsResult mockResult = mock(CreateAclsResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(
                Map.of(MY_USER_READ, KafkaFuture.completedFuture(null), MY_USER_WRITE, KafkaFuture.completedFuture(null),
                        MY_USER_2_READ, mockMyUser2Future, MY_USER_2_WRITE, KafkaFuture.completedFuture(null))
        );

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<AclBinding>> aclBindingsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.createAcls(aclBindingsCaptor.capture())).thenReturn(mockResult);

        // Test
        AddAclsBatchReconciler reconciler = new AddAclsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<Collection<AclBinding>>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", List.of(MY_USER_READ, MY_USER_WRITE), myUserFuture));

            CompletableFuture<ReconcileResult<Collection<AclBinding>>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", List.of(MY_USER_2_READ, MY_USER_2_WRITE), myUser2Future));

            // Wait for completion
            ReconcileResult<Collection<AclBinding>> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ExecutionException myUser2Exception = assertThrows(ExecutionException.class, () -> myUser2Future.get(1_000, TimeUnit.MILLISECONDS));

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(instanceOf(ReconcileResult.Created.class)));
            assertThat(myUserResult.resource().size(), is(2));
            assertThat(myUserResult.resource(), hasItems(MY_USER_READ, MY_USER_WRITE));

            assertThat(myUser2Exception.getCause().getMessage(), is("ACL creation failed"));

            // Test request
            assertThat(aclBindingsCaptor.getAllValues().size(), is(1));
            assertThat(aclBindingsCaptor.getValue().size(), is(4));
            assertThat(aclBindingsCaptor.getValue(), hasItems(MY_USER_READ, MY_USER_WRITE, MY_USER_2_READ, MY_USER_2_WRITE));
        } finally {
            reconciler.stop();
        }
    }
}
