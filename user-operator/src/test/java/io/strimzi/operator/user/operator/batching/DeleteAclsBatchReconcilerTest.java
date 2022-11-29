/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.operator.AdminApiOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteAclsBatchReconcilerTest {
    // Requests used for testing
    private static final AclBindingFilter MY_USER_READ = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TOPIC, "my-topic", PatternType.LITERAL),
            new AccessControlEntryFilter("User:my-user", "*", AclOperation.READ, AclPermissionType.ALLOW)
    );
    private static final AclBindingFilter MY_USER_WRITE = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TOPIC, "my-topic", PatternType.LITERAL),
            new AccessControlEntryFilter("User:my-user", "*", AclOperation.WRITE, AclPermissionType.ALLOW)
    );
    private static final AclBindingFilter MY_USER_2_READ = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
            new AccessControlEntryFilter("User:my-user2", "*", AclOperation.READ, AclPermissionType.ALLOW)
    );
    private static final AclBindingFilter MY_USER_2_WRITE = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
            new AccessControlEntryFilter("User:my-user2", "*", AclOperation.WRITE, AclPermissionType.ALLOW)
    );
    private static final AclBindingFilter MY_USER_3_READ = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
            new AccessControlEntryFilter("User:my-user3", "*", AclOperation.READ, AclPermissionType.ALLOW)
    );

    @Test
    public void testBatching() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        DeleteAclsResult mockResult = mock(DeleteAclsResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(
                Map.of(MY_USER_READ, KafkaFuture.completedFuture(null), MY_USER_WRITE, KafkaFuture.completedFuture(null),
                        MY_USER_2_READ, KafkaFuture.completedFuture(null), MY_USER_2_WRITE, KafkaFuture.completedFuture(null),
                        MY_USER_3_READ, KafkaFuture.completedFuture(null))
        );

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<AclBindingFilter>> aclBindingsFilterCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.deleteAcls(aclBindingsFilterCaptor.capture())).thenReturn(mockResult);

        // Test
        DeleteAclsBatchReconciler reconciler = new DeleteAclsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", List.of(MY_USER_READ, MY_USER_WRITE), myUserFuture));

            CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", List.of(MY_USER_2_READ, MY_USER_2_WRITE), myUser2Future));

            CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> myUser3Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user3", List.of(MY_USER_3_READ), myUser3Future));

            // Wait for completion
            ReconcileResult<Collection<AclBindingFilter>> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ReconcileResult<Collection<AclBindingFilter>> myUser2Result = myUser2Future.get(1_000, TimeUnit.MILLISECONDS);
            ReconcileResult<Collection<AclBindingFilter>> myUser3Result = myUser3Future.get(1_000, TimeUnit.MILLISECONDS);

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(ReconcileResult.deleted()));

            assertThat(myUser2Result, is(notNullValue()));
            assertThat(myUser2Result, is(ReconcileResult.deleted()));

            assertThat(myUser3Result, is(notNullValue()));
            assertThat(myUser3Result, is(ReconcileResult.deleted()));

            // Test request
            assertThat(aclBindingsFilterCaptor.getAllValues().size(), is(1));
            assertThat(aclBindingsFilterCaptor.getValue().size(), is(5));
            assertThat(aclBindingsFilterCaptor.getValue(), hasItems(MY_USER_READ, MY_USER_WRITE, MY_USER_2_READ, MY_USER_2_WRITE, MY_USER_3_READ));
        } finally {
            reconciler.stop();
        }
    }

    @Test
    public void testCompleteFailurePropagation() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        KafkaFuture<Collection<AclBinding>> mockFuture = mock(KafkaFuture.class);
        when(mockFuture.toCompletionStage()).thenReturn(CompletableFuture.failedStage(new RuntimeException("Something failed")));

        DeleteAclsResult mockResult = mock(DeleteAclsResult.class);
        when(mockResult.all()).thenReturn(mockFuture);

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<AclBindingFilter>> aclBindingsFilterCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.deleteAcls(aclBindingsFilterCaptor.capture())).thenReturn(mockResult);

        // Test
        DeleteAclsBatchReconciler reconciler = new DeleteAclsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", List.of(MY_USER_READ, MY_USER_WRITE), myUserFuture));

            CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", List.of(MY_USER_2_READ, MY_USER_2_WRITE), myUser2Future));

            // Wait for completion
            ExecutionException myUserException = assertThrows(ExecutionException.class, () -> myUserFuture.get(1_000, TimeUnit.MILLISECONDS));
            ExecutionException myUser2Exception = assertThrows(ExecutionException.class, () -> myUser2Future.get(1_000, TimeUnit.MILLISECONDS));

            // Test results
            assertThat(myUserException.getCause().getMessage(), is("Something failed"));
            assertThat(myUser2Exception.getCause().getMessage(), is("Something failed"));

            // Test request
            assertThat(aclBindingsFilterCaptor.getAllValues().size(), is(1));
            assertThat(aclBindingsFilterCaptor.getValue().size(), is(4));
            assertThat(aclBindingsFilterCaptor.getValue(), hasItems(MY_USER_READ, MY_USER_WRITE, MY_USER_2_READ, MY_USER_2_WRITE));
        } finally {
            reconciler.stop();
        }
    }

    @Test
    public void testPartialFailurePropagation() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        KafkaFuture<DeleteAclsResult.FilterResults> mockMyUser2Future = mock(KafkaFuture.class);
        when(mockMyUser2Future.isCompletedExceptionally()).thenReturn(true);
        when(mockMyUser2Future.getNow(any())).thenThrow(new RuntimeException("Something failed"));

        DeleteAclsResult mockResult = mock(DeleteAclsResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(
                Map.of(MY_USER_READ, KafkaFuture.completedFuture(null), MY_USER_WRITE, KafkaFuture.completedFuture(null),
                        MY_USER_2_READ, mockMyUser2Future, MY_USER_2_WRITE, KafkaFuture.completedFuture(null))
        );

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<AclBindingFilter>> aclBindingsFilterCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.deleteAcls(aclBindingsFilterCaptor.capture())).thenReturn(mockResult);

        // Test
        DeleteAclsBatchReconciler reconciler = new DeleteAclsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", List.of(MY_USER_READ, MY_USER_WRITE), myUserFuture));

            CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", List.of(MY_USER_2_READ, MY_USER_2_WRITE), myUser2Future));

            // Wait for completion
            ReconcileResult<Collection<AclBindingFilter>> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ExecutionException myUser2Exception = assertThrows(ExecutionException.class, () -> myUser2Future.get(1_000, TimeUnit.MILLISECONDS));

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(ReconcileResult.deleted()));

            assertThat(myUser2Exception.getCause().getMessage(), is("ACL deletion failed"));

            // Test request
            assertThat(aclBindingsFilterCaptor.getAllValues().size(), is(1));
            assertThat(aclBindingsFilterCaptor.getValue().size(), is(4));
            assertThat(aclBindingsFilterCaptor.getValue(), hasItems(MY_USER_READ, MY_USER_WRITE, MY_USER_2_READ, MY_USER_2_WRITE));
        } finally {
            reconciler.stop();
        }
    }
}
