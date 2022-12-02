/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.operator.AdminApiOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
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

public class ScramShaCredentialsBatchReconcilerTest {
    // Requests used for testing => user 1
    private static final UserScramCredentialUpsertion MY_USER_1 = new UserScramCredentialUpsertion("my-user", new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096), "my-password".getBytes(StandardCharsets.UTF_8), new BigInteger(130, new SecureRandom()).toString(36).getBytes(StandardCharsets.UTF_8));
    private static final UserScramCredentialDeletion MY_USER_2 = new UserScramCredentialDeletion("my-user2", ScramMechanism.SCRAM_SHA_512);

    @Test
    public void testBatching() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        AlterUserScramCredentialsResult mockResult = mock(AlterUserScramCredentialsResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(Map.of("my-user", KafkaFuture.completedFuture(null), "my-user2", KafkaFuture.completedFuture(null)));

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<UserScramCredentialAlteration>> credentialAlterationsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.alterUserScramCredentials(credentialAlterationsCaptor.capture())).thenReturn(mockResult);

        // Test
        ScramShaCredentialsBatchReconciler reconciler = new ScramShaCredentialsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", MY_USER_1, myUserFuture));

            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", MY_USER_2, myUser2Future));

            // Wait for completion
            ReconcileResult<UserScramCredentialAlteration> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ReconcileResult<UserScramCredentialAlteration> myUser2Result = myUser2Future.get(1_000, TimeUnit.MILLISECONDS);

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(instanceOf(ReconcileResult.Patched.class)));
            assertThat(myUserResult.resource(), is(MY_USER_1));

            assertThat(myUser2Result, is(notNullValue()));
            assertThat(myUser2Result, is(instanceOf(ReconcileResult.Patched.class)));
            assertThat(myUser2Result.resource(), is(MY_USER_2));

            // Test request
            assertThat(credentialAlterationsCaptor.getAllValues().size(), is(1));
            assertThat(credentialAlterationsCaptor.getValue().size(), is(2));
            assertThat(credentialAlterationsCaptor.getValue(), hasItems(MY_USER_1, MY_USER_2));
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

        AlterUserScramCredentialsResult mockResult = mock(AlterUserScramCredentialsResult.class);
        when(mockResult.all()).thenReturn(mockFuture);

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<UserScramCredentialAlteration>> credentialAlterationsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.alterUserScramCredentials(credentialAlterationsCaptor.capture())).thenReturn(mockResult);

        // Test
        ScramShaCredentialsBatchReconciler reconciler = new ScramShaCredentialsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", MY_USER_1, myUserFuture));

            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", MY_USER_2, myUser2Future));

            // Wait for completion
            ExecutionException myUserException = assertThrows(ExecutionException.class, () -> myUserFuture.get(1_000, TimeUnit.MILLISECONDS));
            ExecutionException myUser2Exception = assertThrows(ExecutionException.class, () -> myUser2Future.get(1_000, TimeUnit.MILLISECONDS));

            // Test results
            assertThat(myUserException.getCause().getMessage(), is("Something failed"));
            assertThat(myUser2Exception.getCause().getMessage(), is("Something failed"));

            // Test request
            assertThat(credentialAlterationsCaptor.getAllValues().size(), is(1));
            assertThat(credentialAlterationsCaptor.getValue().size(), is(2));
            assertThat(credentialAlterationsCaptor.getValue(), hasItems(MY_USER_1, MY_USER_2));
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

        AlterUserScramCredentialsResult mockResult = mock(AlterUserScramCredentialsResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(Map.of("my-user", KafkaFuture.completedFuture(null), "my-user2", mockMyUser2Future));

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<UserScramCredentialAlteration>> credentialAlterationsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.alterUserScramCredentials(credentialAlterationsCaptor.capture())).thenReturn(mockResult);

        // Test
        ScramShaCredentialsBatchReconciler reconciler = new ScramShaCredentialsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", MY_USER_1, myUserFuture));

            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", MY_USER_2, myUser2Future));

            // Wait for completion
            ReconcileResult<UserScramCredentialAlteration> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ExecutionException myUser2Exception = assertThrows(ExecutionException.class, () -> myUser2Future.get(1_000, TimeUnit.MILLISECONDS));

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(instanceOf(ReconcileResult.Patched.class)));
            assertThat(myUserResult.resource(), is(MY_USER_1));

            assertThat(myUser2Exception.getCause().getMessage(), is("Something failed"));

            // Test request
            assertThat(credentialAlterationsCaptor.getAllValues().size(), is(1));
            assertThat(credentialAlterationsCaptor.getValue().size(), is(2));
            assertThat(credentialAlterationsCaptor.getValue(), hasItems(MY_USER_1, MY_USER_2));
        } finally {
            reconciler.stop();
        }
    }

    @Test
    public void testDeletingDeletedUser() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        KafkaFuture<Void> mockMyUser2Future = mock(KafkaFuture.class);
        when(mockMyUser2Future.isCompletedExceptionally()).thenReturn(true);
        when(mockMyUser2Future.getNow(any())).thenThrow(new ResourceNotFoundException("NotFound"));

        AlterUserScramCredentialsResult mockResult = mock(AlterUserScramCredentialsResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(Map.of("my-user", KafkaFuture.completedFuture(null), "my-user2", mockMyUser2Future));

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<UserScramCredentialAlteration>> credentialAlterationsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.alterUserScramCredentials(credentialAlterationsCaptor.capture())).thenReturn(mockResult);

        // Test
        ScramShaCredentialsBatchReconciler reconciler = new ScramShaCredentialsBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", MY_USER_1, myUserFuture));

            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", MY_USER_2, myUser2Future));

            // Wait for completion
            ReconcileResult<UserScramCredentialAlteration> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ReconcileResult<UserScramCredentialAlteration> myUser2Result = myUser2Future.get(1_000, TimeUnit.MILLISECONDS);

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(instanceOf(ReconcileResult.Patched.class)));
            assertThat(myUserResult.resource(), is(MY_USER_1));

            assertThat(myUser2Result, is(notNullValue()));
            assertThat(myUser2Result, is(instanceOf(ReconcileResult.Noop.class)));

            // Test request
            assertThat(credentialAlterationsCaptor.getAllValues().size(), is(1));
            assertThat(credentialAlterationsCaptor.getValue().size(), is(2));
            assertThat(credentialAlterationsCaptor.getValue(), hasItems(MY_USER_1, MY_USER_2));
        } finally {
            reconciler.stop();
        }
    }
}
