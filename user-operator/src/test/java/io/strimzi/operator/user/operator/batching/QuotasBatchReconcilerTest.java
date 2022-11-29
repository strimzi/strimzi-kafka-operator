/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.operator.AdminApiOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QuotasBatchReconcilerTest {
    // Requests used for testing => user 1
    private static final ClientQuotaEntity MY_USER_ENTITY = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "my-user"));
    private static final Collection<ClientQuotaAlteration.Op> MY_USER_QUOTAS = List.of(
            new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0),
            new ClientQuotaAlteration.Op("consumer_byte_rate", 2048.0)
    );
    private static final ClientQuotaAlteration MY_USER_ALTERATION = new ClientQuotaAlteration(MY_USER_ENTITY, MY_USER_QUOTAS);

    // Requests used for testing => user 2
    private static final ClientQuotaEntity MY_USER_2_ENTITY = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "my-user2"));
    private static final Collection<ClientQuotaAlteration.Op> MY_USER_2_QUOTAS = List.of(
            new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0),
            new ClientQuotaAlteration.Op("consumer_byte_rate", 2048.0)
    );
    private static final ClientQuotaAlteration MY_USER_2_ALTERATION = new ClientQuotaAlteration(MY_USER_2_ENTITY, MY_USER_2_QUOTAS);

    @Test
    public void testBatching() throws InterruptedException, ExecutionException, TimeoutException {
        // Mock Admin client
        Admin mockClient = mock(Admin.class);

        // Mock result
        AlterClientQuotasResult mockResult = mock(AlterClientQuotasResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(Map.of(MY_USER_ENTITY, KafkaFuture.completedFuture(null), MY_USER_2_ENTITY, KafkaFuture.completedFuture(null)));

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<ClientQuotaAlteration>> clientQuotaAlterationsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.alterClientQuotas(clientQuotaAlterationsCaptor.capture())).thenReturn(mockResult);

        // Test
        QuotasBatchReconciler reconciler = new QuotasBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<ClientQuotaAlteration>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", MY_USER_ALTERATION, myUserFuture));

            CompletableFuture<ReconcileResult<ClientQuotaAlteration>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", MY_USER_2_ALTERATION, myUser2Future));

            // Wait for completion
            ReconcileResult<ClientQuotaAlteration> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ReconcileResult<ClientQuotaAlteration> myUser2Result = myUser2Future.get(1_000, TimeUnit.MILLISECONDS);

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(instanceOf(ReconcileResult.Patched.class)));
            assertThat(myUserResult.resource(), is(MY_USER_ALTERATION));

            assertThat(myUser2Result, is(notNullValue()));
            assertThat(myUser2Result, is(instanceOf(ReconcileResult.Patched.class)));
            assertThat(myUser2Result.resource(), is(MY_USER_2_ALTERATION));

            // Test request
            assertThat(clientQuotaAlterationsCaptor.getAllValues().size(), is(1));
            assertThat(clientQuotaAlterationsCaptor.getValue().size(), is(2));
            assertThat(clientQuotaAlterationsCaptor.getValue(), hasItems(MY_USER_ALTERATION, MY_USER_2_ALTERATION));
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

        AlterClientQuotasResult mockResult = mock(AlterClientQuotasResult.class);
        when(mockResult.all()).thenReturn(mockFuture);

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<ClientQuotaAlteration>> clientQuotaAlterationsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.alterClientQuotas(clientQuotaAlterationsCaptor.capture())).thenReturn(mockResult);

        // Test
        QuotasBatchReconciler reconciler = new QuotasBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<ClientQuotaAlteration>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", MY_USER_ALTERATION, myUserFuture));

            CompletableFuture<ReconcileResult<ClientQuotaAlteration>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", MY_USER_2_ALTERATION, myUser2Future));

            // Wait for completion
            ExecutionException myUserException = assertThrows(ExecutionException.class, () -> myUserFuture.get(1_000, TimeUnit.MILLISECONDS));
            ExecutionException myUser2Exception = assertThrows(ExecutionException.class, () -> myUser2Future.get(1_000, TimeUnit.MILLISECONDS));

            // Test results
            assertThat(myUserException.getCause().getMessage(), is("Something failed"));
            assertThat(myUser2Exception.getCause().getMessage(), is("Something failed"));

            // Test request
            assertThat(clientQuotaAlterationsCaptor.getAllValues().size(), is(1));
            assertThat(clientQuotaAlterationsCaptor.getValue().size(), is(2));
            assertThat(clientQuotaAlterationsCaptor.getValue(), hasItems(MY_USER_ALTERATION, MY_USER_2_ALTERATION));
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

        AlterClientQuotasResult mockResult = mock(AlterClientQuotasResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mockResult.values()).thenReturn(Map.of(MY_USER_ENTITY, KafkaFuture.completedFuture(null), MY_USER_2_ENTITY, mockMyUser2Future));

        // Mock call
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<List<ClientQuotaAlteration>> clientQuotaAlterationsCaptor = ArgumentCaptor.forClass(List.class);
        when(mockClient.alterClientQuotas(clientQuotaAlterationsCaptor.capture())).thenReturn(mockResult);

        // Test
        QuotasBatchReconciler reconciler = new QuotasBatchReconciler(mockClient, 10, 5, 10);
        reconciler.start();

        try {
            // Enqueue reconciliations
            CompletableFuture<ReconcileResult<ClientQuotaAlteration>> myUserFuture = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user", MY_USER_ALTERATION, myUserFuture));

            CompletableFuture<ReconcileResult<ClientQuotaAlteration>> myUser2Future = new CompletableFuture<>();
            reconciler.enqueue(new AdminApiOperator.ReconcileRequest<>(Reconciliation.DUMMY_RECONCILIATION, "my-user2", MY_USER_2_ALTERATION, myUser2Future));

            // Wait for completion
            ReconcileResult<ClientQuotaAlteration> myUserResult = myUserFuture.get(1_000, TimeUnit.MILLISECONDS);
            ExecutionException myUser2Exception = assertThrows(ExecutionException.class, () -> myUser2Future.get(1_000, TimeUnit.MILLISECONDS));

            // Test results
            assertThat(myUserResult, is(notNullValue()));
            assertThat(myUserResult, is(instanceOf(ReconcileResult.Patched.class)));
            assertThat(myUserResult.resource(), is(MY_USER_ALTERATION));

            assertThat(myUser2Exception.getCause().getMessage(), is("Something failed"));

            // Test request
            assertThat(clientQuotaAlterationsCaptor.getAllValues().size(), is(1));
            assertThat(clientQuotaAlterationsCaptor.getValue().size(), is(2));
            assertThat(clientQuotaAlterationsCaptor.getValue(), hasItems(MY_USER_ALTERATION, MY_USER_2_ALTERATION));
        } finally {
            reconciler.stop();
        }
    }
}
