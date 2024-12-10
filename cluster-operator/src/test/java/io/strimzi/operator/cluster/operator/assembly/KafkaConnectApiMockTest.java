/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaConnectApiMockTest {
    private final BackOff backOff = new BackOff(1L, 2, 3);

    @Test
    public void testStatusWithBackOffSucceedingImmediately() {
        Queue<CompletableFuture<Map<String, Object>>> statusResults = new ArrayBlockingQueue<>(1);
        statusResults.add(CompletableFuture.completedFuture(Collections.emptyMap()));

        KafkaConnectApi api = new MockKafkaConnectApi(statusResults);
        api.statusWithBackOff(Reconciliation.DUMMY_RECONCILIATION, backOff, "some-host", 8083, "some-connector")
                .whenComplete((r, e) -> assertThat(e, nullValue())).join();
    }

    @Test
    public void testStatusWithBackOffSucceedingEventually() {
        Queue<CompletableFuture<Map<String, Object>>> statusResults = new ArrayBlockingQueue<>(3);
        statusResults.add(CompletableFuture.failedFuture(new CompletionException(new ConnectRestException(null, null, 404, null, null))));
        statusResults.add(CompletableFuture.failedFuture(new CompletionException(new ConnectRestException(null, null, 404, null, null))));
        statusResults.add(CompletableFuture.completedFuture(Collections.emptyMap()));

        KafkaConnectApi api = new MockKafkaConnectApi(statusResults);

        api.statusWithBackOff(Reconciliation.DUMMY_RECONCILIATION, backOff, "some-host", 8083, "some-connector")
                .whenComplete((r, e) -> assertThat(e, nullValue())).join();
    }

    @Test
    public void testStatusWithBackOffFailingRepeatedly() {
        Queue<CompletableFuture<Map<String, Object>>> statusResults = new ArrayBlockingQueue<>(4);
        statusResults.add(CompletableFuture.failedFuture(new CompletionException(new ConnectRestException(null, null, 404, null, null))));
        statusResults.add(CompletableFuture.failedFuture(new CompletionException(new ConnectRestException(null, null, 404, null, null))));
        statusResults.add(CompletableFuture.failedFuture(new CompletionException(new ConnectRestException(null, null, 404, null, null))));
        statusResults.add(CompletableFuture.failedFuture(new CompletionException(new ConnectRestException(null, null, 404, null, null))));

        KafkaConnectApi api = new MockKafkaConnectApi(statusResults);
        assertThrows(Exception.class, api.statusWithBackOff(Reconciliation.DUMMY_RECONCILIATION, backOff, "some-host", 8083, "some-connector")
                .whenComplete((r, e) -> {
                    assertThat(e.getMessage(), containsString("404"));
                    assertThat(statusResults.size(), is(0));
                })::join);
    }

    @Test
    public void testStatusWithBackOffOtherExceptionStillFails() {
        Queue<CompletableFuture<Map<String, Object>>> statusResults = new ArrayBlockingQueue<>(1);
        statusResults.add(CompletableFuture.failedFuture(new CompletionException(new ConnectRestException(null, null, 500, null, null))));

        KafkaConnectApi api = new MockKafkaConnectApi(statusResults);

        assertThrows(Exception.class, api.statusWithBackOff(Reconciliation.DUMMY_RECONCILIATION, backOff, "some-host", 8083, "some-connector")
                .whenComplete((r, e) -> assertThat(e.getMessage(), containsString("500")))::join);
    }

    static class MockKafkaConnectApi extends KafkaConnectApiImpl   {
        private final Queue<CompletableFuture<Map<String, Object>>> statusResults;

        public MockKafkaConnectApi(Queue<CompletableFuture<Map<String, Object>>> statusResults) {
            super();
            this.statusResults = statusResults;
        }

        @Override
        public CompletableFuture<Map<String, Object>> status(Reconciliation reconciliation, String host, int port, String connectorName) {
            return statusResults.remove();
        }
    }
}
