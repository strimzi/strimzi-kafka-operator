/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;


import org.junit.Test;

import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Promise;

public class BrokerStateProviderTest {

    @Test
    public void testBrokerInRecoveryState() {
        BrokerStateProvider brokerStateProvider = spy(new BrokerStateProvider(new Reconciliation("test", "kafka", "namespace", "my-cluster"), null, "mycluster", "namespace", null, null));
        doAnswer(invocation -> {
            Promise<String> result = Promise.promise();
            result.complete("{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}");
            return result;
        }).when(brokerStateProvider).doGet(any());

        BrokerState actual = brokerStateProvider.getBrokerState("mypod");
        assertTrue("broker is not recovery as expected", actual.isBrokerInRecovery());
        assertEquals(10, actual.remainingLogsToRecover());
        assertEquals(100, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testBrokerInRunningState() {
        BrokerStateProvider brokerStateProvider = spy(new BrokerStateProvider(new Reconciliation("test", "kafka", "namespace", "my-cluster"), null, "mycluster", "namespace", null, null));
        doAnswer(invocation -> {
            Promise<String> result = Promise.promise();
            result.complete("{\"brokerState\":3}");
            return result;
        }).when(brokerStateProvider).doGet(any());

        BrokerState actual = brokerStateProvider.getBrokerState("mypod");
        assertEquals(3, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testInvalidJsonResponse() {
        BrokerStateProvider brokerStateProvider = spy(new BrokerStateProvider(new Reconciliation("test", "kafka", "namespace", "my-cluster"), null, "mycluster", "namespace", null, null));
        doAnswer(invocation -> {
            Promise<String> result = Promise.promise();
            result.complete("&\"brokerState\":3&");
            return result;
        }).when(brokerStateProvider).doGet(any());

        BrokerState actual = brokerStateProvider.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }
}
