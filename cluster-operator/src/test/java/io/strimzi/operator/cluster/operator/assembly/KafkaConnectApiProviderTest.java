/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests that the Kafka Connect and Kafka MirrorMaker 2 operators close the Kafka Connect REST API client
 * (and with it its HttpClient and the threads it owns) once they are done with it.
 */
public class KafkaConnectApiProviderTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private static final Reconciliation RECONCILIATION = new Reconciliation("test", "KafkaConnect", "my-namespace", "my-connect");

    private static Vertx vertx;

    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void afterAll() {
        vertx.close();
    }

    private KafkaConnectAssemblyOperator operator(KafkaConnectApi api, AtomicReference<Reconciliation> providerReconciliation) {
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        return new KafkaConnectAssemblyOperator(vertx, PFA, ResourceUtils.supplierWithMocks(false), config, reconciliation -> {
            providerReconciliation.set(reconciliation);
            return api;
        });
    }

    @Test
    public void testClientIsClosedOnlyAfterActionSucceeds() {
        KafkaConnectApi api = mock(KafkaConnectApi.class);
        AtomicReference<Reconciliation> providerReconciliation = new AtomicReference<>();
        KafkaConnectAssemblyOperator operator = operator(api, providerReconciliation);

        Promise<String> action = Promise.promise();
        Future<String> result = operator.withConnectApi(RECONCILIATION, client -> {
            assertThat(client, is(sameInstance(api)));
            return action.future();
        });

        assertThat(providerReconciliation.get(), is(sameInstance(RECONCILIATION)));
        verify(api, never()).close();

        action.complete("done");

        assertThat(result.succeeded(), is(true));
        assertThat(result.result(), is("done"));
        verify(api).close();
    }

    @Test
    public void testClientIsClosedAfterActionFails() {
        KafkaConnectApi api = mock(KafkaConnectApi.class);
        KafkaConnectAssemblyOperator operator = operator(api, new AtomicReference<>());

        Promise<String> action = Promise.promise();
        Future<String> result = operator.withConnectApi(RECONCILIATION, client -> action.future());

        verify(api, never()).close();

        RuntimeException error = new RuntimeException("Failed");
        action.fail(error);

        assertThat(result.failed(), is(true));
        assertThat(result.cause(), is(sameInstance(error)));
        verify(api).close();
    }

    @Test
    public void testClientIsClosedWhenActionThrows() {
        KafkaConnectApi api = mock(KafkaConnectApi.class);
        KafkaConnectAssemblyOperator operator = operator(api, new AtomicReference<>());

        RuntimeException error = new RuntimeException("Failed");
        Future<String> result = operator.withConnectApi(RECONCILIATION, client -> {
            throw error;
        });

        assertThat(result.failed(), is(true));
        assertThat(result.cause(), is(sameInstance(error)));
        verify(api).close();
    }

    @Test
    public void testClosedClientDoesNotAcceptNewRequests() {
        KafkaConnectApi api = new DefaultKafkaConnectApiProvider().createKafkaConnectApi(RECONCILIATION);
        assertThat(api, is(instanceOf(KafkaConnectApiImpl.class)));

        api.close();

        // A running HTTP client would fail with a connection error instead
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> api.list(RECONCILIATION, "localhost", 1).toCompletableFuture().get());
        assertThat(e.getCause(), is(instanceOf(IOException.class)));
        assertThat(e.getCause().getMessage(), is("closed"));
    }
}
