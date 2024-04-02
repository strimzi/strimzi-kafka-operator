/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube3.MockKube3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserControllerMockTest {
    public static final String NAME = "user";

    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private SecretOperator secretOperator;
    private CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps;
    private KafkaUserOperator mockKafkaUserOperator;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaUserCrd()
                .build();
        mockKube.start();
        client = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        secretOperator = new SecretOperator(ForkJoinPool.commonPool(), client);
        kafkaUserOps = new CrdOperator<>(ForkJoinPool.commonPool(), client, KafkaUser.class, KafkaUserList.class, "KafkaUser");
        mockKafkaUserOperator = mock(KafkaUserOperator.class);
    }

    @AfterEach
    public void afterEach() {
        client.namespaces().withName(namespace).delete();
    }

    @Test
    public void testReconciliationCrAndSecret() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(namespace, Map.of(), 120000, 10, 1, ""),
                secretOperator,
                kafkaUserOps,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            kafkaUserOps.resource(namespace, ResourceUtils.createKafkaUserTls(namespace)).create();
            kafkaUserOps.resource(namespace, NAME).waitUntilCondition(KafkaUser.isReady(), 10_000, TimeUnit.MILLISECONDS);

            KafkaUser user = kafkaUserOps.get(namespace, NAME);

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));
            assertThat(user.getStatus().getObservedGeneration(), is(1L));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing

            // Test that secret change triggers reconciliation
            secretOperator.resource(namespace, ResourceUtils.createUserSecretTls(namespace)).create();

            // Secret watch should trigger 3rd reconciliation => but we have no other way to know it happened apart from the metrics
            // So we wait for the metrics to be updated
            TestUtils.waitFor(
                    "Wait for 3rd reconciliation",
                    100,
                    10_000,
                    () -> metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count() == 3
            );

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(3.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(3.0));
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testReconciliationCrAndPrefixedSecret() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(namespace, Map.of(), 120000, 10, 1, "prefix-"),
                secretOperator,
                kafkaUserOps,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            kafkaUserOps.resource(namespace, ResourceUtils.createKafkaUserTls(namespace)).create();
            kafkaUserOps.resource(namespace, NAME).waitUntilCondition(KafkaUser.isReady(), 10_000, TimeUnit.MILLISECONDS);

            KafkaUser user = kafkaUserOps.get(namespace, NAME);

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));
            assertThat(user.getStatus().getObservedGeneration(), is(1L));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing

            // Test that secret change triggers reconciliation
            Secret userSecret = ResourceUtils.createUserSecretTls(namespace);
            userSecret.getMetadata().setName("prefix-" + NAME);
            secretOperator.resource(namespace, userSecret).create();

            // Secret watch should trigger 3rd reconciliation => but we have no other way to know it happened apart from the metrics
            // So we wait for the metrics to be updated
            TestUtils.waitFor(
                    "Wait for 3rd reconciliation",
                    100,
                    10_000,
                    () -> metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count() == 3
            );

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(3.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(3.0));
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testPausedReconciliation() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(namespace, Map.of(), 120000, 10, 1, ""),
                secretOperator,
                kafkaUserOps,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            KafkaUser pausedUser = ResourceUtils.createKafkaUserTls(namespace);
            pausedUser.getMetadata().setAnnotations(Map.of("strimzi.io/pause-reconciliation", "true"));
            kafkaUserOps.resource(namespace, pausedUser).create();

            TestUtils.waitFor(
                    "KafkaUser to be paused",
                    100,
                    10_000,
                    () -> {
                        KafkaUser u = kafkaUserOps.get(namespace, NAME);
                        return u.getStatus() != null
                                && u.getStatus().getConditions() != null
                                && u.getStatus().getConditions().stream().filter(c -> "ReconciliationPaused".equals(c.getType())).findFirst().orElse(null) != null;
                    }
            );

            KafkaUser user = kafkaUserOps.get(namespace, NAME);

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, never()).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES_PAUSED).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0)));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0)));
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testFailedReconciliation() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> CompletableFuture.failedFuture(new RuntimeException("Something failed")));

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(namespace, Map.of(), 120000, 10, 1, ""),
                secretOperator,
                kafkaUserOps,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            kafkaUserOps.resource(namespace, ResourceUtils.createKafkaUserTls(namespace)).create();

            TestUtils.waitFor(
                    "KafkaUser to be failed",
                    100,
                    10_000,
                    () -> {
                        KafkaUser u = kafkaUserOps.get(namespace, NAME);
                        return u != null
                                && u.getStatus() != null
                                && u.getStatus().getConditions() != null
                                && u.getStatus().getConditions().stream().filter(c -> "NotReady".equals(c.getType())).findFirst().orElse(null) != null;
                    }
            );

            KafkaUser user = kafkaUserOps.get(namespace, NAME);

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_FAILED).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0)));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0)));
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testSelectors() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(namespace, Map.of("select", "yes"), 120000, 10, 1, ""),
                secretOperator,
                kafkaUserOps,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            KafkaUser wrongLabel = ResourceUtils.createKafkaUserTls(namespace);
            wrongLabel.getMetadata().setName("other-user");
            wrongLabel.getMetadata().setLabels(Map.of("select", "no"));

            KafkaUser matchingLabel = ResourceUtils.createKafkaUserTls(namespace);
            matchingLabel.getMetadata().setLabels(Map.of("select", "yes"));

            kafkaUserOps.resource(namespace, wrongLabel).create();
            kafkaUserOps.resource(namespace, matchingLabel).create();
            kafkaUserOps.resource(namespace, NAME).waitUntilCondition(KafkaUser.isReady(), 10_000, TimeUnit.MILLISECONDS);

            // Check resource
            KafkaUser matchingUser = kafkaUserOps.get(namespace, NAME);
            assertThat(matchingUser.getStatus(), is(notNullValue()));
            assertThat(matchingUser.getStatus().getObservedGeneration(), is(1L));

            KafkaUser wrongUSer = kafkaUserOps.get(namespace, "other-user");
            assertThat(wrongUSer.getStatus(), is(nullValue()));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testPeriodicalReconciliation() throws InterruptedException {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        CountDownLatch periods = new CountDownLatch(2); // We will wait for 2 periodical reconciliations
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });
        when(mockKafkaUserOperator.getAllUsers(any())).thenAnswer(i -> {
            periods.countDown();
            return CompletableFuture.completedFuture(Set.of(new NamespaceAndName(namespace, NAME)));
        });

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(namespace, Map.of(), 500, 10, 1, ""),
                secretOperator,
                kafkaUserOps,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            kafkaUserOps.resource(namespace, ResourceUtils.createKafkaUserTls(namespace)).create();
            kafkaUserOps.resource(namespace, NAME).waitUntilCondition(KafkaUser.isReady(), 10_000, TimeUnit.MILLISECONDS);

            KafkaUser user = kafkaUserOps.get(namespace, NAME);

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));
            assertThat(user.getStatus().getObservedGeneration(), is(1L));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            periods.await();

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(3.0))); // Might be 3 or 4, depends on the timing
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(3.0))); // Might be 3 or 4, depends on the timing
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_PERIODICAL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(greaterThanOrEqualTo(2.0))); // At least 2, depends on timing
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testReconciliationWith409Error()   {
        testReconciliationWithClientErrorStatusUpdate(409, "Conflict");
    }

    @Test
    public void testReconciliationWith404Error()   {
        testReconciliationWithClientErrorStatusUpdate(404, "Not Found");
    }

    @Test
    public void testReconciliationWith500Error()   {
        testReconciliationWithClientErrorStatusUpdate(500, "Internal Server Error");
    }

    // Utility method called from other tests with different values
    private void testReconciliationWithClientErrorStatusUpdate(int errorCode, String errorDescription) {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });

        AtomicBoolean statusUpdateInvoked = new AtomicBoolean(false);
        var spiedKafkaUserOps = spy(kafkaUserOps);

        doAnswer(i -> {
            KubernetesClientException error = new KubernetesClientException(errorDescription + " (expected)", errorCode, null);
            statusUpdateInvoked.set(true);
            return CompletableFuture.failedStage(error);
        }).when(spiedKafkaUserOps).updateStatusAsync(any(), any());

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(namespace, Map.of(), 5, 1, 1, ""),
                secretOperator,
                spiedKafkaUserOps,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            kafkaUserOps.resource(namespace, ResourceUtils.createKafkaUserTls(namespace)).create();
            kafkaUserOps.resource(namespace, NAME).waitUntilCondition(i -> statusUpdateInvoked.get(), 10_000, TimeUnit.MILLISECONDS);

            KafkaUser user = kafkaUserOps.get(namespace, NAME);

            // Check resource - status remains null although `updateStatusAsync` was invoked
            assertThat(user.getStatus(), is(nullValue()));

            // Reconcile involved
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(1.0));
        } finally {
            controller.stop();
        }
    }

    @Test
    void testReconciliationWithRuntimeErrorStatusUpdate() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });

        var spiedKafkaUserOps = spy(kafkaUserOps);
        AtomicBoolean statusUpdateInvoked = new AtomicBoolean(false);

        doAnswer(i -> {
            statusUpdateInvoked.set(true);
            return CompletableFuture.failedStage(new RuntimeException("Test exception (expected)"));
        }).when(spiedKafkaUserOps).updateStatusAsync(any(), any());

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(namespace, Map.of(), 5, 1, 1, ""),
                secretOperator,
                spiedKafkaUserOps,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            kafkaUserOps.resource(namespace, ResourceUtils.createKafkaUserTls(namespace)).create();
            kafkaUserOps.resource(namespace, NAME).waitUntilCondition(i -> statusUpdateInvoked.get(), 10_000, TimeUnit.MILLISECONDS);

            KafkaUser user = kafkaUserOps.get(namespace, NAME);

            // Check resource - status remains null although `updateStatusAsync` was invoked
            assertThat(user.getStatus(), is(nullValue()));

            // Reconcile involved
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RESOURCES).tag("kind", "KafkaUser").tag("namespace", namespace).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(1.0));
            assertThat(metrics.meterRegistry().get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "KafkaUser").tag("namespace", namespace).counter().count(), is(1.0));
        } finally {
            controller.stop();
        }
    }
}
