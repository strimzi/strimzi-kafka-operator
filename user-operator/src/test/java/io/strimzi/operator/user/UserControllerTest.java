/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.status.KafkaUserStatus;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube2.MockKube2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
public class UserControllerTest {
    public static final String NAMESPACE = "namespace";
    public static final String NAME = "user";

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;

    @BeforeEach
    public void beforeEach() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaUserCrd()
                .build();
        mockKube.start();
    }

    @AfterEach
    public void afterEach() {
        mockKube.stop();
    }

    @Test
    public void testReconciliationCrAndSecret() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        KafkaUserOperator mockKafkaUserOperator = mock(KafkaUserOperator.class);
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(Map.of(), 120000, 10, 1, ""),
                client,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).resource(ResourceUtils.createKafkaUserTls()).create();
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).waitUntilCondition(KafkaUser.isReady(), 10_000, TimeUnit.MILLISECONDS);

            KafkaUser user = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).get();

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));
            assertThat(user.getStatus().getObservedGeneration(), is(1L));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get("strimzi.resources").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing

            // Test that secret change triggers reconciliation
            client.secrets().inNamespace(NAMESPACE).resource(ResourceUtils.createUserSecretTls()).create();

            // Secret watch should trigger 3rd reconciliation => but we have no other way to know it happened apart from the metrics
            // So we wait for the metrics to be updated
            TestUtils.waitFor(
                    "Wait for 3rd reconciliation",
                    100,
                    10_000,
                    () -> metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count() == 3
            );

            // Check metrics
            assertThat(metrics.meterRegistry().get("strimzi.resources").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(3.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(3.0));
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testReconciliationCrAndPrefixedSecret() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        KafkaUserOperator mockKafkaUserOperator = mock(KafkaUserOperator.class);
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(Map.of(), 120000, 10, 1, "prefix-"),
                client,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).resource(ResourceUtils.createKafkaUserTls()).create();
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).waitUntilCondition(KafkaUser.isReady(), 10_000, TimeUnit.MILLISECONDS);

            KafkaUser user = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).get();

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));
            assertThat(user.getStatus().getObservedGeneration(), is(1L));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get("strimzi.resources").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing

            // Test that secret change triggers reconciliation
            Secret userSecret = ResourceUtils.createUserSecretTls();
            userSecret.getMetadata().setName("prefix-" + NAME);
            client.secrets().inNamespace(NAMESPACE).resource(userSecret).create();

            // Secret watch should trigger 3rd reconciliation => but we have no other way to know it happened apart from the metrics
            // So we wait for the metrics to be updated
            TestUtils.waitFor(
                    "Wait for 3rd reconciliation",
                    100,
                    10_000,
                    () -> metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count() == 3
            );

            // Check metrics
            assertThat(metrics.meterRegistry().get("strimzi.resources").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(3.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(3.0));
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testPausedReconciliation() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        KafkaUserOperator mockKafkaUserOperator = mock(KafkaUserOperator.class);

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(Map.of(), 120000, 10, 1, ""),
                client,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            KafkaUser pausedUser = ResourceUtils.createKafkaUserTls();
            pausedUser.getMetadata().setAnnotations(Map.of("strimzi.io/pause-reconciliation", "true"));
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).resource(pausedUser).create();

            TestUtils.waitFor(
                    "KafkaUser to be paused",
                    100,
                    10_000,
                    () -> {
                        KafkaUser u = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).get();
                        return u.getStatus() != null
                                && u.getStatus().getConditions() != null
                                && u.getStatus().getConditions().stream().filter(c -> "ReconciliationPaused".equals(c.getType())).findFirst().orElse(null) != null;
                    }
            );

            KafkaUser user = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).get();

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, never()).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get("strimzi.resources").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.resources.paused").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0)));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0)));
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testFailedReconciliation() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        KafkaUserOperator mockKafkaUserOperator = mock(KafkaUserOperator.class);
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> CompletableFuture.failedFuture(new RuntimeException("Something failed")));

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(Map.of(), 120000, 10, 1, ""),
                client,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).resource(ResourceUtils.createKafkaUserTls()).create();

            TestUtils.waitFor(
                    "KafkaUser to be failed",
                    100,
                    10_000,
                    () -> {
                        KafkaUser u = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).get();
                        return u != null
                                && u.getStatus() != null
                                && u.getStatus().getConditions() != null
                                && u.getStatus().getConditions().stream().filter(c -> "NotReady".equals(c.getType())).findFirst().orElse(null) != null;
                    }
            );

            KafkaUser user = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).get();

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));

            // Check metrics
            assertThat(metrics.meterRegistry().get("strimzi.resources").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.failed").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0)));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0)));
        } finally {
            controller.stop();
        }
    }

    @Test
    public void testSelectors() {
        // Prepare metrics registry
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        // Mock the UserOperator
        KafkaUserOperator mockKafkaUserOperator = mock(KafkaUserOperator.class);
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(Map.of("select", "yes"), 120000, 10, 1, ""),
                client,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            KafkaUser wrongLabel = ResourceUtils.createKafkaUserTls();
            wrongLabel.getMetadata().setName("other-user");
            wrongLabel.getMetadata().setLabels(Map.of("select", "no"));

            KafkaUser matchingLabel = ResourceUtils.createKafkaUserTls();
            matchingLabel.getMetadata().setLabels(Map.of("select", "yes"));

            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).resource(wrongLabel).create();
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).resource(matchingLabel).create();
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).waitUntilCondition(KafkaUser.isReady(), 10_000, TimeUnit.MILLISECONDS);

            // Check resource
            KafkaUser matchingUser = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).get();
            assertThat(matchingUser.getStatus(), is(notNullValue()));
            assertThat(matchingUser.getStatus().getObservedGeneration(), is(1L));

            KafkaUser wrongUSer = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName("other-user").get();
            assertThat(wrongUSer.getStatus(), is(nullValue()));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            // Check metrics
            assertThat(metrics.meterRegistry().get("strimzi.resources").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(1.0))); // Might be 1 or 2, depends on the timing
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
        KafkaUserOperator mockKafkaUserOperator = mock(KafkaUserOperator.class);
        when(mockKafkaUserOperator.reconcile(any(), any(), any())).thenAnswer(i -> {
            KafkaUserStatus status = new KafkaUserStatus();
            StatusUtils.setStatusConditionAndObservedGeneration(i.getArgument(1), status, (Throwable) null);
            return CompletableFuture.completedFuture(status);
        });
        when(mockKafkaUserOperator.getAllUsers(any())).thenAnswer(i -> {
            periods.countDown();
            return CompletableFuture.completedFuture(Set.of(new NamespaceAndName(NAMESPACE, NAME)));
        });

        // Create User Controller
        UserController controller = new UserController(
                ResourceUtils.createUserOperatorConfigForUserControllerTesting(Map.of(), 500, 10, 1, ""),
                client,
                mockKafkaUserOperator,
                metrics
        );

        controller.start();

        // Test
        try {
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).resource(ResourceUtils.createKafkaUserTls()).create();
            Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).waitUntilCondition(KafkaUser.isReady(), 10_000, TimeUnit.MILLISECONDS);

            KafkaUser user = Crds.kafkaUserOperation(client).inNamespace(NAMESPACE).withName(NAME).get();

            // Check resource
            assertThat(user.getStatus(), is(notNullValue()));
            assertThat(user.getStatus().getObservedGeneration(), is(1L));

            // Paused resource => nothing should be reconciled
            verify(mockKafkaUserOperator, atLeast(1)).reconcile(any(), any(), any());

            periods.await();

            // Check metrics
            assertThat(metrics.meterRegistry().get("strimzi.resources").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).gauge().value(), is(1.0));
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.successful").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(3.0))); // Might be 3 or 4, depends on the timing
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(3.0))); // Might be 3 or 4, depends on the timing
            assertThat(metrics.meterRegistry().get("strimzi.reconciliations.periodical").tag("kind", "KafkaUser").tag("namespace", NAMESPACE).counter().count(), is(greaterThanOrEqualTo(2.0))); // At least 2, depends on timing
        } finally {
            controller.stop();
        }
    }
}
