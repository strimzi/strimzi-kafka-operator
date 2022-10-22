/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
// TestInstance lifecycle set to per class so that @BeforeAll and @AfterAll methods are non static.
// Methods must be be non static as they make a non-static call to getCrd()
// to correctly setup the test environment before the tests.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCustomResourceOperatorIT<C extends KubernetesClient, T extends CustomResource, L extends DefaultKubernetesResourceList<T>> {
    protected static final Logger LOGGER = LogManager.getLogger(AbstractCustomResourceOperatorIT.class);
    protected static final String RESOURCE_NAME = "my-test-resource";
    protected static final Condition READY_CONDITION = new ConditionBuilder()
            .withType("Ready")
            .withStatus("True")
            .build();

    protected static Vertx vertx;
    protected static KubernetesClient client;
    private static KubeClusterResource cluster;
    private WorkerExecutor sharedWorkerExecutor;

    protected abstract CrdOperator<C, T, L> operator();
    protected abstract String getCrd();
    protected abstract String getCrdName();
    protected abstract String getNamespace();
    protected abstract T getResource(String name);
    protected abstract T getResourceWithModifications(T resourceInCluster);
    protected abstract T getResourceWithNewReadyStatus(T resourceInCluster);
    protected abstract void assertReady(VertxTestContext context, T modifiedCustomResource);

    @BeforeAll
    public void before() {
        String namespace = getNamespace();
        cluster = KubeClusterResource.getInstance();
        cluster.setNamespace(namespace);

        assertDoesNotThrow(() -> KubeCluster.bootstrap(), "Could not bootstrap server");
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        client = new KubernetesClientBuilder().build();

        if (cluster.getNamespace() != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Namespace {} is already created, going to delete it", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }

        LOGGER.info("Creating namespace: {}", namespace);
        kubeClient().createNamespace(namespace);
        cmdKubeClient().waitForResourceCreation("Namespace", namespace);

        LOGGER.info("Creating CRD");
        cluster.createCustomResources(getCrd());
        cluster.waitForCustomResourceDefinition(getCrdName());
        LOGGER.info("Created CRD");
    }

    @AfterAll
    public void after() {
        sharedWorkerExecutor.close();
        vertx.close();

        String namespace = getNamespace();
        if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Deleting namespace {} after tests run", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }
    }

    @Test
    public void testUpdateStatus(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        CrdOperator<C, T, L> op = operator();

        LOGGER.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
                .onComplete(context.succeeding(pfa -> context.verify(() -> {
                    assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                            pfa.getKubernetesVersion().compareTo(KubernetesVersion.MINIMAL_SUPPORTED_VERSION), CoreMatchers.is(not(lessThan(0))));
                })))

                .compose(pfa -> {
                    LOGGER.info("Creating resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName));
                })
                .onComplete(context.succeedingThenComplete())
                .compose(rrCreated -> {
                    T newStatus = getResourceWithNewReadyStatus(rrCreated.resource());

                    LOGGER.info("Updating resource status");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
                })
                .onComplete(context.succeedingThenComplete())

                .compose(rrModified -> op.getAsync(namespace, resourceName))
                .onComplete(context.succeeding(modifiedCustomResource -> context.verify(() -> {
                    assertReady(context, modifiedCustomResource);
                })))

                .compose(rrModified -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .onComplete(context.succeeding(rrDeleted ->  async.flag()));
    }


    /**
     * Tests what happens when the resource is deleted while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusAfterResourceDeletedThrowsKubernetesClientException(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        CrdOperator<C, T, L> op = operator();

        AtomicReference<T> newStatus = new AtomicReference<>();

        LOGGER.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
                .onComplete(context.succeeding(pfa -> context.verify(() -> {
                    assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                            pfa.getKubernetesVersion().compareTo(KubernetesVersion.MINIMAL_SUPPORTED_VERSION), CoreMatchers.is(not(lessThan(0))));
                })))
                .compose(pfa -> {
                    LOGGER.info("Creating resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName));
                })
                .onComplete(context.succeedingThenComplete())

                .compose(rr -> {
                    LOGGER.info("Saving resource with status change prior to deletion");
                    newStatus.set(getResourceWithNewReadyStatus(op.get(namespace, resourceName)));
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .onComplete(context.succeedingThenComplete())
                .compose(i -> {
                    LOGGER.info("Wait for confirmed deletion");
                    return op.waitFor(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, 100L, 10_000L, (n, ns) -> operator().get(namespace, resourceName) == null);
                })
                .compose(i -> {
                    LOGGER.info("Updating resource with new status - should fail");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus.get());
                })
                .onComplete(context.failing(e -> context.verify(() -> {
                    assertThat(e, instanceOf(KubernetesClientException.class));
                    async.flag();
                })));
    }

    /**
     * Tests what happens when the resource is modified while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusAfterResourceUpdated(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        CrdOperator<C, T, L> op = operator();

        Promise updateStatus = Promise.promise();

        LOGGER.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
                .onComplete(context.succeeding(pfa -> context.verify(() -> {
                    assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                            pfa.getKubernetesVersion().compareTo(KubernetesVersion.MINIMAL_SUPPORTED_VERSION), CoreMatchers.is(not(lessThan(0))));
                })))
                .compose(pfa -> {
                    LOGGER.info("Creating resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName));
                })
                .onComplete(context.succeedingThenComplete())
                .compose(rrCreated -> {
                    T updated = getResourceWithModifications(rrCreated.resource());
                    T newStatus = getResourceWithNewReadyStatus(rrCreated.resource());

                    LOGGER.info("Updating resource (mocking an update due to some other reason)");
                    op.operation().inNamespace(namespace).withName(resourceName).patch(updated);

                    LOGGER.info("Updating resource status after underlying resource has changed");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
                })
                .onComplete(context.succeeding(res -> context.verify(() -> {
                    assertThat(res.getMetadata().getName(), is(resourceName));
                    assertThat(res.getMetadata().getNamespace(), is(namespace));
                    updateStatus.complete();
                })));

        updateStatus.future().compose(v -> {
            LOGGER.info("Deleting resource");
            return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
        })
        .onComplete(context.succeeding(v -> async.flag()));
    }

    protected String getResourceName(String name) {
        return name + "-" + new Random().nextInt(Integer.MAX_VALUE);
    }
}

