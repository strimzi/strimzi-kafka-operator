/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Random;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public abstract class AbstractNamespacedResourceOperatorIT<C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> {
    protected static final Logger LOGGER = LogManager.getLogger(AbstractNamespacedResourceOperatorIT.class);
    public static final String RESOURCE_NAME = "my-test-resource";
    private static WorkerExecutor sharedWorkerExecutor;
    protected String resourceName;
    protected static Vertx vertx;
    protected static KubernetesClient client;
    protected static String namespace = "resource-operator-it-namespace";

    private static KubeClusterResource cluster;

    @BeforeEach
    public void renameResource() {
        this.resourceName = getResourceName(RESOURCE_NAME);
    }

    @BeforeAll
    public static void before() {
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
    }

    @AfterAll
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
        if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Deleting namespace {} after tests run", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }
    }

    abstract AbstractNamespacedResourceOperator<C, T, L, R> operator();
    abstract T getOriginal();
    abstract T getModified();
    abstract void assertResources(VertxTestContext context, T expected, T actual);

    @Test
    public void testCreateModifyDelete(VertxTestContext context)    {
        Checkpoint async = context.checkpoint();
        AbstractNamespacedResourceOperator<C, T, L, R> op = operator();

        T newResource = getOriginal();
        T modResource = getModified();

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, newResource)
            .onComplete(context.succeeding(rrCreated -> {
                T created = op.get(namespace, resourceName);

                context.verify(() -> assertThat(created, is(notNullValue())));
                assertResources(context, newResource, created);
            }))
            .compose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, modResource))
            .onComplete(context.succeeding(rrModified -> {
                T modified = op.get(namespace, resourceName);

                context.verify(() -> assertThat(modified, is(notNullValue())));
                assertResources(context, modResource, modified);
            }))
            .compose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null))
            .onComplete(context.succeeding(rrDeleted -> {
                // it seems the resource is cached for some time so we need wait for it to be null
                context.verify(() -> {
                        Util.waitFor(Reconciliation.DUMMY_RECONCILIATION, vertx, "resource deletion " + resourceName, "deleted", 1000,
                                30_000, () -> op.get(namespace, resourceName) == null)
                                .onComplete(del -> {
                                    assertThat(op.get(namespace, resourceName), is(nullValue()));
                                    async.flag();
                                });
                    }
                );
            }));
    }

    protected String getResourceName(String name) {
        return name + "-" + new Random().nextInt(Integer.MAX_VALUE);
    }
}

