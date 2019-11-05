/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.test.k8s.KubeCluster;
import io.strimzi.test.k8s.NoClusterException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.strimzi.test.BaseITST.cmdKubeClient;
import static io.strimzi.test.BaseITST.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public abstract class AbstractResourceOperatorIT<C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList/*<T>*/, D, R extends Resource<T, D>> {
    protected static final Logger log = LogManager.getLogger(AbstractResourceOperatorIT.class);
    public static final String RESOURCE_NAME = "my-test-resource";
    protected static Vertx vertx;
    protected static KubernetesClient client;
    protected static String namespace = "resource-operator-it-namespace";

    @BeforeAll
    public static void before() {
        try {
            KubeCluster.bootstrap();
        } catch (NoClusterException e) {
            assumeTrue(false, e.getMessage());
        }
        vertx = Vertx.vertx();
        client = new DefaultKubernetesClient();

        if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
            log.warn("Namespace {} is already created, going to delete it", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }

        log.info("Creating namespace: {}", namespace);
        kubeClient().createNamespace(namespace);
        cmdKubeClient().waitForResourceCreation("Namespace", namespace);
    }

    @AfterAll
    public static void after() {
        if (vertx != null) {
            vertx.close();
        }
        if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
            log.warn("Deleting namespace {} after tests run", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }
    }

    abstract AbstractResourceOperator<C, T, L, D, R> operator();
    abstract T getOriginal();
    abstract T getModified();
    abstract void assertResources(VertxTestContext context, T expected, T actual);

    @Test
    public void testFullCycle(VertxTestContext context)    {
        Checkpoint async = context.checkpoint();
        AbstractResourceOperator<C, T, L, D, R> op = operator();

        T newResource = getOriginal();
        T modResource = getModified();

        Future<ReconcileResult<T>> createFuture = op.reconcile(namespace, RESOURCE_NAME, newResource);

        createFuture.setHandler(create -> {
            if (create.succeeded()) {
                T created = op.get(namespace, RESOURCE_NAME);

                if (created == null)    {
                    context.failNow(new Throwable("Failed to get created Resource"));
                    async.flag();
                } else  {
                    assertResources(context, newResource, created);

                    Future<ReconcileResult<T>> modifyFuture = op.reconcile(namespace, RESOURCE_NAME, modResource);
                    modifyFuture.setHandler(modify -> {
                        if (modify.succeeded()) {
                            T modified = op.get(namespace, RESOURCE_NAME);

                            if (modified == null)    {
                                context.failNow(new Throwable("Failed to get modified Resource"));
                                async.flag();
                            } else {
                                assertResources(context, modResource, modified);

                                Future<ReconcileResult<T>> deleteFuture = op.reconcile(namespace, RESOURCE_NAME, null);
                                deleteFuture.setHandler(delete -> {
                                    if (delete.succeeded()) {
                                        T deleted = op.get(namespace, RESOURCE_NAME);

                                        if (deleted == null)    {
                                            async.flag();
                                        } else {
                                            context.failNow(new Throwable("Failed to delete Resource"));
                                            async.flag();
                                        }
                                    } else {
                                        context.failNow(delete.cause());
                                        async.flag();
                                    }
                                });
                            }
                        } else {
                            context.failNow(modify.cause());
                            async.flag();
                        }
                    });
                }

            } else {
                context.failNow(create.cause());
                async.flag();
            }
        });
    }
}

