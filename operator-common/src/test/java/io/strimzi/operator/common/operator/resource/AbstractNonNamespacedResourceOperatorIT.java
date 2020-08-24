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
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
public abstract class AbstractNonNamespacedResourceOperatorIT<C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList/*<T>*/, D, R extends Resource<T, D>> {
    public static final String RESOURCE_NAME = "my-resource";
    protected static Vertx vertx;
    protected static KubernetesClient client;

    @BeforeAll
    public static void before() {
        assertDoesNotThrow(() -> KubeCluster.bootstrap(), "Could not bootstrap server");
        vertx = Vertx.vertx();
        client = new DefaultKubernetesClient();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    abstract AbstractNonNamespacedResourceOperator<C, T, L, D, R> operator();
    abstract T getOriginal();
    abstract T getModified();
    abstract void assertResources(VertxTestContext context, T expected, T actual);

    @Test
    public void testCreateModifyDelete(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = operator();

        T newResource = getOriginal();
        T modResource = getModified();

        op.reconcile(RESOURCE_NAME, newResource)
            .onComplete(context.succeeding(rrCreate -> context.verify(() -> {
                T created = op.get(RESOURCE_NAME);

                assertThat("Failed to get created Resource", created, is(notNullValue()));
                assertResources(context, newResource, created);
            })))
            .compose(rr -> op.reconcile(RESOURCE_NAME, modResource))
            .onComplete(context.succeeding(rrModified -> context.verify(() -> {
                T modified = (T) op.get(RESOURCE_NAME);

                assertThat("Failed to get modified Resource", modified, is(notNullValue()));
                assertResources(context, modResource, modified);
            })))
            .compose(rr -> op.reconcile(RESOURCE_NAME, null))
            .onComplete(context.succeeding(rrDelete -> context.verify(() -> {
                T deleted = (T) op.get(RESOURCE_NAME);

                assertThat("Failed to get modified Resource", deleted, is(nullValue()));
                async.flag();
            })));
    }
}

