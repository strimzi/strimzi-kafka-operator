/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public abstract class AbstractNamespacedResourceOperatorServerSideApplyIT<C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> extends AbstractNamespacedResourceOperatorIT<C, T, L, R> {
    protected static final Logger LOGGER = LogManager.getLogger(AbstractNamespacedResourceOperatorServerSideApplyIT.class);

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
            }));

        T currentResource = op.get(namespace, resourceName);
        Map<String, String> annotations = new HashMap<>(currentResource.getMetadata().getAnnotations());
        annotations.put("my-annotation", "my-value");

        op.operation().inNamespace(namespace).withName(resourceName).edit(res -> {
            res.getMetadata().setAnnotations(annotations);
            return res;
        });

        currentResource = op.get(namespace, resourceName);

        assertThat(currentResource.getMetadata().getAnnotations(), is(annotations));

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, modResource)
            .onComplete(context.succeeding(rrModified -> {
                T modified = op.get(namespace, resourceName);

                context.verify(() -> assertThat(modified, is(notNullValue())));
                assertThat(modified.getMetadata().getAnnotations(), is(annotations));
                assertResources(context, modResource, modified);
            }))
            .compose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null))
            .onComplete(context.succeeding(rrDeleted -> {
                // it seems the resource is cached for some time, so we need wait for it to be null
                context.verify(() -> {
                        VertxUtil.waitFor(Reconciliation.DUMMY_RECONCILIATION, vertx, "resource deletion " + resourceName, "deleted", 1000,
                                30_000, () -> op.get(namespace, resourceName) == null)
                                .onComplete(del -> {
                                    assertThat(op.get(namespace, resourceName), is(nullValue()));
                                    async.flag();
                                });
                    }
                );
            }));
    }
}

