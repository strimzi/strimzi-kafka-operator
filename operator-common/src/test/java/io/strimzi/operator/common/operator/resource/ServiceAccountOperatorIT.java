/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Util;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(VertxExtension.class)
public class ServiceAccountOperatorIT extends AbstractResourceOperatorIT<KubernetesClient, ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> {
    @Override
    protected AbstractResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> operator() {
        return new ServiceAccountOperator(vertx, client, true);
    }

    @Override
    protected ServiceAccount getOriginal()  {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .build();
    }

    @Override
    protected ServiceAccount getModified()  {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("foo2", "bar2"))
                .endMetadata()
                .build();
    }

    @Test
    @Override
    public void testCreateModifyDelete(VertxTestContext context)    {
        Checkpoint async = context.checkpoint();
        ServiceAccountOperator op = new ServiceAccountOperator(vertx, client, true);

        ServiceAccount newResource = getOriginal();
        ServiceAccount modResource = getModified();

        List<ObjectReference> secrets = new ArrayList<>();

        op.reconcile(namespace, resourceName, newResource)
                .onComplete(context.succeeding(rrCreated -> {
                    ServiceAccount created = op.get(namespace, resourceName);

                    context.verify(() -> assertThat(created, Matchers.is(notNullValue())));
                    assertResources(context, newResource, created);
                    secrets.addAll(created.getSecrets());
                }))
                .compose(rr -> op.reconcile(namespace, resourceName, modResource))
                .onComplete(context.succeeding(rrModified -> {
                    ServiceAccount modified = op.get(namespace, resourceName);

                    context.verify(() -> assertThat(modified, Matchers.is(notNullValue())));
                    assertResources(context, modResource, modified);
                    context.verify(() -> assertThat(modified.getSecrets(), is(secrets)));
                }))
                .compose(rr -> op.reconcile(namespace, resourceName, null))
                .onComplete(context.succeeding(rrDeleted -> {
                    // it seems the resource is cached for some time so we need wait for it to be null
                    context.verify(() -> Util.waitFor(vertx, "resource deletion " + resourceName, "deleted", 1000,
                            30_000, () -> op.get(namespace, resourceName) == null)
                            .onComplete(del -> {
                                assertThat(op.get(namespace, resourceName), Matchers.is(nullValue()));
                                async.flag();
                            })
                    );
                }));
    }

    @Override
    protected void assertResources(VertxTestContext context, ServiceAccount expected, ServiceAccount actual)   {
        context.verify(() -> assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName())));
        context.verify(() -> assertThat(actual.getMetadata().getNamespace(), is(expected.getMetadata().getNamespace())));
        context.verify(() -> assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels())));
    }
}
