/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ServiceAccountResource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperatorIT;
import io.strimzi.test.TestUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ServiceAccountOperatorIT extends AbstractNamespacedResourceOperatorIT<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> {
    @Override
    public AbstractNamespacedResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> operator() {
        return new ServiceAccountOperator(asyncExecutor, client, false);
    }

    @Override
    public ServiceAccount getOriginal()  {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .build();
    }

    @Override
    public ServiceAccount getModified()  {
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
    public void testCreateModifyDelete()    {
        ServiceAccountOperator op = new ServiceAccountOperator(asyncExecutor, client, false);

        ServiceAccount newResource = getOriginal();
        ServiceAccount modResource = getModified();

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, newResource)
                .whenComplete((rrCreated, error) -> {
                    assertNull(error);
                    ServiceAccount created = op.get(namespace, resourceName);

                    assertThat(created, Matchers.is(notNullValue()));
                    assertResources(newResource, created);
                })
                .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, modResource))
                .whenComplete((rrModified, error) -> {
                    assertNull(error);
                    ServiceAccount modified = op.get(namespace, resourceName);

                    assertThat(modified, Matchers.is(notNullValue()));
                    assertResources(modResource, modified);
                })
                .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null))
                .whenComplete(TestUtils::assertSuccessful)
                .thenCompose(rr -> resourceSupport.waitFor(
                        Reconciliation.DUMMY_RECONCILIATION,
                        "resource deletion " + resourceName,
                        "deleted",
                        1000,
                        30_000,
                        () -> op.get(namespace, resourceName) == null))
                .thenRun(() -> assertThat(op.get(namespace, resourceName), Matchers.is(nullValue()))));
    }

    @Override
    public void assertResources(ServiceAccount expected, ServiceAccount actual)   {
        assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName()));
        assertThat(actual.getMetadata().getNamespace(), is(expected.getMetadata().getNamespace()));
        assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels()));
    }
}
