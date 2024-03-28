/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.fabric8.kubernetes.api.model.storage.StorageClassList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class StorageClassOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        StorageClass, StorageClassList, Resource<StorageClass>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient, StorageClass, StorageClassList, Resource<StorageClass>> operator() {
        return new StorageClassOperator(vertx, client);
    }

    @Override
    protected StorageClass getOriginal()  {
        return new StorageClassBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withLabels(singletonMap("state", "new"))
                .endMetadata()
                .withReclaimPolicy("Delete")
                .withProvisioner("kubernetes.io/aws-ebs")
                .withParameters(singletonMap("type", "gp2"))
                .withVolumeBindingMode("Immediate")
                .build();
    }

    @Override
    protected StorageClass getModified()  {
        // Most of the fields seem to be immutable, we patch only labels
        return new StorageClassBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withReclaimPolicy("Delete")
                .withProvisioner("kubernetes.io/aws-ebs")
                .withParameters(singletonMap("type", "gp2"))
                .withVolumeBindingMode("Immediate")
                .build();
    }

    @Override
    protected void assertResources(VertxTestContext context, StorageClass expected, StorageClass actual) {
        context.verify(() -> {
            assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName()));
            assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels()));
            assertThat(actual.getReclaimPolicy(), is(expected.getReclaimPolicy()));
            assertThat(actual.getProvisioner(), is(expected.getProvisioner()));
            assertThat(actual.getParameters(), is(expected.getParameters()));
        });
    }
}
