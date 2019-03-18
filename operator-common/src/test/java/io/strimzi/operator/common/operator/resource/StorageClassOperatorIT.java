/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.storage.DoneableStorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.fabric8.kubernetes.api.model.storage.StorageClassList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class StorageClassOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        StorageClass, StorageClassList, DoneableStorageClass, Resource<StorageClass, DoneableStorageClass>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient, StorageClass, StorageClassList, DoneableStorageClass, Resource<StorageClass, DoneableStorageClass>> operator() {
        return new StorageClassOperator(vertx, client);
    }

    @Override
    protected StorageClass getOriginal()  {
        return new StorageClassBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
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
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withReclaimPolicy("Delete")
                .withProvisioner("kubernetes.io/aws-ebs")
                .withParameters(singletonMap("type", "gp2"))
                .withVolumeBindingMode("Immediate")
                .build();
    }

    @Override
    protected void assertResources(TestContext context, StorageClass expected, StorageClass actual)   {
        context.assertEquals(expected.getMetadata().getName(), actual.getMetadata().getName());
        context.assertEquals(expected.getMetadata().getLabels(), actual.getMetadata().getLabels());
        context.assertEquals(expected.getReclaimPolicy(), actual.getReclaimPolicy());
        context.assertEquals(expected.getProvisioner(), actual.getProvisioner());
        context.assertEquals(expected.getParameters(), actual.getParameters());
    }
}
