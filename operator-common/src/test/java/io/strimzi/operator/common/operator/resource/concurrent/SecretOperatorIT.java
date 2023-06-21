/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SecretOperatorIT extends AbstractNamespacedResourceOperatorIT<KubernetesClient, Secret, SecretList, Resource<Secret>> {

    @Override
    Secret getOriginal() {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();
    }

    @Override
    Secret getModified() {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("foo", "baz"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAZ"))
                .build();
    }

    @Override
    void assertResources(Secret expected, Secret actual) {
        assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels()));
        assertThat(actual.getData(), is(expected.getData()));
    }

    @Override
    AbstractNamespacedResourceOperator<KubernetesClient, Secret, SecretList, Resource<Secret>> operator() {
        return new SecretOperator(asyncExecutor, client);
    }
}
