/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.dsl.Resource;

class CustomResourceMockBuilder<T extends CustomResource, L extends KubernetesResource & KubernetesResourceList<T>, D extends Doneable<T>> extends MockBuilder<T, L, D, Resource<T, D>> {
    public CustomResourceMockBuilder(MockKube.MockedCrd<T, L, D> mockedCrd) {
        super(mockedCrd.getCrClass(), mockedCrd.getCrListClass(), mockedCrd.getCrDoneableClass(), castClass(Resource.class), mockedCrd.getInstances());
    }
}
