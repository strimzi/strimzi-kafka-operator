/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;

public abstract class ScalableResourceOperatorTest<C extends KubernetesClient,
            T extends HasMetadata,
            L extends KubernetesResourceList,
            D extends Doneable<T>,
            R extends Resource<T, D>>
        extends AbtractReadyResourceOperatorTest<C, T, L, D, R> {

}
