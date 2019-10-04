/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.vertx.core.Future;

import java.util.function.Consumer;

public interface Watchy {
    Future<Watch> createWatch(String watchNamespace, Consumer<KubernetesClientException> onClose);
}
