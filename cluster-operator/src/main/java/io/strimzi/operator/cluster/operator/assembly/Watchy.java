package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.vertx.core.Future;

import java.util.function.Consumer;

public interface Watchy {
    public Future<Watch> createWatch(String watchNamespace, Consumer<KubernetesClientException> onClose);
}
