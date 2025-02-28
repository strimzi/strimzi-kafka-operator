/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.access;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.kafka.access.model.KafkaAccess;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;

public class KafkaAccessResource implements ResourceType<KafkaAccess> {
    @Override
    public String getKind() {
        return KafkaAccess.KIND;
    }

    @Override
    public KafkaAccess get(String namespace, String name) {
        return kafkaAccessClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaAccess resource) {
        kafkaAccessClient().resource(resource).create();
    }

    @Override
    public void delete(KafkaAccess resource) {
        kafkaAccessClient().resource(resource).delete();
    }

    @Override
    public void update(KafkaAccess resource) {
        kafkaAccessClient().resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaAccess resource) {
        return kafkaAccessClient().resource(resource).isReady();
    }

    public static MixedOperation<KafkaAccess, KubernetesResourceList<KafkaAccess>, Resource<KafkaAccess>> kafkaAccessClient() {
        return ResourceManager.kubeClient().getClient().resources(KafkaAccess.class);
    }
}