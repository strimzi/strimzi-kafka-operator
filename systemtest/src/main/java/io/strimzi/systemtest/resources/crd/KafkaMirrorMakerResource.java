/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.test.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;

public class KafkaMirrorMakerResource implements ResourceType<KafkaMirrorMaker> {

    @Override
    public String getKind() {
        return KafkaMirrorMaker.RESOURCE_KIND;
    }
    @Override
    public KafkaMirrorMaker get(String namespace, String name) {
        return kafkaMirrorMakerClient().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaMirrorMaker resource) {
        kafkaMirrorMakerClient().inNamespace(resource.getMetadata().getNamespace()).createOrReplace(resource);
    }
    @Override
    public void delete(KafkaMirrorMaker resource) throws Exception {
        kafkaMirrorMakerClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }
    @Override
    public boolean isReady(KafkaMirrorMaker resource) {
        return ResourceManager.waitForResourceStatus(kafkaMirrorMakerClient(), resource, CustomResourceStatus.Ready);
    }
    @Override
    public void refreshResource(KafkaMirrorMaker existing, KafkaMirrorMaker newResource) {
        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
        existing.setStatus(newResource.getStatus());
    }
    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, Resource<KafkaMirrorMaker>> kafkaMirrorMakerClient() {
        return Crds.mirrorMakerV1Beta2Operation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceMirrorMakerResource(String resourceName, Consumer<KafkaMirrorMaker> editor) {
        ResourceManager.replaceCrdResource(KafkaMirrorMaker.class, KafkaMirrorMakerList.class, resourceName, editor);
    }
}
