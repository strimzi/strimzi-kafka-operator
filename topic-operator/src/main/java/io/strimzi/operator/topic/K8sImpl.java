/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/** Partial Implementation of Kubernetes */
public class K8sImpl implements K8s {

    private final static Logger LOGGER = LogManager.getLogger(K8sImpl.class);

    private final Labels labels;
    private final String namespace;

    private final KubernetesClient client;
    private final CrdOperator<KubernetesClient, KafkaTopic, KafkaTopicList> crdOperator;

    private final Vertx vertx;

    /**
     * Constructor
     *
     * @param vertx  Instance of vertx
     * @param client    Instance of Kubernetes client
     * @param labels    Cluster label
     * @param namespace   Namespace where the cluster is deployed
     */
    public K8sImpl(Vertx vertx, KubernetesClient client, Labels labels, String namespace) {
        this.vertx = vertx;
        this.client = client;
        this.crdOperator = new CrdOperator<>(vertx, client, KafkaTopic.class, KafkaTopicList.class, KafkaTopic.RESOURCE_KIND);
        this.labels = labels;
        this.namespace = namespace;
    }

    /**
     * Creates the Kafka topic resource
     *
     * @param topicResource  The topic resource that need to be created
     * @return  Future which completes with result of the request. If the request was successful, this returns the Kafka topic
     */
    @Override
    public Future<KafkaTopic> createResource(KafkaTopic topicResource) {
        Promise<KafkaTopic> handler = Promise.promise();
        vertx.executeBlocking(future -> {
            try {
                KafkaTopic kafkaTopic = operation().inNamespace(namespace).resource(topicResource).create();
                LOGGER.debug("KafkaTopic {} created with version {}->{}",
                        kafkaTopic.getMetadata().getName(),
                        topicResource.getMetadata() != null ? topicResource.getMetadata().getResourceVersion() : null,
                        kafkaTopic.getMetadata().getResourceVersion());
                future.complete(kafkaTopic);
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
        return handler.future();
    }

    /**
     * Updates the Kafka topic resource
     *
     * @param topicResource  The topic resource that need to be updated
     * @return  Future which completes with result of the request. If the request was successful, this returns the updated Kafka topic
     */
    @Override
    public Future<KafkaTopic> updateResource(KafkaTopic topicResource) {
        Promise<KafkaTopic> handler = Promise.promise();
        vertx.executeBlocking(future -> {
            try {
                KafkaTopic kafkaTopic = operation().inNamespace(namespace).withName(topicResource.getMetadata().getName()).patch(PatchContext.of(PatchType.JSON), topicResource);
                LOGGER.debug("KafkaTopic {} updated with version {}->{}",
                        kafkaTopic != null && kafkaTopic.getMetadata() != null ? kafkaTopic.getMetadata().getName() : null,
                        topicResource.getMetadata() != null ? topicResource.getMetadata().getResourceVersion() : null,
                        kafkaTopic != null && kafkaTopic.getMetadata() != null ? kafkaTopic.getMetadata().getResourceVersion() : null);
                future.complete(kafkaTopic);
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
        return handler.future();
    }

    /**
     * Updates the status of the Kafka topic resource
     *
     * @param ctx            Reconciliation Marker
     * @param topicResource  The topic resource that need to be created
     * @return  Future which completes with result of the request. If the request was successful, this returns the Kafka topic with updated status
     */
    @Override
    public Future<KafkaTopic> updateResourceStatus(Reconciliation ctx, KafkaTopic topicResource) {
        return crdOperator.updateStatusAsync(ctx, topicResource);
    }

    /**
     * Deletes the Kafka topic resource
     *
     * @param reconciliation Reconciliation marker
     * @param resourceName  The topic resource name that need to be deleted
     * @return  Future which completes when the resource is deleted successfully.
     */
    @Override
    public Future<Void> deleteResource(Reconciliation reconciliation, ResourceName resourceName) {
        Promise<Void> handler = Promise.promise();
        vertx.executeBlocking(future -> {
            try {
                // Delete the resource by the topic name, because neither ZK nor Kafka know the resource name
                operation().inNamespace(namespace).withName(resourceName.toString()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

                Util.waitFor(reconciliation, vertx, "sync resource deletion " + resourceName, "deleted", 1000, Long.MAX_VALUE, () -> {
                    KafkaTopic kafkaTopic = operation().inNamespace(namespace).withName(resourceName.toString()).get();
                    boolean notExists = kafkaTopic == null;
                    LOGGER.debug("KafkaTopic {} deleted {}", resourceName.toString(), notExists);
                    return notExists;
                }).onComplete(future);
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
        return handler.future();
    }

    private MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> operation() {
        return client.resources(KafkaTopic.class, KafkaTopicList.class);
    }

    /**
     * Lists the Kafka topics
     *
     * @return  Future which completes with result of the request. If the request was successful, this returns a list of Kafka topics
     */
    @Override
    public Future<List<KafkaTopic>> listResources() {
        return crdOperator.listAsync(namespace, io.strimzi.operator.common.model.Labels.fromMap(labels.labels()));
    }

    /**
     * Lists the Kafka topic based on the resource name
     *
     * @param resourceName Name of the resource
     * @return  Future which completes with result of the request. If the request was successful, this returns a Kafka topic
     */
    @Override
    public Future<KafkaTopic> getFromName(ResourceName resourceName) {
        return crdOperator.getAsync(namespace, resourceName.toString());
    }

    /**
     * Create the given k8s event
     */
    @Override
    public Future<Void> createEvent(Event event) {
        Promise<Void> handler = Promise.promise();
        vertx.executeBlocking(future -> {
            try {
                try {
                    LOGGER.debug("Creating event {}", event);
                    client.v1().events().inNamespace(namespace).resource(event).create();
                } catch (KubernetesClientException e) {
                    LOGGER.error("Error creating event {}", event, e);
                }
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
        return handler.future();
    }
}
