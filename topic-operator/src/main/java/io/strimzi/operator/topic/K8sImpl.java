/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.DoneableKafkaTopic;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class K8sImpl implements K8s {

    private final static Logger LOGGER = LogManager.getLogger(TopicOperator.class);

    private final LabelPredicate resourcePredicate;
    private final String namespace;

    private KubernetesClient client;

    private Vertx vertx;

    public K8sImpl(Vertx vertx, KubernetesClient client, LabelPredicate resourcePredicate, String namespace) {
        this.vertx = vertx;
        this.client = client;
        this.resourcePredicate = resourcePredicate;
        this.namespace = namespace;
    }

    @Override
    public void createResource(KafkaTopic topicResource, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                operation().inNamespace(namespace).create(topicResource);
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void updateResource(KafkaTopic topicResource, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                operation().inNamespace(namespace).createOrReplace(topicResource);
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void deleteResource(ResourceName resourceName, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                // Delete the resource by the topic name, because neither ZK nor Kafka know the resource name
                operation().inNamespace(namespace).withName(resourceName.toString()).delete();
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    private MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> operation() {
        return client.customResources(Crds.topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    @Override
    public void listMaps(Handler<AsyncResult<List<KafkaTopic>>> handler) {
        vertx.executeBlocking(future -> {
            try {
                future.complete(operation().inNamespace(namespace).withLabels(resourcePredicate.labels()).list().getItems());
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void getFromName(ResourceName resourceName, Handler<AsyncResult<KafkaTopic>> handler) {
        vertx.executeBlocking(future -> {
            try {
                future.complete(operation().inNamespace(namespace).withName(resourceName.toString()).get());
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);

    }

    /**
     * Create the given k8s event
     */
    @Override
    public void createEvent(Event event, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                try {
                    LOGGER.debug("Creating event {}", event);
                    client.events().inNamespace(namespace).create(event);
                } catch (KubernetesClientException e) {
                    LOGGER.error("Error creating event {}", event, e);
                }
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }
}
