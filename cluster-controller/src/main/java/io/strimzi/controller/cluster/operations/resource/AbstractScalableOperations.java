/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AbstractOperations} that can be scaled up and down in addition to the usual operations.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <D> The doneable variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractScalableOperations<C, T extends HasMetadata, L extends KubernetesResourceList/*<T>*/, D, R extends ScalableResource<T, D>>
        extends AbstractReadyOperations<C, T, L, D, R> {

    private static final Logger log = LoggerFactory.getLogger(AbstractScalableOperations.class.getName());

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     * @param resourceKind The kind of resource.
     */
    public AbstractScalableOperations(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    private R resource(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name);
    }

    /**
     * Asynchronously scale up the resource given by {@code namespace} and {@code name} to have the scale given by
     * {@code scaleTo}, returning a future for the outcome..
     * @param namespace The namespace of the resource to scale.
     * @param name The name of the resource to scale.
     * @param scaleTo The desired scale.
     */
    public Future<Void> scaleUp(String namespace, String name, int scaleTo) {
        Future<Void> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    log.info("Scaling up to {} replicas", scaleTo);
                    resource(namespace, name).scale(scaleTo, true);
                    future.complete();
                } catch (Exception e) {
                    log.error("Caught exception while scaling up", e);
                    future.fail(e);
                }
            },
            false,
            fut.completer()
        );
        return fut;
    }

    /**
     * Asynchronously scale down the resource given by {@code namespace} and {@code name} to have the scale given by
     * {@code scaleTo}, returning a future for the outcome.
     * @param namespace The namespace of the resource to scale.
     * @param name The name of the resource to scale.
     * @param scaleTo The desired scale.
     */
    public Future<Void> scaleDown(String namespace, String name, int scaleTo) {
        Future<Void> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    Object gettable = resource(namespace, name).get();
                    int nextReplicas;

                    if (gettable instanceof StatefulSet) {
                        nextReplicas = ((StatefulSet) resource(namespace, name).get()).getSpec().getReplicas();
                    } else if (gettable instanceof Deployment) {
                        nextReplicas = ((Deployment) resource(namespace, name).get()).getSpec().getReplicas();
                    } else if (gettable instanceof DeploymentConfig) {
                        nextReplicas = ((DeploymentConfig) resource(namespace, name).get()).getSpec().getReplicas();
                    } else {
                        future.fail("Unknown resource type: " + gettable.getClass().getCanonicalName());
                        return;
                    }

                    while (nextReplicas > scaleTo) {
                        nextReplicas--;
                        log.info("Scaling down from {} to {}", nextReplicas + 1, nextReplicas);
                        resource(namespace, name).scale(nextReplicas, true);
                    }

                    future.complete();
                } catch (Exception e) {
                    log.error("Caught exception while scaling down", e);
                    future.fail(e);
                }
            },
            false,
            fut.completer()
        );
        return fut;
    }
}
