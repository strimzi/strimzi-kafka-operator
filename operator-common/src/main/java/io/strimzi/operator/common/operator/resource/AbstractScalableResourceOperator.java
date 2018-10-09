/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An {@link AbstractResourceOperator} that can be scaled up and down in addition to the usual operations.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <D> The doneable variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractScalableResourceOperator<C extends KubernetesClient,
            T extends HasMetadata,
            L extends KubernetesResourceList/*<T>*/,
            D extends Doneable<T>,
            R extends ScalableResource<T, D>>
        extends AbstractReadyResourceOperator<C, T, L, D, R> {

    public static final String STRIMZI_CLUSTER_OPERATOR_DOMAIN = "operator.strimzi.io";
    public static final String ANNOTATION_GENERATION = STRIMZI_CLUSTER_OPERATOR_DOMAIN + "/generation";
    public static final String ANNOTATION_MANUAL_DELETE_POD_AND_PVC = STRIMZI_CLUSTER_OPERATOR_DOMAIN + "/delete-pod-and-pvc";

    private final Logger log = LogManager.getLogger(getClass());

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     * @param resourceKind The kind of resource.
     */
    public AbstractScalableResourceOperator(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    private R resource(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name);
    }

    /**
     * Asynchronously scale up the resource given by {@code namespace} and {@code name} to have the scale given by
     * {@code scaleTo}, returning a future for the outcome.
     * If the resource does not exist, or has a current scale >= the given {@code scaleTo}, then complete successfully.
     * @param namespace The namespace of the resource to scale.
     * @param name The name of the resource to scale.
     * @param scaleTo The desired scale.
     * @return A future whose value is the scale after the operation.
     * If the scale was initially > the given {@code scaleTo} then this value will be the original scale,
     * The value will be null if the resource didn't exist (hence no scaling occurred).
     */
    public Future<Integer> scaleUp(String namespace, String name, int scaleTo) {
        Future<Integer> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    Integer currentScale = currentScale(namespace, name);
                    if (currentScale != null && currentScale < scaleTo) {
                        log.info("Scaling up to {} replicas", scaleTo);
                        resource(namespace, name).scale(scaleTo, true);
                        currentScale = scaleTo;
                    }
                    future.complete(currentScale);
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

    protected abstract Integer currentScale(String namespace, String name);

    /**
     * Asynchronously scale down the resource given by {@code namespace} and {@code name} to have the scale given by
     * {@code scaleTo}, returning a future for the outcome.
     * If the resource does not exists, is has a current scale <= the given {@code scaleTo} then complete successfully.
     * @param namespace The namespace of the resource to scale.
     * @param name The name of the resource to scale.
     * @param scaleTo The desired scale.
     * @return A future whose value is the scale after the operation.
     * If the scale was initially < the given {@code scaleTo} then this value will be the original scale,
     * The value will be null if the resource didn't exist (hence no scaling occurred).
     */
    public Future<Integer> scaleDown(String namespace, String name, int scaleTo) {
        Future<Integer> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    Integer nextReplicas = currentScale(namespace, name);
                    if (nextReplicas != null) {
                        while (nextReplicas > scaleTo) {
                            nextReplicas--;
                            log.info("Scaling down from {} to {}", nextReplicas + 1, nextReplicas);
                            resource(namespace, name).scale(nextReplicas, true);
                        }
                    }
                    future.complete(nextReplicas);
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
