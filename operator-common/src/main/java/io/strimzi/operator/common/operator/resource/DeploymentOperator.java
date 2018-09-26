/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Deployment}s.
 */
public class DeploymentOperator extends AbstractScalableResourceOperator<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> {

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public DeploymentOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Deployment");
    }

    @Override
    protected MixedOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> operation() {
        return client.extensions().deployments();
    }

    @Override
    protected Integer currentScale(String namespace, String name) {
        Deployment deployment = get(namespace, name);
        if (deployment != null) {
            return deployment.getSpec().getReplicas();
        } else {
            return null;
        }
    }

    /**
     * Asynchronously roll the deployment returning a Future which will complete once all the pods have been rolled
     * and the Deployment is ready.
     */
    public Future<Void> rollingUpdate(String namespace, String name, long operationTimeoutMs) {
        return getAsync(namespace, name)
                .compose(deployment -> reconcile(namespace, name, incrementGeneration(deployment)))
                .compose(ignored -> readiness(namespace, name, 1_000, operationTimeoutMs));
    }

    private Deployment incrementGeneration(Deployment deployment) {
        String generationStr = deployment.getSpec().getTemplate().getMetadata().getAnnotations().get(ANNOTATION_GENERATION);
        int generation = 0;
        if (generationStr != null && !generationStr.isEmpty()) {
            generation = Integer.parseInt(generationStr);
            generation++;
        }
        return new DeploymentBuilder(deployment)
                .editSpec()
                    .editTemplate()
                        .editMetadata()
                            .addToAnnotations(ANNOTATION_GENERATION, Integer.toString(generation))
                        .endMetadata()
                    .endTemplate()
                .endSpec()
            .build();
    }
}
