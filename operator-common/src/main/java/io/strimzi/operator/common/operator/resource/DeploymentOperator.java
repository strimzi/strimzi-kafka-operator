/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.HashMap;

/**
 * Operations for {@code Deployment}s.
 */
public class DeploymentOperator extends AbstractScalableResourceOperator<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> {

    private final PodOperator podOperations;
    private final SecretOperator secretOperations;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public DeploymentOperator(Vertx vertx, KubernetesClient client) {
        this(vertx, client, new PodOperator(vertx, client), new SecretOperator(vertx, client));
    }

    public DeploymentOperator(Vertx vertx, KubernetesClient client, PodOperator podOperations, SecretOperator secretOperations) {
        super(vertx, client, "Deployment");
        this.podOperations = podOperations;
        this.secretOperations = secretOperations;
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
                .compose(deployment -> deletePod(namespace, name))
                .compose(ignored -> readiness(namespace, name, 1_000, operationTimeoutMs));
    }

    public Future<ReconcileResult<Pod>> deletePod(String namespace, String name) {
        Labels labels = Labels.fromMap(null).withName(name);
        String podName = podOperations.list(namespace, labels).get(0).getMetadata().getName();
        return podOperations.reconcile(namespace, podName, null);
    }

    @Override
    protected Future<ReconcileResult<Deployment>> internalCreate(String namespace, String name, Deployment desired) {
        copyClusterCaCertGeneration(namespace, desired);
        return super.internalCreate(namespace, name, desired);
    }

    @Override
    protected Future<ReconcileResult<Deployment>> internalPatch(String namespace, String name, Deployment current, Deployment desired, boolean cascading) {
        if (current.getMetadata().getAnnotations() != null) {
            String k8sRev = current.getMetadata().getAnnotations().get("deployment.kubernetes.io/revision");
            if (k8sRev != null) {
                if (desired.getMetadata().getAnnotations() == null) {
                    desired.getMetadata().setAnnotations(new HashMap<>(1));
                }
                desired.getMetadata().getAnnotations().put("deployment.kubernetes.io/revision", k8sRev);
            }
        }

        copyClusterCaCertGeneration(namespace, desired);
        return super.internalPatch(namespace, name, current, desired, cascading);
    }

    private static ObjectMeta templateMetadata(Deployment resource) {
        return resource.getSpec().getTemplate().getMetadata();
    }

    private void copyClusterCaCertGeneration(String namespace, Deployment desired) {
        String clusterName = desired.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        Secret clusterCaCertSecret = secretOperations.get(namespace, KafkaResources.clusterCaCertificateSecretName(clusterName));
        templateMetadata(desired).getAnnotations()
                .put(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, clusterCaCertSecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
    }
}
