/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.DeploymentCondition;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.DeployableScalableResource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code DeploymentConfigs}s.
 */
public class DeploymentConfigOperator extends AbstractScalableNamespacedResourceOperator<OpenShiftClient, DeploymentConfig,
        DeploymentConfigList, DeployableScalableResource<DeploymentConfig>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public DeploymentConfigOperator(Vertx vertx, OpenShiftClient client) {
        super(vertx, client, "DeploymentConfig");
    }

    @Override
    protected MixedOperation<DeploymentConfig, DeploymentConfigList, DeployableScalableResource<DeploymentConfig>> operation() {
        return client.deploymentConfigs();
    }

    @Override
    protected Integer currentScale(String namespace, String name) {
        DeploymentConfig deploymentConfig = get(namespace, name);
        if (deploymentConfig != null) {
            return deploymentConfig.getSpec().getReplicas();
        } else {
            return null;
        }
    }

    @Override
    protected Future<ReconcileResult<DeploymentConfig>> internalUpdate(Reconciliation reconciliation, String namespace, String name, DeploymentConfig current, DeploymentConfig desired) {
        desired.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(current.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        return super.internalUpdate(reconciliation, namespace, name, current, desired);
    }

    /**
     * Asynchronously polls the deployment configuration until either the observed generation matches the desired
     * generation sequence number or timeout.
     *
     * @param reconciliation The reconciliation
     * @param namespace The namespace.
     * @param name The resource name.
     * @param pollIntervalMs The polling interval
     * @param timeoutMs The timeout
     * @return  A future which completes when the observed generation of the deployment configuration matches the
     * generation sequence number of the desired state.
     */
    public Future<Void> waitForObserved(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(reconciliation, namespace, name, "observed", pollIntervalMs, timeoutMs, this::isObserved);
    }

    /**
     * Check if a deployment configuration has been observed.
     *
     * @param namespace The namespace.
     * @param name The resource name.
     * @return Whether the deployment has been observed.
     */
    private boolean isObserved(String namespace, String name) {
        DeploymentConfig dep = get(namespace, name);
        if (dep != null)   {
            // Get the roll out status
            //     => Sometimes it takes OCP some time before the generations are updated.
            //        So we need to check the conditions in addition to detect such situation.
            boolean rollOutNotStarting = true;
            DeploymentCondition progressing = getProgressingCondition(dep);

            if (progressing != null)    {
                rollOutNotStarting = progressing.getReason() != null && !"Unknown".equals(progressing.getStatus());
            }

            return dep.getMetadata().getGeneration().equals(dep.getStatus().getObservedGeneration())
                    && rollOutNotStarting;
        } else {
            return false;
        }
    }

    /**
     * Retrieves the Progressing condition from the DeploymentConfig status
     *
     * @param dep   DeploymentConfig resource
     * @return      Progressing condition
     */
    private DeploymentCondition getProgressingCondition(DeploymentConfig dep)  {
        if (dep.getStatus() != null
                && dep.getStatus().getConditions() != null) {
            return dep.getStatus().getConditions().stream().filter(condition -> "Progressing".equals(condition.getType())).findFirst().orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Due to the separation of Fabric8 Kubernetes and OpenShift clients, the DeploymentConfig needs its own isReady()
     * method instead of using isReady() from AbstractReadyResourceOperator because it would not pass Readiness.isReadinessApplicable()
     *
     * @param namespace The namespace.
     * @param name The resource name.
     * @return Whether the resource is in the Ready state.
     */
    @Override
    public boolean isReady(String namespace, String name) {
        DeployableScalableResource<DeploymentConfig> resourceOp = operation().inNamespace(namespace).withName(name);
        DeploymentConfig resource = resourceOp.get();
        
        if (resource != null)   {
            return resourceOp.isReady();
        } else {
            return false;
        }
    }
}
