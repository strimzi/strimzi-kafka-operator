/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.JmxTransResources;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.vertx.core.Future;

/**
 * JMX Trans is not supported anymore. The JmxTransReconciler class is still used to delete the JMXTrans deployment in
 * case it was used by a previous Strimzi version. The only functionality here is now deleting the JMXTrans deployment.
 * This will be completely removed in Strimzi 0.40.0.
 */
public class JmxTransReconciler {
    private final Reconciliation reconciliation;

    private final DeploymentOperator deploymentOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final ConfigMapOperator configMapOperator;

    /**
     * Constructs the JMX Trans reconciler
     *
     * @param reconciliation Reconciliation marker
     * @param supplier       Supplier with Kubernetes Resource Operators
     */
    public JmxTransReconciler(
            Reconciliation reconciliation,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;

        this.deploymentOperator = supplier.deploymentOperations;
        this.configMapOperator = supplier.configMapOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. Since JMX Trans is removed, this
     * now only deletes the resources in case of upgrade.
     *
     * @return Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile()    {
        return serviceAccount()
                .compose(i -> configMap())
                .compose(i -> deployment());
    }

    /**
     * Deletes the ConfigMap with JMX Trans configuration
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> configMap() {
        return configMapOperator
                .reconcile(reconciliation, reconciliation.namespace(), JmxTransResources.configMapName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletes the JMX Trans Service Account
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(reconciliation, reconciliation.namespace(), JmxTransResources.serviceAccountName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Deletes the JMX Trans Deployment
     *
     * @return Future which completes when the reconciliation is done
     */
    protected Future<Void> deployment() {
        return deploymentOperator
                .reconcile(reconciliation, reconciliation.namespace(), JmxTransResources.deploymentName(reconciliation.name()), null)
                .map((Void) null);
    }
}