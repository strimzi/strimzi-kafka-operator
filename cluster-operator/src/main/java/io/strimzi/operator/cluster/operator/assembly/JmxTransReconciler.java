/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.JmxTransResources;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.JmxTrans;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.vertx.core.Future;

import java.util.List;

/**
 * Class used for reconciliation of JMX Trans deployment. This class contains both the steps of the JMX Trans
 * reconciliation pipeline and is also used to store the state between them.
 */
public class JmxTransReconciler {
    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final JmxTrans jmxTrans;

    private final DeploymentOperator deploymentOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final ConfigMapOperator configMapOperator;

    private String configMapHashStub = null;

    /**
     * Constructs the JMX Trans reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param operationTimeoutMs        Timeout for Kubernetes operations
     * @param kafkaAssembly             The Kafka custom resource
     * @param deploymentOperator        The Deployment operator for working with Kubernetes Deployments
     * @param configMapOperator         The Config Map operator for working with Kubernetes Config Maps
     * @param serviceAccountOperator    The Service Account operator for working with Kubernetes Service Accounts
     */
    public JmxTransReconciler(
            Reconciliation reconciliation,
            long operationTimeoutMs,
            Kafka kafkaAssembly,
            DeploymentOperator deploymentOperator,
            ConfigMapOperator configMapOperator,
            ServiceAccountOperator serviceAccountOperator
    ) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = operationTimeoutMs;
        this.jmxTrans = JmxTrans.fromCrd(reconciliation, kafkaAssembly);

        this.deploymentOperator = deploymentOperator;
        this.configMapOperator = configMapOperator;
        this.serviceAccountOperator = serviceAccountOperator;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     *
     * @return                  Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets)    {
        return jmxTransServiceAccount()
                .compose(i -> jmxTransConfigMap())
                .compose(i -> jmxTransDeployment(imagePullPolicy, imagePullSecrets))
                .compose(i -> jmxTransDeploymentReady());
    }

    /**
     * Manages the JMX Trans Config Map with the JMX Trans specific configuration
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> jmxTransConfigMap() {
        if (jmxTrans != null)   {
            ConfigMap configMap = jmxTrans.generateConfigMap();
            configMapHashStub = Util.hashStub(configMap.getData().getOrDefault(JmxTrans.JMXTRANS_CONFIGMAP_KEY, ""));

            return configMapOperator
                    .reconcile(reconciliation, reconciliation.namespace(), JmxTransResources.configMapName(reconciliation.name()), configMap)
                    .map((Void) null);
        } else {
            return configMapOperator
                    .reconcile(reconciliation, reconciliation.namespace(), JmxTransResources.configMapName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Manages the JMX Trans Service Account
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> jmxTransServiceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        JmxTransResources.serviceAccountName(reconciliation.name()),
                        jmxTrans != null ? jmxTrans.generateServiceAccount() : null
                ).map((Void) null);
    }

    /**
     * Manages the JMX Trans Deployment
     *
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> jmxTransDeployment(ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (jmxTrans != null) {
            Deployment deployment = jmxTrans.generateDeployment(imagePullPolicy, imagePullSecrets);

            Annotations.annotations(deployment.getSpec().getTemplate()).put(JmxTrans.ANNO_JMXTRANS_CONFIG_MAP_HASH, configMapHashStub);

            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), JmxTransResources.deploymentName(reconciliation.name()), deployment)
                    .map((Void) null);
        } else {
            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), JmxTransResources.deploymentName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Waits for the JMX Trans deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    Future<Void> jmxTransDeploymentReady() {
        if (this.jmxTrans != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(),  this.jmxTrans.getName(), 1_000, operationTimeoutMs)
                    .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), this.jmxTrans.getName(), 1_000, operationTimeoutMs));
        } else {
            return Future.succeededFuture();
        }
    }
}