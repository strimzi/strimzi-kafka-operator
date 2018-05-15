/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.ServiceOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.ArrayList;
import java.util.List;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractAssemblyOperator {

    private static final Logger log = LogManager.getLogger(KafkaConnectAssemblyOperator.class.getName());
    private final ServiceOperator serviceOperations;
    private final DeploymentOperator deploymentOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param deploymentOperations For operating on Deployments
     * @param serviceOperations For operating on Services
     */
    public KafkaConnectAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                        ConfigMapOperator configMapOperations,
                                        DeploymentOperator deploymentOperations,
                                        ServiceOperator serviceOperations) {
        super(vertx, isOpenShift, AssemblyType.CONNECT, configMapOperations);
        this.serviceOperations = serviceOperations;
        this.deploymentOperations = deploymentOperations;
    }

    @Override
    protected void createOrUpdate(Reconciliation reconciliation, ConfigMap assemblyCm, Handler<AsyncResult<Void>> handler) {

        String namespace = reconciliation.namespace();
        String name = reconciliation.assemblyName();
        KafkaConnectCluster connect;
        try {
            connect = KafkaConnectCluster.fromConfigMap(assemblyCm);
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
            return;
        }
        log.debug("{}: Updating Kafka Connect cluster", reconciliation, name, namespace);
        Future<Void> chainFuture = Future.future();
        deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas())
                .compose(scale -> serviceOperations.reconcile(namespace, connect.getName(), connect.generateService()))
                .compose(i -> deploymentOperations.reconcile(namespace, connect.getName(), connect.generateDeployment()))
                .compose(i -> deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()).map((Void) null))
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(handler);
    }

    @Override
    protected void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String assemblyName = reconciliation.assemblyName();
        String name = KafkaConnectCluster.kafkaConnectClusterName(assemblyName);
        CompositeFuture.join(serviceOperations.reconcile(namespace, name, null),
            deploymentOperations.reconcile(namespace, name, null))
            .map((Void) null).setHandler(handler);
    }

    @Override
    protected List<HasMetadata> getResources(String namespace) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(serviceOperations.list(namespace, Labels.forType(AssemblyType.CONNECT)));
        result.addAll(deploymentOperations.list(namespace, Labels.forType(AssemblyType.CONNECT)));
        return result;
    }
}
