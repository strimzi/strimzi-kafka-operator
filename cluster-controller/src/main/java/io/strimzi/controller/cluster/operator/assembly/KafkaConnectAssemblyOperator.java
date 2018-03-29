/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.controller.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.controller.cluster.operator.resource.DeploymentOperator;
import io.strimzi.controller.cluster.operator.resource.ServiceOperator;
import io.strimzi.controller.cluster.model.KafkaConnectCluster;
import io.strimzi.controller.cluster.model.Labels;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractAssemblyOperator<Deployment> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectAssemblyOperator.class.getName());
    private static final String CLUSTER_TYPE_CONNECT = "kafka-connect";
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
        super(vertx, isOpenShift, CLUSTER_TYPE_CONNECT, "Kafka Connect", configMapOperations);
        this.serviceOperations = serviceOperations;
        this.deploymentOperations = deploymentOperations;
    }

    @Override
    protected void createOrUpdate(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        ConfigMap connectConfigMap = configMapOperations.get(namespace, name);
        KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(connectConfigMap);
        log.info("Updating Kafka Connect cluster {} in namespace {}", name, namespace);
        Future<Void> chainFuture = Future.future();
        deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas())
                .compose(scale -> serviceOperations.reconcile(namespace, connect.getName(), connect.generateService()))
                .compose(i -> deploymentOperations.reconcile(namespace, connect.getName(), connect.generateDeployment()))
                .compose(i -> deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()).map((Void) null))
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(handler);
    }

    @Override
    protected void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        Deployment dep = deploymentOperations.get(namespace, KafkaConnectCluster.kafkaConnectClusterName(name));
        KafkaConnectCluster connect = KafkaConnectCluster.fromAssembly(namespace, name, dep);
        List<Future> result = new ArrayList<>(3);
        result.add(serviceOperations.reconcile(namespace, connect.getName(), null));
        result.add(deploymentOperations.reconcile(namespace, connect.getName(), null));
        CompositeFuture.join(result).map((Void) null).setHandler(handler);
    }

    @Override
    protected List<Deployment> getResources(String namespace, Labels selector) {
        return deploymentOperations.list(namespace, selector);
    }
}
