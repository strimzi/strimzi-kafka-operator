/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaConnectS2ICluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.operator.resource.BuildConfigOperator;
import io.strimzi.operator.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.cluster.operator.resource.ImageStreamOperator;
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
 * <p>Assembly operator for a "Kafka Connect S2I" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 *     <li>An ImageBuildStream</li>
 *     <li>A BuildConfig</li>
 * </ul>
 */
public class KafkaConnectS2IAssemblyOperator extends AbstractAssemblyOperator {

    private static final Logger log = LogManager.getLogger(KafkaConnectS2IAssemblyOperator.class.getName());
    private final ServiceOperator serviceOperations;
    private final DeploymentConfigOperator deploymentConfigOperations;
    private final ImageStreamOperator imagesStreamOperations;
    private final BuildConfigOperator buildConfigOperations;

    /**
     * @param vertx                      The Vertx instance
     * @param isOpenShift                Whether we're running with OpenShift
     * @param configMapOperations        For operating on ConfigMaps
     * @param deploymentConfigOperations For operating on Deployments
     * @param serviceOperations          For operating on Services
     * @param imagesStreamOperations     For operating on ImageStreams, may be null
     * @param buildConfigOperations      For operating on BuildConfigs, may be null
     */
    public KafkaConnectS2IAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                           ConfigMapOperator configMapOperations,
                                           DeploymentConfigOperator deploymentConfigOperations,
                                           ServiceOperator serviceOperations,
                                           ImageStreamOperator imagesStreamOperations,
                                           BuildConfigOperator buildConfigOperations) {
        super(vertx, isOpenShift, AssemblyType.CONNECT_S2I, configMapOperations);
        this.serviceOperations = serviceOperations;
        this.deploymentConfigOperations = deploymentConfigOperations;
        this.imagesStreamOperations = imagesStreamOperations;
        this.buildConfigOperations = buildConfigOperations;
    }

    @Override
    public void createOrUpdate(Reconciliation reconciliation, ConfigMap assemblyCm, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        if (isOpenShift) {
            KafkaConnectS2ICluster connect;
            try {
                connect = KafkaConnectS2ICluster.fromConfigMap(assemblyCm);
            } catch (Exception e) {
                handler.handle(Future.failedFuture(e));
                return;
            }
            Future<Void> chainFuture = Future.future();

            deploymentConfigOperations.scaleDown(namespace, connect.getName(), connect.getReplicas())
                    .compose(scale -> serviceOperations.reconcile(namespace, connect.getName(), connect.generateService()))
                    .compose(i -> deploymentConfigOperations.reconcile(namespace, connect.getName(), connect.generateDeploymentConfig()))
                    .compose(i -> imagesStreamOperations.reconcile(namespace, connect.getSourceImageStreamName(), connect.generateSourceImageStream()))
                    .compose(i -> imagesStreamOperations.reconcile(namespace, connect.getName(), connect.generateTargetImageStream()))
                    .compose(i -> buildConfigOperations.reconcile(namespace, connect.getName(), connect.generateBuildConfig()))
                    .compose(i -> deploymentConfigOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()).map((Void) null))
                    .compose(chainFuture::complete, chainFuture);
            chainFuture.setHandler(handler);
        } else {
            handler.handle(Future.failedFuture("S2I only available on OpenShift"));
        }
    }

    @Override
    protected void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        if (isOpenShift) {
            String namespace = reconciliation.namespace();
            String assemblyName = reconciliation.assemblyName();
            String clusterName = KafkaConnectS2ICluster.kafkaConnectClusterName(assemblyName);
            CompositeFuture.join(serviceOperations.reconcile(namespace, clusterName, null),
                deploymentConfigOperations.reconcile(namespace, clusterName, null),
                imagesStreamOperations.reconcile(namespace, KafkaConnectS2ICluster.getSourceImageStreamName(clusterName), null),
                imagesStreamOperations.reconcile(namespace, clusterName, null),
                buildConfigOperations.reconcile(namespace, clusterName, null))
            .map((Void) null).setHandler(handler);
        } else {
            handler.handle(Future.failedFuture("S2I only available on OpenShift"));
        }
    }

    @Override
    protected List<HasMetadata> getResources(String namespace) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(serviceOperations.list(namespace, Labels.forType(AssemblyType.CONNECT_S2I)));
        result.addAll(deploymentConfigOperations.list(namespace, Labels.forType(AssemblyType.CONNECT_S2I)));
        result.addAll(imagesStreamOperations.list(namespace, Labels.forType(AssemblyType.CONNECT_S2I)));
        result.addAll(buildConfigOperations.list(namespace, Labels.forType(AssemblyType.CONNECT_S2I)));
        return result;
    }

}
