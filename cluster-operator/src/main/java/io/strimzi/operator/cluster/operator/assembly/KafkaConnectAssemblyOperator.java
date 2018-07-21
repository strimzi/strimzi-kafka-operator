/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.DoneableKafkaConnectAssembly;
import io.strimzi.api.kafka.KafkaConnectAssemblyList;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.SecretOperator;
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
public class KafkaConnectAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaConnectAssembly, KafkaConnectAssemblyList, DoneableKafkaConnectAssembly, Resource<KafkaConnectAssembly, DoneableKafkaConnectAssembly>> {

    private static final Logger log = LogManager.getLogger(KafkaConnectAssemblyOperator.class.getName());
    private final ServiceOperator serviceOperations;
    private final DeploymentOperator deploymentOperations;
    private final ConfigMapOperator configMapOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param deploymentOperations For operating on Deployments
     * @param serviceOperations For operating on Services
     * @param secretOperations For operating on Secrets
     */
    public KafkaConnectAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                        CertManager certManager,
                                        CrdOperator<KubernetesClient, KafkaConnectAssembly, KafkaConnectAssemblyList, DoneableKafkaConnectAssembly> connectOperator,
                                        ConfigMapOperator configMapOperations,
                                        DeploymentOperator deploymentOperations,
                                        ServiceOperator serviceOperations,
                                        SecretOperator secretOperations) {
        super(vertx, isOpenShift, AssemblyType.CONNECT, certManager, connectOperator, secretOperations);
        this.configMapOperations = configMapOperations;
        this.serviceOperations = serviceOperations;
        this.deploymentOperations = deploymentOperations;
    }

    @Override
    protected void createOrUpdate(Reconciliation reconciliation, KafkaConnectAssembly kafkaConnectAssembly, List<Secret> assemblySecrets, Handler<AsyncResult<Void>> handler) {

        String namespace = reconciliation.namespace();
        String name = reconciliation.assemblyName();
        KafkaConnectCluster connect;
        try {
            connect = KafkaConnectCluster.fromCrd(kafkaConnectAssembly);
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
            return;
        }
        ConfigMap logAndMetricsConfigMap = connect.generateMetricsAndLogConfigMap(connect.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) connect.getLogging()).getName()) :
                null);
        log.debug("{}: Updating Kafka Connect cluster", reconciliation, name, namespace);
        Future<Void> chainFuture = Future.future();
        deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas())
                .compose(scale -> serviceOperations.reconcile(namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> configMapOperations.reconcile(namespace, connect.getAncillaryConfigName(), logAndMetricsConfigMap))
                .compose(i -> deploymentOperations.reconcile(namespace, connect.getName(), connect.generateDeployment()))
                .compose(i -> deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()).map((Void) null))
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(handler);
    }

    @Override
    protected void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String assemblyName = reconciliation.assemblyName();
        String clusterName = KafkaConnectCluster.kafkaConnectClusterName(assemblyName);

        CompositeFuture.join(serviceOperations.reconcile(namespace, KafkaConnectCluster.serviceName(assemblyName), null),
            configMapOperations.reconcile(namespace, KafkaConnectCluster.logAndMetricsConfigName(assemblyName), null),
            deploymentOperations.reconcile(namespace, clusterName, null))
            .map((Void) null).setHandler(handler);
    }

    @Override
    protected List<HasMetadata> getResources(String namespace, Labels selector) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(serviceOperations.list(namespace, selector));
        result.addAll(deploymentOperations.list(namespace, selector));
        result.addAll(resourceOperator.list(namespace, selector));
        return result;
    }
}
