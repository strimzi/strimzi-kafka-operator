/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> {

    private static final Logger log = LogManager.getLogger(KafkaConnectAssemblyOperator.class.getName());
    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "/logging";
    private final DeploymentOperator deploymentOperations;
    private final KafkaVersion.Lookup versions;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config) {
        this(vertx, pfa, supplier, config, connect -> new KafkaConnectApiImpl(vertx));
    }

    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider) {
        super(vertx, pfa, KafkaConnect.RESOURCE_KIND, supplier.connectOperator, supplier, config, connectClientProvider);
        this.deploymentOperations = supplier.deploymentOperations;
        this.versions = config.versions();
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnect) {
        Future<Void> createOrUpdateFuture = Future.future();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        KafkaConnectCluster connect;
        KafkaConnectStatus kafkaConnectStatus = new KafkaConnectStatus();
        try {
            if (kafkaConnect.getSpec() == null) {
                log.error("{}: Resource lacks spec property", reconciliation, kafkaConnect.getMetadata().getName());
                throw new InvalidResourceException("spec property is required");
            }
            connect = KafkaConnectCluster.fromCrd(kafkaConnect, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, Future.failedFuture(e));
            return this.maybeUpdateStatusCommon((CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect>) resourceOperator, kafkaConnect, reconciliation,
                    KafkaConnect::getStatus, kafkaConnectStatus,
                (connect1, status) -> {
                    return new KafkaConnectBuilder(connect1).withStatus(status).build();
                });
        }

        ConfigMap logAndMetricsConfigMap = connect.generateMetricsAndLogConfigMap(connect.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) connect.getLogging()).getName()) :
                null);

        Map<String, String> annotations = new HashMap<>();
        annotations.put(ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(connect.ANCILLARY_CM_KEY_LOG_CONFIG));

        log.debug("{}: Updating Kafka Connect cluster", reconciliation, name, namespace);
        Future<Void> chainFuture = Future.future();
        connectServiceAccount(namespace, connect)
                .compose(i -> deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas()))
                .compose(scale -> serviceOperations.reconcile(namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> configMapOperations.reconcile(namespace, connect.getAncillaryConfigName(), logAndMetricsConfigMap))
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, connect.getName(), connect.generatePodDisruptionBudget()))
                .compose(i -> deploymentOperations.reconcile(namespace, connect.getName(), connect.generateDeployment(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                .compose(i -> deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> deploymentOperations.readiness(namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> reconcileConnectors(reconciliation, kafkaConnect))
                .compose(i -> chainFuture.complete(), chainFuture)
                .setHandler(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, reconciliationResult);
                    kafkaConnectStatus.setUrl(KafkaConnectResources.url(connect.getCluster(), namespace, KafkaConnectCluster.REST_API_PORT));

                    this.maybeUpdateStatusCommon(resourceOperator, kafkaConnect, reconciliation, KafkaConnect::getStatus, kafkaConnectStatus,
                        (connect1, status) -> new KafkaConnectBuilder(connect1).withStatus(status).build()).setHandler(statusResult -> {
                            // If both features succeeded, createOrUpdate succeeded as well
                            // If one or both of them failed, we prefer the reconciliation failure as the main error
                            if (reconciliationResult.succeeded() && statusResult.succeeded()) {
                                createOrUpdateFuture.complete();
                            } else if (reconciliationResult.failed()) {
                                createOrUpdateFuture.fail(reconciliationResult.cause());
                            } else {
                                createOrUpdateFuture.fail(statusResult.cause());
                            }
                        });
                });
        return createOrUpdateFuture;
    }

    private Future<ReconcileResult<ServiceAccount>> connectServiceAccount(String namespace, KafkaConnectCluster connect) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaConnectResources.serviceAccountName(connect.getCluster()),
                connect.generateServiceAccount());
    }
}
