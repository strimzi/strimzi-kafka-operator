/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
public class KafkaConnectAssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>, KafkaConnectStatus> {

    private static final Logger log = LogManager.getLogger(KafkaConnectAssemblyOperator.class.getName());
    private final DeploymentOperator deploymentOperations;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final KafkaVersion.Lookup versions;
    private final CrdOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I> connectS2IOperations;

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
        this(vertx, pfa, supplier, config, connectClientProvider, KafkaConnectCluster.REST_API_PORT);
    }
    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider, int port) {
        super(vertx, pfa, KafkaConnect.RESOURCE_KIND, supplier.connectOperator, supplier, config, connectClientProvider, port);
        this.deploymentOperations = supplier.deploymentOperations;
        this.connectS2IOperations = supplier.connectS2IOperator;
        this.networkPolicyOperator = supplier.networkPolicyOperator;

        this.versions = config.versions();
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnect) {
        Promise<Void> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();
        KafkaConnectCluster connect;
        KafkaConnectStatus kafkaConnectStatus = new KafkaConnectStatus();
        try {
            if (kafkaConnect.getSpec() == null) {
                log.error("{}: Resource {} lacks spec property", reconciliation, kafkaConnect.getMetadata().getName());
                throw new InvalidResourceException("spec property is required");
            }
            connect = KafkaConnectCluster.fromCrd(kafkaConnect, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, Future.failedFuture(e));
            return this.maybeUpdateStatusCommon(resourceOperator, kafkaConnect, reconciliation,
                    kafkaConnectStatus,
                (connect1, status) ->
                    new KafkaConnectBuilder(connect1).withStatus(status).build()
                );
        }

        ConfigMap logAndMetricsConfigMap = connect.generateMetricsAndLogConfigMap(connect.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) connect.getLogging()).getName()) :
                null);

        Map<String, String> annotations = new HashMap<>(1);
        annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH,
                Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(logAndMetricsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG))));

        String desiredLogging = logAndMetricsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);

        log.debug("{}: Updating Kafka Connect cluster", reconciliation);

        Future<KafkaConnectS2I> connectS2ICheck;
        if (connectS2IOperations != null)   {
            connectS2ICheck = connectS2IOperations.getAsync(kafkaConnect.getMetadata().getNamespace(), kafkaConnect.getMetadata().getName());
        } else {
            connectS2ICheck = Future.succeededFuture(null);
        }

        boolean connectHasZeroReplicas = connect.getReplicas() == 0;

        connectS2ICheck
                .compose(otherConnect -> {
                    if (otherConnect != null
                            // There is a KafkaConnectS2I with the same name which is older than this KafkaConnect
                            && kafkaConnect.getMetadata().getCreationTimestamp().compareTo(otherConnect.getMetadata().getCreationTimestamp()) > 0)    {
                        return Future.failedFuture("Both KafkaConnect and KafkaConnectS2I exist with the same name. " +
                                "KafkaConnectS2I is older and will be used while this custom resource will be ignored.");
                    } else {
                        return Future.succeededFuture();
                    }
                })
                .compose(i -> connectServiceAccount(namespace, connect))
                .compose(i -> networkPolicyOperator.reconcile(namespace, connect.getName(), connect.generateNetworkPolicy(pfa.isNamespaceAndPodSelectorNetworkPolicySupported(), isUseResources(kafkaConnect))))
                .compose(i -> deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas()))
                .compose(scale -> serviceOperations.reconcile(namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> configMapOperations.reconcile(namespace, connect.getAncillaryConfigMapName(), logAndMetricsConfigMap))
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, connect.getName(), connect.generatePodDisruptionBudget()))
                .compose(i -> deploymentOperations.reconcile(namespace, connect.getName(), connect.generateDeployment(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                .compose(i -> deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> connectHasZeroReplicas ? Future.succeededFuture() : deploymentOperations.readiness(namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> reconcileConnectors(reconciliation, kafkaConnect, kafkaConnectStatus, connectHasZeroReplicas, desiredLogging))
                .onComplete(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, reconciliationResult);

                    if (!connectHasZeroReplicas) {
                        kafkaConnectStatus.setUrl(KafkaConnectResources.url(connect.getCluster(), namespace, KafkaConnectCluster.REST_API_PORT));
                    }

                    kafkaConnectStatus.setReplicas(connect.getReplicas());
                    kafkaConnectStatus.setLabelSelector(connect.getSelectorLabels().toSelectorString());

                    this.maybeUpdateStatusCommon(resourceOperator, kafkaConnect, reconciliation, kafkaConnectStatus,
                        (connect1, status) -> new KafkaConnectBuilder(connect1).withStatus(status).build()).onComplete(statusResult -> {
                            // If both features succeeded, createOrUpdate succeeded as well
                            // If one or both of them failed, we prefer the reconciliation failure as the main error
                            if (reconciliationResult.succeeded() && statusResult.succeeded()) {
                                createOrUpdatePromise.complete();
                            } else if (reconciliationResult.failed()) {
                                createOrUpdatePromise.fail(reconciliationResult.cause());
                            } else {
                                createOrUpdatePromise.fail(statusResult.cause());
                            }
                        });
                });

        return createOrUpdatePromise.future();
    }

    private Future<ReconcileResult<ServiceAccount>> connectServiceAccount(String namespace, KafkaConnectCluster connect) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaConnectResources.serviceAccountName(connect.getCluster()),
                connect.generateServiceAccount());
    }
}
