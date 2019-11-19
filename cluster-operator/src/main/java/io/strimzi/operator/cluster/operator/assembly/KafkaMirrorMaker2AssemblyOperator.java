/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker2;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnectorSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

/**
 * <p>Assembly operator for a "Kafka Mirror Maker 2" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 *     <li>A set of Mirror Maker 2 connectors</li>
 * </ul>
 */
public class KafkaMirrorMaker2AssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List, DoneableKafkaMirrorMaker2, Resource<KafkaMirrorMaker2, DoneableKafkaMirrorMaker2>> {

    private static final Logger log = LogManager.getLogger(KafkaMirrorMaker2AssemblyOperator.class.getName());
    private final DeploymentOperator deploymentOperations;
    private final KafkaVersion.Lookup versions;

    public static final String MIRRORMAKER2_SOURCE_CONNECTOR_SUFFIX = ".MirrorSourceConnector";
    public static final String MIRRORMAKER2_CHECKPOINT_CONNECTOR_SUFFIX = ".MirrorCheckpointConnector";
    public static final String MIRRORMAKER2_HEARTBEAT_CONNECTOR_SUFFIX = ".MirrorHeartbeatConnector";
    public static final Map<String, Function<KafkaMirrorMaker2MirrorSpec, KafkaConnectorSpec>> MIRRORMAKER2_CONNECTORS = new HashMap<>();

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaMirrorMaker2AssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config) {
        this(vertx, pfa, supplier, config, connect -> new KafkaConnectApiImpl(vertx));
    }

    public KafkaMirrorMaker2AssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider) {
        super(vertx, pfa, KafkaMirrorMaker2.RESOURCE_KIND, supplier.mirrorMaker2Operator, supplier, config, connectClientProvider);
        this.deploymentOperations = supplier.deploymentOperations;
        this.versions = config.versions();

        MIRRORMAKER2_CONNECTORS.put(MIRRORMAKER2_SOURCE_CONNECTOR_SUFFIX, mirror -> mirror.getSourceConnector());
        MIRRORMAKER2_CONNECTORS.put(MIRRORMAKER2_CHECKPOINT_CONNECTOR_SUFFIX, mirror -> mirror.getCheckpointConnector());
        MIRRORMAKER2_CONNECTORS.put(MIRRORMAKER2_HEARTBEAT_CONNECTOR_SUFFIX, mirror -> mirror.getHeartbeatConnector());
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        Promise<Void> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster;
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();
        try {
            if (kafkaMirrorMaker2.getSpec() == null) {
                log.error("{}: Resource lacks spec property", reconciliation, kafkaMirrorMaker2.getMetadata().getName());
                throw new InvalidResourceException("spec property is required");
            }
            mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(kafkaMirrorMaker2, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaMirrorMaker2, kafkaMirrorMaker2Status, Future.failedFuture(e));
            return this.maybeUpdateStatusCommon(resourceOperator, kafkaMirrorMaker2, reconciliation, kafkaMirrorMaker2Status,
                (mirrormaker2, status) -> {
                    return new KafkaMirrorMaker2Builder(mirrormaker2).withStatus(status).build();
                });
        }

        ConfigMap logAndMetricsConfigMap = mirrorMaker2Cluster.generateMetricsAndLogConfigMap(mirrorMaker2Cluster.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) mirrorMaker2Cluster.getLogging()).getName()) :
                null);

        Map<String, String> annotations = new HashMap<>();
        annotations.put(ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(mirrorMaker2Cluster.ANCILLARY_CM_KEY_LOG_CONFIG));

        log.debug("{}: Updating Kafka Mirror Maker 2 cluster", reconciliation, name, namespace);
        Promise<Void> chainPromise = Promise.promise();
        mirrorMaker2ServiceAccount(namespace, mirrorMaker2Cluster)
                .compose(i -> deploymentOperations.scaleDown(namespace, mirrorMaker2Cluster.getName(), mirrorMaker2Cluster.getReplicas()))
                .compose(scale -> serviceOperations.reconcile(namespace, mirrorMaker2Cluster.getServiceName(), mirrorMaker2Cluster.generateService()))
                .compose(i -> configMapOperations.reconcile(namespace, mirrorMaker2Cluster.getAncillaryConfigName(), logAndMetricsConfigMap))
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, mirrorMaker2Cluster.getName(), mirrorMaker2Cluster.generatePodDisruptionBudget()))
                .compose(i -> deploymentOperations.reconcile(namespace, mirrorMaker2Cluster.getName(), mirrorMaker2Cluster.generateDeployment(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                .compose(i -> deploymentOperations.scaleUp(namespace, mirrorMaker2Cluster.getName(), mirrorMaker2Cluster.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(namespace, mirrorMaker2Cluster.getName(), 1_000, operationTimeoutMs))
                .compose(i -> deploymentOperations.readiness(namespace, mirrorMaker2Cluster.getName(), 1_000, operationTimeoutMs))
                .compose(i -> reconcileConnectors(reconciliation, kafkaMirrorMaker2))
                .compose(i -> {
                    chainPromise.complete();
                    return chainPromise.future();
                })
                .setHandler(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaMirrorMaker2, kafkaMirrorMaker2Status, reconciliationResult);
                    kafkaMirrorMaker2Status.setUrl(KafkaMirrorMaker2Resources.url(mirrorMaker2Cluster.getCluster(), namespace, KafkaMirrorMaker2Cluster.REST_API_PORT));

                    this.maybeUpdateStatusCommon(resourceOperator, kafkaMirrorMaker2, reconciliation, kafkaMirrorMaker2Status,
                        (mirrormaker2, status) -> new KafkaMirrorMaker2Builder(mirrormaker2).withStatus(status).build()).setHandler(statusResult -> {
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

    private Future<ReconcileResult<ServiceAccount>> mirrorMaker2ServiceAccount(String namespace, KafkaMirrorMaker2Cluster mirrorMaker2Cluster) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaMirrorMaker2Resources.serviceAccountName(mirrorMaker2Cluster.getCluster()),
                mirrorMaker2Cluster.generateServiceAccount());
    }

    @Override
    protected String getServiceName(String name) {
        return KafkaMirrorMaker2Resources.serviceName(name);
    }

    /**
     * Reconcile all the mirror maker 2 connectors selected by the given mirror maker 2 instance.
     * @param reconciliation The reconciliation
     * @param connect The mirror maker 2
     * @return A future, failed if any of the connectors' could not be reconciled.
     */
    @Override
    protected Future<Void> reconcileConnectors(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        String mirrorMaker2Name = kafkaMirrorMaker2.getMetadata().getName();
        if (kafkaMirrorMaker2.getSpec() == null) {
            return maybeUpdateMirrorMaker2Status(reconciliation, kafkaMirrorMaker2,
                    new InvalidResourceException("spec property is required"));
        }
        List<KafkaMirrorMaker2MirrorSpec> mirrors = Optional.ofNullable(kafkaMirrorMaker2.getSpec().getMirrors())
                .orElse(Collections.emptyList());
        String host = getServiceName(mirrorMaker2Name);
        KafkaConnectApi apiClient = getKafkaConnectApi();
        return apiClient.list(host, KafkaConnectCluster.REST_API_PORT).compose(deleteMirrorMaker2ConnectorNames -> {

            MIRRORMAKER2_CONNECTORS.entrySet().stream()
                    .forEach(connectorEntry -> deleteMirrorMaker2ConnectorNames.removeAll(mirrors.stream()
                            .filter(mirror -> connectorEntry.getValue().apply(mirror) != null) // filter out non-existent connectors
                            .map(mirror -> mirror.getSourceCluster() + "->" + mirror.getTargetCluster() + connectorEntry.getKey())
                            .collect(Collectors.toSet())));
            log.debug("{}: {}} cluster: delete mirror maker 2 connectors: {}", reconciliation, kind(), deleteMirrorMaker2ConnectorNames);
            Stream<Future<Void>> deletionFutures = deleteMirrorMaker2ConnectorNames.stream()
                    .map(connectorName -> apiClient.delete(host, KafkaConnectCluster.REST_API_PORT, connectorName));
            Stream<Future<Void>> createUpdateFutures = mirrors.stream()
                    .map(mirror -> reconcileMirrorMaker2Connectors(reconciliation, host, apiClient, kafkaMirrorMaker2, mirror));
            return CompositeFuture.join(Stream.concat(deletionFutures, createUpdateFutures).collect(Collectors.toList())).map((Void) null);
        });
    }

    private Future<Void> reconcileMirrorMaker2Connectors(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, KafkaMirrorMaker2 mirrorMaker2, KafkaMirrorMaker2MirrorSpec mirror) {
        String targetClusterAlias = mirror.getTargetCluster();
        String sourceClusterAlias = mirror.getSourceCluster();
        if (targetClusterAlias == null) {
            return maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2,
                    new InvalidResourceException("targetCluster property is required"));
        } else if (sourceClusterAlias == null) {
            return maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2,
                    new InvalidResourceException("sourceCluster property is required"));
        }
        List<KafkaMirrorMaker2ClusterSpec> clusters = Optional.ofNullable(mirrorMaker2.getSpec().getClusters())
            .orElse(Collections.emptyList());
        Map<String, KafkaMirrorMaker2ClusterSpec> clusterMap = clusters.stream()
            .filter(cluster -> targetClusterAlias.equals(cluster.getAlias()) || sourceClusterAlias.equals(cluster.getAlias()))
            .collect(Collectors.toMap(KafkaMirrorMaker2ClusterSpec::getAlias, Function.identity()));

        if (!clusterMap.containsKey(targetClusterAlias)) {
            return maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2,
                    new InvalidResourceException("targetCluster with alias " + mirror.getTargetCluster() + " cannot be found in the list of clusters"));
        } else if (!clusterMap.containsKey(sourceClusterAlias)) {
            return maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2,
                    new InvalidResourceException("sourceCluster with alias " + mirror.getSourceCluster() + " cannot be found in the list of clusters"));
        }
        
        return CompositeFuture.join(MIRRORMAKER2_CONNECTORS.entrySet().stream()
                    .filter(entry -> entry.getValue().apply(mirror) != null) // filter out non-existent connectors
                    .map(entry -> {
                        String connectorName = sourceClusterAlias + "->" + targetClusterAlias + entry.getKey();
                        KafkaConnectorSpec connectorSpec = prepareMirrorMaker2Connector(mirror, clusterMap.get(sourceClusterAlias), clusterMap.get(targetClusterAlias), entry.getValue().apply(mirror));
                        log.debug("{}: {}} cluster: creating/updating connector {} config: {}", reconciliation, kind(), connectorName, asJson(connectorSpec).toString());
                        return apiClient.createOrUpdatePutRequest(host, KafkaConnectCluster.REST_API_PORT, connectorName, asJson(connectorSpec));
                    })                            
                    .collect(Collectors.toList()))
                    .map((Void) null);
    }

    private static KafkaConnectorSpec prepareMirrorMaker2Connector(KafkaMirrorMaker2MirrorSpec mirror, KafkaMirrorMaker2ClusterSpec sourceCluster, KafkaMirrorMaker2ClusterSpec targetCluster, KafkaConnectorSpec connectorSpec) {
        Map<String, Object> config = connectorSpec.getConfig();
        config.put("target.cluster.alias", mirror.getTargetCluster());
        config.put("target.cluster.bootstrap.servers", targetCluster.getBootstrapServers());
        config.putAll(targetCluster.getConfig());
        config.putAll(targetCluster.getAdditionalProperties());
        config.put("source.cluster.alias", mirror.getSourceCluster());
        config.put("source.cluster.bootstrap.servers", sourceCluster.getBootstrapServers());
        config.putAll(sourceCluster.getConfig());
        config.putAll(sourceCluster.getAdditionalProperties());

        config.put("topics", mirror.getTopics());
        config.put("groups", mirror.getGroups());
        config.putAll(mirror.getAdditionalProperties());
        return connectorSpec;
    }

    private Future<Void> maybeUpdateMirrorMaker2Status(Reconciliation reconciliation, KafkaMirrorMaker2 mirrorMaker2, Throwable error) {
        KafkaMirrorMaker2Status status = new KafkaMirrorMaker2Status();
        if (error != null) {
            log.warn("{}: Error reconciling mirror maker 2 {}", reconciliation, mirrorMaker2.getMetadata().getName(), error);
        }
        StatusUtils.setStatusConditionAndObservedGeneration(mirrorMaker2, status, error != null ? Future.failedFuture(error) : Future.succeededFuture());
        return maybeUpdateStatusCommon(resourceOperator, mirrorMaker2, reconciliation, status,
            (mirror1, status2) -> {
                return new KafkaMirrorMaker2Builder(mirror1).withStatus(status2).build();
            });
    }

}
