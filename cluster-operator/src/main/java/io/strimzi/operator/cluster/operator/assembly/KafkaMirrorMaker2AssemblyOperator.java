/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.common.tracing.JaegerTracing;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.connector.AutoRestartStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnectorSpec;
import io.strimzi.api.kafka.model.connector.KafkaConnectorSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ConnectorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Spec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.AuthenticationUtils;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.VertxUtil;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_CONNECTOR;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_TASK;
import static java.util.Collections.emptyMap;

/**
 * <p>Assembly operator for a "Kafka MirrorMaker 2" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 *     <li>A set of MirrorMaker 2 connectors</li>
 * </ul>
 */
public class KafkaMirrorMaker2AssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>, KafkaMirrorMaker2Spec, KafkaMirrorMaker2Status> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaMirrorMaker2AssemblyOperator.class.getName());

    private static final String MIRRORMAKER2_CONNECTOR_PACKAGE = "org.apache.kafka.connect.mirror";
    private static final String MIRRORMAKER2_SOURCE_CONNECTOR_SUFFIX = ".MirrorSourceConnector";
    private static final String MIRRORMAKER2_CHECKPOINT_CONNECTOR_SUFFIX = ".MirrorCheckpointConnector";
    private static final String MIRRORMAKER2_HEARTBEAT_CONNECTOR_SUFFIX = ".MirrorHeartbeatConnector";
    private static final Map<String, Function<KafkaMirrorMaker2MirrorSpec, KafkaMirrorMaker2ConnectorSpec>> MIRRORMAKER2_CONNECTORS = new HashMap<>(3);

    static {
        MIRRORMAKER2_CONNECTORS.put(MIRRORMAKER2_SOURCE_CONNECTOR_SUFFIX, KafkaMirrorMaker2MirrorSpec::getSourceConnector);
        MIRRORMAKER2_CONNECTORS.put(MIRRORMAKER2_CHECKPOINT_CONNECTOR_SUFFIX, KafkaMirrorMaker2MirrorSpec::getCheckpointConnector);
        MIRRORMAKER2_CONNECTORS.put(MIRRORMAKER2_HEARTBEAT_CONNECTOR_SUFFIX, KafkaMirrorMaker2MirrorSpec::getHeartbeatConnector);
    }

    private static final String TARGET_CLUSTER_PREFIX = "target.cluster.";
    private static final String SOURCE_CLUSTER_PREFIX = "source.cluster.";

    private static final String STORE_LOCATION_ROOT = "/tmp/kafka/clusters/";
    private static final String TRUSTSTORE_SUFFIX = ".truststore.p12";
    private static final String KEYSTORE_SUFFIX = ".keystore.p12";
    private static final String CONNECT_CONFIG_FILE = "/tmp/strimzi-connect.properties";
    private static final String CONNECTORS_CONFIG_FILE = "/tmp/strimzi-mirrormaker2-connector.properties";

    /**
     * Constructor
     *
     * @param vertx     The Vertx instance
     * @param pfa       Platform features availability properties
     * @param supplier  Supplies the operators for different resources
     * @param config    ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaMirrorMaker2AssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config) {
        this(vertx, pfa, supplier, config, connect -> new KafkaConnectApiImpl(vertx));
    }

    /**
     * Constructor used for tests
     *
     * @param vertx                     The Vertx instance
     * @param pfa                       Platform features availability properties
     * @param supplier                  Supplies the operators for different resources
     * @param config                    ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     * @param connectClientProvider     Connect REST APi client provider
     */
    protected KafkaMirrorMaker2AssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider) {
        super(vertx, pfa, KafkaMirrorMaker2.RESOURCE_KIND, supplier.mirrorMaker2Operator, supplier, config, connectClientProvider, KafkaConnectCluster.REST_API_PORT);
    }

    @Override
    protected Future<KafkaMirrorMaker2Status> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster;
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();
        try {
            mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(reconciliation, kafkaMirrorMaker2, versions, sharedEnvironmentProvider);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaMirrorMaker2, kafkaMirrorMaker2Status, e);
            return Future.failedFuture(new ReconciliationException(kafkaMirrorMaker2Status, e));
        }

        Promise<KafkaMirrorMaker2Status> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();

        Map<String, String> podAnnotations = new HashMap<>(1);

        final AtomicReference<String> desiredLogging = new AtomicReference<>();
        final AtomicReference<Deployment> deployment = new AtomicReference<>();
        final AtomicReference<StrimziPodSet> podSet = new AtomicReference<>();

        boolean hasZeroReplicas = mirrorMaker2Cluster.getReplicas() == 0;
        String initCrbName = KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(kafkaMirrorMaker2.getMetadata().getName(), namespace);
        ClusterRoleBinding initCrb = mirrorMaker2Cluster.generateClusterRoleBinding();

        LOGGER.debugCr(reconciliation, "Updating Kafka MirrorMaker 2 cluster");

        controllerResources(reconciliation, mirrorMaker2Cluster, deployment, podSet)
                .compose(i -> connectServiceAccount(reconciliation, namespace, KafkaMirrorMaker2Resources.serviceAccountName(mirrorMaker2Cluster.getCluster()), mirrorMaker2Cluster))
                .compose(i -> connectInitClusterRoleBinding(reconciliation, initCrbName, initCrb))
                .compose(i -> connectNetworkPolicy(reconciliation, namespace, mirrorMaker2Cluster, true))
                .compose(i -> manualRollingUpdate(reconciliation, mirrorMaker2Cluster))
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, mirrorMaker2Cluster.getServiceName(), mirrorMaker2Cluster.generateService()))
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, mirrorMaker2Cluster.getComponentName(), mirrorMaker2Cluster.generateHeadlessService()))
                .compose(i -> generateMetricsAndLoggingConfigMap(reconciliation, namespace, mirrorMaker2Cluster))
                .compose(logAndMetricsConfigMap -> {
                    String logging = logAndMetricsConfigMap.getData().get(mirrorMaker2Cluster.logging().configMapKey());
                    podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging)));
                    desiredLogging.set(logging);
                    return configMapOperations.reconcile(reconciliation, namespace, logAndMetricsConfigMap.getMetadata().getName(), logAndMetricsConfigMap);
                })
                .compose(i -> ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperations, mirrorMaker2Cluster))
                .compose(i -> podDisruptionBudgetOperator.reconcile(reconciliation, namespace, mirrorMaker2Cluster.getComponentName(), mirrorMaker2Cluster.generatePodDisruptionBudget()))
                .compose(i -> generateAuthHash(namespace, kafkaMirrorMaker2.getSpec()))
                .compose(hash -> {
                    podAnnotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString(hash));
                    return Future.succeededFuture();
                })
                .compose(i -> new KafkaConnectMigration(
                                reconciliation,
                                mirrorMaker2Cluster,
                                null,
                                podAnnotations,
                                operationTimeoutMs,
                                pfa.isOpenshift(),
                                imagePullPolicy,
                                imagePullSecrets,
                                null,
                                deploymentOperations,
                                podSetOperations,
                                podOperations
                        )
                        .migrateFromDeploymentToStrimziPodSets(deployment.get(), podSet.get()))
                .compose(i -> reconcilePodSet(reconciliation, mirrorMaker2Cluster, podAnnotations))
                .compose(i -> hasZeroReplicas ? Future.succeededFuture() : reconcileConnectors(reconciliation, kafkaMirrorMaker2, mirrorMaker2Cluster, kafkaMirrorMaker2Status, desiredLogging.get()))
                .map((Void) null)
                .onComplete(reconciliationResult -> {
                    List<Condition> conditions = kafkaMirrorMaker2Status.getConditions();
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaMirrorMaker2, kafkaMirrorMaker2Status, reconciliationResult.cause());

                    if (!hasZeroReplicas) {
                        kafkaMirrorMaker2Status.setUrl(KafkaMirrorMaker2Resources.url(mirrorMaker2Cluster.getCluster(), namespace, KafkaMirrorMaker2Cluster.REST_API_PORT));
                    }
                    if (conditions != null && !conditions.isEmpty()) {
                        kafkaMirrorMaker2Status.addConditions(conditions);
                    }
                    kafkaMirrorMaker2Status.setReplicas(mirrorMaker2Cluster.getReplicas());
                    kafkaMirrorMaker2Status.setLabelSelector(mirrorMaker2Cluster.getSelectorLabels().toSelectorString());

                    if (reconciliationResult.succeeded())   {
                        createOrUpdatePromise.complete(kafkaMirrorMaker2Status);
                    } else {
                        createOrUpdatePromise.fail(new ReconciliationException(kafkaMirrorMaker2Status, reconciliationResult.cause()));
                    }
                });
        return createOrUpdatePromise.future();
    }

    private Future<Void> controllerResources(Reconciliation reconciliation,
                                             KafkaMirrorMaker2Cluster mirrorMaker2Cluster,
                                             AtomicReference<Deployment> deploymentReference,
                                             AtomicReference<StrimziPodSet> podSetReference)   {
        return Future
                .join(deploymentOperations.getAsync(reconciliation.namespace(), mirrorMaker2Cluster.getComponentName()), podSetOperations.getAsync(reconciliation.namespace(), mirrorMaker2Cluster.getComponentName()))
                .compose(res -> {
                    deploymentReference.set(res.resultAt(0));
                    podSetReference.set(res.resultAt(1));

                    return Future.succeededFuture();
                });
    }

    private Future<Void> reconcilePodSet(Reconciliation reconciliation,
                                         KafkaConnectCluster connect,
                                         Map<String, String> podAnnotations)  {
        return podSetOperations.reconcile(reconciliation, reconciliation.namespace(), connect.getComponentName(), connect.generatePodSet(connect.getReplicas(), null, podAnnotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, null))
                .compose(reconciliationResult -> {
                    KafkaConnectRoller roller = new KafkaConnectRoller(reconciliation, connect, operationTimeoutMs, podOperations);
                    return roller.maybeRoll(PodSetUtils.podNames(reconciliationResult.resource()), pod -> KafkaConnectRoller.needsRollingRestart(reconciliationResult.resource(), pod));
                })
                .compose(i -> podSetOperations.readiness(reconciliation, reconciliation.namespace(), connect.getComponentName(), 1_000, operationTimeoutMs));
    }

    @Override
    protected KafkaMirrorMaker2Status createStatus(KafkaMirrorMaker2 ignored) {
        return new KafkaMirrorMaker2Status();
    }

    /**
     * Generates a hash from the trusted TLS certificates that can be used to spot if it has changed.
     *
     * @param namespace               Namespace of the MirrorMaker2 cluster
     * @param kafkaMirrorMaker2Spec   KafkaMirrorMaker2Spec object
     * @return                        Future for tracking the asynchronous result of generating the TLS auth hash
     */
    Future<Integer> generateAuthHash(String namespace, KafkaMirrorMaker2Spec kafkaMirrorMaker2Spec) {
        Promise<Integer> authHash = Promise.promise();
        if (kafkaMirrorMaker2Spec.getClusters() == null) {
            authHash.complete(0);
        } else {
            Future.join(kafkaMirrorMaker2Spec.getClusters()
                            .stream()
                            .map(cluster -> {
                                List<CertSecretSource> trustedCertificates = cluster.getTls() == null ? Collections.emptyList() : cluster.getTls().getTrustedCertificates();
                                return VertxUtil.authTlsHash(secretOperations, namespace, cluster.getAuthentication(), trustedCertificates);
                            }).collect(Collectors.toList())
                    )
                    .onSuccess(hashes -> {
                        int hash = hashes.<Integer>list()
                            .stream()
                            .mapToInt(i -> i)
                            .sum();
                        authHash.complete(hash);
                    }).onFailure(authHash::fail);
        }
        return authHash.future();
    }

    /**
     * Reconcile all the MirrorMaker 2 connectors selected by the given MirrorMaker 2 instance.
     * @param reconciliation The reconciliation
     * @param kafkaMirrorMaker2 The MirrorMaker 2
     * @return A future, failed if any of the connectors could not be reconciled.
     */
    protected Future<Void> reconcileConnectors(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaMirrorMaker2Cluster mirrorMaker2Cluster, KafkaMirrorMaker2Status mirrorMaker2Status, String desiredLogging) {
        String mirrorMaker2Name = kafkaMirrorMaker2.getMetadata().getName();
        if (kafkaMirrorMaker2.getSpec() == null) {
            return maybeUpdateMirrorMaker2Status(reconciliation, kafkaMirrorMaker2,
                    new InvalidResourceException("spec property is required"));
        }
        List<KafkaMirrorMaker2MirrorSpec> mirrors = ModelUtils.asListOrEmptyList(kafkaMirrorMaker2.getSpec().getMirrors());
        String host = KafkaMirrorMaker2Resources.qualifiedServiceName(mirrorMaker2Name, reconciliation.namespace());
        KafkaConnectApi apiClient = getKafkaConnectApi();
        return apiClient.list(reconciliation, host, KafkaConnectCluster.REST_API_PORT).compose(deleteMirrorMaker2ConnectorNames -> {

            for (Map.Entry<String, Function<KafkaMirrorMaker2MirrorSpec, KafkaMirrorMaker2ConnectorSpec>> connectorEntry : MIRRORMAKER2_CONNECTORS.entrySet()) {
                deleteMirrorMaker2ConnectorNames.removeAll(mirrors.stream()
                        .filter(mirror -> connectorEntry.getValue().apply(mirror) != null) // filter out non-existent connectors
                        .map(mirror -> mirror.getSourceCluster() + "->" + mirror.getTargetCluster() + connectorEntry.getKey())
                        .collect(Collectors.toSet()));
            }
            LOGGER.debugCr(reconciliation, "delete MirrorMaker 2 connectors: {}", deleteMirrorMaker2ConnectorNames);
            Stream<Future<Void>> deletionFutures = deleteMirrorMaker2ConnectorNames.stream()
                    .map(connectorName -> apiClient.delete(reconciliation, host, KafkaConnectCluster.REST_API_PORT, connectorName));
            Stream<Future<Void>> createUpdateFutures = mirrors.stream()
                    .map(mirror -> reconcileMirrorMaker2Connectors(reconciliation, host, apiClient, kafkaMirrorMaker2, mirror, mirrorMaker2Cluster, mirrorMaker2Status, desiredLogging));
            return Future.join(Stream.concat(deletionFutures, createUpdateFutures).collect(Collectors.toList())).map((Void) null);
        });
    }

    private Future<Void> reconcileMirrorMaker2Connectors(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, KafkaMirrorMaker2 mirrorMaker2, KafkaMirrorMaker2MirrorSpec mirror, KafkaMirrorMaker2Cluster mirrorMaker2Cluster, KafkaMirrorMaker2Status mirrorMaker2Status, String desiredLogging) {
        String targetClusterAlias = mirror.getTargetCluster();
        String sourceClusterAlias = mirror.getSourceCluster();
        if (targetClusterAlias == null) {
            return maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2,
                    new InvalidResourceException("targetCluster property is required"));
        } else if (sourceClusterAlias == null) {
            return maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2,
                    new InvalidResourceException("sourceCluster property is required"));
        }
        List<KafkaMirrorMaker2ClusterSpec> clusters = ModelUtils.asListOrEmptyList(mirrorMaker2.getSpec().getClusters());
        Map<String, KafkaMirrorMaker2ClusterSpec> clusterMap = clusters.stream()
            .filter(cluster -> targetClusterAlias.equals(cluster.getAlias()) || sourceClusterAlias.equals(cluster.getAlias()))
            .collect(Collectors.toMap(KafkaMirrorMaker2ClusterSpec::getAlias, Function.identity()));

        if (!clusterMap.containsKey(targetClusterAlias)) {
            return maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2,
                    new InvalidResourceException("targetCluster with alias " + mirror.getTargetCluster() + " cannot be found in the list of clusters at spec.clusters"));
        } else if (!clusterMap.containsKey(sourceClusterAlias)) {
            return maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2,
                    new InvalidResourceException("sourceCluster with alias " + mirror.getSourceCluster() + " cannot be found in the list of clusters at spec.clusters"));
        }

        return Future.join(MIRRORMAKER2_CONNECTORS.entrySet().stream()
                    .filter(entry -> entry.getValue().apply(mirror) != null) // filter out non-existent connectors
                    .map(entry -> {
                        String connectorName = sourceClusterAlias + "->" + targetClusterAlias + entry.getKey();
                        String className = MIRRORMAKER2_CONNECTOR_PACKAGE + entry.getKey();

                        KafkaMirrorMaker2ConnectorSpec mm2ConnectorSpec = entry.getValue().apply(mirror);
                        @SuppressWarnings("deprecation")
                        KafkaConnectorSpec connectorSpec = new KafkaConnectorSpecBuilder()
                                .withClassName(className)
                                .withConfig(mm2ConnectorSpec.getConfig())
                                .withPause(mm2ConnectorSpec.getPause())
                                .withState(mm2ConnectorSpec.getState())
                                .withTasksMax(mm2ConnectorSpec.getTasksMax())
                                .build();

                        prepareMirrorMaker2ConnectorConfig(reconciliation, mirror, clusterMap.get(sourceClusterAlias), clusterMap.get(targetClusterAlias), connectorSpec, mirrorMaker2Cluster);
                        LOGGER.debugCr(reconciliation, "creating/updating connector {} config: {}", connectorName, connectorSpec.getConfig());
                        return reconcileMirrorMaker2Connector(reconciliation, mirrorMaker2, apiClient, host, connectorName, connectorSpec, mirrorMaker2Status);
                    })
                    .collect(Collectors.toList()))
                    .map((Void) null)
                    .compose(i -> apiClient.updateConnectLoggers(reconciliation, host, KafkaConnectCluster.REST_API_PORT, desiredLogging, mirrorMaker2Cluster.defaultLogConfig()))
                    .compose(i -> {
                        boolean failedConnector = mirrorMaker2Status.getConnectors().stream()
                                .anyMatch(connector -> {
                                    @SuppressWarnings({ "rawtypes" })
                                    Object state = ((Map) connector.getOrDefault("connector", emptyMap())).get("state");
                                    return "FAILED".equalsIgnoreCase(state.toString());
                                });
                        if (failedConnector) {
                            return Future.failedFuture("One or more connectors are in FAILED state");
                        } else {
                            return Future.succeededFuture();
                        }
                    })
                    .map((Void) null);
    }

    @SuppressWarnings("deprecation")
    private static void prepareMirrorMaker2ConnectorConfig(Reconciliation reconciliation, KafkaMirrorMaker2MirrorSpec mirror, KafkaMirrorMaker2ClusterSpec sourceCluster, KafkaMirrorMaker2ClusterSpec targetCluster, KafkaConnectorSpec connectorSpec, KafkaMirrorMaker2Cluster mirrorMaker2Cluster) {
        Map<String, Object> config = connectorSpec.getConfig();
        addClusterToMirrorMaker2ConnectorConfig(config, targetCluster, TARGET_CLUSTER_PREFIX);
        addClusterToMirrorMaker2ConnectorConfig(config, sourceCluster, SOURCE_CLUSTER_PREFIX);

        if (mirror.getTopicsPattern() != null) {
            config.put("topics", mirror.getTopicsPattern());
        }

        String topicsExcludePattern = mirror.getTopicsExcludePattern();
        String topicsBlacklistPattern = mirror.getTopicsBlacklistPattern();
        if (topicsExcludePattern != null && topicsBlacklistPattern != null) {
            LOGGER.warnCr(reconciliation, "Both topicsExcludePattern and topicsBlacklistPattern mirror properties are present, ignoring topicsBlacklistPattern as it is deprecated");
        }
        String topicsExclude = topicsExcludePattern != null ? topicsExcludePattern : topicsBlacklistPattern;
        if (topicsExclude != null) {
            config.put("topics.exclude", topicsExclude);
        }

        if (mirror.getGroupsPattern() != null) {
            config.put("groups", mirror.getGroupsPattern());
        }

        String groupsExcludePattern = mirror.getGroupsExcludePattern();
        String groupsBlacklistPattern = mirror.getGroupsBlacklistPattern();
        if (groupsExcludePattern != null && groupsBlacklistPattern != null) {
            LOGGER.warnCr(reconciliation, "Both groupsExcludePattern and groupsBlacklistPattern mirror properties are present, ignoring groupsBlacklistPattern as it is deprecated");
        }
        String groupsExclude = groupsExcludePattern != null ? groupsExcludePattern :  groupsBlacklistPattern;
        if (groupsExclude != null) {
            config.put("groups.exclude", groupsExclude);
        }

        if (mirrorMaker2Cluster.getTracing() != null)   {
            if (JaegerTracing.TYPE_JAEGER.equals(mirrorMaker2Cluster.getTracing().getType())) {
                LOGGER.warnCr(reconciliation, "Tracing type \"{}\" is not supported anymore and will be ignored", JaegerTracing.TYPE_JAEGER);
            } else if (OpenTelemetryTracing.TYPE_OPENTELEMETRY.equals(mirrorMaker2Cluster.getTracing().getType())) {
                config.put("consumer.interceptor.classes", OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME);
                config.put("producer.interceptor.classes", OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME);
            }
        }

        // setting client.rack here because the consumer is created by the connector
        String clientRackKey = "consumer.client.rack";
        config.put(clientRackKey, "${file:" + CONNECT_CONFIG_FILE + ":" + clientRackKey + "}");

        config.putAll(mirror.getAdditionalProperties());
    }

    /* test */ static void addClusterToMirrorMaker2ConnectorConfig(Map<String, Object> config, KafkaMirrorMaker2ClusterSpec cluster, String configPrefix) {
        config.put(configPrefix + "alias", cluster.getAlias());
        config.put(configPrefix + AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());

        String securityProtocol = addTLSConfigToMirrorMaker2ConnectorConfig(config, cluster, configPrefix);

        if (cluster.getAuthentication() != null) {
            if (cluster.getAuthentication() instanceof KafkaClientAuthenticationTls) {
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, STORE_LOCATION_ROOT + cluster.getAlias() + KEYSTORE_SUFFIX);
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "${file:" + CONNECTORS_CONFIG_FILE + ":" + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG + "}");
            } else if (cluster.getAuthentication() instanceof KafkaClientAuthenticationPlain plainAuthentication) {
                securityProtocol = cluster.getTls() != null ? "SASL_SSL" : "SASL_PLAINTEXT";
                config.put(configPrefix + SaslConfigs.SASL_MECHANISM, "PLAIN");
                config.put(configPrefix + SaslConfigs.SASL_JAAS_CONFIG,
                        AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.plain.PlainLoginModule",
                                Map.of("username", plainAuthentication.getUsername(),
                                        "password", "${file:" + CONNECTORS_CONFIG_FILE + ":" + cluster.getAlias() + ".sasl.password}")));
            } else if (cluster.getAuthentication() instanceof KafkaClientAuthenticationScram scramAuthentication) {
                securityProtocol = cluster.getTls() != null ? "SASL_SSL" : "SASL_PLAINTEXT";
                config.put(configPrefix + SaslConfigs.SASL_MECHANISM, scramAuthentication instanceof KafkaClientAuthenticationScramSha256 ? "SCRAM-SHA-256" : "SCRAM-SHA-512");
                config.put(configPrefix + SaslConfigs.SASL_JAAS_CONFIG,
                        AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.scram.ScramLoginModule",
                                Map.of("username", scramAuthentication.getUsername(),
                                        "password", "${file:" + CONNECTORS_CONFIG_FILE + ":" + cluster.getAlias() + ".sasl.password}")));
            } else if (cluster.getAuthentication() instanceof KafkaClientAuthenticationOAuth oauthAuthentication) {
                securityProtocol = cluster.getTls() != null ? "SASL_SSL" : "SASL_PLAINTEXT";
                config.put(configPrefix + SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
                config.put(configPrefix + SaslConfigs.SASL_JAAS_CONFIG,
                        oauthJaasConfig(cluster, oauthAuthentication));
                config.put(configPrefix + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
            }
        }

        if (securityProtocol != null) {
            config.put(configPrefix + AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }

        config.putAll(cluster.getConfig().entrySet().stream()
                .collect(Collectors.toMap(entry -> configPrefix + entry.getKey(), Map.Entry::getValue)));
        config.putAll(cluster.getAdditionalProperties());
    }

    private static String oauthJaasConfig(KafkaMirrorMaker2ClusterSpec cluster,
                                          KafkaClientAuthenticationOAuth oauth) {

        var oauthConfig = cluster.getAuthentication() instanceof KafkaClientAuthenticationOAuth ? AuthenticationUtils.oauthJaasOptions((KafkaClientAuthenticationOAuth) cluster.getAuthentication()) : Map.<String, String>of();
        Map<String, String> jaasOptions = new HashMap<>(oauthConfig);
        if (oauth.getClientSecret() != null) {
            jaasOptions.put("oauth.client.secret", "${file:" + CONNECTORS_CONFIG_FILE + ":" + cluster.getAlias() + ".oauth.client.secret}");
        }
        if (oauth.getAccessToken() != null) {
            jaasOptions.put("oauth.access.token", "${file:" + CONNECTORS_CONFIG_FILE + ":" + cluster.getAlias() + ".oauth.access.token}");
        }
        if (oauth.getRefreshToken() != null) {
            jaasOptions.put("oauth.refresh.token", "${file:" + CONNECTORS_CONFIG_FILE + ":" + cluster.getAlias() + ".oauth.refresh.token}");
        }
        if (oauth.getPasswordSecret() != null) {
            jaasOptions.put("oauth.password.grant.password", "${file:" + CONNECTORS_CONFIG_FILE + ":" + cluster.getAlias() + ".oauth.password.grant.password}");
        }
        if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
            jaasOptions.put("oauth.ssl.truststore.location", "/tmp/kafka/clusters/\"" + cluster.getAlias() + "\"-oauth.truststore.p12");
            jaasOptions.put("oauth.ssl.truststore.password", "\"${file:" + CONNECTORS_CONFIG_FILE + ":oauth.ssl.truststore.password}\"");
            jaasOptions.put("oauth.ssl.truststore.type", "PKCS12");
        }
        return AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", jaasOptions);
    }


    private static String addTLSConfigToMirrorMaker2ConnectorConfig(Map<String, Object> config, KafkaMirrorMaker2ClusterSpec cluster, String configPrefix) {
        String securityProtocol = null;
        if (cluster.getTls() != null) {
            securityProtocol = "SSL";
            if (cluster.getTls().getTrustedCertificates() != null && !cluster.getTls().getTrustedCertificates().isEmpty()) {
                config.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
                config.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, STORE_LOCATION_ROOT + cluster.getAlias() + TRUSTSTORE_SUFFIX);
                config.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "${file:" + CONNECTORS_CONFIG_FILE + ":" + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG + "}");
            }
        }
        return securityProtocol;
    }

    private Future<Void> reconcileMirrorMaker2Connector(Reconciliation reconciliation, KafkaMirrorMaker2 mirrorMaker2, KafkaConnectApi apiClient, String host, String connectorName, KafkaConnectorSpec connectorSpec, KafkaMirrorMaker2Status mirrorMaker2Status) {
        return maybeCreateOrUpdateConnector(reconciliation, host, apiClient, connectorName, connectorSpec, mirrorMaker2)
                .onComplete(result -> {
                    if (result.succeeded()) {
                        mirrorMaker2Status.addConditions(result.result().conditions);
                        mirrorMaker2Status.getConnectors().add(result.result().statusResult);
                        mirrorMaker2Status.getConnectors().sort(new ConnectorsComparatorByName());
                        var autoRestart = result.result().autoRestart;
                        if (autoRestart != null) {
                            autoRestart.setConnectorName(connectorName);
                            mirrorMaker2Status.getAutoRestartStatuses().add(autoRestart);
                            mirrorMaker2Status.getAutoRestartStatuses().sort(Comparator.comparing(AutoRestartStatus::getConnectorName));
                        }
                    } else {
                        maybeUpdateMirrorMaker2Status(reconciliation, mirrorMaker2, result.cause());
                    }
                }).compose(ignored -> Future.succeededFuture());
    }

    private Future<Void> maybeUpdateMirrorMaker2Status(Reconciliation reconciliation, KafkaMirrorMaker2 mirrorMaker2, Throwable error) {
        KafkaMirrorMaker2Status status = new KafkaMirrorMaker2Status();
        if (error != null) {
            LOGGER.warnCr(reconciliation, "Error reconciling MirrorMaker 2 {}", mirrorMaker2.getMetadata().getName(), error);
        }
        StatusUtils.setStatusConditionAndObservedGeneration(mirrorMaker2, status, error);
        return maybeUpdateStatusCommon(resourceOperator, mirrorMaker2, reconciliation, status,
            (mirror1, status2) -> new KafkaMirrorMaker2Builder(mirror1).withStatus(status2).build());
    }

    /**
     * This comparator compares two maps where connectors' configurations are stored.
     * The comparison is done by using only one property - 'name'
     */
    static class ConnectorsComparatorByName implements Comparator<Map<String, Object>>, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Map<String, Object> m1, Map<String, Object> m2) {
            String name1 = m1.get("name") == null ? "" : m1.get("name").toString();
            String name2 = m2.get("name") == null ? "" : m2.get("name").toString();
            return name1.compareTo(name2);
        }
    }


    /**
     * Whether the provided resource has the strimzi.io/restart-connector annotation, and it's value matches the supplied connectorName
     *
     * @param resource resource instance to check
     * @param connectorName connectorName name of the MM2 connector to check
     * @return true if the provided resource instance has the strimzi.io/restart-connector annotation; false otherwise
     */
    @Override
    @SuppressWarnings({ "rawtypes" })
    protected boolean hasRestartAnnotation(CustomResource resource, String connectorName) {
        String restartAnnotationConnectorName = Annotations.stringAnnotation(resource, ANNO_STRIMZI_IO_RESTART_CONNECTOR, null);
        return connectorName.equals(restartAnnotationConnectorName);
    }

    /**
     * Return the ID of the connector task to be restarted if the provided resource instance has the strimzi.io/restart-connector-task annotation
     *
     * @param resource resource instance to check
     * @param connectorName connectorName name of the MM2 connector to check
     * @return the ID of the task to be restarted if the provided KafkaConnector resource instance has the strimzi.io/restart-connector-task annotation or -1 otherwise.
     */
    @SuppressWarnings({ "rawtypes" })
    protected int getRestartTaskAnnotationTaskID(CustomResource resource, String connectorName) {
        int taskID = -1;
        String connectorTask = Annotations.stringAnnotation(resource, ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK, "").trim();
        Matcher connectorTaskMatcher = ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN.matcher(connectorTask);
        if (connectorTaskMatcher.matches() && connectorTaskMatcher.group(ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_CONNECTOR).equals(connectorName)) {
            taskID = Integer.parseInt(connectorTaskMatcher.group(ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_TASK));
        }
        return taskID;
    }

    /**
     * Patches the KafkaMirrorMaker2 CR to remove the strimzi.io/restart-connector annotation, as
     * the restart action specified by user has been completed.
     */
    @Override
    @SuppressWarnings({ "rawtypes" })
    protected Future<Void> removeRestartAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaMirrorMaker2) resource, ANNO_STRIMZI_IO_RESTART_CONNECTOR);
    }


    /**
     * Patches the KafkaMirrorMaker2 CR to remove the strimzi.io/restart-connector-task annotation, as
     * the restart action specified by user has been completed.
     */
    @Override
    @SuppressWarnings({ "rawtypes" })
    protected Future<Void> removeRestartTaskAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaMirrorMaker2) resource, ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK);
    }

    /**
     * Patches the KafkaMirrorMaker2 CR to remove the supplied annotation
     */
    protected Future<Void> removeAnnotation(Reconciliation reconciliation, KafkaMirrorMaker2 resource, String annotationKey) {
        LOGGER.debugCr(reconciliation, "Removing annotation {}", annotationKey);
        KafkaMirrorMaker2 patchedKafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(resource)
            .editMetadata()
            .removeFromAnnotations(annotationKey)
            .endMetadata()
            .build();
        return resourceOperator.patchAsync(reconciliation, patchedKafkaMirrorMaker2)
            .compose(ignored -> Future.succeededFuture());
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     * @param reconciliation    The Reconciliation identification
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null)
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }
}
