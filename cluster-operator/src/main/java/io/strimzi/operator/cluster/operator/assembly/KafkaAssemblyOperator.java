/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaSpec;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.config.ConfigParameter;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Assembly operator for the Kafka custom resource. It manages the following components:
 *   - Kafka cluster
 *   - Entity operator
 *   - Cruise Control
 *   - Kafka Exporter
 */
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaList, Resource<Kafka>, KafkaSpec, KafkaStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAssemblyOperator.class.getName());

    private static final Properties PROPERTIES = new Properties();
    /**
     * Version of the Strimzi operator (based on project version from the pom.xml)
     */
    /* test */ static final String OPERATOR_VERSION;

    static {
        InputStream propertiesFile = KafkaAssemblyOperator.class.getResourceAsStream("/.properties");
        try {
            try {
                PROPERTIES.load(propertiesFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                propertiesFile.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        OPERATOR_VERSION = PROPERTIES.getProperty("version");
    }

    /**
     * Intentionally shadowing inherited field to make it in specific implementation dedicated for the Kafka assembly
     */
    private final KafkaAssemblyOperatorMetricsHolder metrics;

    /* test */ final ClusterOperatorConfig config;
    /* test */ final ResourceOperatorSupplier supplier;

    private final CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> nodePoolOperator;
    protected Clock clock;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param certManager Certificate manager
     * @param passwordGenerator Password generator
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                 CertManager certManager, PasswordGenerator passwordGenerator,
                                 ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
        super(vertx, pfa, Kafka.RESOURCE_KIND, certManager, passwordGenerator,
                supplier.kafkaOperator, supplier, config);
        this.config = config;
        this.supplier = supplier;

        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.kafkaOperator = supplier.kafkaOperator;
        this.nodePoolOperator = supplier.kafkaNodePoolOperator;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.metrics = new KafkaAssemblyOperatorMetricsHolder(Kafka.RESOURCE_KIND, config.getCustomResourceSelector(), supplier.metricsProvider);
        this.clock = Clock.systemUTC();
    }

    @Override
    @SuppressWarnings({"checkstyle:NPathComplexity"})
    public Future<KafkaStatus> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
        Promise<KafkaStatus> createOrUpdatePromise = Promise.promise();
        ReconciliationState reconcileState = createReconciliationState(reconciliation, kafkaAssembly);

        reconcile(reconcileState).onComplete(reconcileResult -> {
            KafkaStatus status = reconcileState.kafkaStatus;
            Condition condition;

            if (kafkaAssembly.getMetadata().getGeneration() != null)    {
                status.setObservedGeneration(kafkaAssembly.getMetadata().getGeneration());
            }

            // When some of the fields are not set in the new status (for example because the reconciliation failed),
            // but the existing resource has them set in its status, we copy them over.
            if (kafkaAssembly.getStatus() != null)  {
                if (status.getClusterId() == null
                        && kafkaAssembly.getStatus().getClusterId() != null)  {
                    // Copy the Cluster ID if needed
                    status.setClusterId(kafkaAssembly.getStatus().getClusterId());
                }

                if (kafkaAssembly.getStatus().getOperatorLastSuccessfulVersion() != null)  {
                    // Copy the last successful operator version if needed
                    status.setOperatorLastSuccessfulVersion(kafkaAssembly.getStatus().getOperatorLastSuccessfulVersion());
                }

                if (status.getKafkaVersion() == null
                        && kafkaAssembly.getStatus().getKafkaVersion() != null)  {
                    // Copy the Kafka version if needed
                    status.setKafkaVersion(kafkaAssembly.getStatus().getKafkaVersion());
                }

                if (status.getKafkaMetadataVersion() == null
                        && kafkaAssembly.getStatus().getKafkaMetadataVersion() != null)  {
                    // Copy the metadata version if needed
                    status.setKafkaMetadataVersion(kafkaAssembly.getStatus().getKafkaMetadataVersion());
                }

                if (status.getKafkaMetadataState() == null
                        && kafkaAssembly.getStatus().getKafkaMetadataState() != null)  {
                    // Copy the metadata state if needed
                    status.setKafkaMetadataState(kafkaAssembly.getStatus().getKafkaMetadataState());
                }

                if (status.getRegisteredNodeIds() == null
                        && kafkaAssembly.getStatus().getRegisteredNodeIds() != null)  {
                    // Copy the list of registered node IDs if needed
                    status.setRegisteredNodeIds(kafkaAssembly.getStatus().getRegisteredNodeIds());
                }

                if (status.getAutoRebalance() == null
                        && kafkaAssembly.getStatus().getAutoRebalance() != null
                        && reconcileState.isAutoRebalancingEnabled()) {
                    // Copy the auto-rebalance state if needed (i.e. reconciliation failed) but only if
                    // the auto-rebalance is enabled otherwise I could reset it to null if needed
                    status.setAutoRebalance(kafkaAssembly.getStatus().getAutoRebalance());
                }
            }

            if (reconcileResult.succeeded())    {
                condition = new ConditionBuilder()
                        .withLastTransitionTime(StatusUtils.iso8601(clock.instant()))
                        .withType("Ready")
                        .withStatus("True")
                        .build();

                // successful reconcile, write operator version to successful reconcile field
                status.setOperatorLastSuccessfulVersion(OPERATOR_VERSION);
                status.addCondition(condition);
                createOrUpdatePromise.complete(status);
            } else {
                condition = new ConditionBuilder()
                        .withLastTransitionTime(StatusUtils.iso8601(clock.instant()))
                        .withType("NotReady")
                        .withStatus("True")
                        .withReason(reconcileResult.cause().getClass().getSimpleName())
                        .withMessage(reconcileResult.cause().getMessage())
                        .build();

                status.addCondition(condition);
                createOrUpdatePromise.fail(new ReconciliationException(status, reconcileResult.cause()));
            }
        });

        return createOrUpdatePromise.future();
    }

    Future<Void> reconcile(ReconciliationState reconcileState)  {
        Promise<Void> chainPromise = Promise.promise();

        boolean nonMigratedCluster = ReconcilerUtils.nonMigratedCluster(reconcileState.kafkaAssembly);
        boolean kraftEnabled = ReconcilerUtils.kraftEnabled(reconcileState.kafkaAssembly);
        boolean nodePoolsEnabled = ReconcilerUtils.nodePoolsEnabled(reconcileState.kafkaAssembly);

        if (nonMigratedCluster || !kraftEnabled || !nodePoolsEnabled) {
            throw new InvalidConfigurationException("Strimzi " + OPERATOR_VERSION + " supports only KRaft-based Apache Kafka clusters. Please make sure your cluster is migrated to KRaft before using Strimzi " + OPERATOR_VERSION + ".");
        }

        reconcileState.initialStatus()
                // Preparation steps => prepare cluster descriptions, handle CA creation or changes
                .compose(state -> state.reconcileCas(clock))
                .compose(state -> state.emitCertificateSecretMetrics())
                .compose(state -> state.versionChange())

                // Run reconciliations of the different components
                .compose(state -> state.reconcileKafka(clock))
                .compose(state -> state.reconcileEntityOperator(clock))
                .compose(state -> state.reconcileCruiseControl(clock))
                .compose(state -> state.reconcileKafkaExporter(clock))
                .compose(state -> state.reconcileKafkaAutoRebalancing())

                // Finish the reconciliation
                .map((Void) null)
                .onComplete(chainPromise);

        return chainPromise.future();
    }

    ReconciliationState createReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
        return new ReconciliationState(reconciliation, kafkaAssembly);
    }

    /**
     * Hold the mutable state during a reconciliation
     */
    class ReconciliationState {
        private final String namespace;
        private final String name;
        private final Kafka kafkaAssembly;
        private final Reconciliation reconciliation;

        /* test */ KafkaVersionChange versionChange;

        /* test */ ClusterCa clusterCa;
        /* test */ ClientsCa clientsCa;

        // Needed by Cruise control to configure the cluster, its nodes and their storage and resource configuration
        private Set<NodeRef> kafkaBrokerNodes;
        private Map<String, Storage> kafkaBrokerStorage;
        private Map<String, ResourceRequirements> kafkaBrokerResources;
        // needed to take information for the auto-rebalancing on scaling via Cruise Control
        private Set<Integer> scalingDownBlockedNodes;

        /* test */ KafkaStatus kafkaStatus = new KafkaStatus();

        ReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            this.reconciliation = reconciliation;
            this.kafkaAssembly = kafkaAssembly;
            this.namespace = kafkaAssembly.getMetadata().getNamespace();
            this.name = kafkaAssembly.getMetadata().getName();
        }

        /**
         * Updates the Status field of the Kafka CR. It diffs the desired status against the current status and calls
         * the update only when there is any difference in non-timestamp fields.
         *
         * @param desiredStatus The KafkaStatus which should be set
         *
         * @return  Future which completes when the status subresource is updated
         */
        Future<Void> updateStatus(KafkaStatus desiredStatus) {
            Promise<Void> updateStatusPromise = Promise.promise();

            kafkaOperator.getAsync(namespace, name).onComplete(getRes -> {
                if (getRes.succeeded())    {
                    Kafka kafka = getRes.result();

                    if (kafka != null) {
                        if ((Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1ALPHA1).equals(kafka.getApiVersion()))   {
                            LOGGER.warnCr(reconciliation, "The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", kafka.getApiVersion());
                            updateStatusPromise.complete();
                        } else {
                            KafkaStatus currentStatus = kafka.getStatus();

                            StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                            if (!ksDiff.isEmpty()) {
                                Kafka resourceWithNewStatus = new KafkaBuilder(kafka).withStatus(desiredStatus).build();

                                kafkaOperator.updateStatusAsync(reconciliation, resourceWithNewStatus).onComplete(updateRes -> {
                                    if (updateRes.succeeded()) {
                                        LOGGER.debugCr(reconciliation, "Completed status update");
                                        updateStatusPromise.complete();
                                    } else {
                                        LOGGER.errorCr(reconciliation, "Failed to update status", updateRes.cause());
                                        updateStatusPromise.fail(updateRes.cause());
                                    }
                                });
                            } else {
                                LOGGER.debugCr(reconciliation, "Status did not change");
                                updateStatusPromise.complete();
                            }
                        }
                    } else {
                        LOGGER.errorCr(reconciliation, "Current Kafka resource not found");
                        updateStatusPromise.fail("Current Kafka resource not found");
                    }
                } else {
                    LOGGER.errorCr(reconciliation, "Failed to get the current Kafka resource and its status", getRes.cause());
                    updateStatusPromise.fail(getRes.cause());
                }
            });

            return updateStatusPromise.future();
        }

        /**
         * Sets the initial status when the Kafka resource is created and the cluster starts deploying.
         *
         * @return  Future which returns when the initial state is set
         */
        Future<ReconciliationState> initialStatus() {
            Promise<ReconciliationState> initialStatusPromise = Promise.promise();

            kafkaOperator.getAsync(namespace, name).onComplete(getRes -> {
                if (getRes.succeeded())    {
                    Kafka kafka = getRes.result();

                    if (kafka != null && kafka.getStatus() == null) {
                        LOGGER.debugCr(reconciliation, "Setting the initial status for a new resource");

                        Condition deployingCondition = new ConditionBuilder()
                                .withLastTransitionTime(StatusUtils.iso8601(clock.instant()))
                                .withType("NotReady")
                                .withStatus("True")
                                .withReason("Creating")
                                .withMessage("Kafka cluster is being deployed")
                                .build();

                        KafkaStatus initialStatus = new KafkaStatusBuilder()
                                .addToConditions(deployingCondition)
                                .build();

                        updateStatus(initialStatus).map(this).onComplete(initialStatusPromise);
                    } else {
                        LOGGER.debugCr(reconciliation, "Status is already set. No need to set initial status");
                        initialStatusPromise.complete(this);
                    }
                } else {
                    LOGGER.errorCr(reconciliation, "Failed to get the current Kafka resource and its status", getRes.cause());
                    initialStatusPromise.fail(getRes.cause());
                }
            });

            return initialStatusPromise.future();
        }

        private Storage getOldStorage(HasMetadata sts)  {
            Storage storage = null;

            if (sts != null)    {
                String jsonStorage = Annotations.stringAnnotation(sts, Annotations.ANNO_STRIMZI_IO_STORAGE, null);

                if (jsonStorage != null)    {
                    storage = ModelUtils.decodeStorageFromJson(jsonStorage);
                }
            }

            return storage;
        }

        /**
         * Provider method for CaReconciler. Overriding this method can be used to get mocked creator.
         *
         * @return  CaReconciler instance
         */
        CaReconciler caReconciler()   {
            return new CaReconciler(reconciliation, kafkaAssembly, config, supplier, vertx, certManager, passwordGenerator);
        }

        /**
         * Creates the CaReconciler instance and reconciles the Clients and Cluster CAs. The resulting CAs are stored
         * in the ReconciliationState and used later to reconcile the operands.
         *
         * @param clock     The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *                  That time is used for checking maintenance windows
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileCas(Clock clock)    {
            return caReconciler()
                    .reconcile(clock)
                    .compose(cas -> {
                        this.clusterCa = cas.clusterCa();
                        this.clientsCa = cas.clientsCa();
                        return Future.succeededFuture(this);
                    });
        }

        /**
         * Emits the certificate expiration metric for cluster CA and client CA
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> emitCertificateSecretMetrics() {
            long serverCertificateExpiration = this.clusterCa.getCertificateExpirationDateEpoch();
            metrics.clusterCaCertificateExpiration(this.name, this.namespace).set(serverCertificateExpiration);

            long clientCertificateExpiration = this.clientsCa.getCertificateExpirationDateEpoch();
            metrics.clientCaCertificateExpiration(this.name, this.namespace).set(clientCertificateExpiration);

            return Future.succeededFuture(this);
        }

        /**
         * Provider method for VersionChangeCreator. Overriding this method can be used to get mocked creator.
         *
         * @return  VersionChangeCreator instance
         */
        KRaftVersionChangeCreator versionChangeCreator()   {
            return new KRaftVersionChangeCreator(reconciliation, kafkaAssembly, config, supplier);
        }

        /**
         * Creates the KafkaVersionChange instance describing the version changes in this reconciliation.
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> versionChange()    {
            return versionChangeCreator()
                    .reconcile()
                    .compose(versionChange -> {
                        this.versionChange = versionChange;
                        return Future.succeededFuture(this);
                    });
        }

        /**
         * Provider method for Kafka reconciler. Overriding this method can be used to get mocked reconciler. This
         * method expects that the information about current storage and replicas are collected and passed as arguments.
         * Overriding this method can be used to get mocked reconciler.
         *
         * @param nodePools         List of node pools belonging to this cluster
         * @param kafkaCluster      The KafkaCluster model instance
         *
         * @return  KafkaReconciler instance
         */
        KafkaReconciler kafkaReconciler(List<KafkaNodePool> nodePools, KafkaCluster kafkaCluster) {
            return new KafkaReconciler(
                    reconciliation,
                    kafkaAssembly,
                    nodePools,
                    kafkaCluster,
                    clusterCa,
                    clientsCa,
                    config,
                    supplier,
                    pfa,
                    vertx
            );
        }

        /**
         * Provider method for Kafka reconciler. Overriding this method can be used to get mocked reconciler. This
         * method has to first collect some information about the current Kafka cluster such as current storage
         * configuration or current number of replicas.
         *
         * @return  Future with Kafka reconciler
         */
        Future<KafkaReconciler> kafkaReconciler()   {
            Labels kafkaSelectorLabels = Labels.EMPTY
                    .withStrimziKind(reconciliation.kind())
                    .withStrimziCluster(reconciliation.name())
                    .withStrimziName(KafkaResources.kafkaComponentName(reconciliation.name()));

            Future<List<KafkaNodePool>> nodePoolFuture;
            if (ReconcilerUtils.nodePoolsEnabled(kafkaAssembly)) {
                // Node Pools are enabled
                nodePoolFuture = nodePoolOperator.listAsync(namespace, Labels.fromMap(Map.of(Labels.STRIMZI_CLUSTER_LABEL, name)));
            } else {
                nodePoolFuture = Future.succeededFuture(null);
            }

            Future<List<StrimziPodSet>> podSetFuture = strimziPodSetOperator.listAsync(namespace, kafkaSelectorLabels);

            return Future.join(podSetFuture, nodePoolFuture)
                    .compose(res -> {
                        List<StrimziPodSet> podSets = res.resultAt(0);
                        List<KafkaNodePool> nodePools = res.resultAt(1);

                        if (ReconcilerUtils.nodePoolsEnabled(kafkaAssembly)
                                && (nodePools == null || nodePools.isEmpty())) {
                            throw new InvalidConfigurationException("KafkaNodePools are enabled, but no KafkaNodePools found for Kafka cluster " + name);
                        }

                        Map<String, Storage> oldStorage = new HashMap<>();

                        if (podSets != null) {
                            // One or more PodSets exist => we go on and use them
                            for (StrimziPodSet podSet : podSets) {
                                oldStorage.put(podSet.getMetadata().getName(), getOldStorage(podSet));
                            }
                        }

                        KafkaClusterCreator kafkaClusterCreator =
                                new KafkaClusterCreator(vertx, reconciliation, config, supplier);
                        return kafkaClusterCreator
                                .prepareKafkaCluster(kafkaAssembly, nodePools, oldStorage, versionChange, kafkaStatus, true)
                                .compose(kafkaCluster -> {
                                    // We store this for use with Cruise Control later. As these configurations might
                                    // not be exactly the same as in the original custom resource (for example because
                                    // of un-allowed storage changes being reverted) they are passed this way from the
                                    // KafkaCluster object and not from the custom resource.
                                    kafkaBrokerNodes = kafkaCluster.brokerNodes();
                                    kafkaBrokerStorage = kafkaCluster.getStorageByPoolName();
                                    kafkaBrokerResources = kafkaCluster.getBrokerResourceRequirementsByPoolName();
                                    scalingDownBlockedNodes = kafkaClusterCreator.scalingDownBlockedNodes();

                                    return Future.succeededFuture(kafkaReconciler(nodePools, kafkaCluster));
                                });
                    });
        }

        /**
         * Run the reconciliation pipeline for Kafka
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileKafka(Clock clock)    {
            return kafkaReconciler()
                    .compose(reconciler -> reconciler.reconcile(kafkaStatus, clock))
                    .map(this);
        }

        /**
         * Provider method for Kafka Exporter reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  Kafka Exporter reconciler
         */
        KafkaExporterReconciler kafkaExporterReconciler()   {
            return new KafkaExporterReconciler(
                    reconciliation,
                    config,
                    supplier,
                    kafkaAssembly,
                    versions,
                    clusterCa
            );
        }

        /**
         * Run the reconciliation pipeline for the Kafka Exporter
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileKafkaExporter(Clock clock)    {
            return kafkaExporterReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, clock)
                    .map(this);
        }

        /**
         * Provider method for Cruise Control reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  Cruise Control reconciler
         */
        CruiseControlReconciler cruiseControlReconciler()   {
            return new CruiseControlReconciler(
                    reconciliation,
                    config,
                    supplier,
                    passwordGenerator,
                    kafkaAssembly,
                    versions,
                    kafkaBrokerNodes,
                    kafkaBrokerStorage,
                    kafkaBrokerResources,
                    clusterCa
            );
        }

        /**
         * Run the reconciliation pipeline for the Cruise Control
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileCruiseControl(Clock clock)    {
            return cruiseControlReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, clock)
                    .map(this);
        }

        /**
         * Provider method for Entity Operator reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  Entity Operator reconciler
         */
        EntityOperatorReconciler entityOperatorReconciler()   {
            return new EntityOperatorReconciler(
                    reconciliation,
                    config,
                    supplier,
                    kafkaAssembly,
                    clusterCa
            );
        }

        /**
         * Provider method for the Kafka auto-rebalancing reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return Kafka auto-rebalancing reconciler
         */
        KafkaAutoRebalancingReconciler kafkaAutoRebalancingReconciler() {
            return new KafkaAutoRebalancingReconciler(reconciliation, kafkaAssembly, supplier, scalingDownBlockedNodes);
        }

        /**
         * Based on the autorebalance being enabled or not within Cruise Control configuration:
         * - run the reconciliation pipeline for Kafka auto-rebalancing ...
         * - ... create the auto-rebalance status on Idle if the cluster is still in the creation phase (no auto-rebalancing to run yet) or ...
         * - ... reset to null the auto-rebalance status because the auto-rebalance was disabled
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileKafkaAutoRebalancing() {
            if (isAutoRebalancingEnabled()) {
                return kafkaAutoRebalancingReconciler()
                        .reconcile(kafkaStatus)
                        .map(this);
            } else {
                LOGGER.debugCr(reconciliation, "Cruise Control or inner autorebalance field not defined in the Kafka custom resource, no auto-rebalancing to reconcile");
                // enforce no auto-rebalance status, if we are disabling Cruise Control and/or auto-rebalance from its configuration
                kafkaStatus.setAutoRebalance(null);
                return Future.succeededFuture(this);
            }
        }

        /**
         * Run the reconciliation pipeline for the Entity Operator
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileEntityOperator(Clock clock)    {
            return entityOperatorReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, clock)
                    .map(this);
        }

        /**
         * @return true if the autorebalance is enabled within the Cruise Control configuration, false otherwise
         */
        boolean isAutoRebalancingEnabled() {
            return kafkaAssembly.getSpec().getCruiseControl() != null && kafkaAssembly.getSpec().getCruiseControl().getAutoRebalance() != null;
        }
    }

    @Override
    protected KafkaStatus createStatus(Kafka kafka) {
        KafkaStatus status = new KafkaStatus();

        // We copy the cluster ID if set
        if (kafka.getStatus() != null && kafka.getStatus().getClusterId() != null)  {
            status.setClusterId(kafka.getStatus().getClusterId());
        }

        return status;
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     * @param reconciliation    The Reconciliation identification
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null)
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }

    /**
     * Remove the metrics specific to the kind implementing it.
     *
     * @param desiredNames  Set of resources which should be reconciled
     * @param namespace     The namespace to reconcile, or {@code *} to reconcile across all namespaces.
     */
    @Override
    public void removeMetrics(Set<NamespaceAndName> desiredNames, String namespace) {
        if (ConfigParameter.ANY_NAMESPACE.equals(namespace)) {
            metrics.removeMetricsForCertificates(key ->
                    // When watching all namespaces, we remove all metrics that do not belong to existing clusters
                    !desiredNames.contains(new NamespaceAndName(key.getNamespace(), key.getClusterName())));
        } else {
            metrics.removeMetricsForCertificates(key ->
                    // When watching only one namespace, we remove all metrics that belong to our namespace but not to an existing cluster
                    // We ignore the metrics from other namespaces
                    namespace.equals(key.getNamespace()) && !desiredNames.contains(new NamespaceAndName(key.getNamespace(), key.getClusterName())));
        }
    }

    /**
     * Create Kubernetes watch for KafkaNodePool resources.
     *
     * @param namespace     Namespace where to watch for the resources
     *
     * @return  A future which completes when the watcher has been created
     */
    public Future<ReconnectingWatcher<KafkaNodePool>> createNodePoolWatch(String namespace) {
        return VertxUtil.async(vertx, () -> new ReconnectingWatcher<>(nodePoolOperator, KafkaNodePool.RESOURCE_KIND, namespace, null, this::nodePoolEventHandler));
    }

    /**
     * Event handler called when the KafkaNodePool watch receives an event.
     *
     * @param action    An Action describing the type of the event
     * @param resource  The resource for which the event was triggered
     */
    /* test */ void nodePoolEventHandler(Watcher.Action action, KafkaNodePool resource) {
        String name = resource.getMetadata().getName();
        String namespace = resource.getMetadata().getNamespace();

        switch (action) {
            case ADDED, DELETED, MODIFIED -> maybeEnqueueReconciliation(action, resource);
            case ERROR -> {
                LOGGER.errorCr(new Reconciliation("watch", resource.getKind(), namespace, name), "Error action: {} {} in namespace {} ", resource.getKind(), namespace, name);
                reconcileAll("watch error", namespace, ignored -> {
                });
            }
            default -> {
                LOGGER.errorCr(new Reconciliation("watch", resource.getKind(), namespace, name), "Unknown action: {} in namespace {}", resource.getKind(), namespace, name);
                reconcileAll("watch unknown", namespace, ignored -> {
                });
            }
        }
    }

    /**
     * Checks the KafkaNodePool resource and decides if a reconciliation should be triggered. This decision is based on
     * whether there is a matching Kafka resource, if it matches the selector etc.
     *
     * @param action    Action describing the event
     * @param resource  KafkaNodePool resource to which the event happened
     */
    private void maybeEnqueueReconciliation(Watcher.Action action, KafkaNodePool resource) {
        if (resource.getMetadata().getLabels() != null
                && resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL) != null)    {
            String kafkaName = resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
            Kafka kafka = kafkaOperator.get(resource.getMetadata().getNamespace(), kafkaName);

            if (kafka != null
                    && Util.matchesSelector(selector(), kafka)) {
                if (ReconcilerUtils.nodePoolsEnabled(kafka)) {
                    Reconciliation reconciliation = new Reconciliation("watch", kind(), kafka.getMetadata().getNamespace(), kafkaName);
                    LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action);
                    enqueueReconciliation(reconciliation);
                } else {
                    LOGGER.warnOp("{} {} in namespace {} was {}, but the Kafka cluster {} to which it belongs does not have {} support enabled", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action, kafkaName, resource.getKind());
                }
            } else if (kafka == null) {
                LOGGER.warnOp("{} {} in namespace {} was {}, but the Kafka cluster {} to which it belongs does not exist", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action, kafkaName);
            } else {
                LOGGER.debugOp("{} {} in namespace {} was {}, but the Kafka cluster {} to which it belongs is not managed by this operator instance", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action, kafkaName);
            }
        } else {
            LOGGER.warnOp("{} {} in namespace {} was {}, but does not contain {} label", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace(), action, Labels.STRIMZI_CLUSTER_LABEL);
        }
    }

    /**
     * Utility method for enqueueing reconciliation from he KafkaNodePool event handler. A separate method is used to allow testing of the enqueue handler.
     *
     * @param reconciliation    Reconciliation marker
     */
    /* test */ void enqueueReconciliation(Reconciliation reconciliation) {
        reconcile(reconciliation);
    }
}
