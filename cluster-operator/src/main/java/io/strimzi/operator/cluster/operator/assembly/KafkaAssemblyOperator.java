/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KRaftUtils;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.Date;
import java.util.function.Supplier;

import static io.strimzi.operator.cluster.model.AbstractModel.ANNO_STRIMZI_IO_STORAGE;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaList, Resource<Kafka>, KafkaSpec, KafkaStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAssemblyOperator.class.getName());

    /* test */ final ClusterOperatorConfig config;
    /* test */ final ResourceOperatorSupplier supplier;

    private final FeatureGates featureGates;

    private final StatefulSetOperator stsOperations;
    private final CrdOperator<KubernetesClient, Kafka, KafkaList> crdOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final KubernetesRestartEventPublisher eventsPublisher;

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
        this.featureGates = config.featureGates();
        this.stsOperations = supplier.stsOperations;
        this.crdOperator = supplier.kafkaOperator;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.eventsPublisher = supplier.restartEventsPublisher;
    }

    @Override
    public Future<KafkaStatus> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
        Promise<KafkaStatus> createOrUpdatePromise = Promise.promise();
        ReconciliationState reconcileState = createReconciliationState(reconciliation, kafkaAssembly);

        reconcile(reconcileState).onComplete(reconcileResult -> {
            KafkaStatus status = reconcileState.kafkaStatus;
            Condition condition;

            if (kafkaAssembly.getMetadata().getGeneration() != null)    {
                status.setObservedGeneration(kafkaAssembly.getMetadata().getGeneration());
            }

            if (reconcileResult.succeeded())    {
                condition = new ConditionBuilder()
                        .withLastTransitionTime(StatusUtils.iso8601(dateSupplier()))
                        .withType("Ready")
                        .withStatus("True")
                        .build();

                status.addCondition(condition);
                createOrUpdatePromise.complete(status);
            } else {
                condition = new ConditionBuilder()
                        .withLastTransitionTime(StatusUtils.iso8601(dateSupplier()))
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

        // Validates features which are currently not supported in KRaft mode
        if (featureGates.useKRaftEnabled()) {
            try {
                KRaftUtils.validateKafkaCrForKRaft(reconcileState.kafkaAssembly.getSpec());
            } catch (InvalidResourceException e)    {
                return Future.failedFuture(e);
            }
        }

        reconcileState.initialStatus()
                // Preparation steps => prepare cluster descriptions, handle CA creation or changes
                .compose(state -> state.reconcileCas(this::dateSupplier))
                .compose(state -> state.versionChange())

                // Run reconciliations of the different components
                .compose(state -> featureGates.useKRaftEnabled() ? Future.succeededFuture(state) : state.reconcileZooKeeper(this::dateSupplier))
                .compose(state -> state.reconcileKafka(this::dateSupplier))
                .compose(state -> state.reconcileEntityOperator(this::dateSupplier))
                .compose(state -> state.reconcileCruiseControl(this::dateSupplier))
                .compose(state -> state.reconcileKafkaExporter(this::dateSupplier))
                .compose(state -> state.reconcileJmxTrans())

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

        // Needed by Cruise Control => due to illegal changes to the storage configuration, this cannot be just taken
        // from the Kafka CR and needs to be passed form the KafkaCluster model.
        private Storage kafkaStorage;

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

            crdOperator.getAsync(namespace, name).onComplete(getRes -> {
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

                                crdOperator.updateStatusAsync(reconciliation, resourceWithNewStatus).onComplete(updateRes -> {
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

            crdOperator.getAsync(namespace, name).onComplete(getRes -> {
                if (getRes.succeeded())    {
                    Kafka kafka = getRes.result();

                    if (kafka != null && kafka.getStatus() == null) {
                        LOGGER.debugCr(reconciliation, "Setting the initial status for a new resource");

                        Condition deployingCondition = new ConditionBuilder()
                                .withLastTransitionTime(StatusUtils.iso8601(dateSupplier()))
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
                String jsonStorage = Annotations.stringAnnotation(sts, ANNO_STRIMZI_IO_STORAGE, null);

                if (jsonStorage != null)    {
                    storage = ModelUtils.decodeStorageFromJson(jsonStorage);
                }
            }

            return storage;
        }

        /**
         * Utility method to extract current number of replicas from an existing StatefulSet
         *
         * @param sts   StatefulSet from which the replicas count should be extracted
         *
         * @return      Number of replicas
         */
        private int currentReplicas(StatefulSet sts)  {
            if (sts != null && sts.getSpec() != null)   {
                return sts.getSpec().getReplicas();
            } else {
                return 0;
            }
        }

        /**
         * Utility method to extract current number of replicas from an existing StrimziPodSet
         *
         * @param podSet    PodSet from which the replicas count should be extracted
         *
         * @return          Number of replicas
         */
        private int currentReplicas(StrimziPodSet podSet)  {
            if (podSet != null && podSet.getSpec() != null && podSet.getSpec().getPods() != null)   {
                return podSet.getSpec().getPods().size();
            } else {
                return 0;
            }
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
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileCas(Supplier<Date> dateSupplier)    {
            return caReconciler()
                    .reconcile(dateSupplier)
                    .compose(cas -> {
                        this.clusterCa = cas.clusterCa;
                        this.clientsCa = cas.clientsCa;
                        return Future.succeededFuture(this);
                    });
        }

        /**
         * Provider method for VersionChangeCreator. Overriding this method can be used to get mocked creator.
         *
         * @return  VersionChangeCreator instance
         */
        VersionChangeCreator versionChangeCreator()   {
            return new VersionChangeCreator(reconciliation, kafkaAssembly, config, supplier);
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
         * Provider method for ZooKeeper reconciler. Overriding this method can be used to get mocked reconciler. This
         * method has to first collect some information about the current ZooKeeper cluster such as current storage
         * configuration or current number of replicas.
         *
         * @return  Future with ZooKeeper reconciler
         */
        Future<ZooKeeperReconciler> zooKeeperReconciler()   {
            Future<StatefulSet> stsFuture = stsOperations.getAsync(namespace, KafkaResources.zookeeperStatefulSetName(name));
            Future<StrimziPodSet> podSetFuture = strimziPodSetOperator.getAsync(namespace, KafkaResources.zookeeperStatefulSetName(name));

            return CompositeFuture.join(stsFuture, podSetFuture)
                    .compose(res -> {
                        StatefulSet sts = res.resultAt(0);
                        StrimziPodSet podSet = res.resultAt(1);

                        int currentReplicas = 0;
                        Storage oldStorage = null;

                        if (sts != null && podSet != null)  {
                            // Both StatefulSet and PodSet exist => we create the description based on the feature gate
                            if (featureGates.useStrimziPodSetsEnabled())    {
                                oldStorage = getOldStorage(podSet);
                                currentReplicas = currentReplicas(podSet);
                            } else {
                                oldStorage = getOldStorage(sts);
                                currentReplicas = currentReplicas(sts);
                            }
                        } else if (sts != null) {
                            // StatefulSet exists, PodSet does not exist => we create the description from the StatefulSet
                            oldStorage = getOldStorage(sts);
                            currentReplicas = currentReplicas(sts);
                        } else if (podSet != null) {
                            //PodSet exists, StatefulSet does not => we create the description from the PodSet
                            oldStorage = getOldStorage(podSet);
                            currentReplicas = currentReplicas(podSet);
                        }

                        ZooKeeperReconciler reconciler = new ZooKeeperReconciler(
                                reconciliation,
                                vertx,
                                config,
                                supplier,
                                pfa,
                                kafkaAssembly,
                                versionChange,
                                oldStorage,
                                currentReplicas,
                                clusterCa
                        );

                        return Future.succeededFuture(reconciler);
                    });
        }

        /**
         * Run the reconciliation pipeline for the ZooKeeper
         *
         * @param   dateSupplier  Date supplier used to check maintenance windows
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileZooKeeper(Supplier<Date> dateSupplier)    {
            return zooKeeperReconciler()
                    .compose(reconciler -> reconciler.reconcile(kafkaStatus, dateSupplier))
                    .map(this);
        }

        /**
         * Provider method for Kafka reconciler. Overriding this method can be used to get mocked reconciler. This
         * method expects that the information about current storage and replicas are collected and passed as arguments.
         * Overriding this method can be used to get mocked reconciler.
         *
         * @param oldStorage        Current storage configuration of the running cluster. 0 if the cluster is not running yet.
         * @param currentReplicas   Current number of replicas in the Kafka cluster. Null if it is not running yet.
         *
         * @return  KafkaReconciler instance
         */
        KafkaReconciler kafkaReconciler(Storage oldStorage, int currentReplicas) {
            return new KafkaReconciler(
                    reconciliation,
                    kafkaAssembly, oldStorage, currentReplicas, clusterCa, clientsCa, versionChange, config, supplier, pfa, vertx
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
            Future<StatefulSet> stsFuture = stsOperations.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name));
            Future<StrimziPodSet> podSetFuture = strimziPodSetOperator.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name));

            return CompositeFuture.join(stsFuture, podSetFuture)
                    .compose(res -> {
                        StatefulSet sts = res.resultAt(0);
                        StrimziPodSet podSet = res.resultAt(1);

                        int currentReplicas = 0;
                        Storage oldStorage = null;

                        if (sts != null && podSet != null)  {
                            // Both StatefulSet and PodSet exist => we create the description based on the feature gate
                            if (featureGates.useStrimziPodSetsEnabled())    {
                                oldStorage = getOldStorage(podSet);
                                currentReplicas = currentReplicas(podSet);
                            } else {
                                oldStorage = getOldStorage(sts);
                                currentReplicas = currentReplicas(sts);
                            }
                        } else if (sts != null) {
                            // StatefulSet exists, PodSet does not exist => we create the description from the StatefulSet
                            oldStorage = getOldStorage(sts);
                            currentReplicas = currentReplicas(sts);
                        } else if (podSet != null) {
                            //PodSet exists, StatefulSet does not => we create the description from the PodSet
                            oldStorage = getOldStorage(podSet);
                            currentReplicas = currentReplicas(podSet);
                        }

                        KafkaReconciler reconciler = kafkaReconciler(oldStorage, currentReplicas);

                        // We store this for use with Cruise Control later
                        kafkaStorage = reconciler.kafkaStorage();

                        return Future.succeededFuture(reconciler);
                    });
        }

        /**
         * Run the reconciliation pipeline for the ZooKeeper
         *
         * @param   dateSupplier  Date supplier used to check maintenance windows
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileKafka(Supplier<Date> dateSupplier)    {
            return kafkaReconciler()
                    .compose(reconciler -> reconciler.reconcile(kafkaStatus, dateSupplier))
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
         * @param dateSupplier  Date supplier used to check maintenance windows
         *
         * @return              Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileKafkaExporter(Supplier<Date> dateSupplier)    {
            return kafkaExporterReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, dateSupplier)
                    .map(this);
        }

        /**
         * Provider method for JMX Trans reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  JMX Trans reconciler
         */
        JmxTransReconciler jmxTransReconciler()   {
            return new JmxTransReconciler(
                    reconciliation,
                    config,
                    supplier,
                    kafkaAssembly
            );
        }

        /**
         * Run the reconciliation pipeline for the JMX Trans
         *
         * @return              Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileJmxTrans()    {
            return jmxTransReconciler()
                    .reconcile(imagePullPolicy, imagePullSecrets)
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
                    kafkaAssembly,
                    versions,
                    kafkaStorage,
                    clusterCa
            );
        }

        /**
         * Run the reconciliation pipeline for the Cruise Control
         *
         * @param dateSupplier  Date supplier used to check maintenance windows
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileCruiseControl(Supplier<Date> dateSupplier)    {
            return cruiseControlReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, dateSupplier)
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
                    versions,
                    clusterCa
            );
        }

        /**
         * Run the reconciliation pipeline for the Entity Operator
         *
         * @param dateSupplier  Date supplier used to check maintenance windows
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileEntityOperator(Supplier<Date> dateSupplier)    {
            return entityOperatorReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, dateSupplier)
                    .map(this);
        }
    }

    /* test */ Date dateSupplier() {
        return new Date();
    }

    @Override
    protected KafkaStatus createStatus() {
        return new KafkaStatus();
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     * @param reconciliation    The Reconciliation identification
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null)
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }
}
