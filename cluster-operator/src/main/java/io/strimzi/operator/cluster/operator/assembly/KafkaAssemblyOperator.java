/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaExporterResources;
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
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZooKeeperRoller;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.strimzi.operator.cluster.model.AbstractModel.ANNO_STRIMZI_IO_STORAGE;
import static java.util.Collections.emptyMap;

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

    private final long operationTimeoutMs;
    private final FeatureGates featureGates;

    private final StatefulSetOperator stsOperations;
    private final DeploymentOperator deploymentOperations;
    private final PodOperator podOperations;
    private final CrdOperator<KubernetesClient, Kafka, KafkaList> crdOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final AdminClientProvider adminClientProvider;
    private final ZookeeperLeaderFinder zookeeperLeaderFinder;
    private final KubernetesRestartEventPublisher eventsPublisher;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param certManager Certificate manager
     * @param passwordGenerator Password generator
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     * @param eventsPublisher publishes pod restarts reasons as Kubernetes events
     */
    public KafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                 CertManager certManager, PasswordGenerator passwordGenerator,
                                 ResourceOperatorSupplier supplier, ClusterOperatorConfig config,
                                 KubernetesRestartEventPublisher eventsPublisher) {
        super(vertx, pfa, Kafka.RESOURCE_KIND, certManager, passwordGenerator,
                supplier.kafkaOperator, supplier, config);
        this.config = config;
        this.supplier = supplier;

        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.featureGates = config.featureGates();
        this.stsOperations = supplier.stsOperations;
        this.deploymentOperations = supplier.deploymentOperations;
        this.podOperations = supplier.podOperations;
        this.crdOperator = supplier.kafkaOperator;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.adminClientProvider = supplier.adminClientProvider;
        this.zookeeperLeaderFinder = supplier.zookeeperLeaderFinder;
        this.eventsPublisher = eventsPublisher;
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

        reconcileState.initialStatus()
                // Preparation steps => prepare cluster descriptions, handle CA creation or changes
                .compose(state -> state.reconcileCas(this::dateSupplier))
                .compose(state -> state.clusterOperatorSecret(this::dateSupplier))
                .compose(state -> state.versionChange())
                // Roll everything if a new CA is added to the trust store.
                .compose(state -> state.rollingUpdateForNewCaKey())
                // Remove older Cluster CA certificates if renewal happened with a new CA private key
                .compose(state -> state.maybeRemoveOldClusterCaCertificates())

                // Run reconciliations of the different components
                .compose(state -> state.reconcileZooKeeper(this::dateSupplier))
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

        private Secret oldCoSecret;

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

        /**
         * Asynchronously reconciles the cluster and clients CA secrets.
         * The cluster CA secret has to have the name determined by {@link AbstractModel#clusterCaCertSecretName(String)}.
         * The clients CA secret has to have the name determined by {@link KafkaResources#clientsCaCertificateSecretName(String)}.
         * Within both the secrets the current certificate is stored under the key {@code ca.crt}
         * and the current key is stored under the key {@code ca.key}.
         */
        @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
        Future<ReconciliationState> reconcileCas(Supplier<Date> dateSupplier) {
            Labels selectorLabels = Labels.EMPTY.withStrimziKind(reconciliation.kind()).withStrimziCluster(reconciliation.name());
            Labels caLabels = Labels.generateDefaultLabels(kafkaAssembly, Labels.APPLICATION_NAME, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
            Promise<ReconciliationState> resultPromise = Promise.promise();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        String clusterCaCertName = AbstractModel.clusterCaCertSecretName(name);
                        String clusterCaKeyName = AbstractModel.clusterCaKeySecretName(name);
                        String clientsCaCertName = KafkaResources.clientsCaCertificateSecretName(name);
                        String clientsCaKeyName = KafkaResources.clientsCaKeySecretName(name);
                        Secret clusterCaCertSecret = null;
                        Secret clusterCaKeySecret = null;
                        Secret clientsCaCertSecret = null;
                        Secret clientsCaKeySecret = null;
                        Secret brokersSecret = null;
                        List<Secret> clusterSecrets = secretOperations.list(reconciliation.namespace(), selectorLabels);
                        for (Secret secret : clusterSecrets) {
                            String secretName = secret.getMetadata().getName();
                            if (secretName.equals(clusterCaCertName)) {
                                clusterCaCertSecret = secret;
                            } else if (secretName.equals(clusterCaKeyName)) {
                                clusterCaKeySecret = secret;
                            } else if (secretName.equals(clientsCaCertName)) {
                                clientsCaCertSecret = secret;
                            } else if (secretName.equals(clientsCaKeyName)) {
                                clientsCaKeySecret = secret;
                            } else if (secretName.equals(KafkaResources.kafkaSecretName(name))) {
                                brokersSecret = secret;
                            }
                        }
                        OwnerReference ownerRef = new OwnerReferenceBuilder()
                                .withApiVersion(kafkaAssembly.getApiVersion())
                                .withKind(kafkaAssembly.getKind())
                                .withName(kafkaAssembly.getMetadata().getName())
                                .withUid(kafkaAssembly.getMetadata().getUid())
                                .withBlockOwnerDeletion(false)
                                .withController(false)
                                .build();

                        CertificateAuthority clusterCaConfig = kafkaAssembly.getSpec().getClusterCa();

                        // When we are not supposed to generate the CA, but it does not exist, we should just throw an error
                        checkCustomCaSecret(clusterCaConfig, clusterCaCertSecret, clusterCaKeySecret, "Cluster CA");

                        Map<String, String> clusterCaCertLabels = emptyMap();
                        Map<String, String> clusterCaCertAnnotations = emptyMap();

                        if (kafkaAssembly.getSpec().getKafka() != null
                                && kafkaAssembly.getSpec().getKafka().getTemplate() != null
                                && kafkaAssembly.getSpec().getKafka().getTemplate().getClusterCaCert() != null
                                && kafkaAssembly.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata() != null) {
                            clusterCaCertLabels = kafkaAssembly.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getLabels();
                            clusterCaCertAnnotations = kafkaAssembly.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getAnnotations();
                        }

                        this.clusterCa = new ClusterCa(reconciliation, certManager, passwordGenerator, name, clusterCaCertSecret,
                                clusterCaKeySecret,
                                ModelUtils.getCertificateValidity(clusterCaConfig),
                                ModelUtils.getRenewalDays(clusterCaConfig),
                                clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority(), clusterCaConfig != null ? clusterCaConfig.getCertificateExpirationPolicy() : null);
                        this.clusterCa.initCaSecrets(clusterSecrets);
                        clusterCa.createRenewOrReplace(
                                reconciliation.namespace(), reconciliation.name(), caLabels.toMap(),
                                clusterCaCertLabels, clusterCaCertAnnotations,
                                clusterCaConfig != null && !clusterCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef,
                                Util.isMaintenanceTimeWindowsSatisfied(reconciliation, getMaintenanceTimeWindows(), dateSupplier));

                        CertificateAuthority clientsCaConfig = kafkaAssembly.getSpec().getClientsCa();

                        // When we are not supposed to generate the CA, but it does not exist, we should just throw an error
                        checkCustomCaSecret(clientsCaConfig, clientsCaCertSecret, clientsCaKeySecret, "Clients CA");

                        this.clientsCa = new ClientsCa(reconciliation, certManager,
                                passwordGenerator, clientsCaCertName,
                                clientsCaCertSecret, clientsCaKeyName,
                                clientsCaKeySecret,
                                ModelUtils.getCertificateValidity(clientsCaConfig),
                                ModelUtils.getRenewalDays(clientsCaConfig),
                                clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority(), clientsCaConfig != null ? clientsCaConfig.getCertificateExpirationPolicy() : null);
                        this.clientsCa.initBrokerSecret(brokersSecret);
                        clientsCa.createRenewOrReplace(reconciliation.namespace(), reconciliation.name(),
                                caLabels.toMap(), emptyMap(), emptyMap(),
                                clientsCaConfig != null && !clientsCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef,
                                Util.isMaintenanceTimeWindowsSatisfied(reconciliation, getMaintenanceTimeWindows(), dateSupplier));

                        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                        List<Future> secretReconciliations = new ArrayList<>(2);

                        if (clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority())   {
                            Future<ReconcileResult<Secret>> clusterSecretReconciliation = secretOperations.reconcile(reconciliation, reconciliation.namespace(), clusterCaCertName, this.clusterCa.caCertSecret())
                                    .compose(ignored -> secretOperations.reconcile(reconciliation, reconciliation.namespace(), clusterCaKeyName, this.clusterCa.caKeySecret()));
                            secretReconciliations.add(clusterSecretReconciliation);
                        }

                        if (clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority())   {
                            Future<ReconcileResult<Secret>> clientsSecretReconciliation = secretOperations.reconcile(reconciliation, reconciliation.namespace(), clientsCaCertName, this.clientsCa.caCertSecret())
                                .compose(ignored -> secretOperations.reconcile(reconciliation, reconciliation.namespace(), clientsCaKeyName, this.clientsCa.caKeySecret()));
                            secretReconciliations.add(clientsSecretReconciliation);
                        }

                        CompositeFuture.join(secretReconciliations).onComplete(res -> {
                            if (res.succeeded())    {
                                future.complete(this);
                            } else {
                                future.fail(res.cause());
                            }
                        });
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                resultPromise
            );
            return resultPromise.future();
        }

        /**
         * Utility method for checking the Secret existence when custom CA is used. The custom CA is configured but the
         * secrets do not exist, it will throw InvalidConfigurationException.
         *
         * @param ca            The CA Configuration from the Custom Resource
         * @param certSecret    Secret with the certificate public key
         * @param keySecret     Secret with the certificate private key
         * @param caDescription The name of the CA for which this check is executed ("Cluster CA" or "Clients CA" - used
         *                      in the exception message)
         */
        public void checkCustomCaSecret(CertificateAuthority ca, Secret certSecret, Secret keySecret, String caDescription)   {
            if (ca != null && !ca.isGenerateCertificateAuthority() && (certSecret == null || keySecret == null))   {
                throw new InvalidConfigurationException(caDescription + " should not be generated, but the secrets were not found.");
            }
        }

        /**
         * Remove older cluster CA certificates if present in the corresponding Secret
         * after a renewal by replacing the corresponding CA private key
         */
        Future<ReconciliationState> maybeRemoveOldClusterCaCertificates() {
            // building the selector for Kafka related components
            Labels labels =  Labels.forStrimziCluster(name).withStrimziKind(Kafka.RESOURCE_KIND);
            return podOperations.listAsync(namespace, labels)
                    .compose(pods -> {
                        // still no Pods, a new Kafka cluster is under creation
                        if (pods.isEmpty()) {
                            return Future.succeededFuture();
                        }
                        int clusterCaCertGeneration = clusterCa.certGeneration();
                        LOGGER.debugCr(reconciliation, "Current cluster CA cert generation {}", clusterCaCertGeneration);
                        // only if all Kafka related components pods are updated to the new cluster CA cert generation,
                        // there is the possibility that we should remove the older cluster CA from the Secret and stores
                        for (Pod pod : pods) {
                            int podClusterCaCertGeneration = Integer.parseInt(pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION));
                            LOGGER.debugCr(reconciliation, "Pod {} cluster CA cert generation {}", pod.getMetadata().getName(), podClusterCaCertGeneration);
                            if (clusterCaCertGeneration != podClusterCaCertGeneration) {
                                return Future.succeededFuture();
                            }
                        }
                        LOGGER.debugCr(reconciliation, "Maybe there are old cluster CA certificates to remove");
                        this.clusterCa.maybeDeleteOldCerts();
                        return Future.succeededFuture(this.clusterCa);
                    })
                    .compose(ca -> {
                        if (ca != null && ca.certsRemoved()) {
                            return secretOperations.reconcile(reconciliation, namespace, AbstractModel.clusterCaCertSecretName(name), ca.caCertSecret());
                        } else {
                            return Future.succeededFuture();
                        }
                    })
                    .map(this);
        }

        /**
         * Perform a rolling update of the cluster so that CA certificates get added to their truststores,
         * or expired CA certificates get removed from their truststores.
         * Note this is only necessary when the CA certificate has changed due to a new CA key.
         * It is not necessary when the CA certificate is replace while retaining the existing key.
         */
        Future<ReconciliationState> rollingUpdateForNewCaKey() {
            List<String> reason = new ArrayList<>(2);
            RestartReasons kafkaRollReasons = RestartReasons.empty();

            if (this.clusterCa.keyReplaced()) {
                kafkaRollReasons.add(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED);
                reason.add("trust new cluster CA certificate signed by new key");
            }
            if (this.clientsCa.keyReplaced()) {
                kafkaRollReasons.add(RestartReason.CLIENT_CA_CERT_KEY_REPLACED);
                reason.add("trust new clients CA certificate signed by new key");
            }
            if (!reason.isEmpty()) {
                Future<Void> zkRollFuture;
                Function<Pod, List<String>> rollPodAndLogReason = pod -> {
                    LOGGER.debugCr(reconciliation, "Rolling Pod {} to {}", pod.getMetadata().getName(), reason);
                    return reason;
                };

                Function<Pod, RestartReasons> rollKafkaPodWithReasons = pod -> {
                    LOGGER.debugCr(reconciliation, "Rolling Pod {} to {}", pod.getMetadata().getName(), kafkaRollReasons.getAllReasonNotes());
                    return  kafkaRollReasons;
                };


                if (this.clusterCa.keyReplaced()) {
                    // ZooKeeper is rolled only for new Cluster CA key
                    Labels zkSelectorLabels = Labels
                            .generateDefaultLabels(kafkaAssembly, ZookeeperCluster.APPLICATION_NAME, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME)
                            .withStrimziName(KafkaResources.zookeeperStatefulSetName(name))
                            .strimziSelectorLabels();

                    zkRollFuture = new ZooKeeperRoller(podOperations, zookeeperLeaderFinder, operationTimeoutMs)
                            .maybeRollingUpdate(reconciliation, zkSelectorLabels, rollPodAndLogReason, clusterCa.caCertSecret(), oldCoSecret);
                } else {
                    zkRollFuture = Future.succeededFuture();
                }

                return zkRollFuture
                        .compose(i -> {
                            if (featureGates.useStrimziPodSetsEnabled())   {
                                return strimziPodSetOperator.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name))
                                        .compose(podSet -> {
                                            if (podSet != null) {
                                                return Future.succeededFuture(KafkaCluster.generatePodList(reconciliation.name(), podSet.getSpec().getPods().size()));
                                            } else {
                                                return Future.succeededFuture(List.<String>of());
                                            }
                                        });
                            } else {
                                return stsOperations.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name))
                                        .compose(sts -> {
                                            if (sts != null)    {
                                                return Future.succeededFuture(KafkaCluster.generatePodList(reconciliation.name(), sts.getSpec().getReplicas()));
                                            } else {
                                                return Future.succeededFuture(List.<String>of());
                                            }
                                        });
                            }
                        })
                        .compose(replicas ->
                                new KafkaRoller(
                                        reconciliation,
                                        vertx,
                                        podOperations,
                                        1_000,
                                        operationTimeoutMs,
                                        () -> new BackOff(250, 2, 10),
                                        replicas,
                                        clusterCa.caCertSecret(),
                                        oldCoSecret,
                                        adminClientProvider,
                                        brokerId -> null,
                                        null,
                                        null,
                                        true,
                                        eventsPublisher
                                ).rollingRestart(rollKafkaPodWithReasons))
                        .compose(i -> {
                            if (this.clusterCa.keyReplaced()) {
                                // EO, KE and CC need to be rolled only for new Cluster CA key.
                                return rollDeploymentIfExists(KafkaResources.entityOperatorDeploymentName(name), reason.toString())
                                        .compose(i2 -> rollDeploymentIfExists(KafkaExporterResources.deploymentName(name), reason.toString()))
                                        .compose(i2 -> rollDeploymentIfExists(CruiseControlResources.deploymentName(name), reason.toString()));
                            } else {
                                return Future.succeededFuture();
                            }
                        })
                        .map(i -> this);
            } else {
                return Future.succeededFuture(this);
            }
        }

        /**
         * Rolls deployments when they exist. This method is used by the CA renewal to roll deployments.
         *
         * @param deploymentName    Name of the deployment which should be rolled if it exists
         * @param reasons   Reasons for which it is being rolled
         * @return  Succeeded future if it succeeded, failed otherwise.
         */
        Future<Void> rollDeploymentIfExists(String deploymentName, String reasons)  {
            return deploymentOperations.getAsync(namespace, deploymentName)
                    .compose(dep -> {
                        if (dep != null) {
                            LOGGER.infoCr(reconciliation, "Rolling Deployment {} to {}", deploymentName, reasons);
                            return deploymentOperations.rollingUpdate(reconciliation, namespace, deploymentName, operationTimeoutMs);
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }

        private List<String> getMaintenanceTimeWindows() {
            return kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        }

        Future<ReconciliationState> clusterOperatorSecret(Supplier<Date> dateSupplier) {
            oldCoSecret = clusterCa.clusterOperatorSecret();

            Labels labels = Labels.fromResource(kafkaAssembly)
                    .withStrimziKind(reconciliation.kind())
                    .withStrimziCluster(reconciliation.name())
                    .withKubernetesName(Labels.APPLICATION_NAME)
                    .withKubernetesInstance(reconciliation.name())
                    .withKubernetesPartOf(reconciliation.name())
                    .withKubernetesManagedBy(AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);

            OwnerReference ownerRef = new OwnerReferenceBuilder()
                    .withApiVersion(kafkaAssembly.getApiVersion())
                    .withKind(kafkaAssembly.getKind())
                    .withName(kafkaAssembly.getMetadata().getName())
                    .withUid(kafkaAssembly.getMetadata().getUid())
                    .withBlockOwnerDeletion(false)
                    .withController(false)
                    .build();

            Secret secret = ModelUtils.buildSecret(reconciliation, clusterCa, clusterCa.clusterOperatorSecret(), namespace,
                    ClusterOperator.secretName(name), "cluster-operator", "cluster-operator",
                    labels, ownerRef, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, getMaintenanceTimeWindows(), dateSupplier));

            return secretOperations.reconcile(reconciliation, namespace, ClusterOperator.secretName(name), secret)
                    .map(this);
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
                    kafkaAssembly, oldStorage, currentReplicas, clusterCa, clientsCa, versionChange, config, supplier, pfa, vertx, eventsPublisher
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
