/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.listener.NodeAddressType;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.KafkaUpgradeException;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.model.StorageDiff;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.KafkaSpecChecker;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZooKeeperRoller;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.IngressV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RoleOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;

import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.AbstractModel.ANNO_STRIMZI_IO_STORAGE;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedIVVersions;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedVersions;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaList, Resource<Kafka>, KafkaSpec, KafkaStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAssemblyOperator.class.getName());

    private final long operationTimeoutMs;
    private final int zkAdminSessionTimeoutMs;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final FeatureGates featureGates;
    private final boolean isNetworkPolicyGeneration;

    private final StatefulSetOperator stsOperations;
    private final RouteOperator routeOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;
    private final RoleBindingOperator roleBindingOperations;
    private final RoleOperator roleOperations;
    private final PodOperator podOperations;
    private final IngressOperator ingressOperations;
    private final IngressV1Beta1Operator ingressV1Beta1Operations;
    private final StorageClassOperator storageClassOperator;
    private final NodeOperator nodeOperator;
    private final CrdOperator<KubernetesClient, Kafka, KafkaList> crdOperator;
    private final CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> strimziPodSetOperator;
    private final ZookeeperScalerProvider zkScalerProvider;
    private final AdminClientProvider adminClientProvider;
    private final ZookeeperLeaderFinder zookeeperLeaderFinder;

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
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.zkAdminSessionTimeoutMs = config.getZkAdminSessionTimeoutMs();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.featureGates = config.featureGates();
        this.routeOperations = supplier.routeOperations;
        this.stsOperations = supplier.stsOperations;
        this.pvcOperations = supplier.pvcOperations;
        this.deploymentOperations = supplier.deploymentOperations;
        this.roleBindingOperations = supplier.roleBindingOperations;
        this.roleOperations = supplier.roleOperations;
        this.podOperations = supplier.podOperations;
        this.ingressOperations = supplier.ingressOperations;
        this.ingressV1Beta1Operations = supplier.ingressV1Beta1Operations;
        this.storageClassOperator = supplier.storageClassOperations;
        this.crdOperator = supplier.kafkaOperator;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.nodeOperator = supplier.nodeOperator;
        this.zkScalerProvider = supplier.zkScalerProvider;
        this.adminClientProvider = supplier.adminClientProvider;
        this.zookeeperLeaderFinder = supplier.zookeeperLeaderFinder;
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

    /**
     * Run the reconciliation pipeline for the Kafka cluster
     *
     * @param reconciliationState   Reconciliation State
     *
     * @return                      Future with Reconciliation State
     */
    Future<ReconciliationState> reconcileKafka(ReconciliationState reconciliationState)    {
        return reconciliationState.checkKafkaSpec()
                .compose(state -> state.kafkaModelWarnings())
                .compose(state -> state.kafkaManualPodCleaning())
                .compose(state -> state.kafkaNetPolicy())
                .compose(state -> state.kafkaManualRollingUpdate())
                .compose(state -> state.kafkaPvcs())
                .compose(state -> state.kafkaInitServiceAccount())
                .compose(state -> state.kafkaInitClusterRoleBinding())
                .compose(state -> state.kafkaScaleDown())
                .compose(state -> state.kafkaServices())
                .compose(state -> state.kafkaRoutes())
                .compose(state -> state.kafkaIngresses())
                .compose(state -> state.kafkaIngressesV1Beta1())
                .compose(state -> state.kafkaInternalServicesReady())
                .compose(state -> state.kafkaLoadBalancerServicesReady())
                .compose(state -> state.kafkaNodePortServicesReady())
                .compose(state -> state.kafkaRoutesReady())
                .compose(state -> state.kafkaIngressesReady())
                .compose(state -> state.kafkaIngressesV1Beta1Ready())
                .compose(state -> state.kafkaGenerateCertificates(this::dateSupplier))
                .compose(state -> state.customListenerCertificates())
                .compose(state -> state.kafkaConfigurationConfigMaps())
                .compose(state -> state.kafkaBrokersSecret())
                .compose(state -> state.kafkaJmxSecret())
                .compose(state -> state.kafkaPodDisruptionBudget())
                .compose(state -> state.kafkaPodDisruptionBudgetV1Beta1())
                .compose(state -> state.kafkaStatefulSet())
                .compose(state -> state.kafkaPodSet())
                .compose(state -> state.kafkaRollToAddOrRemoveVolumes())
                .compose(state -> state.kafkaRollingUpdate())
                .compose(state -> state.kafkaScaleUp())
                .compose(state -> state.kafkaPodsReady())
                .compose(state -> state.kafkaServiceEndpointReady())
                .compose(state -> state.kafkaHeadlessServiceEndpointReady())
                .compose(state -> state.kafkaGetClusterId())
                .compose(state -> state.kafkaPersistentClaimDeletion())
                .compose(state -> state.kafkaConfigurationConfigMapsCleanup())
                // This has to run after all possible rolling updates which might move the pods to different nodes
                .compose(state -> state.kafkaNodePortExternalListenerStatus())
                .compose(state -> state.kafkaCustomCertificatesToStatus());
    }

    Future<Void> reconcile(ReconciliationState reconcileState)  {
        Promise<Void> chainPromise = Promise.promise();

        reconcileState.initialStatus()
                // Preparation steps => prepare cluster descriptions, handle CA creation or changes
                .compose(state -> state.reconcileCas(this::dateSupplier))
                .compose(state -> state.clusterOperatorSecret(this::dateSupplier))
                .compose(state -> state.getKafkaClusterDescription())
                .compose(state -> state.prepareVersionChange())
                // Roll everything if a new CA is added to the trust store.
                .compose(state -> state.rollingUpdateForNewCaKey())
                // Remove older Cluster CA certificates if renewal happened with a new CA private key
                .compose(state -> state.maybeRemoveOldClusterCaCertificates())

                // Run reconciliations of the different components
                .compose(state -> state.reconcileZooKeeper(this::dateSupplier))
                .compose(state -> reconcileKafka(state))
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

        private boolean kafkaStsAlreadyExists = false;
        private String currentStsVersion;
        private KafkaVersionChange versionChange;
        private String highestLogMessageFormatVersion;
        private String highestInterBrokerProtocolVersion;

        /* test */ ClusterCa clusterCa;
        /* test */ ClientsCa clientsCa;

        private KafkaCluster kafkaCluster = null;
        private Integer kafkaCurrentReplicas = null;
        private Storage oldKafkaStorage = null;
        /* test */ KafkaStatus kafkaStatus = new KafkaStatus();

        private ReconcileResult<StatefulSet> kafkaStsDiffs;
        private ReconcileResult<StrimziPodSet> kafkaPodSetDiffs;
        private final Set<String> kafkaBootstrapDnsName = new HashSet<>();
        /* test */ final Map<Integer, Map<String, String>> kafkaAdvertisedHostnames = new HashMap<>();
        /* test */ final Map<Integer, Map<String, String>> kafkaAdvertisedPorts = new HashMap<>();
        private final Map<Integer, Set<String>> kafkaBrokerDnsNames = new HashMap<>();
        /* test */ final Map<String, Integer> kafkaBootstrapNodePorts = new HashMap<>();

        private String kafkaLogging = "";
        private String kafkaLoggingAppendersHash = "";
        private Map<Integer, String> kafkaBrokerConfigurationHash = new HashMap<>();
        @SuppressFBWarnings(value = "SS_SHOULD_BE_STATIC", justification = "Field cannot be static in inner class in Java 11")
        private final int sharedConfigurationId = -1; // "Fake" broker ID used to indicate hash stored for all brokers when shared configuration is used

        private Secret oldCoSecret;

        /* test */ Set<String> fsResizingRestartRequest = new HashSet<>();

        // Certificate change indicators
        private boolean existingKafkaCertsChanged = false;

        // Custom Listener certificates
        private final Map<String, String> customListenerCertificates = new HashMap<>();
        private final Map<String, String> customListenerCertificateThumbprints = new HashMap<>();

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
         * @return
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
         * @return
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
         * Checks the requested Kafka spec for potential issues, and adds warnings and advice for best
         * practice to the status.
         */
        Future<ReconciliationState> checkKafkaSpec() {
            KafkaSpecChecker checker = new KafkaSpecChecker(kafkaAssembly.getSpec(), versions, kafkaCluster);
            List<Condition> warnings = checker.run();
            kafkaStatus.addConditions(warnings);
            return Future.succeededFuture(this);
        }

        /**
         * Takes the warning conditions from the Model and adds them in the KafkaStatus
         */
        Future<ReconciliationState> kafkaModelWarnings() {
            kafkaStatus.addConditions(kafkaCluster.getWarningConditions());
            return Future.succeededFuture(this);
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
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
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

                        // When we are not supposed to generate the CA but it does not exist, we should just throw an error
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

                        // When we are not supposed to generate the CA but it does not exist, we should just throw an error
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

                        List<Future> secretReconciliations = new ArrayList<>(2);

                        if (clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority())   {
                            Future clusterSecretReconciliation = secretOperations.reconcile(reconciliation, reconciliation.namespace(), clusterCaCertName, this.clusterCa.caCertSecret())
                                    .compose(ignored -> secretOperations.reconcile(reconciliation, reconciliation.namespace(), clusterCaKeyName, this.clusterCa.caKeySecret()));
                            secretReconciliations.add(clusterSecretReconciliation);
                        }

                        if (clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority())   {
                            Future clientsSecretReconciliation = secretOperations.reconcile(reconciliation, reconciliation.namespace(), clientsCaCertName, this.clientsCa.caCertSecret())
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
            if (this.clusterCa.keyReplaced()) {
                reason.add("trust new cluster CA certificate signed by new key");
            }
            if (this.clientsCa.keyReplaced()) {
                reason.add("trust new clients CA certificate signed by new key");
            }
            if (!reason.isEmpty()) {
                Future<Void> zkRollFuture;
                Function<Pod, List<String>> rollPodAndLogReason = pod -> {
                    LOGGER.debugCr(reconciliation, "Rolling Pod {} to {}", pod.getMetadata().getName(), reason);
                    return reason;
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
                                        kafkaCluster.getKafkaVersion(),
                                        true
                                ).rollingRestart(rollPodAndLogReason))
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
                            LOGGER.debugCr(reconciliation, "Rolling Deployment {} to {}", deploymentName, reasons);
                            return deploymentOperations.rollingUpdate(reconciliation, namespace, deploymentName, operationTimeoutMs);
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }

        /**
         * Does rolling update of Kafka pods based on the annotation on Pod level
         *
         * @return  Future with the result of the rolling update
         */
        Future<Void> kafkaManualPodRollingUpdate() {
            return podOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(pods -> {
                        List<String> podsToRoll = new ArrayList<>(0);

                        for (Pod pod : pods)    {
                            if (Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                                podsToRoll.add(pod.getMetadata().getName());
                            }
                        }

                        if (!podsToRoll.isEmpty())  {
                            return maybeRollKafka(kafkaCluster.getReplicas(), pod -> {
                                if (pod != null && podsToRoll.contains(pod.getMetadata().getName())) {
                                    LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation on a pod", pod.getMetadata().getName());
                                    return singletonList("manual rolling update annotation on a pod");
                                } else {
                                    return new ArrayList<>();
                                }
                            });
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }

        /**
         * Does manual rolling update of Kafka pods based on an annotation on the StatefulSet or on the Pods. Annotation
         * on StatefulSet level triggers rolling update of all pods. Annotation on pods trigeres rolling update only of
         * the selected pods. If the annotation is present on both StatefulSet and one or more pods, only one rolling
         * update of all pods occurs.
         *
         * @return  Future with the result of the rolling update
         */
        Future<ReconciliationState> kafkaManualRollingUpdate() {
            Future<HasMetadata> futureController;
            if (featureGates.useStrimziPodSetsEnabled())   {
                futureController = strimziPodSetOperator.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name)).map(podSet -> (HasMetadata) podSet);
            } else {
                futureController = stsOperations.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name)).map(sts -> (HasMetadata) sts);
            }

            return futureController.compose(controller -> {
                if (controller != null) {
                    if (Annotations.booleanAnnotation(controller, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                        // User trigger rolling update of the whole cluster
                        return maybeRollKafka(kafkaCluster.getReplicas(), pod -> {
                            if (pod == null) {
                                throw new ConcurrentDeletionException("Unexpectedly pod no longer exists during roll of StatefulSet.");
                            }
                            LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation",
                                    pod.getMetadata().getName());
                            return singletonList("manual rolling update");
                        });
                    } else {
                        // The controller is not annotated to roll all pods.
                        // But maybe the individual pods are annotated to restart only some of them.
                        return kafkaManualPodRollingUpdate();
                    }
                } else {
                    // Controller does not exist => nothing to roll
                    return Future.succeededFuture();
                }
            }).map(this);
        }

        /**
         * Analyses the Kafka broker versions together with log.message.format.version and inter.broker.protocol.version
         * fields and decides how to deal with possible upgrade or downgrade. When no version change is happening, it
         * just sets the default values for log.message.format.version and inter.broker.protocol.version fields. If
         * there is some version change happening, it either sets the versions for it or it throws an exception if
         * version change is not possible.
         *
         * @return
         */
        @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
        Future<ReconciliationState> prepareVersionChange() {
            if (versionChange.isNoop()) {
                LOGGER.debugCr(reconciliation, "{}: No Kafka version change", reconciliation);

                if (kafkaCluster.getInterBrokerProtocolVersion() == null) {
                    // When IBPV is not set, we set it to current Kafka version
                    kafkaCluster.setInterBrokerProtocolVersion(kafkaCluster.getKafkaVersion().protocolVersion());

                    if (highestInterBrokerProtocolVersion != null
                            && !kafkaCluster.getKafkaVersion().protocolVersion().equals(highestInterBrokerProtocolVersion)) {
                        LOGGER.infoCr(reconciliation, "Upgrading Kafka inter.broker.protocol.version from {} to {}", highestInterBrokerProtocolVersion, kafkaCluster.getKafkaVersion().protocolVersion());

                        if (compareDottedIVVersions(kafkaCluster.getKafkaVersion().protocolVersion(), "3.0") >= 0) {
                            // From Kafka 3.0.0, the LMFV is ignored when IBPV is set to 3.0 or higher
                            // We set the LMFV immediately to the same version as IPBV to avoid unnecessary rolling update
                            kafkaCluster.setLogMessageFormatVersion(kafkaCluster.getKafkaVersion().messageVersion());
                        } else if (kafkaCluster.getLogMessageFormatVersion() == null
                            && highestLogMessageFormatVersion != null) {
                            // For Kafka versions older than 3.0.0, IBPV and LMFV should not change in the same rolling
                            // update. When this rolling update is going to change the IPBV, we keep the old LMFV
                            kafkaCluster.setLogMessageFormatVersion(highestLogMessageFormatVersion);
                        }
                    }
                }

                if (kafkaCluster.getLogMessageFormatVersion() == null) {
                    // When LMFV is not set, we set it to current Kafka version
                    kafkaCluster.setLogMessageFormatVersion(kafkaCluster.getKafkaVersion().messageVersion());

                    if (highestLogMessageFormatVersion != null &&
                            !kafkaCluster.getKafkaVersion().messageVersion().equals(highestLogMessageFormatVersion)) {
                        LOGGER.infoCr(reconciliation, "Upgrading Kafka log.message.format.version from {} to {}", highestLogMessageFormatVersion, kafkaCluster.getKafkaVersion().messageVersion());
                    }
                }

                return Future.succeededFuture(this);
            } else {
                if (versionChange.isUpgrade()) {
                    LOGGER.infoCr(reconciliation, "Kafka is upgrading from {} to {}", versionChange.from().version(), versionChange.to().version());

                    // We make sure that the highest log.message.format.version or inter.broker.protocol.version
                    // used by any of the brokers is not higher than the broker version we upgrade from.
                    if ((highestLogMessageFormatVersion != null && compareDottedIVVersions(versionChange.from().messageVersion(), highestLogMessageFormatVersion) < 0)
                            || (highestInterBrokerProtocolVersion != null && compareDottedIVVersions(versionChange.from().protocolVersion(), highestInterBrokerProtocolVersion) < 0)) {
                        LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used by the brokers have to be lower or equal to the Kafka broker version we upgrade from ({})", highestLogMessageFormatVersion, highestInterBrokerProtocolVersion, versionChange.from().version());
                        throw new KafkaUpgradeException("log.message.format.version (" + highestLogMessageFormatVersion + ") and inter.broker.protocol.version (" + highestInterBrokerProtocolVersion + ") used by the brokers have to be lower or equal to the Kafka broker version we upgrade from (" + versionChange.from().version() + ")");
                    }

                    String desiredLogMessageFormat = kafkaCluster.getLogMessageFormatVersion();
                    String desiredInterBrokerProtocol = kafkaCluster.getInterBrokerProtocolVersion();

                    // The desired log.message.format.version will be configured in the new brokers. And therefore it
                    // cannot be higher that the Kafka version we are upgrading from. If it is, we override it with the
                    // version we are upgrading from. If it is not set, we set it to the version we are upgrading from.
                    if (desiredLogMessageFormat == null
                            || compareDottedIVVersions(versionChange.from().messageVersion(), desiredLogMessageFormat) < 0) {
                        kafkaCluster.setLogMessageFormatVersion(versionChange.from().messageVersion());
                    }

                    // The desired inter.broker.protocol.version will be configured in the new brokers. And therefore it
                    // cannot be higher that the Kafka version we are upgrading from. If it is, we override it with the
                    // version we are upgrading from. If it is not set, we set it to the version we are upgrading from.
                    if (desiredInterBrokerProtocol == null
                            || compareDottedIVVersions(versionChange.from().protocolVersion(), desiredInterBrokerProtocol) < 0) {
                        kafkaCluster.setInterBrokerProtocolVersion(versionChange.from().protocolVersion());
                    }
                } else {
                    // Has to be a downgrade
                    LOGGER.infoCr(reconciliation, "Kafka is downgrading from {} to {}", versionChange.from().version(), versionChange.to().version());

                    // The currently used log.message.format.version and inter.broker.protocol.version cannot be higher
                    // than the version we are downgrading to. If it is we fail the reconciliation. If they are not set,
                    // we assume that it will use the default value which is the from version. In such case we fail the
                    // reconciliation as well.
                    if (highestLogMessageFormatVersion == null
                            || compareDottedIVVersions(versionChange.to().messageVersion(), highestLogMessageFormatVersion) < 0
                            || highestInterBrokerProtocolVersion == null
                            || compareDottedIVVersions(versionChange.to().protocolVersion(), highestInterBrokerProtocolVersion) < 0) {
                        LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to ({})", highestLogMessageFormatVersion, highestInterBrokerProtocolVersion, versionChange.to().version());
                        throw new KafkaUpgradeException("log.message.format.version (" + highestLogMessageFormatVersion + ") and inter.broker.protocol.version (" + highestInterBrokerProtocolVersion + ") used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to (" + versionChange.to().version() + ")");
                    }

                    String desiredLogMessageFormat = kafkaCluster.getLogMessageFormatVersion();
                    String desiredInterBrokerProtocol = kafkaCluster.getInterBrokerProtocolVersion();

                    // If log.message.format.version is not set, we set it to the version we are downgrading to.
                    if (desiredLogMessageFormat == null) {
                        desiredLogMessageFormat = versionChange.to().messageVersion();
                        kafkaCluster.setLogMessageFormatVersion(versionChange.to().messageVersion());
                    }

                    // If inter.broker.protocol.version is not set, we set it to the version we are downgrading to.
                    if (desiredInterBrokerProtocol == null) {
                        desiredInterBrokerProtocol = versionChange.to().protocolVersion();
                        kafkaCluster.setInterBrokerProtocolVersion(versionChange.to().protocolVersion());
                    }

                    // Either log.message.format.version or inter.broker.protocol.version are higher than the Kafka
                    // version we are downgrading to. This should normally not happen since that should not pass the CR
                    // validation. But we still double check it as safety.
                    if (compareDottedIVVersions(versionChange.to().messageVersion(), desiredLogMessageFormat) < 0
                            || compareDottedIVVersions(versionChange.to().protocolVersion(), desiredInterBrokerProtocol) < 0) {
                        LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used in the Kafka CR have to be set and be lower or equal to the Kafka broker version we downgrade to ({})", highestLogMessageFormatVersion, highestInterBrokerProtocolVersion, versionChange.to().version());
                        throw new KafkaUpgradeException("log.message.format.version and inter.broker.protocol.version used in the Kafka CR have to be set and be lower or equal to the Kafka broker version we downgrade to");
                    }
                }
            }

            return Future.succeededFuture(this);
        }

        /**
         * Gets asynchronously a secret with certificate. If it doesn't exist, it throws exception.
         *
         * @param namespace     Namespace where the secret is
         * @param secretName    Name of the secret
         *
         * @return              Secret with the certificates
         */
        private Future<Secret> getSecret(String namespace, String secretName)  {
            return secretOperations.getAsync(namespace, secretName).compose(secret -> {
                if (secret == null) {
                    return Future.failedFuture(Util.missingSecretException(namespace, secretName));
                } else {
                    return Future.succeededFuture(secret);
                }
            });
        }

        /**
         * Rolls Kafka pods if needed
         *
         * @param replicas Number of Kafka replicas which should be considered for rolling
         * @param podNeedsRestart this function serves as a predicate whether to roll pod or not
         *
         * @return succeeded future if kafka pod was rolled and is ready
         */
        Future<Void> maybeRollKafka(int replicas, Function<Pod, List<String>> podNeedsRestart) {
            return maybeRollKafka(replicas, podNeedsRestart, true);
        }

        /**
         * Rolls Kafka pods if needed
         *
         * @param replicas Number of Kafka replicas which should be considered for rolling
         * @param podNeedsRestart this function serves as a predicate whether to roll pod or not
         * @param allowReconfiguration defines whether the rolling update should also attempt to do dynamic reconfiguration or not
         *
         * @return succeeded future if kafka pod was rolled and is ready
         */
        Future<Void> maybeRollKafka(int replicas, Function<Pod, List<String>> podNeedsRestart, boolean allowReconfiguration) {
            return ReconcilerUtils.clientSecrets(reconciliation, secretOperations)
                    .compose(compositeFuture ->
                            new KafkaRoller(
                                    reconciliation,
                                    vertx,
                                    podOperations,
                                    1_000,
                                    operationTimeoutMs,
                                    () -> new BackOff(250, 2, 10),
                                    KafkaCluster.generatePodList(reconciliation.name(), replicas),
                                    compositeFuture.resultAt(0),
                                    compositeFuture.resultAt(1),
                                    adminClientProvider,
                                    brokerId -> {
                                        if (featureGates.useStrimziPodSetsEnabled()) {
                                            return kafkaCluster.generatePerBrokerBrokerConfiguration(brokerId, kafkaAdvertisedHostnames, kafkaAdvertisedPorts, featureGates.controlPlaneListenerEnabled());
                                        } else {
                                            return kafkaCluster.generateSharedBrokerConfiguration(featureGates.controlPlaneListenerEnabled());
                                        }
                                    },
                                    kafkaLogging,
                                    kafkaCluster.getKafkaVersion(),
                                    allowReconfiguration
                            ).rollingRestart(podNeedsRestart));
        }

        Future<ReconciliationState> withVoid(Future<?> r) {
            return r.map(this);
        }

        /**
         * Reconciles Secret with certificates and evaluates whether any existing certificates inside changed.
         *
         * @param secretName    Name of the secret
         * @param secret        The new secret
         * @return              Future with True if the existing certificates changed and False if they didn't
         */
        Future<Boolean> updateCertificateSecretWithDiff(String secretName, Secret secret)   {
            return secretOperations.getAsync(namespace, secretName)
                    .compose(oldSecret -> secretOperations.reconcile(reconciliation, namespace, secretName, secret)
                            .map(res -> {
                                if (res instanceof ReconcileResult.Patched) {
                                    // The secret is patched and some changes to the existing certificates actually occurred
                                    return ModelUtils.doExistingCertificatesDiffer(oldSecret, res.resource());
                                }

                                return false;
                            })
                    );
        }

        /*test*/ Future<ReconciliationState> getKafkaClusterDescription() {
            return getKafkaSetDescription()
                    .compose(ignore -> {
                        this.kafkaCluster = KafkaCluster.fromCrd(reconciliation, kafkaAssembly, versions, oldKafkaStorage, kafkaCurrentReplicas);
                        this.kafkaBootstrapDnsName.addAll(ListenersUtils.alternativeNames(kafkaCluster.getListeners()));

                        return podOperations.listAsync(namespace, this.kafkaCluster.getSelectorLabels());
                    }).compose(pods -> {
                        String lowestKafkaVersion = currentStsVersion;
                        String highestKafkaVersion = currentStsVersion;

                        for (Pod pod : pods)    {
                            // Collect the different annotations from the pods
                            String currentVersion = Annotations.stringAnnotation(pod, ANNO_STRIMZI_IO_KAFKA_VERSION, null);
                            String currentMessageFormat = Annotations.stringAnnotation(pod, KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, null);
                            String currentIbp = Annotations.stringAnnotation(pod, KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, null);

                            // We find the highest and lowest used Kafka version. This is used to detect any upgrades or
                            // downgrades which failed in the middle and continue with them.
                            if (currentVersion != null) {
                                if (highestKafkaVersion == null)  {
                                    highestKafkaVersion = currentVersion;
                                } else if (compareDottedVersions(highestKafkaVersion, currentVersion) < 0) {
                                    highestKafkaVersion = currentVersion;
                                }

                                if (lowestKafkaVersion == null)  {
                                    lowestKafkaVersion = currentVersion;
                                } else if (compareDottedVersions(lowestKafkaVersion, currentVersion) > 0) {
                                    lowestKafkaVersion = currentVersion;
                                }
                            }

                            // We find the highest used log.message.format.version. This is later used to validate
                            // upgrades or downgrades.
                            if (currentMessageFormat != null) {
                                if (highestLogMessageFormatVersion == null)  {
                                    highestLogMessageFormatVersion = currentMessageFormat;
                                } else if (compareDottedIVVersions(highestLogMessageFormatVersion, currentMessageFormat) < 0) {
                                    highestLogMessageFormatVersion = currentMessageFormat;
                                }
                            }

                            // We find the highest used inter.broker.protocol.version. This is later used to validate
                            // upgrades or downgrades.
                            if (currentIbp != null)  {
                                if (highestInterBrokerProtocolVersion == null)  {
                                    highestInterBrokerProtocolVersion = currentIbp;
                                } else if (compareDottedIVVersions(highestInterBrokerProtocolVersion, currentIbp) < 0) {
                                    highestInterBrokerProtocolVersion = currentIbp;
                                }
                            }
                        }

                        // We decide what is the current Kafka version used and create the KafkaVersionChange object
                        // describing the situation.
                        if (lowestKafkaVersion == null)   {
                            if (!kafkaStsAlreadyExists && pods.isEmpty())  {
                                // No version found in Pods or StatefulSet because they do not exist => This means we
                                // are dealing with a brand new Kafka cluster or a Kafka cluster with deleted
                                // StatefulSet and Pods. New cluster does not need an upgrade. And cluster without
                                // StatefulSet / Pods cannot be rolled. So we can just deploy a new one with the desired
                                // version. Therefore we can use the desired version and set the version change to noop.
                                this.versionChange = new KafkaVersionChange(kafkaCluster.getKafkaVersion(), kafkaCluster.getKafkaVersion());
                            } else {
                                // Either Pods or StatefulSet already exist. But none of them contains the version
                                // annotation. This suggests they are not created by the current versions of Strimzi.
                                // Without the annotation, we cannot detect the Kafka version and decide on upgrade.
                                LOGGER.warnCr(reconciliation, "Kafka Pods or StatefulSet exist, but do not contain the {} annotation to detect their version. Kafka upgrade cannot be detected.", ANNO_STRIMZI_IO_KAFKA_VERSION);
                                throw new KafkaUpgradeException("Kafka Pods or StatefulSet exist, but do not contain the " + ANNO_STRIMZI_IO_KAFKA_VERSION + " annotation to detect their version. Kafka upgrade cannot be detected.");
                            }
                        } else if (lowestKafkaVersion.equals(highestKafkaVersion)) {
                            // All brokers have the same version. We can use it as the current version.
                            this.versionChange = new KafkaVersionChange(versions.version(lowestKafkaVersion), kafkaCluster.getKafkaVersion());
                        } else if (compareDottedVersions(highestKafkaVersion, kafkaCluster.getKafkaVersion().version()) > 0)    {
                            // Highest Kafka version used by the brokers is higher then desired => suspected downgrade
                            this.versionChange = new KafkaVersionChange(versions.version(highestKafkaVersion), kafkaCluster.getKafkaVersion());
                        } else {
                            // Highest Kafka version used by the brokers is equal or lower than desired => suspected upgrade
                            this.versionChange = new KafkaVersionChange(versions.version(lowestKafkaVersion), kafkaCluster.getKafkaVersion());
                        }

                        return Future.succeededFuture(this);
                    });
        }

        /**
         * Gets the Kafka cluster description. It checks whether StatefulSets or StrimziPodSets are used and uses the
         * appropriate method to get the description data.
         *
         * @return
         */
        Future<Void> getKafkaSetDescription()   {
            Future<StatefulSet> stsFuture = stsOperations.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name));
            Future<StrimziPodSet> podSetFuture = strimziPodSetOperator.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name));

            return CompositeFuture.join(stsFuture, podSetFuture)
                    .compose(res -> {
                        StatefulSet sts = res.resultAt(0);
                        StrimziPodSet podSet = res.resultAt(1);

                        if (sts != null && podSet != null)  {
                            // Both StatefulSet and PodSet exist => we create the description based on the feature gate
                            if (featureGates.useStrimziPodSetsEnabled())    {
                                kafkaPodSetDescription(podSet);
                            } else {
                                kafkaStatefulSetDescription(sts);
                            }
                        } else if (sts != null) {
                            // StatefulSet exists, PodSet does nto exist => we create the description from the StatefulSet
                            kafkaStatefulSetDescription(sts);
                        } else if (podSet != null) {
                            //PodSet exists, StatefulSet does not => we create the description from the PodSet
                            kafkaPodSetDescription(podSet);
                        } else {
                            // Neither StatefulSet nor PodSet exists => we just initialize the current replicas
                            this.kafkaCurrentReplicas = 0;
                        }

                        return Future.succeededFuture();
                    });
        }

        /**
         * Initializes the Kafka description based on a StatefulSet.
         */
        void kafkaStatefulSetDescription(StatefulSet sts)   {
            this.kafkaCurrentReplicas = 0;
            if (sts != null && sts.getSpec() != null)   {
                this.kafkaCurrentReplicas = sts.getSpec().getReplicas();
                this.currentStsVersion = Annotations.annotations(sts).get(ANNO_STRIMZI_IO_KAFKA_VERSION);
                this.oldKafkaStorage = getOldStorage(sts);
                this.kafkaStsAlreadyExists = true;
            }
        }

        /**
         * Initializes the Kafka description based on a StrimziPodSet.
         */
        void kafkaPodSetDescription(StrimziPodSet podSet)   {
            this.kafkaCurrentReplicas = 0;
            if (podSet != null && podSet.getSpec() != null)   {
                this.kafkaCurrentReplicas = podSet.getSpec().getPods().size();
                this.currentStsVersion = Annotations.annotations(podSet).get(ANNO_STRIMZI_IO_KAFKA_VERSION);
                this.oldKafkaStorage = getOldStorage(podSet);
                this.kafkaStsAlreadyExists = true;
            }
        }

        Future<ReconciliationState> withKafkaStsDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.kafkaStsDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> withKafkaPodSetDiff(Future<ReconcileResult<StrimziPodSet>> r) {
            return r.map(rr -> {
                this.kafkaPodSetDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> kafkaInitServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    kafkaCluster.getServiceAccountName(),
                    kafkaCluster.generateServiceAccount()));
        }

        Future<ReconciliationState> kafkaInitClusterRoleBinding() {
            ClusterRoleBinding desired = kafkaCluster.generateClusterRoleBinding(namespace);

            return withVoid(withIgnoreRbacError(reconciliation,
                    clusterRoleBindingOperations.reconcile(reconciliation,
                        KafkaResources.initContainerClusterRoleBindingName(name, namespace),
                        desired),
                    desired
            ));
        }

        Future<ReconciliationState> kafkaScaleDown() {
            if (kafkaCurrentReplicas != null
                    && kafkaCurrentReplicas != 0
                    && kafkaCurrentReplicas > kafkaCluster.getReplicas()) {
                // The previous (current) number of replicas is bigger than desired => we should scale-down
                LOGGER.infoCr(reconciliation, "Scaling Kafka down from {} to {} replicas", kafkaCurrentReplicas, kafkaCluster.getReplicas());
                
                if (featureGates.useStrimziPodSetsEnabled())   {
                    Set<String> desiredPodNames = new HashSet<>(kafkaCluster.getReplicas());
                    for (int i = 0; i < kafkaCluster.getReplicas(); i++) {
                        desiredPodNames.add(kafkaCluster.getPodName(i));
                    }

                    return strimziPodSetOperator.getAsync(namespace, kafkaCluster.getName())
                            .compose(podSet -> {
                                if (podSet == null) {
                                    return Future.succeededFuture();
                                } else {
                                    List<Map<String, Object>> desiredPods = podSet.getSpec().getPods().stream()
                                            .filter(pod -> desiredPodNames.contains(PodSetUtils.mapToPod(pod).getMetadata().getName()))
                                            .collect(Collectors.toList());
                                    
                                    StrimziPodSet scaledDownPodSet = new StrimziPodSetBuilder(podSet)
                                            .editSpec()
                                                .withPods(desiredPods)
                                            .endSpec()
                                            .build();
                                    
                                    return strimziPodSetOperator.reconcile(reconciliation, namespace, kafkaCluster.getName(), scaledDownPodSet);
                                }
                            })
                            .map(this);
                } else {
                    return withVoid(stsOperations.scaleDown(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
                }
            } else {
                // Previous replica count is unknown (because the PodSet / StatefulSet did not exist) or is smaller or equal to
                // desired replicas => no need to scale-down
                return Future.succeededFuture(this);
            }
        }

        /**
         * Makes sure all desired services are updated and the rest is deleted
         *
         * @return
         */
        Future<ReconciliationState> kafkaServices() {
            List<Service> services = new ArrayList<>();
            services.add(kafkaCluster.generateService());
            services.add(kafkaCluster.generateHeadlessService());
            services.addAll(kafkaCluster.generateExternalBootstrapServices());

            int replicas = kafkaCluster.getReplicas();
            for (int i = 0; i < replicas; i++) {
                services.addAll(kafkaCluster.generateExternalServices(i));
            }

            Future fut = serviceOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingServices -> {
                        List<Future> serviceFutures = new ArrayList<>(services.size());
                        List<String> existingServiceNames = existingServices.stream().map(svc -> svc.getMetadata().getName()).collect(Collectors.toList());

                        LOGGER.debugCr(reconciliation, "Reconciling existing Services {} against the desired services", existingServiceNames);

                        // Update desired services
                        for (Service service : services) {
                            String serviceName = service.getMetadata().getName();
                            existingServiceNames.remove(serviceName);
                            serviceFutures.add(serviceOperations.reconcile(reconciliation, namespace, serviceName, service));
                        }

                        LOGGER.debugCr(reconciliation, "Services {} should be deleted", existingServiceNames);

                        // Delete services which match our selector but are not desired anymore
                        for (String serviceName : existingServiceNames) {
                            serviceFutures.add(serviceOperations.reconcile(reconciliation, namespace, serviceName, null));
                        }

                        return CompositeFuture.join(serviceFutures);
                    });

            return withVoid(fut);
        }

        /**
         * Makes sure all desired routes are updated and the rest is deleted
         *
         * @return
         */
        Future<ReconciliationState> kafkaRoutes() {
            List<Route> routes = new ArrayList<>(kafkaCluster.generateExternalBootstrapRoutes());

            if (routes.size() > 0) {
                if (pfa.hasRoutes()) {
                    int replicas = kafkaCluster.getReplicas();
                    for (int i = 0; i < replicas; i++) {
                        routes.addAll(kafkaCluster.generateExternalRoutes(i));
                    }

                    Future fut = routeOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                            .compose(existingRoutes -> {
                                List<Future> routeFutures = new ArrayList<>(routes.size());
                                List<String> existingRouteNames = existingRoutes.stream().map(route -> route.getMetadata().getName()).collect(Collectors.toList());

                                LOGGER.debugCr(reconciliation, "Reconciling existing Routes {} against the desired routes", existingRouteNames);

                                // Update desired routes
                                for (Route route : routes) {
                                    String routeName = route.getMetadata().getName();
                                    existingRouteNames.remove(routeName);
                                    routeFutures.add(routeOperations.reconcile(reconciliation, namespace, routeName, route));
                                }

                                LOGGER.debugCr(reconciliation, "Routes {} should be deleted", existingRouteNames);

                                // Delete routes which match our selector but are not desired anymore
                                for (String routeName : existingRouteNames) {
                                    routeFutures.add(routeOperations.reconcile(reconciliation, namespace, routeName, null));
                                }

                                return CompositeFuture.join(routeFutures);
                            });

                    return withVoid(fut);
                } else {
                    LOGGER.warnCr(reconciliation, "The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster {} using routes is not possible.", name);
                    return withVoid(Future.failedFuture("The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster " + name + " using routes is not possible."));
                }
            }

            return withVoid(Future.succeededFuture());
        }

        /**
         * Makes sure all desired ingresses are updated and the rest is deleted
         *
         * @return
         */
        Future<ReconciliationState> kafkaIngresses() {
            if (!pfa.hasIngressV1()) {
                return Future.succeededFuture(this);
            }

            List<Ingress> ingresses = new ArrayList<>(kafkaCluster.generateExternalBootstrapIngresses());

            int replicas = kafkaCluster.getReplicas();
            for (int i = 0; i < replicas; i++) {
                ingresses.addAll(kafkaCluster.generateExternalIngresses(i));
            }

            Future fut = ingressOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingIngresses -> {
                        List<Future> ingressFutures = new ArrayList<>(ingresses.size());
                        List<String> existingIngressNames = existingIngresses.stream().map(ingress -> ingress.getMetadata().getName()).collect(Collectors.toList());

                        LOGGER.debugCr(reconciliation, "Reconciling existing Ingresses {} against the desired ingresses", existingIngressNames);

                        // Update desired ingresses
                        for (Ingress ingress : ingresses) {
                            String ingressName = ingress.getMetadata().getName();
                            existingIngressNames.remove(ingressName);
                            ingressFutures.add(ingressOperations.reconcile(reconciliation, namespace, ingressName, ingress));
                        }

                        LOGGER.debugCr(reconciliation, "Ingresses {} should be deleted", existingIngressNames);

                        // Delete ingresses which match our selector but are not desired anymore
                        for (String ingressName : existingIngressNames) {
                            ingressFutures.add(ingressOperations.reconcile(reconciliation, namespace, ingressName, null));
                        }

                        return CompositeFuture.join(ingressFutures);
                    });

            return withVoid(fut);
        }

        /**
         * Makes sure all desired ingresses are updated and the rest is deleted
         *
         * @return
         */
        Future<ReconciliationState> kafkaIngressesV1Beta1() {
            if (pfa.hasIngressV1()) {
                return Future.succeededFuture(this);
            }

            List<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingresses = new ArrayList<>(kafkaCluster.generateExternalBootstrapIngressesV1Beta1());

            int replicas = kafkaCluster.getReplicas();
            for (int i = 0; i < replicas; i++) {
                ingresses.addAll(kafkaCluster.generateExternalIngressesV1Beta1(i));
            }

            Future fut = ingressV1Beta1Operations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingIngresses -> {
                        List<Future> ingressFutures = new ArrayList<>(ingresses.size());
                        List<String> existingIngressNames = existingIngresses.stream().map(ingress -> ingress.getMetadata().getName()).collect(Collectors.toList());

                        LOGGER.debugCr(reconciliation, "Reconciling existing v1beta1 Ingresses {} against the desired ingresses", existingIngressNames);

                        // Update desired ingresses
                        for (io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress ingress : ingresses) {
                            String ingressName = ingress.getMetadata().getName();
                            existingIngressNames.remove(ingressName);
                            ingressFutures.add(ingressV1Beta1Operations.reconcile(reconciliation, namespace, ingressName, ingress));
                        }

                        LOGGER.debugCr(reconciliation, "V1beta1 ingresses {} should be deleted", existingIngressNames);

                        // Delete ingresses which match our selector but are not desired anymore
                        for (String ingressName : existingIngressNames) {
                            ingressFutures.add(ingressV1Beta1Operations.reconcile(reconciliation, namespace, ingressName, null));
                        }

                        return CompositeFuture.join(ingressFutures);
                    });

            return withVoid(fut);
        }

        /**
         * Get the cluster Id of the Kafka cluster
         * 
         * @return
         */
        Future<ReconciliationState> kafkaGetClusterId() {
            return ReconcilerUtils.clientSecrets(reconciliation, secretOperations)
                    .compose(compositeFuture -> {
                        LOGGER.debugCr(reconciliation, "Attempt to get clusterId");
                        Promise<ReconciliationState> resultPromise = Promise.promise();
                        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                                future -> {
                                    Admin kafkaAdmin = null;

                                    try {
                                        String bootstrapHostname = KafkaResources.bootstrapServiceName(this.name) + "." + this.namespace + ".svc:" + KafkaCluster.REPLICATION_PORT;
                                        LOGGER.debugCr(reconciliation, "Creating AdminClient for clusterId using {}", bootstrapHostname);
                                        kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, compositeFuture.resultAt(0), compositeFuture.resultAt(1), "cluster-operator");
                                        kafkaStatus.setClusterId(kafkaAdmin.describeCluster().clusterId().get());
                                    } catch (KafkaException e) {
                                        LOGGER.warnCr(reconciliation, "Kafka exception getting clusterId {}", e.getMessage());
                                    } catch (InterruptedException e) {
                                        LOGGER.warnCr(reconciliation, "Interrupted exception getting clusterId {}", e.getMessage());
                                    } catch (ExecutionException e) {
                                        LOGGER.warnCr(reconciliation, "Execution exception getting clusterId {}", e.getMessage());
                                    } finally {
                                        if (kafkaAdmin != null) {
                                            kafkaAdmin.close();
                                        }
                                    }

                                    future.complete(this);
                                },
                                true,
                                resultPromise);
                        return resultPromise.future();
                    });
        }

        /**
         * Utility method which helps to register the advertised hostnames for a specific listener of a specific broker.
         * The broker hostname passed to this method is based on the infrastructure (Service, Load Balancer, etc.).
         * This method in addition checks for any overrides and uses them if configured.
         *
         * @param brokerId          ID of the broker to which this hostname belongs
         * @param listener          The Listener for which is this hostname used
         * @param brokerHostname    The hostname which might be used for the broker when no overrides are configured
         */
        void registerAdvertisedHostname(int brokerId, GenericKafkaListener listener, String brokerHostname)   {
            kafkaAdvertisedHostnames
                    .computeIfAbsent(brokerId, id -> new HashMap<>())
                    .put(ListenersUtils.envVarIdentifier(listener), kafkaCluster.getAdvertisedHostname(listener, brokerId, brokerHostname));
        }

        /**
         * Utility method which helps to register the advertised port for a specific listener of a specific broker.
         * The broker port passed to this method is based on the infrastructure (Service, Load Balancer, etc.).
         * This method in addition checks for any overrides and uses them if configured.
         *
         * @param brokerId      ID of the broker to which this port belongs
         * @param listener      The Listener for which is this port used
         * @param brokerPort    The port which might be used for the broker when no overrides are configured
         */
        void registerAdvertisedPort(int brokerId, GenericKafkaListener listener, int brokerPort)   {
            kafkaAdvertisedPorts
                    .computeIfAbsent(brokerId, id -> new HashMap<>())
                    .put(ListenersUtils.envVarIdentifier(listener), kafkaCluster.getAdvertisedPort(listener, brokerId, brokerPort));
        }

        Future<ReconciliationState> kafkaInternalServicesReady()   {
            for (GenericKafkaListener listener : ListenersUtils.internalListeners(kafkaCluster.getListeners())) {
                boolean useServiceDnsDomain = (listener.getConfiguration() != null && listener.getConfiguration().getUseServiceDnsDomain() != null)
                        ? listener.getConfiguration().getUseServiceDnsDomain() : false;

                // Set status based on bootstrap service
                String bootstrapAddress = getInternalServiceHostname(ListenersUtils.backwardsCompatibleBootstrapServiceName(name, listener), useServiceDnsDomain);

                ListenerStatus ls = new ListenerStatusBuilder()
                        .withName(listener.getName())
                        .withAddresses(new ListenerAddressBuilder()
                                .withHost(bootstrapAddress)
                                .withPort(listener.getPort())
                                .build())
                        .build();

                addListenerStatus(ls);

                // Set advertised hostnames and ports
                for (int brokerId = 0; brokerId < kafkaCluster.getReplicas(); brokerId++) {
                    String brokerAddress;

                    if (useServiceDnsDomain) {
                        brokerAddress = DnsNameGenerator.podDnsNameWithClusterDomain(namespace, KafkaResources.brokersServiceName(name), KafkaResources.kafkaStatefulSetName(name) + "-" + brokerId);
                    } else {
                        brokerAddress = DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace, KafkaResources.brokersServiceName(name), KafkaResources.kafkaStatefulSetName(name) + "-" + brokerId);
                    }

                    String userConfiguredAdvertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, brokerId);
                    if (userConfiguredAdvertisedHostname != null && listener.isTls()) {
                        // If user configured a custom advertised hostname, add it to the SAN names used in the certificate
                        kafkaBrokerDnsNames.computeIfAbsent(brokerId, k -> new HashSet<>(1)).add(userConfiguredAdvertisedHostname);
                    }

                    registerAdvertisedHostname(brokerId, listener, brokerAddress);
                    registerAdvertisedPort(brokerId, listener, listener.getPort());
                }
            }

            return Future.succeededFuture(this);
        }

        /**
         * Makes sure all services related to load balancers are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all Load Balancer type listeners:
         *      1) Checks if the bootstrap service has been provisioned (has a loadbalancer address)
         *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
         *      3) Checks if the broker services have been provisioned (have a loadbalancer address)
         *      4) Collects the loadbalancer addresses for certificates and advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaLoadBalancerServicesReady() {
            List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.loadBalancerListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

            for (GenericKafkaListener listener : loadBalancerListeners) {
                String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(name, listener);

                List<String> bootstrapListenerAddressList = new ArrayList<>(kafkaCluster.getReplicas());

                Future perListenerFut = Future.succeededFuture().compose(i -> {
                    if (ListenersUtils.skipCreateBootstrapService(listener)) {
                        return Future.succeededFuture();
                    } else {
                        return serviceOperations.hasIngressAddress(reconciliation, namespace, bootstrapServiceName, 1_000, operationTimeoutMs)
                                .compose(res -> serviceOperations.getAsync(namespace, bootstrapServiceName))
                                .compose(svc -> {
                                    String bootstrapAddress;

                                    if (svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null) {
                                        bootstrapAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                    } else {
                                        bootstrapAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                    }

                                    LOGGER.debugCr(reconciliation, "Found address {} for Service {}", bootstrapAddress, bootstrapServiceName);

                                    kafkaBootstrapDnsName.add(bootstrapAddress);
                                    bootstrapListenerAddressList.add(bootstrapAddress);
                                    return Future.succeededFuture();
                                });
                    }
                }).compose(res -> {
                    List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                    for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                        perPodFutures.add(
                                serviceOperations.hasIngressAddress(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                        );
                    }

                    return CompositeFuture.join(perPodFutures);
                }).compose(res -> {
                    List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                    for (int brokerId = 0; brokerId < kafkaCluster.getReplicas(); brokerId++)  {
                        final int finalBrokerId = brokerId;
                        Future<Void> perBrokerFut = serviceOperations.getAsync(namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, brokerId, listener))
                            .compose(svc -> {
                                String brokerAddress;

                                if (svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null)    {
                                    brokerAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                } else {
                                    brokerAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                }
                                LOGGER.debugCr(reconciliation, "Found address {} for Service {}", brokerAddress, svc.getMetadata().getName());

                                if (ListenersUtils.skipCreateBootstrapService(listener)) {
                                    bootstrapListenerAddressList.add(brokerAddress);
                                }
                                kafkaBrokerDnsNames.computeIfAbsent(finalBrokerId, k -> new HashSet<>(2)).add(brokerAddress);

                                String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);
                                if (advertisedHostname != null) {
                                    kafkaBrokerDnsNames.get(finalBrokerId).add(ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId));
                                }

                                registerAdvertisedHostname(finalBrokerId, listener, brokerAddress);
                                registerAdvertisedPort(finalBrokerId, listener, listener.getPort());

                                return Future.succeededFuture();
                            });

                        perPodFutures.add(perBrokerFut);
                    }

                    return CompositeFuture.join(perPodFutures);
                }).compose(res -> {
                    ListenerStatus ls = new ListenerStatusBuilder()
                        .withName(listener.getName())
                        .withAddresses(bootstrapListenerAddressList.stream()
                                .map(listenerAddress -> new ListenerAddressBuilder().withHost(listenerAddress)
                                        .withPort(listener.getPort())
                                        .build())
                                .collect(Collectors.toList()))
                        .build();
                    addListenerStatus(ls);

                    return Future.succeededFuture();
                });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        /**
         * Makes sure all services related to node ports are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all NodePort type listeners:
         *      1) Checks if the bootstrap service has been provisioned (has a node port)
         *      2) Collects the node port for use in CR status
         *      3) Checks it the broker services have been provisioned (have a node port)
         *      4) Collects the node ports for advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaNodePortServicesReady() {
            List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.nodePortListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

            for (GenericKafkaListener listener : loadBalancerListeners) {
                String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(name, listener);

                Future perListenerFut = serviceOperations.hasNodePort(reconciliation, namespace, bootstrapServiceName, 1_000, operationTimeoutMs)
                        .compose(res -> serviceOperations.getAsync(namespace, bootstrapServiceName))
                        .compose(svc -> {
                            Integer externalBootstrapNodePort = svc.getSpec().getPorts().get(0).getNodePort();
                            LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", externalBootstrapNodePort, bootstrapServiceName);
                            kafkaBootstrapNodePorts.put(ListenersUtils.identifier(listener), externalBootstrapNodePort);

                            return Future.succeededFuture();
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        serviceOperations.hasNodePort(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int brokerId = 0; brokerId < kafkaCluster.getReplicas(); brokerId++)  {
                                final int finalBrokerId = brokerId;
                                Future<Void> perBrokerFut = serviceOperations.getAsync(namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, brokerId, listener))
                                        .compose(svc -> {
                                            Integer externalBrokerNodePort = svc.getSpec().getPorts().get(0).getNodePort();
                                            LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", externalBrokerNodePort, svc.getMetadata().getName());

                                            registerAdvertisedPort(finalBrokerId, listener, externalBrokerNodePort);

                                            String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);

                                            if (advertisedHostname != null) {
                                                kafkaBrokerDnsNames.computeIfAbsent(finalBrokerId, k -> new HashSet<>(1)).add(advertisedHostname);
                                            }

                                            registerAdvertisedHostname(finalBrokerId, listener, nodePortAddressEnvVar(listener));

                                            return Future.succeededFuture();
                                        });

                                perPodFutures.add(perBrokerFut);
                            }

                            return CompositeFuture.join(perPodFutures);
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        /**
         * Generates the name of the environment variable which will contain the advertised address for given listener. The
         * environment variable will be different for Node Port listeners which need to consume the address from the init
         * container corresponding to their preferred node.
         *
         * @param listener              The listener
         * @return                      The environment variable which will have the address
         */
        String nodePortAddressEnvVar(GenericKafkaListener listener)  {
            String preferredNodeAddressType;
            NodeAddressType preferredType = ListenersUtils.preferredNodeAddressType(listener);

            if (preferredType != null)  {
                preferredNodeAddressType = preferredType.toValue().toUpperCase(Locale.ENGLISH);
            } else {
                preferredNodeAddressType = "DEFAULT";
            }

            return String.format("${STRIMZI_NODEPORT_%s_ADDRESS}", preferredNodeAddressType);
        }

        Future<ReconciliationState> kafkaNodePortExternalListenerStatus() {
            List<Node> allNodes = new ArrayList<>();

            if (!ListenersUtils.nodePortListeners(kafkaCluster.getListeners()).isEmpty())   {
                return nodeOperator.listAsync(Labels.EMPTY)
                        .compose(result -> {
                            allNodes.addAll(result);
                            return podOperations.listAsync(namespace, kafkaCluster.getSelectorLabels());
                        })
                        .map(pods -> {
                            Map<Integer, Node> brokerNodes = new HashMap<>(kafkaCluster.getReplicas());

                            for (Pod broker : pods) {
                                String podName = broker.getMetadata().getName();
                                Integer podIndex = getPodIndexFromPodName(podName);

                                if (broker.getStatus() != null && broker.getStatus().getHostIP() != null) {
                                    String hostIP = broker.getStatus().getHostIP();
                                    allNodes.stream()
                                            .filter(node -> {
                                                if (node.getStatus() != null && node.getStatus().getAddresses() != null) {
                                                    return node.getStatus().getAddresses().stream().anyMatch(address -> hostIP.equals(address.getAddress()));
                                                } else {
                                                    return false;
                                                }
                                            })
                                            .findFirst()
                                            .ifPresent(podNode -> brokerNodes.put(podIndex, podNode));
                                }
                            }

                            for (GenericKafkaListener listener : ListenersUtils.nodePortListeners(kafkaCluster.getListeners())) {
                                // Set is used to ensure each node/port is listed only once. It is later converted to List.
                                Set<ListenerAddress> statusAddresses = new HashSet<>(brokerNodes.size());

                                for (Map.Entry<Integer, Node> entry : brokerNodes.entrySet())   {
                                    String advertisedHost = ListenersUtils.brokerAdvertisedHost(listener, entry.getKey());
                                    ListenerAddress address;

                                    if (advertisedHost != null)    {
                                        address = new ListenerAddressBuilder()
                                                .withHost(advertisedHost)
                                                .withPort(kafkaBootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                                .build();
                                    } else {
                                        address = new ListenerAddressBuilder()
                                                .withHost(NodeUtils.findAddress(entry.getValue().getStatus().getAddresses(), ListenersUtils.preferredNodeAddressType(listener)))
                                                .withPort(kafkaBootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                                .build();
                                    }

                                    statusAddresses.add(address);
                                }

                                ListenerStatus ls = new ListenerStatusBuilder()
                                        .withName(listener.getName())
                                        .withAddresses(new ArrayList<>(statusAddresses))
                                        .build();
                                addListenerStatus(ls);
                            }

                            return this;
                        });
            } else {
                return Future.succeededFuture(this);
            }
        }

        /**
         * Makes sure all routes are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all routes:
         *      1) Checks if the bootstrap route has been provisioned (has a loadbalancer address)
         *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
         *      3) Checks it the broker routes have been provisioned (have an address)
         *      4) Collects the route addresses for certificates and advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaRoutesReady() {
            List<GenericKafkaListener> routeListeners = ListenersUtils.routeListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(routeListeners.size());

            for (GenericKafkaListener listener : routeListeners) {
                String bootstrapRouteName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(name, listener);

                Future perListenerFut = routeOperations.hasAddress(reconciliation, namespace, bootstrapRouteName, 1_000, operationTimeoutMs)
                        .compose(res -> routeOperations.getAsync(namespace, bootstrapRouteName))
                        .compose(route -> {
                            String bootstrapAddress = route.getStatus().getIngress().get(0).getHost();
                            LOGGER.debugCr(reconciliation, "Found address {} for Route {}", bootstrapAddress, bootstrapRouteName);

                            kafkaBootstrapDnsName.add(bootstrapAddress);

                            ListenerStatus ls = new ListenerStatusBuilder()
                                    .withName(listener.getName())
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost(bootstrapAddress)
                                            .withPort(KafkaCluster.ROUTE_PORT)
                                            .build())
                                    .build();
                            addListenerStatus(ls);

                            return Future.succeededFuture();
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        routeOperations.hasAddress(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int brokerId = 0; brokerId < kafkaCluster.getReplicas(); brokerId++)  {
                                final int finalBrokerId = brokerId;
                                Future<Void> perBrokerFut = routeOperations.getAsync(namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, brokerId, listener))
                                        .compose(route -> {
                                            String brokerAddress = route.getStatus().getIngress().get(0).getHost();
                                            LOGGER.debugCr(reconciliation, "Found address {} for Route {}", brokerAddress, route.getMetadata().getName());

                                            kafkaBrokerDnsNames.computeIfAbsent(finalBrokerId, k -> new HashSet<>(2)).add(brokerAddress);

                                            String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);
                                            if (advertisedHostname != null) {
                                                kafkaBrokerDnsNames.get(finalBrokerId).add(ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId));
                                            }

                                            registerAdvertisedHostname(finalBrokerId, listener, brokerAddress);
                                            registerAdvertisedPort(finalBrokerId, listener, KafkaCluster.ROUTE_PORT);

                                            return Future.succeededFuture();
                                        });

                                perPodFutures.add(perBrokerFut);
                            }

                            return CompositeFuture.join(perPodFutures);
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        /**
         * Makes sure all ingresses are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all ingresses:
         *      1) Checks if the bootstrap ingress has been provisioned (has a loadbalancer address)
         *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
         *      3) Checks it the broker ingresses have been provisioned (have an address)
         *      4) Collects the route addresses for certificates and advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaIngressesReady() {
            if (!pfa.hasIngressV1()) {
                return Future.succeededFuture(this);
            }

            List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(ingressListeners.size());

            for (GenericKafkaListener listener : ingressListeners) {
                String bootstrapIngressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(name, listener);

                Future perListenerFut = ingressOperations.hasIngressAddress(reconciliation, namespace, bootstrapIngressName, 1_000, operationTimeoutMs)
                        .compose(res -> {
                            String bootstrapAddress = listener.getConfiguration().getBootstrap().getHost();
                            LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", bootstrapAddress, bootstrapIngressName);

                            kafkaBootstrapDnsName.add(bootstrapAddress);

                            ListenerStatus ls = new ListenerStatusBuilder()
                                    .withName(listener.getName())
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost(bootstrapAddress)
                                            .withPort(KafkaCluster.ROUTE_PORT)
                                            .build())
                                    .build();
                            addListenerStatus(ls);

                            // Check if broker ingresses are ready
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        ingressOperations.hasIngressAddress(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            for (int brokerId = 0; brokerId < kafkaCluster.getReplicas(); brokerId++)  {
                                final int finalBrokerId = brokerId;
                                String brokerAddress = listener.getConfiguration().getBrokers().stream()
                                        .filter(broker -> broker.getBroker() == finalBrokerId)
                                        .map(GenericKafkaListenerConfigurationBroker::getHost)
                                        .findAny()
                                        .orElse(null);
                                LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", brokerAddress, ListenersUtils.backwardsCompatibleBrokerServiceName(name, brokerId, listener));

                                kafkaBrokerDnsNames.computeIfAbsent(brokerId, k -> new HashSet<>(2)).add(brokerAddress);

                                String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);
                                if (advertisedHostname != null) {
                                    kafkaBrokerDnsNames.get(finalBrokerId).add(ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId));
                                }

                                registerAdvertisedHostname(finalBrokerId, listener, brokerAddress);
                                registerAdvertisedPort(finalBrokerId, listener, KafkaCluster.INGRESS_PORT);
                            }

                            return Future.succeededFuture();
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        /**
         * Makes sure all ingresses are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all ingresses:
         *      1) Checks if the bootstrap ingress has been provisioned (has a loadbalancer address)
         *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
         *      3) Checks it the broker ingresses have been provisioned (have an address)
         *      4) Collects the route addresses for certificates and advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaIngressesV1Beta1Ready() {
            if (pfa.hasIngressV1()) {
                return Future.succeededFuture(this);
            }

            List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(ingressListeners.size());

            for (GenericKafkaListener listener : ingressListeners) {
                String bootstrapIngressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(name, listener);

                Future perListenerFut = ingressV1Beta1Operations.hasIngressAddress(reconciliation, namespace, bootstrapIngressName, 1_000, operationTimeoutMs)
                        .compose(res -> {
                            String bootstrapAddress = listener.getConfiguration().getBootstrap().getHost();
                            LOGGER.debugCr(reconciliation, "Using address {} for v1beta1 Ingress {}", bootstrapAddress, bootstrapIngressName);

                            kafkaBootstrapDnsName.add(bootstrapAddress);

                            ListenerStatus ls = new ListenerStatusBuilder()
                                    .withName(listener.getName())
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost(bootstrapAddress)
                                            .withPort(KafkaCluster.ROUTE_PORT)
                                            .build())
                                    .build();
                            addListenerStatus(ls);

                            // Check if broker ingresses are ready
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        ingressV1Beta1Operations.hasIngressAddress(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            for (int brokerId = 0; brokerId < kafkaCluster.getReplicas(); brokerId++)  {
                                final int finalBrokerId = brokerId;
                                String brokerAddress = listener.getConfiguration().getBrokers().stream()
                                        .filter(broker -> broker.getBroker() == finalBrokerId)
                                        .map(GenericKafkaListenerConfigurationBroker::getHost)
                                        .findAny()
                                        .orElse(null);
                                LOGGER.debugCr(reconciliation, "Using address {} for v1beta1 Ingress {}", brokerAddress, ListenersUtils.backwardsCompatibleBrokerServiceName(name, brokerId, listener));

                                kafkaBrokerDnsNames.computeIfAbsent(brokerId, k -> new HashSet<>(2)).add(brokerAddress);

                                String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);
                                if (advertisedHostname != null) {
                                    kafkaBrokerDnsNames.get(finalBrokerId).add(ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId));
                                }

                                registerAdvertisedHostname(finalBrokerId, listener, brokerAddress);
                                registerAdvertisedPort(finalBrokerId, listener, KafkaCluster.INGRESS_PORT);
                            }

                            return Future.succeededFuture();
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        Future<ReconciliationState> kafkaGenerateCertificates(Supplier<Date> dateSupplier) {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        kafkaCluster.generateCertificates(kafkaAssembly,
                                clusterCa, kafkaBootstrapDnsName, kafkaBrokerDnsNames,
                                Util.isMaintenanceTimeWindowsSatisfied(reconciliation, getMaintenanceTimeWindows(), dateSupplier));
                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                },
                true,
                resultPromise);
            return resultPromise.future();
        }

        Future<ReconciliationState> customListenerCertificates() {
            List<String> secretNames = kafkaCluster.getListeners().stream()
                    .filter(listener -> listener.isTls()
                            && listener.getConfiguration() != null
                            && listener.getConfiguration().getBrokerCertChainAndKey() != null)
                    .map(listener -> listener.getConfiguration().getBrokerCertChainAndKey().getSecretName())
                    .distinct()
                    .collect(Collectors.toList());
            LOGGER.debugCr(reconciliation, "Validating secret {} with custom TLS listener certificates", secretNames);

            List<Future> secretFutures = new ArrayList<>(secretNames.size());
            Map<String, Secret> customSecrets = new HashMap<>(secretNames.size());

            for (String secretName : secretNames)   {
                Future fut = secretOperations.getAsync(namespace, secretName)
                        .compose(secret -> {
                            if (secret != null) {
                                customSecrets.put(secretName, secret);
                                LOGGER.debugCr(reconciliation, "Found secrets {} with custom TLS listener certificate", secretName);
                            }

                            return Future.succeededFuture();
                        });

                secretFutures.add(fut);
            }

            Future customCertificatesFuture = CompositeFuture.join(secretFutures)
                    .compose(res -> {
                        List<String> errors = new ArrayList<>();

                        for (GenericKafkaListener listener : kafkaCluster.getListeners())   {
                            if (listener.isTls()
                                    && listener.getConfiguration() != null
                                    && listener.getConfiguration().getBrokerCertChainAndKey() != null)  {
                                CertAndKeySecretSource customCert = listener.getConfiguration().getBrokerCertChainAndKey();
                                Secret secret = customSecrets.get(customCert.getSecretName());

                                if (secret != null) {
                                    if (!secret.getData().containsKey(customCert.getCertificate())) {
                                        errors.add("Secret " + customCert.getSecretName() + " does not contain certificate under the key " + customCert.getCertificate() + ".");
                                    } else if (!secret.getData().containsKey(customCert.getKey())) {
                                        errors.add("Secret " + customCert.getSecretName() + " does not contain custom certificate private key under the key " + customCert.getKey() + ".");
                                    } else  {
                                        byte[] publicKeyBytes = Base64.getDecoder().decode(secret.getData().get(customCert.getCertificate()));
                                        customListenerCertificates.put(listener.getName(), new String(publicKeyBytes, StandardCharsets.US_ASCII));
                                        customListenerCertificateThumbprints.put(listener.getName(), getCertificateThumbprint(secret, customCert));
                                    }
                                } else {
                                    errors.add("Secret " + customCert.getSecretName() + " with custom TLS certificate does not exist.");
                                }

                            }
                        }

                        if (errors.isEmpty())   {
                            return Future.succeededFuture();
                        } else {
                            LOGGER.errorCr(reconciliation, "Failed to process Secrets with custom certificates: {}", errors);
                            return Future.failedFuture(new InvalidResourceException("Failed to process Secrets with custom certificates: " + errors));
                        }
                    });

            return withVoid(customCertificatesFuture);
        }

        /**
         * Generates hash stub of the certificate which is used to track when the certificate changes and rolling update needs to be triggered.
         *
         * @param certSecret        Secrets with the certificate
         * @param customCertSecret  Identified where in the secret can you get the right certificate
         *
         * @return                  Hash stub of the certificate
         */
        String getCertificateThumbprint(Secret certSecret, CertAndKeySecretSource customCertSecret)   {
            try {
                X509Certificate cert = Ca.cert(certSecret, customCertSecret.getCertificate());
                return Util.hashStub(cert.getEncoded());
            } catch (CertificateEncodingException e) {
                throw new RuntimeException("Failed to get certificate hashStub of " + customCertSecret.getCertificate() + " from Secret " + certSecret.getMetadata().getName(), e);
            }
        }

        /**
         * Generates and creates the ConfigMap with shared configuration for Kafka brokers used in StatefulSets.
         *
         * @param metricsAndLogging     Metrics and Logging configuration
         *
         * @return
         */
        Future<ReconcileResult<ConfigMap>> sharedKafkaConfiguration(MetricsAndLogging metricsAndLogging) {
            ConfigMap sharedCm = kafkaCluster.generateSharedConfigurationConfigMap(metricsAndLogging, kafkaAdvertisedHostnames, kafkaAdvertisedPorts, featureGates.controlPlaneListenerEnabled());

            // BROKER_ADVERTISED_HOSTNAMES_FILENAME or BROKER_ADVERTISED_PORTS_FILENAME have the advertised addresses.
            // If they change, we need to roll the pods. Here we collect their hash to trigger the rolling update.
            String brokerConfiguration = sharedCm.getData().getOrDefault(KafkaCluster.BROKER_ADVERTISED_HOSTNAMES_FILENAME, "");
            brokerConfiguration += sharedCm.getData().getOrDefault(KafkaCluster.BROKER_ADVERTISED_PORTS_FILENAME, "");
            brokerConfiguration += sharedCm.getData().getOrDefault(KafkaCluster.BROKER_LISTENERS_FILENAME, "");

            // Changes to regular Kafka configuration are handled through the KafkaRoller which decides whether to roll the pod or not
            // In addition to that, we have to handle changes to configuration unknown to Kafka -> different plugins (Authorization, Quotas etc.)
            // This is captured here with the unknown configurations and the hash is used to roll the pod when it changes
            KafkaConfiguration kc = KafkaConfiguration.unvalidated(reconciliation, sharedCm.getData().getOrDefault(KafkaCluster.BROKER_CONFIGURATION_FILENAME, ""));

            // We store hash of the broker configurations for later use in StatefulSet / PodSet and in rolling updates
            this.kafkaBrokerConfigurationHash.put(sharedConfigurationId, Util.hashStub(brokerConfiguration + kc.unknownConfigsWithValues(kafkaCluster.getKafkaVersion()).toString()));

            // This is used during Kafka rolling updates -> we have to store it for later
            this.kafkaLogging = sharedCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
            this.kafkaLoggingAppendersHash = Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(this.kafkaLogging));

            return configMapOperations.reconcile(reconciliation, namespace, kafkaCluster.getAncillaryConfigMapName(), sharedCm);
        }

        /**
         * Generates and creates the ConfigMaps with per-broker configuration for Kafka brokers used in PodSets. It will
         * also delete the ConfigMaps for any scaled-down brokers (scale down is done before this is called in the
         * reconciliation)
         *
         * @param metricsAndLogging     Metrics and Logging configuration
         *
         * @return
         */
        Future<CompositeFuture> perBrokerKafkaConfiguration(MetricsAndLogging metricsAndLogging) {
            return configMapOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingConfigMaps -> {
                        // This is used during Kafka rolling updates -> we have to store it for later
                        this.kafkaLogging = kafkaCluster.loggingConfiguration(kafkaCluster.getLogging(), metricsAndLogging.getLoggingCm());
                        this.kafkaLoggingAppendersHash = Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(this.kafkaLogging));

                        List<ConfigMap> desiredConfigMaps = kafkaCluster.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, kafkaAdvertisedHostnames, kafkaAdvertisedPorts, featureGates.controlPlaneListenerEnabled());
                        List<Future> ops = new ArrayList<>(existingConfigMaps.size() + kafkaCluster.getReplicas());

                        // Delete all existing ConfigMaps which are not desired and are not the shared config map
                        List<String> desiredNames = new ArrayList<>(desiredConfigMaps.size() + 1);
                        desiredNames.add(kafkaCluster.getAncillaryConfigMapName()); // We do not want to delete the shared ConfigMap, so we add it here
                        desiredNames.addAll(desiredConfigMaps.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList()));

                        for (ConfigMap cm : existingConfigMaps) {
                            // We delete the cms not on the desired names list
                            if (!desiredNames.contains(cm.getMetadata().getName())) {
                                ops.add(configMapOperations.deleteAsync(reconciliation, namespace, cm.getMetadata().getName(), true));
                            }
                        }

                        // Create / update the desired config maps
                        for (ConfigMap cm : desiredConfigMaps) {
                            String cmName = cm.getMetadata().getName();
                            int brokerId = getPodIndexFromPodName(cmName);

                            // The advertised hostname and port might change. If they change, we need to roll the pods.
                            // Here we collect their hash to trigger the rolling update. For per-broker configuration,
                            // we need just the advertised hostnames / ports for given broker.
                            String brokerConfiguration = kafkaAdvertisedHostnames
                                    .get(brokerId)
                                    .entrySet()
                                    .stream()
                                    .map(kv -> kv.getKey() + "://" + kv.getValue())
                                    .sorted()
                                    .collect(Collectors.joining(" "));
                            brokerConfiguration += kafkaAdvertisedPorts
                                    .get(brokerId)
                                    .entrySet()
                                    .stream()
                                    .map(kv -> kv.getKey() + "://" + kv.getValue())
                                    .sorted()
                                    .collect(Collectors.joining(" "));
                            brokerConfiguration += cm.getData().getOrDefault(KafkaCluster.BROKER_LISTENERS_FILENAME, "");

                            // Changes to regular Kafka configuration are handled through the KafkaRoller which decides whether to roll the pod or not
                            // In addition to that, we have to handle changes to configuration unknown to Kafka -> different plugins (Authorization, Quotas etc.)
                            // This is captured here with the unknown configurations and the hash is used to roll the pod when it changes
                            KafkaConfiguration kc = KafkaConfiguration.unvalidated(reconciliation, cm.getData().getOrDefault(KafkaCluster.BROKER_CONFIGURATION_FILENAME, ""));

                            // We store hash of the broker configurations for later use in Pod and in rolling updates
                            this.kafkaBrokerConfigurationHash.put(brokerId, Util.hashStub(brokerConfiguration + kc.unknownConfigsWithValues(kafkaCluster.getKafkaVersion()).toString()));

                            ops.add(configMapOperations.reconcile(reconciliation, namespace, cmName, cm));
                        }

                        return CompositeFuture.join(ops);
                    });
        }

        /**
         * This method is used to create or update the config maps required by the brokers. It is able to handle both
         * shared configuration and per-broker configuration.
         *
         * It does not do the cleanup when switching between the modes. That is done only at the end of the
         * reconciliation. However, when the per-broker configuration mode is used, it would delete the config maps of
         * the scaled-down brokers since scale-down happens before this is called.
         *
         * @return
         */
        Future<ReconciliationState> kafkaConfigurationConfigMaps() {
            return Util.metricsAndLogging(reconciliation, configMapOperations, namespace, kafkaCluster.getLogging(), kafkaCluster.getMetricsConfigInCm())
                    .compose(metricsAndLoggingCm -> {
                        if (featureGates.useStrimziPodSetsEnabled())    {
                            return withVoid(perBrokerKafkaConfiguration(metricsAndLoggingCm));
                        } else {
                            return withVoid(sharedKafkaConfiguration(metricsAndLoggingCm));
                        }
                    });
        }

        /**
         * This method is used to clean-up the configuration Config Maps. This might be needed when switching between
         * StatefulSets / shared configuration and PodSets / per-broker configuration. This is done in a separate step
         * at the end of the reconciliation to avoid problems if the reconciliation fails (the deleted config map would
         * not allow pods to be restarted etc.)
         *
         * @return
         */
        Future<ReconciliationState> kafkaConfigurationConfigMapsCleanup() {
            if (featureGates.useStrimziPodSetsEnabled())    {
                return withVoid(sharedKafkaConfigurationCleanup());
            } else {
                return withVoid(perBrokerKafkaConfigurationCleanup());
            }
        }

        /**
         * Deletes the ConfigMap with shared Kafka configuration. This needs to be done when switching to per-broker
         * configuration / PodSets.
         *
         * @return
         */
        Future<Void> sharedKafkaConfigurationCleanup() {
            return configMapOperations.deleteAsync(reconciliation, namespace, kafkaCluster.getAncillaryConfigMapName(), true);
        }

        /**
         * Deletes all ConfigMaps with per-broker Kafka configuration. This needs to be done when switching to the
         * shared configuration / StatefulSets
         *
         * @return
         */
        Future<CompositeFuture> perBrokerKafkaConfigurationCleanup() {
            return configMapOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingConfigMaps -> {
                        List<Future> ops = new ArrayList<>(existingConfigMaps.size()); // We will need at most the deletion of existing configmaps + update of new one => hence the + 1

                        // Delete all existing apart from the shared one
                        for (ConfigMap cm : existingConfigMaps) {
                            // We delete the cm if it is not getAncillaryConfigMapName
                            if (!kafkaCluster.getAncillaryConfigMapName().equals(cm.getMetadata().getName())) {
                                ops.add(configMapOperations.deleteAsync(reconciliation, namespace, cm.getMetadata().getName(), true));
                            }
                        }

                        return CompositeFuture.join(ops);
                    });
        }

        Future<ReconciliationState> kafkaBrokersSecret() {
            return updateCertificateSecretWithDiff(KafkaResources.kafkaSecretName(name), kafkaCluster.generateBrokersSecret(clusterCa, clientsCa))
                    .map(changed -> {
                        existingKafkaCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> kafkaJmxSecret() {
            if (kafkaCluster.isJmxAuthenticated()) {
                Future<Secret> secretFuture = secretOperations.getAsync(namespace, KafkaResources.kafkaJmxSecretName(name));
                return secretFuture.compose(res -> {
                    if (res == null) {
                        return withVoid(secretOperations.reconcile(reconciliation, namespace, KafkaResources.kafkaJmxSecretName(name),
                                kafkaCluster.generateJmxSecret()));
                    }
                    return withVoid(Future.succeededFuture(this));
                });

            }
            return withVoid(secretOperations.reconcile(reconciliation, namespace, KafkaResources.kafkaJmxSecretName(name), null));
        }

        Future<ReconciliationState> kafkaNetPolicy() {
            if (isNetworkPolicyGeneration) {
                return withVoid(networkPolicyOperator.reconcile(reconciliation, namespace, KafkaResources.kafkaNetworkPolicyName(name), kafkaCluster.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels)));
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        Future<ReconciliationState> kafkaPodDisruptionBudget() {
            if (!pfa.hasPodDisruptionBudgetV1()) {
                return Future.succeededFuture(this);
            }

            if (featureGates.useStrimziPodSetsEnabled())   {
                return withVoid(podDisruptionBudgetOperator.reconcile(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.generateCustomControllerPodDisruptionBudget()));
            } else {
                return withVoid(podDisruptionBudgetOperator.reconcile(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.generatePodDisruptionBudget()));
            }
        }

        Future<ReconciliationState> kafkaPodDisruptionBudgetV1Beta1() {
            if (pfa.hasPodDisruptionBudgetV1()) {
                return Future.succeededFuture(this);
            }

            if (featureGates.useStrimziPodSetsEnabled())   {
                return withVoid(podDisruptionBudgetV1Beta1Operator.reconcile(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.generateCustomControllerPodDisruptionBudgetV1Beta1()));
            } else {
                return withVoid(podDisruptionBudgetV1Beta1Operator.reconcile(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.generatePodDisruptionBudgetV1Beta1()));
            }
        }

        int getPodIndexFromPodName(String podName)  {
            return Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
        }

        Future<ReconciliationState> kafkaPvcs() {
            List<PersistentVolumeClaim> pvcs = kafkaCluster.generatePersistentVolumeClaims(kafkaCluster.getStorage());

            return new PvcReconciler(reconciliation, pvcOperations, storageClassOperator)
                    .resizeAndReconcilePvcs(podIndex -> KafkaResources.kafkaPodName(name, podIndex), pvcs)
                    .compose(podsToRestart -> {
                        fsResizingRestartRequest.addAll(podsToRestart);
                        return Future.succeededFuture(this);
                    });
        }

        /**
         * Normally, the rolling update of Kafka brokers is done in the sequence best for Kafka (controller is rolled
         * last etc.). This method does a rolling update of Kafka brokers in sequence based on the pod index number.
         * This is needed in some special situation such as storage configuration changes.
         *
         * This method is using the regular rolling update mechanism. But in order to achieve the sequential rolling
         * update it calls it recursively with only subset of the pods being considered for rolling update. Thanks to
         * this, it achieves the right order regardless who is controller but still makes sure that the replicas are
         * in-sync.
         *
         * This is used only for StatefulSets rolling. PodSets do not need it.
         *
         * @param sts               StatefulSet which should be rolled
         * @param podNeedsRestart   Function to tell the rolling restart mechanism if given broker pod needs restart or not
         * @param nextPod           The sequence number of the next pod which should be considered for rolling
         * @param lastPod           Index of the last pod which should be considered for restart
         *
         * @return
         */
        Future<ReconciliationState> maybeRollKafkaInSequence(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, int nextPod, int lastPod) {
            if (nextPod <= lastPod)  {
                final int podToRoll = nextPod;

                return maybeRollKafka(sts.getSpec().getReplicas(), pod -> {
                    if (pod != null && pod.getMetadata().getName().endsWith("-" + podToRoll))    {
                        return podNeedsRestart.apply(pod);
                    } else {
                        return new ArrayList<>();
                    }
                }, false)
                        .compose(ignore -> maybeRollKafkaInSequence(sts, podNeedsRestart, nextPod + 1, lastPod));
            } else {
                // All pods checked for sequential RU => nothing more to do
                return withVoid(Future.succeededFuture());
            }
        }

        /**
         * Checks if any Kafka broker needs rolling update to add or remove JBOD volumes. If it does, we trigger a
         * sequential rolling update because the pods need to be rolled in sequence to add or remove volumes.
         *
         * @return
         */
        Future<ReconciliationState> kafkaRollToAddOrRemoveVolumes() {
            Storage storage = kafkaCluster.getStorage();

            // We do the special rolling update only when:
            //   * JBOD storage is actually used as storage
            //   * and StatefulSets are used
            // StrimziPodSets do not need special rolling update, they can add / remove volumes during regular rolling updates
            if (storage instanceof JbodStorage
                    && !featureGates.useStrimziPodSetsEnabled()) {
                JbodStorage jbodStorage = (JbodStorage) storage;
                return kafkaRollToAddOrRemoveVolumesInStatefulSet(jbodStorage);
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        /**
         * Checks if any Kafka broker needs rolling update to add or remove JBOD volumes. If it does, we trigger a
         * sequential rolling update because the pods need to be rolled in sequence to add or remove volumes.
         *
         * This method is used only for StatefulSets which require special rolling process.
         *
         * @param jbodStorage   Desired storage configuration
         *
         * @return              Future indicating the completion and result of the rolling update
         */
        Future<ReconciliationState> kafkaRollToAddOrRemoveVolumesInStatefulSet(JbodStorage jbodStorage) {
            // We first check if any broker actually needs the rolling update. Only if at least one of them needs it,
            // we trigger it. This check helps to not go through the rolling update if not needed.
            return podOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(pods -> {
                        for (Pod pod : pods) {
                            if (!needsRestartBecauseAddedOrRemovedJbodVolumes(pod, jbodStorage, kafkaCurrentReplicas, kafkaCluster.getReplicas()).isEmpty())   {
                                // At least one broker needs rolling update => we can trigger it without checking the other brokers
                                LOGGER.debugCr(reconciliation, "Kafka brokers needs rolling update to add or remove JBOD volumes");

                                return stsOperations.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name))
                                        .compose(sts -> {
                                            if (sts != null) {
                                                int lastPodIndex = Math.min(kafkaCurrentReplicas, kafkaCluster.getReplicas()) - 1;
                                                return maybeRollKafkaInSequence(sts, podToCheck -> needsRestartBecauseAddedOrRemovedJbodVolumes(podToCheck, jbodStorage, kafkaCurrentReplicas, kafkaCluster.getReplicas()), 0, lastPodIndex);
                                            } else {
                                                // STS does not exist => nothing to roll
                                                return withVoid(Future.succeededFuture());
                                            }
                                        });
                            }
                        }

                        LOGGER.debugCr(reconciliation, "No rolling update of Kafka brokers due to added or removed JBOD volumes is needed");
                        return withVoid(Future.succeededFuture());
                    });
        }

        /**
         * Checks whether the particular pod needs restart because of added or removed JBOD volumes. This method returns
         * a list instead of boolean so that it can be used directly in the rolling update procedure and log proper
         * reasons for the restart.
         *
         * @param pod               Broker pod which should be considered for restart
         * @param desiredStorage    Desired storage configuration
         * @param currentReplicas   Number of Kafka replicas before this reconciliation
         * @param desiredReplicas   Number of desired Kafka replicas
         *
         * @return  Empty list when no restart is needed. List containing the restart reason if the restart is needed.
         */
        private List<String> needsRestartBecauseAddedOrRemovedJbodVolumes(Pod pod, JbodStorage desiredStorage, int currentReplicas, int desiredReplicas)  {
            if (pod != null
                    && pod.getMetadata() != null) {
                String jsonStorage = Annotations.stringAnnotation(pod, ANNO_STRIMZI_IO_STORAGE, null);

                if (jsonStorage != null) {
                    Storage currentStorage = ModelUtils.decodeStorageFromJson(jsonStorage);

                    if (new StorageDiff(reconciliation, currentStorage, desiredStorage, currentReplicas, desiredReplicas).isVolumesAddedOrRemoved())    {
                        return singletonList("JBOD volumes were added or removed");
                    }
                }
            }

            return new ArrayList<>();
        }

        /**
         * Prepares annotations for Kafka pods which are known only in the KafkaAssemblyOperator level. These are later
         * passed to KafkaCluster where there are used when creating the Pod definitions.
         *
         * @return  Map with Pod annotations
         */
        Map<String, String> kafkaPodAnnotations(int brokerId, boolean storageAnnotation)    {
            Map<String, String> podAnnotations = new LinkedHashMap<>(9);
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(ModelUtils.caCertGeneration(this.clusterCa)));
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, String.valueOf(ModelUtils.caCertGeneration(this.clientsCa)));
            podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, kafkaLoggingAppendersHash);
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_BROKER_CONFIGURATION_HASH, kafkaBrokerConfigurationHash.get(brokerId));
            podAnnotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaCluster.getKafkaVersion().version());
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, kafkaCluster.getLogMessageFormatVersion());
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, kafkaCluster.getInterBrokerProtocolVersion());

            // Annotations with custom cert thumbprints to help with rolling updates when they change
            if (!customListenerCertificateThumbprints.isEmpty()) {
                podAnnotations.put(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, customListenerCertificateThumbprints.toString());
            }

            // Storage annotation on Pods is used only for StatefulSets
            if (storageAnnotation) {
                podAnnotations.put(ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(kafkaCluster.getStorage()));
            }

            return podAnnotations;
        }

        Future<ReconciliationState> kafkaStatefulSet() {
            if (!featureGates.useStrimziPodSetsEnabled()) {
                // StatefulSets are enabled => make sure the StatefulSet exists with the right settings
                StatefulSet kafkaSts = kafkaCluster.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, kafkaPodAnnotations(sharedConfigurationId, true));
                return withKafkaStsDiff(stsOperations.reconcile(reconciliation, namespace, kafkaCluster.getName(), kafkaSts));
            } else {
                // StatefulSets are disabled => delete the StatefulSet if it exists
                return stsOperations.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name))
                        .compose(sts -> {
                            if (sts != null)    {
                                return withVoid(stsOperations.deleteAsync(reconciliation, namespace, kafkaCluster.getName(), false));
                            } else {
                                return Future.succeededFuture(this);
                            }
                        });
            }
        }

        Future<ReconciliationState> kafkaPodSet() {
            if (featureGates.useStrimziPodSetsEnabled())   {
                // PodSets are enabled => create/update the StrimziPodSet for Kafka
                int replicas;

                if (kafkaCurrentReplicas != null
                        && kafkaCurrentReplicas != 0
                        && kafkaCurrentReplicas < kafkaCluster.getReplicas())  {
                    // If there is previous replica count & it is smaller than the desired replica count, we use the
                    // previous one because the scale-up will happen only later during the reconciliation
                    replicas = kafkaCurrentReplicas;
                } else {
                    // If there is no previous number of replicas (because the PodSet did not exist) or if the
                    // previous replicas are bigger than desired replicas we use desired replicas (scale-down already
                    // happened)
                    replicas = kafkaCluster.getReplicas();
                }

                StrimziPodSet kafkaPodSet = kafkaCluster.generatePodSet(replicas, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, brokerId -> kafkaPodAnnotations(brokerId, false));
                return withKafkaPodSetDiff(strimziPodSetOperator.reconcile(reconciliation, namespace, KafkaResources.kafkaStatefulSetName(name), kafkaPodSet));
            } else {
                // PodSets are disabled => delete the StrimziPodSet for Kafka
                return strimziPodSetOperator.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name))
                        .compose(podSet -> {
                            if (podSet != null)    {
                                return withVoid(strimziPodSetOperator.deleteAsync(reconciliation, namespace, kafkaCluster.getName(), false));
                            } else {
                                return Future.succeededFuture(this);
                            }
                        });
            }
        }

        Future<ReconciliationState> kafkaRollingUpdate() {
            if (featureGates.useStrimziPodSetsEnabled())   {
                return withVoid(maybeRollKafka(kafkaPodSetDiffs.resource().getSpec().getPods().size(), pod ->
                        ReconcilerUtils.reasonsToRestartPod(reconciliation, kafkaPodSetDiffs.resource(), pod, fsResizingRestartRequest, existingKafkaCertsChanged, this.clusterCa, this.clientsCa)));
            } else {
                return withVoid(maybeRollKafka(kafkaStsDiffs.resource().getSpec().getReplicas(), pod ->
                        ReconcilerUtils.reasonsToRestartPod(reconciliation, kafkaStsDiffs.resource(), pod, fsResizingRestartRequest, existingKafkaCertsChanged, this.clusterCa, this.clientsCa)));
            }
        }

        Future<ReconciliationState> kafkaScaleUp() {
            if (kafkaCurrentReplicas != null
                    && kafkaCurrentReplicas != 0
                    && kafkaCurrentReplicas < kafkaCluster.getReplicas()) {
                // The previous number of replicas is known and is smaller than desired number of replicas
                //   => we need to do scale-up
                LOGGER.infoCr(reconciliation, "Scaling Kafka up from {} to {} replicas", kafkaCurrentReplicas, kafkaCluster.getReplicas());
                
                if (featureGates.useStrimziPodSetsEnabled())   {
                    StrimziPodSet kafkaPodSet = kafkaCluster.generatePodSet(kafkaCluster.getReplicas(), pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, brokerId -> kafkaPodAnnotations(brokerId, false));
                    return withVoid(strimziPodSetOperator.reconcile(reconciliation, namespace, kafkaCluster.getName(), kafkaPodSet));
                } else {
                    return withVoid(stsOperations.scaleUp(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
                }
            } else {
                // Previous number of replicas is not known (because the PodSet os StatefulSet  did not exist) or is
                // bigger than desired replicas => nothing to do
                // (if the previous replica count was not known, the desired count was already used when patching
                // the pod set, so no need to do it again)
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> kafkaPodsReady() {
            return podsReady(kafkaCluster);
        }

        Future<ReconciliationState> podsReady(AbstractModel model) {
            int replicas = model.getReplicas();
            return podsReady(model, replicas);
        }

        Future<ReconciliationState> podsReady(AbstractModel model, int replicas) {
            List<Future> podFutures = new ArrayList<>(replicas);

            for (int i = 0; i < replicas; i++) {
                LOGGER.debugCr(reconciliation, "Checking readiness of pod {}.", model.getPodName(i));
                podFutures.add(podOperations.readiness(reconciliation, namespace, model.getPodName(i), 1_000, operationTimeoutMs));
            }

            return withVoid(CompositeFuture.join(podFutures));
        }

        Future<ReconciliationState> kafkaServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(reconciliation, namespace, kafkaCluster.getServiceName(), 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaHeadlessServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(reconciliation, namespace, kafkaCluster.getHeadlessServiceName(), 1_000, operationTimeoutMs));
        }

        /**
         * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
         *
         * @return
         */
        Future<ReconciliationState> kafkaManualPodCleaning() {
            return new ManualPodCleaner(reconciliation, KafkaResources.kafkaStatefulSetName(name),
                    kafkaCluster.getSelectorLabels(), operationTimeoutMs, featureGates.useStrimziPodSetsEnabled(),
                    stsOperations, strimziPodSetOperator, podOperations, pvcOperations)
                    .maybeManualPodCleaning(kafkaCluster.generatePersistentVolumeClaims(oldKafkaStorage))
                    .map(this);
        }

        /**
         * Deletion of PVCs after the cluster is deleted is handled by owner reference and garbage collection. However,
         * this would not help after scale-downs. Therefore we check if there are any PVCs which should not be present
         * and delete them when they are.
         *
         * This should be called only after the Statefulset reconciliation, rolling update and scale-down when the PVCs
         * are not used anymore by the pods.
         *
         * @return
         */
        Future<ReconciliationState> kafkaPersistentClaimDeletion() {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            Future<List<PersistentVolumeClaim>> futurePvcs = pvcOperations.listAsync(namespace, kafkaCluster.getSelectorLabels());

            futurePvcs.onComplete(res -> {
                if (res.succeeded() && res.result() != null)    {
                    List<String> maybeDeletePvcs = res.result().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = kafkaCluster.generatePersistentVolumeClaims(kafkaCluster.getStorage()).stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    new PvcReconciler(reconciliation, pvcOperations, storageClassOperator)
                            .deletePersistentClaims(maybeDeletePvcs, desiredPvcs)
                            .onComplete(r -> resultPromise.complete(this));
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return resultPromise.future();
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

            return withVoid(secretOperations.reconcile(reconciliation, namespace, ClusterOperator.secretName(name),
                    secret));
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

        void addListenerStatus(ListenerStatus ls)    {
            List<ListenerStatus> current = kafkaStatus.getListeners();
            ArrayList<ListenerStatus> desired;

            if (current != null) {
                desired = new ArrayList<>(current.size() + 1);
                desired.addAll(current);
            } else {
                desired = new ArrayList<>(1);
            }

            desired.add(ls);

            kafkaStatus.setListeners(desired);
        }

        Future<ReconciliationState> kafkaCustomCertificatesToStatus() {
            for (GenericKafkaListener listener : kafkaCluster.getListeners())   {
                if (listener.isTls())   {
                    LOGGER.debugCr(reconciliation, "Adding certificate to status for listener: {}", listener.getName());
                    addCertificateToListener(listener.getName(), customListenerCertificates.get(listener.getName()));
                }
            }

            return Future.succeededFuture(this);
        }

        void addCertificateToListener(String type, String certificate)    {
            if (certificate == null)    {
                // When custom certificate is not used, use the current CA certificate
                certificate = new String(clusterCa.currentCaCertBytes(), StandardCharsets.US_ASCII);
            }

            List<ListenerStatus> listeners = kafkaStatus.getListeners();

            if (listeners != null) {
                ListenerStatus listener = listeners.stream().filter(listenerType -> type.equals(listenerType.getName())).findFirst().orElse(null);

                if (listener != null) {
                    listener.setCertificates(singletonList(certificate));
                }
            }
        }

        String getInternalServiceHostname(String serviceName, boolean useServiceDnsDomain)    {
            if (useServiceDnsDomain)    {
                return DnsNameGenerator.serviceDnsNameWithClusterDomain(namespace, serviceName);
            } else {
                return DnsNameGenerator.serviceDnsNameWithoutClusterDomain(namespace, serviceName);
            }
        }

        /**
         * Utility method to extract current number of replicas from an existing StatefulSet
         *
         * @param sts   StatefulSet from which the replicas count should be extracted
         *
         * @return      Number of replicas
         */
        private Integer currentReplicas(StatefulSet sts)  {
            Integer replicas = null;

            if (sts != null && sts.getSpec() != null)   {
                replicas = sts.getSpec().getReplicas();
            }

            return replicas;
        }

        /**
         * Utility method to extract current number of replicas from an existing StrimziPodSet
         *
         * @param podSet    PodSet from which the replicas count should be extracted
         *
         * @return          Number of replicas
         */
        private Integer currentReplicas(StrimziPodSet podSet)  {
            Integer replicas = null;

            if (podSet != null && podSet.getSpec() != null && podSet.getSpec().getPods() != null)   {
                replicas = podSet.getSpec().getPods().size();
            }

            return replicas;
        }

        /**
         * Provider method for ZooKeeper reconciler. Overriding this method can be used to get mocked reconciler. This
         * method has to first collect some information about the current ZooKeeper cluster such as current storage
         * configuration or current number of replicas.
         *
         * @return  Future with ZooKeeper recocniler
         */
        Future<ZooKeeperReconciler> zooKeeperReconciler()   {
            Future<StatefulSet> stsFuture = stsOperations.getAsync(namespace, KafkaResources.zookeeperStatefulSetName(name));
            Future<StrimziPodSet> podSetFuture = strimziPodSetOperator.getAsync(namespace, KafkaResources.zookeeperStatefulSetName(name));

            return CompositeFuture.join(stsFuture, podSetFuture)
                    .compose(res -> {
                        StatefulSet sts = res.resultAt(0);
                        StrimziPodSet podSet = res.resultAt(1);

                        Integer currentReplicas = null;
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
                            // StatefulSet exists, PodSet does nto exist => we create the description from the StatefulSet
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
                                operationTimeoutMs,
                                kafkaAssembly,
                                versions,
                                versionChange,
                                oldStorage,
                                currentReplicas,
                                clusterCa,
                                operatorNamespace,
                                operatorNamespaceLabels,
                                isNetworkPolicyGeneration,
                                pfa,
                                featureGates,
                                zkAdminSessionTimeoutMs,
                                imagePullPolicy,
                                imagePullSecrets,

                                stsOperations,
                                strimziPodSetOperator,
                                secretOperations,
                                serviceAccountOperations,
                                serviceOperations,
                                pvcOperations,
                                storageClassOperator,
                                configMapOperations,
                                networkPolicyOperator,
                                podDisruptionBudgetOperator,
                                podDisruptionBudgetV1Beta1Operator,
                                podOperations,

                                zkScalerProvider,
                                zookeeperLeaderFinder
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
         * Provider method for Kafka Exporter reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  Kafka Exporter reconciler
         */
        KafkaExporterReconciler kafkaExporterReconciler()   {
            return new KafkaExporterReconciler(
                    reconciliation,
                    operationTimeoutMs,
                    kafkaAssembly,
                    versions,
                    clusterCa,
                    deploymentOperations,
                    secretOperations,
                    serviceAccountOperations
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
                    operationTimeoutMs,
                    kafkaAssembly,
                    deploymentOperations,
                    configMapOperations,
                    serviceAccountOperations
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
                    kafkaAssembly,
                    versions,
                    kafkaCluster.getStorage(),
                    clusterCa,
                    operationTimeoutMs,
                    operatorNamespace,
                    operatorNamespaceLabels,
                    isNetworkPolicyGeneration,
                    deploymentOperations,
                    secretOperations,
                    serviceAccountOperations,
                    serviceOperations,
                    networkPolicyOperator,
                    configMapOperations
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
                    operationTimeoutMs,
                    kafkaAssembly,
                    versions,
                    clusterCa,
                    deploymentOperations,
                    secretOperations,
                    serviceAccountOperations,
                    roleOperations,
                    roleBindingOperations,
                    configMapOperations
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
