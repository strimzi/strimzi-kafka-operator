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
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
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
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaSpec;
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
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.EntityTopicOperator;
import io.strimzi.operator.cluster.model.EntityUserOperator;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.JmxTrans;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.model.StorageDiff;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.KafkaSpecChecker;
import io.strimzi.operator.cluster.operator.resource.PodRevision;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZooKeeperRoller;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScaler;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator;
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
import org.quartz.CronExpression;

import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.text.ParseException;
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
import java.util.TimeZone;
import java.util.function.Function;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG;
import static io.strimzi.operator.cluster.model.AbstractModel.ANNO_STRIMZI_IO_STORAGE;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedIVVersions;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedVersions;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.emptyMap;

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

    Future<Void> reconcile(ReconciliationState reconcileState)  {
        Promise<Void> chainPromise = Promise.promise();

        reconcileState.initialStatus()
                .compose(state -> state.reconcileCas(this::dateSupplier))
                .compose(state -> state.clusterOperatorSecret(this::dateSupplier))
                .compose(state -> state.getKafkaClusterDescription())
                .compose(state -> state.getZookeeperDescription()) // Has to be before the rollingUpdateForNewCaKey
                .compose(state -> state.prepareVersionChange())
                // Roll everything if a new CA is added to the trust store.
                .compose(state -> state.rollingUpdateForNewCaKey())

                .compose(state -> state.zkModelWarnings())
                .compose(state -> state.zkJmxSecret())
                .compose(state -> state.zkManualPodCleaning())
                .compose(state -> state.zkNetPolicy())
                .compose(state -> state.zkManualRollingUpdate())
                .compose(state -> state.zkVersionChange())
                .compose(state -> state.zookeeperServiceAccount())
                .compose(state -> state.zkPvcs())
                .compose(state -> state.zkService())
                .compose(state -> state.zkHeadlessService())
                .compose(state -> state.zkGenerateCertificates(this::dateSupplier))
                .compose(state -> state.zkAncillaryCm())
                .compose(state -> state.zkNodesSecret())
                .compose(state -> state.zkPodDisruptionBudget())
                .compose(state -> state.zkPodDisruptionBudgetV1Beta1())
                .compose(state -> state.zkStatefulSet())
                .compose(state -> state.zkPodSet())
                .compose(state -> state.zkScalingDown())
                .compose(state -> state.zkRollingUpdate())
                .compose(state -> state.zkPodsReady())
                .compose(state -> state.zkScalingUp())
                .compose(state -> state.zkScalingCheck())
                .compose(state -> state.zkServiceEndpointReadiness())
                .compose(state -> state.zkHeadlessServiceEndpointReadiness())
                .compose(state -> state.zkPersistentClaimDeletion())

                .compose(state -> state.checkKafkaSpec())
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
                .compose(state -> state.kafkaCustomCertificatesToStatus())

                .compose(state -> state.getEntityOperatorDescription())
                .compose(state -> state.entityOperatorRole())
                .compose(state -> state.entityTopicOperatorRole())
                .compose(state -> state.entityUserOperatorRole())
                .compose(state -> state.entityOperatorServiceAccount())
                .compose(state -> state.entityOperatorTopicOpRoleBindingForRole())
                .compose(state -> state.entityOperatorUserOpRoleBindingForRole())
                .compose(state -> state.entityOperatorTopicOpAncillaryCm())
                .compose(state -> state.entityOperatorUserOpAncillaryCm())
                .compose(state -> state.entityOperatorSecret())
                .compose(state -> state.entityTopicOperatorSecret(this::dateSupplier))
                .compose(state -> state.entityUserOperatorSecret(this::dateSupplier))
                .compose(state -> state.entityOperatorDeployment())
                .compose(state -> state.entityOperatorReady())

                .compose(state -> state.getCruiseControlDescription())
                .compose(state -> state.cruiseControlNetPolicy())
                .compose(state -> state.cruiseControlServiceAccount())
                .compose(state -> state.cruiseControlAncillaryCm())
                .compose(state -> state.cruiseControlSecret(this::dateSupplier))
                .compose(state -> state.cruiseControlApiSecret())
                .compose(state -> state.cruiseControlDeployment())
                .compose(state -> state.cruiseControlService())
                .compose(state -> state.cruiseControlReady())

                .compose(state -> state.getKafkaExporterDescription())
                .compose(state -> state.kafkaExporterServiceAccount())
                .compose(state -> state.kafkaExporterSecret(this::dateSupplier))
                .compose(state -> state.kafkaExporterDeployment())
                .compose(state -> state.kafkaExporterReady())

                .compose(state -> state.getJmxTransDescription())
                .compose(state -> state.jmxTransServiceAccount())
                .compose(state -> state.jmxTransConfigMap())
                .compose(state -> state.jmxTransDeployment())
                .compose(state -> state.jmxTransDeploymentReady())

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

        /* test */ ZookeeperCluster zkCluster;
        private ConfigMap zkMetricsAndLogsConfigMap;
        private ReconcileResult<StatefulSet> zkStsDiffs;
        private ReconcileResult<StrimziPodSet> zkPodSetDiffs;
        private Integer zkCurrentReplicas = null;

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

        private String zkLoggingHash = "";
        private String kafkaLogging = "";
        private String kafkaLoggingAppendersHash = "";
        private Map<Integer, String> kafkaBrokerConfigurationHash = new HashMap<>();
        @SuppressFBWarnings(value = "SS_SHOULD_BE_STATIC", justification = "Field cannot be static in inner class in Java 11")
        private final int sharedConfigurationId = -1; // "Fake" broker ID used to indicate hash stored for all brokers when shared configuration is used

        /* test */ EntityOperator entityOperator;
        /* test */ Deployment eoDeployment = null;
        private ConfigMap topicOperatorMetricsAndLogsConfigMap = null;
        private ConfigMap userOperatorMetricsAndLogsConfigMap;
        private Secret oldCoSecret;

        CruiseControl cruiseControl;
        Deployment ccDeployment = null;
        private ConfigMap cruiseControlMetricsAndLogsConfigMap;

        /* test */ KafkaExporter kafkaExporter;
        /* test */ Deployment exporterDeployment = null;

        /* test */ Set<String> fsResizingRestartRequest = new HashSet<>();

        // Certificate change indicators
        private boolean existingZookeeperCertsChanged = false;
        private boolean existingKafkaCertsChanged = false;
        private boolean existingKafkaExporterCertsChanged = false;
        private boolean existingEntityTopicOperatorCertsChanged = false;
        private boolean existingEntityUserOperatorCertsChanged = false;
        private boolean existingCruiseControlCertsChanged = false;

        // Custom Listener certificates
        private final Map<String, String> customListenerCertificates = new HashMap<>();
        private final Map<String, String> customListenerCertificateThumbprints = new HashMap<>();

        private JmxTrans jmxTrans = null;
        private ConfigMap jmxTransConfigMap = null;
        private Deployment jmxTransDeployment = null;

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
            KafkaSpecChecker checker = new KafkaSpecChecker(kafkaAssembly.getSpec(), versions, kafkaCluster, zkCluster);
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
         * Takes the warning conditions from the Model and adds them in the KafkaStatus
         */
        Future<ReconciliationState> zkModelWarnings() {
            kafkaStatus.addConditions(zkCluster.getWarningConditions());
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
                            } else if (secretName.equals(KafkaCluster.brokersSecretName(name))) {
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
                                isMaintenanceTimeWindowsSatisfied(dateSupplier));

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
                                isMaintenanceTimeWindowsSatisfied(dateSupplier));

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
                    zkRollFuture = maybeRollZooKeeper(rollPodAndLogReason, clusterCa.caCertSecret(), oldCoSecret);
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
                                                return Future.succeededFuture(new ArrayList<String>());
                                            }
                                        });
                            } else {
                                return stsOperations.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name))
                                        .compose(sts -> {
                                            if (sts != null)    {
                                                return Future.succeededFuture(KafkaCluster.generatePodList(reconciliation.name(), sts.getSpec().getReplicas()));
                                            } else {
                                                return Future.succeededFuture(new ArrayList<String>());
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
                                return rollDeploymentIfExists(EntityOperator.entityOperatorName(name), reason.toString())
                                        .compose(i2 -> rollDeploymentIfExists(KafkaExporter.kafkaExporterName(name), reason.toString()))
                                        .compose(i2 -> rollDeploymentIfExists(CruiseControl.cruiseControlName(name), reason.toString()));
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
         * Does rolling update of Zoo pods based on the annotation on Pod level
         *
         * @return  Future with the result of the rolling update
         */
        Future<Void> zkManualPodRollingUpdate() {
            return podOperations.listAsync(namespace, zkCluster.getSelectorLabels())
                    .compose(pods -> {
                        List<String> podsToRoll = new ArrayList<>(0);

                        for (Pod pod : pods)    {
                            if (Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                                podsToRoll.add(pod.getMetadata().getName());
                            }
                        }

                        if (!podsToRoll.isEmpty())  {
                            return maybeRollZooKeeper(pod -> {
                                if (pod != null && podsToRoll.contains(pod.getMetadata().getName())) {
                                    LOGGER.debugCr(reconciliation, "Rolling ZooKeeper pod {} due to manual rolling update annotation on a pod", pod.getMetadata().getName());
                                    return singletonList("manual rolling update annotation on a pod");
                                } else {
                                    return null;
                                }
                            });
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }

        /**
         * Does manual rolling update of Zoo pods based on an annotation on the StatefulSet or on the Pods. Annotation
         * on StatefulSet level triggers rolling update of all pods. Annotation on pods triggers rolling update only of
         * the selected pods. If the annotation is present on both StatefulSet and one or more pods, only one rolling
         * update of all pods occurs.
         *
         * @return  Future with the result of the rolling update
         */
        Future<ReconciliationState> zkManualRollingUpdate() {
            Future<HasMetadata> futureController;
            if (featureGates.useStrimziPodSetsEnabled())   {
                futureController = strimziPodSetOperator.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name)).map(podSet -> (HasMetadata) podSet);
            } else {
                futureController = stsOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name)).map(sts -> (HasMetadata) sts);
            }

            return futureController.compose(controller -> {
                if (controller != null
                        && Annotations.booleanAnnotation(controller, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                    // User trigger rolling update of the whole cluster
                    return maybeRollZooKeeper(pod -> {
                        LOGGER.debugCr(reconciliation, "Rolling Zookeeper pod {} due to manual rolling update", pod.getMetadata().getName());
                        return singletonList("manual rolling update");
                    });
                } else {
                    // The controller does not exist or is not annotated
                    // But maybe the individual pods are annotated to restart only some of them.
                    return zkManualPodRollingUpdate();
                }
            }).map(i -> this);
        }

        Future<ReconciliationState> zkVersionChange() {
            if (versionChange.isNoop()) {
                LOGGER.debugCr(reconciliation, "Kafka.spec.kafka.version is unchanged therefore no change to Zookeeper is required");
            } else {
                String versionChangeType;

                if (versionChange.isDowngrade()) {
                    versionChangeType = "downgrade";
                } else {
                    versionChangeType = "upgrade";
                }

                if (versionChange.requiresZookeeperChange()) {
                    LOGGER.infoCr(reconciliation, "Kafka {} from {} to {} requires Zookeeper {} from {} to {}",
                            versionChangeType,
                            versionChange.from().version(),
                            versionChange.to().version(),
                            versionChangeType,
                            versionChange.from().zookeeperVersion(),
                            versionChange.to().zookeeperVersion());
                } else {
                    LOGGER.infoCr(reconciliation, "Kafka {} from {} to {} requires no change in Zookeeper version",
                            versionChangeType,
                            versionChange.from().version(),
                            versionChange.to().version());

                }

                // Get the zookeeper image currently set in the Kafka CR or, if that is not set, the image from the target Kafka version
                String newZkImage = versions.kafkaImage(kafkaAssembly.getSpec().getZookeeper().getImage(), versionChange.to().version());
                LOGGER.debugCr(reconciliation, "Setting new Zookeeper image: " + newZkImage);
                this.zkCluster.setImage(newZkImage);
            }

            return Future.succeededFuture(this);
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
         * Returns a composite future with two results => one with the Cluster CA secret and one with the
         * Cluster Operator secret. These are used for the Kafka Admin client.
         *
         * @return
         */
        protected CompositeFuture adminClientSecrets() {
            return CompositeFuture.join(
                    getSecret(namespace, KafkaResources.clusterCaCertificateSecretName(name)),
                    getSecret(namespace, ClusterOperator.secretName(name))
            );
        }

        /**
         * Returns a composite future with two results => one with the Cluster CA secret and one with the
         * Cluster Operator secret. These are used by the ZooKeeper clients => in leader finder and in scaler.
         *
         * @return
         */
        protected CompositeFuture zooKeeperClientSecrets() {
            return CompositeFuture.join(
                    getSecret(namespace, KafkaResources.clusterCaCertificateSecretName(name)),
                    getSecret(namespace, ClusterOperator.secretName(name))
            );
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
            return adminClientSecrets()
                .compose(compositeFuture ->
                        new KafkaRoller(
                                reconciliation,
                                vertx,
                                podOperations,
                                1_000,
                                operationTimeoutMs,
                                () -> new BackOff(250, 2, 10),
                                replicas,
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

        /**
         * Checks if the ZooKeeper cluster needs rolling and if it does, it will roll it.
         *
         * @param podNeedsRestart   Function to determine if the ZooKeeper pod needs to be restarted
         *
         * @return
         */
        Future<Void> maybeRollZooKeeper(Function<Pod, List<String>> podNeedsRestart) {
            return zooKeeperClientSecrets()
                    .compose(compositeFuture -> {
                        Secret clusterCaCertSecret = compositeFuture.resultAt(0);
                        Secret coKeySecret = compositeFuture.resultAt(1);

                        return maybeRollZooKeeper(podNeedsRestart, clusterCaCertSecret, coKeySecret);
                    });
        }

        /**
         * Checks if the ZooKeeper cluster needs rolling and if it does, it will roll it.
         *
         * @param podNeedsRestart       Function to determine if the ZooKeeper pod needs to be restarted
         * @param clusterCaCertSecret   Secret with the Cluster CA certificates
         * @param coKeySecret           Secret with the Cluster Operator certificates
         *
         * @return
         */
        Future<Void> maybeRollZooKeeper(Function<Pod, List<String>> podNeedsRestart, Secret clusterCaCertSecret, Secret coKeySecret) {
            return new ZooKeeperRoller(podOperations, zookeeperLeaderFinder, operationTimeoutMs)
                    .maybeRollingUpdate(reconciliation, zkCluster.getSelectorLabels(), podNeedsRestart, clusterCaCertSecret, coKeySecret);
        }

        Future<ReconciliationState> getZookeeperDescription() {
            return getZookeeperSetDescription()
                    .compose(ignore -> Util.metricsAndLogging(reconciliation, configMapOperations, kafkaAssembly.getMetadata().getNamespace(), zkCluster.getLogging(), zkCluster.getMetricsConfigInCm()))
                    .compose(metricsAndLogging -> {
                        ConfigMap logAndMetricsConfigMap = zkCluster.generateConfigurationConfigMap(metricsAndLogging);
                        this.zkMetricsAndLogsConfigMap = logAndMetricsConfigMap;

                        String loggingConfiguration = zkMetricsAndLogsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
                        this.zkLoggingHash = Util.hashStub(loggingConfiguration);

                        return Future.succeededFuture(this);
                    });
        }

        /**
         * Gets the ZooKeeper cluster description. It checks whether StatefulSets or StrimziPodSets are used and uses the appropriate method to
         *
         * @return
         */
        Future<Void> getZookeeperSetDescription()   {
            Future<StatefulSet> stsFuture = stsOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name));
            Future<StrimziPodSet> podSetFuture = strimziPodSetOperator.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name));

            return CompositeFuture.join(stsFuture, podSetFuture)
                    .compose(res -> {
                        StatefulSet sts = res.resultAt(0);
                        StrimziPodSet podSet = res.resultAt(1);

                        if (sts != null && podSet != null)  {
                            // Both StatefulSet and PodSet exist => we create the description based on the feature gate
                            if (featureGates.useStrimziPodSetsEnabled())    {
                                zookeeperPodSetDescription(podSet);
                            } else {
                                zookeeperStatefulSetDescription(sts);
                            }
                        } else if (sts != null) {
                            // StatefulSet exists, PodSet does nto exist => we create the description from the StatefulSet
                            zookeeperStatefulSetDescription(sts);
                        } else if (podSet != null) {
                            //PodSet exists, StatefulSet does not => we create the description from the PodSet
                            zookeeperPodSetDescription(podSet);
                        } else {
                            // Neither StatefulSet nor PodSet exists => we just create the ZookeeperCluster instance
                            this.zkCluster = ZookeeperCluster.fromCrd(reconciliation, kafkaAssembly, versions, null, 0);
                        }

                        return Future.succeededFuture();
                    });
        }

        /**
         * Initializes the ZooKeeper description based on a StatefulSet.
         */
        void zookeeperStatefulSetDescription(StatefulSet sts) {
            Storage oldStorage = getOldStorage(sts);

            if (sts != null && sts.getSpec() != null)   {
                this.zkCurrentReplicas = sts.getSpec().getReplicas();
            }

            this.zkCluster = ZookeeperCluster.fromCrd(reconciliation, kafkaAssembly, versions, oldStorage, zkCurrentReplicas != null ? zkCurrentReplicas : 0);

            // We are upgrading from previous Strimzi version which has a sidecars. The older sidecar
            // configurations allowed only older versions of TLS to be used by default. But the Zookeeper
            // native TLS support enabled by default only secure TLSv1.2. That is correct, but makes the
            // upgrade hard since Kafka will be unable to connect. So in the first roll, we enable also
            // older TLS versions in Zookeeper so that we can configure the Kafka sidecars to enable
            // TLSv1.2 as well. This will be removed again in the next rolling update of Zookeeper -> done
            // only when Kafka is ready for it.
            if (sts != null
                    && sts.getSpec() != null
                    && sts.getSpec().getTemplate().getSpec().getContainers().size() > 1)   {
                zkCluster.getConfiguration().setConfigOption("ssl.protocol", "TLS");
                zkCluster.getConfiguration().setConfigOption("ssl.enabledProtocols", "TLSv1.2,TLSv1.1,TLSv1");
            }
        }

        /**
         * Initializes the ZooKeeper description based on a StrimziPodSet.
         */
        void zookeeperPodSetDescription(StrimziPodSet podSet) {
            Storage oldStorage = getOldStorage(podSet);

            if (podSet != null && podSet.getSpec() != null)   {
                this.zkCurrentReplicas = podSet.getSpec().getPods().size();
            }

            this.zkCluster = ZookeeperCluster.fromCrd(reconciliation, kafkaAssembly, versions, oldStorage, zkCurrentReplicas != null ? zkCurrentReplicas : 0);
        }

        Future<ReconciliationState> withZkStsDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.zkStsDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> withZkPodSetDiff(Future<ReconcileResult<StrimziPodSet>> r) {
            return r.map(rr -> {
                this.zkPodSetDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> withVoid(Future<?> r) {
            return r.map(this);
        }

        Future<ReconciliationState> zookeeperServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    ZookeeperCluster.containerServiceAccountName(zkCluster.getCluster()),
                    zkCluster.generateServiceAccount()));
        }

        Future<ReconciliationState> zkService() {
            return withVoid(serviceOperations.reconcile(reconciliation, namespace, zkCluster.getServiceName(), zkCluster.generateService()));
        }

        Future<ReconciliationState> zkHeadlessService() {
            return withVoid(serviceOperations.reconcile(reconciliation, namespace, zkCluster.getHeadlessServiceName(), zkCluster.generateHeadlessService()));
        }

        Future<ReconciliationState> zkAncillaryCm() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace, zkCluster.getAncillaryConfigMapName(), zkMetricsAndLogsConfigMap));
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
                                    // The secret is patched and some changes to the existing certificates actually occured
                                    return ModelUtils.doExistingCertificatesDiffer(oldSecret, res.resource());
                                }

                                return false;
                            })
                    );
        }

        Future<ReconciliationState> zkNodesSecret() {
            return updateCertificateSecretWithDiff(ZookeeperCluster.nodesSecretName(name), zkCluster.generateNodesSecret(clusterCa))
                    .map(changed -> {
                        existingZookeeperCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> zkJmxSecret() {
            if (zkCluster.isJmxAuthenticated()) {
                Future<Secret> secretFuture = secretOperations.getAsync(namespace, ZookeeperCluster.jmxSecretName(name));
                return secretFuture.compose(secret -> {
                    if (secret == null) {
                        return withVoid(secretOperations.reconcile(reconciliation, namespace, ZookeeperCluster.jmxSecretName(name),
                                zkCluster.generateJmxSecret()));
                    }
                    return withVoid(Future.succeededFuture(ReconcileResult.noop(secret)));
                });
            }
            return withVoid(secretOperations.reconcile(reconciliation, namespace, ZookeeperCluster.jmxSecretName(name), null));
        }

        Future<ReconciliationState> zkNetPolicy() {
            if (isNetworkPolicyGeneration) {
                return withVoid(networkPolicyOperator.reconcile(reconciliation, namespace, ZookeeperCluster.policyName(name), zkCluster.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels)));
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        Future<ReconciliationState> zkPodDisruptionBudget() {
            if (!pfa.hasPodDisruptionBudgetV1()) {
                return Future.succeededFuture(this);
            }

            if (featureGates.useStrimziPodSetsEnabled())   {
                return withVoid(podDisruptionBudgetOperator.reconcile(reconciliation, namespace, zkCluster.getName(), zkCluster.generateCustomControllerPodDisruptionBudget()));
            } else {
                return withVoid(podDisruptionBudgetOperator.reconcile(reconciliation, namespace, zkCluster.getName(), zkCluster.generatePodDisruptionBudget()));
            }
        }

        Future<ReconciliationState> zkPodDisruptionBudgetV1Beta1() {
            if (pfa.hasPodDisruptionBudgetV1()) {
                return Future.succeededFuture(this);
            }
            
            if (featureGates.useStrimziPodSetsEnabled())   {
                return withVoid(podDisruptionBudgetV1Beta1Operator.reconcile(reconciliation, namespace, zkCluster.getName(), zkCluster.generateCustomControllerPodDisruptionBudgetV1Beta1()));
            } else {
                return withVoid(podDisruptionBudgetV1Beta1Operator.reconcile(reconciliation, namespace, zkCluster.getName(), zkCluster.generatePodDisruptionBudgetV1Beta1()));
            }
        }

        Future<ReconciliationState> zkStatefulSet() {
            if (featureGates.useStrimziPodSetsEnabled())   {
                // StatefulSets are disabled => delete the StatefulSet if it exists
                return stsOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                        .compose(sts -> {
                            if (sts != null)    {
                                return withVoid(stsOperations.deleteAsync(reconciliation, namespace, zkCluster.getName(), false));
                            } else {
                                return Future.succeededFuture(this);
                            }
                        });
            } else {
                // StatefulSets are enabled => make sure the StatefulSet exists with the right settings
                StatefulSet zkSts = zkCluster.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
                Annotations.annotations(zkSts.getSpec().getTemplate()).put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(getCaCertGeneration(this.clusterCa)));
                Annotations.annotations(zkSts.getSpec().getTemplate()).put(Annotations.ANNO_STRIMZI_LOGGING_HASH, zkLoggingHash);
                return withZkStsDiff(stsOperations.reconcile(reconciliation, namespace, zkCluster.getName(), zkSts));
            }
        }

        /**
         * Create the StrimziPodSet for the ZooKeeper cluster with the default number of pods. That means either the
         * number of pods the pod set had before or the number of pods based on the Kafka CR if this is a new cluster.
         * Scale-up and scale-down are down separately.
         *
         * @return
         */
        Future<ReconciliationState> zkPodSet() {
            int replicas = zkCurrentReplicas != null ? zkCurrentReplicas : zkCluster.getReplicas();
            return zkPodSet(replicas);
        }

        /**
         * Create the StrimziPodSet for the ZooKeeper cluster with a specific number of pods. This is used directly
         * during scale-ups or scale-downs.
         *
         * @return
         */
        Future<ReconciliationState> zkPodSet(int replicas) {
            if (featureGates.useStrimziPodSetsEnabled())   {
                Map<String, String> podAnnotations = new LinkedHashMap<>(2);
                podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(getCaCertGeneration(this.clusterCa)));
                podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_HASH, zkLoggingHash);

                StrimziPodSet zkPodSet = zkCluster.generatePodSet(replicas, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, podAnnotations);
                return withZkPodSetDiff(strimziPodSetOperator.reconcile(reconciliation, namespace, zkCluster.getName(), zkPodSet));
            } else {
                return strimziPodSetOperator.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                        .compose(podSet -> {
                            if (podSet != null)    {
                                return withVoid(strimziPodSetOperator.deleteAsync(reconciliation, namespace, zkCluster.getName(), false));
                            } else {
                                return Future.succeededFuture(this);
                            }
                        });
            }
        }

        Future<ReconciliationState> zkRollingUpdate() {
            if (featureGates.useStrimziPodSetsEnabled())   {
                return maybeRollZooKeeper(pod -> getReasonsToRestartPod(zkPodSetDiffs.resource(), pod, existingZookeeperCertsChanged, this.clusterCa))
                        .map(this);
            } else {
                return maybeRollZooKeeper(pod -> getReasonsToRestartPod(zkStsDiffs.resource(), pod, existingZookeeperCertsChanged, this.clusterCa))
                        .map(this);
            }
        }

        /**
         * Prepares the Zookeeper connectionString
         * The format is host1:port1,host2:port2,...
         *
         * Used by the Zookeeper 3.5 Admin client for scaling
         *
         * @param connectToReplicas     Number of replicas from the ZK STS which should be used
         * @return                      The generated Zookeeper connection string
         */
        String zkConnectionString(int connectToReplicas, Function<Integer, String> zkNodeAddress)  {
            // Prepare Zoo connection string. We want to connect only to nodes which existed before
            // scaling and will exist after it is finished
            List<String> zooNodes = new ArrayList<>(connectToReplicas);

            for (int i = 0; i < connectToReplicas; i++)   {
                zooNodes.add(String.format("%s:%d",
                        zkNodeAddress.apply(i),
                        ZookeeperCluster.CLIENT_TLS_PORT));
            }

            return  String.join(",", zooNodes);
        }

        /**
         * Helper method for getting the required secrets with certificates and creating the ZookeeperScaler instance
         * for the given cluster. The ZookeeperScaler instance created by this method should be closed manually after
         * it is not used anymore.
         *
         * @param connectToReplicas     Number of pods from the Zookeeper STS which the scaler should use
         * @return                      Zookeeper scaler instance.
         */
        Future<ZookeeperScaler> zkScaler(int connectToReplicas)  {
            return zooKeeperClientSecrets()
                    .compose(compositeFuture -> {
                        Secret clusterCaCertSecret = compositeFuture.resultAt(0);
                        Secret coKeySecret = compositeFuture.resultAt(1);

                        Function<Integer, String> zkNodeAddress = (Integer i) ->
                                DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace,
                                        KafkaResources.zookeeperHeadlessServiceName(name), zkCluster.getPodName(i));

                        ZookeeperScaler zkScaler = zkScalerProvider.createZookeeperScaler(
                                reconciliation, vertx, zkConnectionString(connectToReplicas, zkNodeAddress), zkNodeAddress,
                                clusterCaCertSecret, coKeySecret, operationTimeoutMs, zkAdminSessionTimeoutMs);

                        return Future.succeededFuture(zkScaler);
                    });
        }

        Future<ReconciliationState> zkScalingUp() {
            int desired = zkCluster.getReplicas();

            if (zkCurrentReplicas != null
                    && zkCurrentReplicas < desired) {
                LOGGER.infoCr(reconciliation, "Scaling Zookeeper up from {} to {} replicas", zkCurrentReplicas, desired);

                return zkScaler(zkCurrentReplicas)
                        .compose(zkScaler -> {
                            Promise<ReconciliationState> scalingPromise = Promise.promise();

                            zkScalingUpByOne(zkScaler, zkCurrentReplicas, desired)
                                    .onComplete(res -> {
                                        zkScaler.close();

                                        if (res.succeeded())    {
                                            scalingPromise.complete(res.result());
                                        } else {
                                            LOGGER.warnCr(reconciliation, "Failed to scale Zookeeper", res.cause());
                                            scalingPromise.fail(res.cause());
                                        }
                                    });

                            return scalingPromise.future();
                        });
            } else {
                // No scaling up => do nothing
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScalingUpByOne(ZookeeperScaler zkScaler, int current, int desired) {
            if (current < desired) {
                return zkScaleUpStatefulSetOrPodSet(current + 1)
                        .compose(ignore -> podOperations.readiness(reconciliation, namespace, zkCluster.getPodName(current), 1_000, operationTimeoutMs))
                        .compose(ignore -> zkScaler.scale(current + 1))
                        .compose(ignore -> zkScalingUpByOne(zkScaler, current + 1, desired));
            } else {
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScaleUpStatefulSetOrPodSet(int desiredScale)   {
            if (featureGates.useStrimziPodSetsEnabled())   {
                return zkPodSet(desiredScale);
            } else {
                return withVoid(stsOperations.scaleUp(reconciliation, namespace, zkCluster.getName(), desiredScale));
            }
        }

        Future<ReconciliationState> zkScalingDown() {
            int desired = zkCluster.getReplicas();

            if (zkCurrentReplicas != null
                    && zkCurrentReplicas > desired) {
                // With scaling
                LOGGER.infoCr(reconciliation, "Scaling Zookeeper down from {} to {} replicas", zkCurrentReplicas, desired);

                // No need to check for pod readiness since we run right after the readiness check
                return zkScaler(desired)
                        .compose(zkScaler -> {
                            Promise<ReconciliationState> scalingPromise = Promise.promise();

                            zkScalingDownByOne(zkScaler, zkCurrentReplicas, desired)
                                    .onComplete(res -> {
                                        zkScaler.close();

                                        if (res.succeeded())    {
                                            scalingPromise.complete(res.result());
                                        } else {
                                            LOGGER.warnCr(reconciliation, "Failed to scale Zookeeper", res.cause());
                                            scalingPromise.fail(res.cause());
                                        }
                                    });

                            return scalingPromise.future();
                        });
            } else {
                // No scaling down => do nothing
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScalingDownByOne(ZookeeperScaler zkScaler, int current, int desired) {
            if (current > desired) {
                return podsReady(zkCluster, current - 1)
                        .compose(ignore -> zkScaler.scale(current - 1))
                        .compose(ignore -> zkScaleDownStatefulSetOrPodSet(current - 1))
                        .compose(ignore -> zkScalingDownByOne(zkScaler, current - 1, desired));
            } else {
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScaleDownStatefulSetOrPodSet(int desiredScale)   {
            if (featureGates.useStrimziPodSetsEnabled())   {
                return zkPodSet(desiredScale)
                        // We wait for the pod to be deleted, otherwise it might disrupt the rolling update
                        .compose(ignore -> podOperations.waitFor(
                                reconciliation,
                                namespace,
                                ZookeeperCluster.zookeeperPodName(name, desiredScale),
                                "to be deleted",
                                1_000L,
                                operationTimeoutMs,
                                (podNamespace, podName) -> podOperations.get(podNamespace, podName) == null)
                        ).map(this);
            } else {
                return withVoid(stsOperations.scaleDown(reconciliation, namespace, zkCluster.getName(), desiredScale));
            }
        }

        Future<ReconciliationState> zkScalingCheck() {
            // No scaling, but we should check the configuration
            // This can cover any previous failures in the Zookeeper reconfiguration
            LOGGER.debugCr(reconciliation, "Verifying that Zookeeper is configured to run with {} replicas", zkCurrentReplicas);

            // No need to check for pod readiness since we run right after the readiness check
            return zkScaler(zkCluster.getReplicas())
                    .compose(zkScaler -> {
                        Promise<ReconciliationState> scalingPromise = Promise.promise();

                        zkScaler.scale(zkCluster.getReplicas()).onComplete(res -> {
                            zkScaler.close();

                            if (res.succeeded())    {
                                scalingPromise.complete(this);
                            } else {
                                LOGGER.warnCr(reconciliation, "Failed to verify Zookeeper configuration", res.cause());
                                scalingPromise.fail(res.cause());
                            }
                        });

                        return scalingPromise.future();
                    });
        }

        Future<ReconciliationState> zkServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(reconciliation, namespace, zkCluster.getServiceName(), 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> zkHeadlessServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(reconciliation, namespace, zkCluster.getHeadlessServiceName(), 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> zkGenerateCertificates(Supplier<Date> dateSupplier) {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        zkCluster.generateCertificates(kafkaAssembly, clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier));
                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                },
                true,
                resultPromise);
            return resultPromise.future();
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
            return adminClientSecrets()
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
                                isMaintenanceTimeWindowsSatisfied(dateSupplier));
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
            return updateCertificateSecretWithDiff(KafkaCluster.brokersSecretName(name), kafkaCluster.generateBrokersSecret(clusterCa, clientsCa))
                    .map(changed -> {
                        existingKafkaCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> kafkaJmxSecret() {
            if (kafkaCluster.isJmxAuthenticated()) {
                Future<Secret> secretFuture = secretOperations.getAsync(namespace, KafkaCluster.jmxSecretName(name));
                return secretFuture.compose(res -> {
                    if (res == null) {
                        return withVoid(secretOperations.reconcile(reconciliation, namespace, KafkaCluster.jmxSecretName(name),
                                kafkaCluster.generateJmxSecret()));
                    }
                    return withVoid(Future.succeededFuture(this));
                });

            }
            return withVoid(secretOperations.reconcile(reconciliation, namespace, KafkaCluster.jmxSecretName(name), null));
        }

        Future<ReconciliationState> kafkaNetPolicy() {
            if (isNetworkPolicyGeneration) {
                return withVoid(networkPolicyOperator.reconcile(reconciliation, namespace, KafkaCluster.networkPolicyName(name), kafkaCluster.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels)));
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

        int getPodIndexFromPvcName(String pvcName)  {
            return Integer.parseInt(pvcName.substring(pvcName.lastIndexOf("-") + 1));
        }

        int getPodIndexFromPodName(String podName)  {
            return Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
        }

        Future<ReconciliationState> maybeResizeReconcilePvcs(List<PersistentVolumeClaim> pvcs, AbstractModel cluster) {
            List<Future> futures = new ArrayList<>(pvcs.size());

            for (PersistentVolumeClaim desiredPvc : pvcs)  {
                Promise<Void> resultPromise = Promise.promise();

                pvcOperations.getAsync(namespace, desiredPvc.getMetadata().getName()).onComplete(res -> {
                    if (res.succeeded())    {
                        PersistentVolumeClaim currentPvc = res.result();

                        if (currentPvc == null || currentPvc.getStatus() == null || !"Bound".equals(currentPvc.getStatus().getPhase())) {
                            // This branch handles the following conditions:
                            // * The PVC doesn't exist yet, we should create it
                            // * The PVC is not Bound and we should reconcile it
                            reconcilePvc(desiredPvc).onComplete(resultPromise);
                        } else if (currentPvc.getStatus().getConditions().stream().anyMatch(cond -> "Resizing".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))))  {
                            // The PVC is Bound but it is already resizing => Nothing to do, we should let it resize
                            LOGGER.debugCr(reconciliation, "The PVC {} is resizing, nothing to do", desiredPvc.getMetadata().getName());
                            resultPromise.complete();
                        } else if (currentPvc.getStatus().getConditions().stream().anyMatch(cond -> "FileSystemResizePending".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))))  {
                            // The PVC is Bound and resized but waiting for FS resizing => We need to restart the pod which is using it
                            String podName = cluster.getPodName(getPodIndexFromPvcName(desiredPvc.getMetadata().getName()));
                            fsResizingRestartRequest.add(podName);
                            LOGGER.infoCr(reconciliation, "The PVC {} is waiting for file system resizing and the pod {} needs to be restarted.", desiredPvc.getMetadata().getName(), podName);
                            resultPromise.complete();
                        } else {
                            // The PVC is Bound and resizing is not in progress => We should check if the SC supports resizing and check if size changed
                            Long currentSize = StorageUtils.parseMemory(currentPvc.getSpec().getResources().getRequests().get("storage"));
                            Long desiredSize = StorageUtils.parseMemory(desiredPvc.getSpec().getResources().getRequests().get("storage"));

                            if (!currentSize.equals(desiredSize))   {
                                // The sizes are different => we should resize (shrinking will be handled in StorageDiff, so we do not need to check that)
                                resizePvc(currentPvc, desiredPvc).onComplete(resultPromise);
                            } else  {
                                // size didn't changed, just reconcile
                                reconcilePvc(desiredPvc).onComplete(resultPromise);
                            }
                        }
                    } else {
                        resultPromise.fail(res.cause());
                    }
                });

                futures.add(resultPromise.future());
            }

            return withVoid(CompositeFuture.all(futures));
        }

        Future<Void> reconcilePvc(PersistentVolumeClaim desired)  {
            Promise<Void> resultPromise = Promise.promise();

            pvcOperations.reconcile(reconciliation, namespace, desired.getMetadata().getName(), desired).onComplete(pvcRes -> {
                if (pvcRes.succeeded()) {
                    resultPromise.complete();
                } else {
                    resultPromise.fail(pvcRes.cause());
                }
            });

            return resultPromise.future();
        }

        Future<Void> resizePvc(PersistentVolumeClaim current, PersistentVolumeClaim desired)  {
            Promise<Void> resultPromise = Promise.promise();

            String storageClassName = current.getSpec().getStorageClassName();

            if (storageClassName != null && !storageClassName.isEmpty()) {
                storageClassOperator.getAsync(storageClassName).onComplete(scRes -> {
                    if (scRes.succeeded()) {
                        StorageClass sc = scRes.result();

                        if (sc == null) {
                            LOGGER.warnCr(reconciliation, "Storage Class {} not found. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", storageClassName, desired.getMetadata().getName());
                            resultPromise.complete();
                        } else if (sc.getAllowVolumeExpansion() == null || !sc.getAllowVolumeExpansion())    {
                            // Resizing not suported in SC => do nothing
                            LOGGER.warnCr(reconciliation, "Storage Class {} does not support resizing of volumes. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", storageClassName, desired.getMetadata().getName());
                            resultPromise.complete();
                        } else  {
                            // Resizing supported by SC => We can reconcile the PVC to have it resized
                            LOGGER.infoCr(reconciliation, "Resizing PVC {} from {} to {}.", desired.getMetadata().getName(), current.getStatus().getCapacity().get("storage").getAmount(), desired.getSpec().getResources().getRequests().get("storage").getAmount());
                            pvcOperations.reconcile(reconciliation, namespace, desired.getMetadata().getName(), desired).onComplete(pvcRes -> {
                                if (pvcRes.succeeded()) {
                                    resultPromise.complete();
                                } else {
                                    resultPromise.fail(pvcRes.cause());
                                }
                            });
                        }
                    } else {
                        LOGGER.errorCr(reconciliation, "Storage Class {} not found. PVC {} cannot be resized.", storageClassName, desired.getMetadata().getName(), scRes.cause());
                        resultPromise.fail(scRes.cause());
                    }
                });
            } else {
                LOGGER.warnCr(reconciliation, "PVC {} does not use any Storage Class and cannot be resized. Reconciliation will proceed without reconciling this PVC.", desired.getMetadata().getName());
                resultPromise.complete();
            }

            return resultPromise.future();
        }

        Future<ReconciliationState> zkPvcs() {
            List<PersistentVolumeClaim> pvcs = zkCluster.generatePersistentVolumeClaims();

            return maybeResizeReconcilePvcs(pvcs, zkCluster);
        }

        Future<ReconciliationState> kafkaPvcs() {
            List<PersistentVolumeClaim> pvcs = kafkaCluster.generatePersistentVolumeClaims(kafkaCluster.getStorage());

            return maybeResizeReconcilePvcs(pvcs, kafkaCluster);
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
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(getCaCertGeneration(this.clusterCa)));
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, String.valueOf(getCaCertGeneration(this.clientsCa)));
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
                        getReasonsToRestartPod(kafkaPodSetDiffs.resource(), pod, existingKafkaCertsChanged, this.clusterCa, this.clientsCa)));
            } else {
                return withVoid(maybeRollKafka(kafkaStsDiffs.resource().getSpec().getReplicas(), pod ->
                        getReasonsToRestartPod(kafkaStsDiffs.resource(), pod, existingKafkaCertsChanged, this.clusterCa, this.clientsCa)));
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

        Future<ReconciliationState> zkPodsReady() {
            if (zkCurrentReplicas != null && zkCurrentReplicas < zkCluster.getReplicas())  {
                // When scaling up we wait only for old pods to be ready, the new ones were not created yet
                return podsReady(zkCluster, zkCurrentReplicas);
            } else {
                return podsReady(zkCluster);
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
         * Will check all Zookeeper pods whether the user requested the pod and PVC deletion through an annotation
         *
         * @return
         */
        Future<ReconciliationState> zkManualPodCleaning() {
            return maybeManualPodCleaning(ZookeeperCluster.zookeeperClusterName(name), zkCluster.getSelectorLabels(), zkCluster.generatePersistentVolumeClaims());
        }

        /**
         * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
         *
         * @return
         */
        Future<ReconciliationState> kafkaManualPodCleaning() {
            return maybeManualPodCleaning(KafkaResources.kafkaStatefulSetName(name), kafkaCluster.getSelectorLabels(), kafkaCluster.generatePersistentVolumeClaims(oldKafkaStorage));
        }

        /**
         * Checks pods belonging to a single controller (e.g. StatefulSet) cluster to see whether the user requested
         * them to be deleted including their PVCs. If the user requested it, it will delete them. But in a single
         * reconciliation, always only one Pod is deleted. If multiple pods are marked for cleanup, they will be done
         * in subsequent reconciliations. This method only checks if cleanup was requested and calls other methods to
         * execute it.
         *
         * @param ctlrResourceName  Name of the controller resource (e.g. StatefulSet)
         * @param selector          Selector for selecting the Pods belonging to this controller
         * @param desiredPvcs       The list of desired PVCs which should be created after the old Pod and PVCs are deleted
         *
         * @return                  Future indicating the result of the cleanup. Returns always success if there are no
         * pods to cleanup.
         */
        Future<ReconciliationState> maybeManualPodCleaning(String ctlrResourceName, Labels selector, List<PersistentVolumeClaim> desiredPvcs) {
            return podOperations
                    .listAsync(namespace, selector)
                    .compose(pods -> {
                        // Only one pod per reconciliation is rolled
                        Pod podToClean = pods
                                .stream()
                                .filter(pod -> Annotations.booleanAnnotation(pod, AbstractScalableResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, false))
                                .findFirst()
                                .orElse(null);

                        if (podToClean == null) {
                            // No pod is annotated for deletion => return success
                            return Future.succeededFuture(this);
                        } else {
                            return manualPodCleaning(ctlrResourceName, podToClean.getMetadata().getName(), selector, desiredPvcs);
                        }
                    });
        }

        /**
         * Cleans a Pod and its PVCs if the user marked them for cleanup (deletion). It will first identify the existing
         * PVCs used by given Pod and the desired PVCs which need to be created after the old PVCs are deleted. Once
         * they are identified, it will start the deletion by deleting the controller resource.
         *
         * @param ctlrResourceName  Name of the controller resource (e.g. StatefulSet)
         * @param podName           Name of the Pod which should be cleaned / deleted
         * @param selector          Selector for selecting the Pods belonging to this controller
         * @param desiredPvcs       The list of desired PVCs which should be created after the old Pod and PVCs are deleted
         *
         * @return                  Future indicating the result of the cleanup
         */
        Future<ReconciliationState> manualPodCleaning(String ctlrResourceName, String podName, Labels selector, List<PersistentVolumeClaim> desiredPvcs) {
            return pvcOperations.listAsync(namespace, selector)
                    .compose(existingPvcs -> {
                        // Find out which PVCs need to be deleted
                        List<PersistentVolumeClaim> deletePvcs;

                        if (existingPvcs != null) {
                            deletePvcs = existingPvcs
                                    .stream()
                                    .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                                    .collect(Collectors.toList());
                        } else {
                            deletePvcs = new ArrayList<>(0);
                        }

                        // Find out which PVCs need to be created
                        List<PersistentVolumeClaim> createPvcs = desiredPvcs
                                .stream()
                                .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                                .collect(Collectors.toList());

                        if (featureGates.useStrimziPodSetsEnabled()) {
                            return cleanPodPvcAndPodSet(ctlrResourceName, podName, createPvcs, deletePvcs);
                        } else {
                            return cleanPodPvcAndStatefulSet(ctlrResourceName, podName, createPvcs, deletePvcs);
                        }
                    })
                    .map(this);
        }

        /**
         * Handles the modification of the StrimziPodSet controlling the pod which should be cleaned. In order
         * to clean the pod and its PVCs, we first need to remove the pod from the StrimziPodSet. Otherwise the
         * StrimziPodSet will break the process by recreating the pods or PVCs. This method first modifies the StrimziPodSet
         * and then calls other method to delete the Pod, PVCs and create the new PVCs. Once this method completes, it
         * will update the StrimziPodSet again. The Pod will be then recreated by the StrimziPodSet and this method just
         * waits for it to become ready.
         *
         * The complete flow looks like this
         *     1. Remove the deleted pod from the PodSet
         *     2. Trigger the Pod and PVC deletion and recreation
         *     3. Recreate the original PodSet
         *     4. Wait for the Pod to be created and become ready
         *
         * @param podSetName    Name of the StrimziPodSet to which this pod belongs
         * @param podName       Name of the Pod which should be cleaned / deleted
         * @param desiredPvcs   The list of desired PVCs which should be created after the old Pod and PVCs are deleted
         * @param currentPvcs   The list of current PVCs which should be deleted
         *
         * @return              Future indicating the result of the cleanup
         */
        Future<Void> cleanPodPvcAndPodSet(String podSetName, String podName, List<PersistentVolumeClaim> desiredPvcs, List<PersistentVolumeClaim> currentPvcs) {
            return strimziPodSetOperator.getAsync(namespace, podSetName)
                    .compose(podSet -> {
                        List<Map<String, Object>> desiredPods = podSet.getSpec().getPods().stream()
                                .filter(pod -> !podName.equals(PodSetUtils.mapToPod(pod).getMetadata().getName()))
                                .collect(Collectors.toList());

                        StrimziPodSet reducedPodSet = new StrimziPodSetBuilder(podSet)
                                .editSpec()
                                    .withPods(desiredPods)
                                .endSpec()
                                .build();

                        return strimziPodSetOperator.reconcile(reconciliation, namespace, podSetName, reducedPodSet)
                                .compose(ignore -> cleanPodAndPvc(podName, desiredPvcs, currentPvcs))
                                .compose(ignore -> {
                                    // We recreate the StrimziPodSet in its old configuration => any further changes have to be done by rolling update
                                    // These fields need to be cleared before recreating the StatefulSet
                                    podSet.getMetadata().setResourceVersion(null);
                                    podSet.getMetadata().setSelfLink(null);
                                    podSet.getMetadata().setUid(null);
                                    podSet.setStatus(null);

                                    return strimziPodSetOperator.reconcile(reconciliation, namespace, podSetName, podSet);
                                })
                                .compose(ignore -> podOperations.readiness(reconciliation, namespace, podName, 1_000L, operationTimeoutMs))
                                .map((Void) null);
                    });
        }

        /**
         * Handles the deletion and recreation of the StatefulSet controlling the pod which should be cleaned. In order
         * to clean the pod and its PVCs, we first need to delete the StatefulSet (non-cascading). Otherwise, the
         * StatefulSet will break the process by recreating the pods or PVCs. This method first deletes the StatefulSet
         * and then calls other method to delete the Pod, PVCs and create the new PVCs. Once this method completes, it
         * will recreate the StatefulSet again. The Pod will be then recreated by the StatefulSet and this method just
         * waits for it to become ready.
         *
         * The complete flow looks like this
         *     1. Delete the STS (non-cascading)
         *     2. Trigger the Pod and PVC deletion and recreation
         *     3. Recreate the STS
         *     4. Wait for the Pod to be created and become ready
         *
         * @param stsName       NAme of the StatefulSet to which this pod belongs
         * @param podName       Name of the Pod which should be cleaned / deleted
         * @param desiredPvcs   The list of desired PVCs which should be created after the old Pod and PVCs are deleted
         * @param currentPvcs   The list of current PVCs which should be deleted
         *
         * @return              Future indicating the result of the cleanup
         */
        Future<Void> cleanPodPvcAndStatefulSet(String stsName, String podName, List<PersistentVolumeClaim> desiredPvcs, List<PersistentVolumeClaim> currentPvcs) {
            return stsOperations.getAsync(namespace, stsName)
                    .compose(sts -> stsOperations.deleteAsync(reconciliation, namespace, stsName, false)
                            .compose(ignore -> cleanPodAndPvc(podName, desiredPvcs, currentPvcs))
                            .compose(ignore -> {
                                // We recreate the StatfulSet in its old configuration => any further changes have to be done by rolling update
                                // These fields need to be cleared before recreating the StatefulSet
                                sts.getMetadata().setResourceVersion(null);
                                sts.getMetadata().setSelfLink(null);
                                sts.getMetadata().setUid(null);
                                sts.setStatus(null);

                                return stsOperations.reconcile(reconciliation, namespace, stsName, sts);
                            })
                            .compose(ignore -> podOperations.readiness(reconciliation, namespace, podName, 1_000L, operationTimeoutMs))
                            .map((Void) null));
        }

        /**
         * This is an internal method which actually executes the deletion of the Pod and PVC. This is a non-trivial
         * since the PVC and the Pod are tightly coupled and one cannot be deleted without the other. It will first
         * trigger the Pod deletion. Once the Pod is deleted, it will delete the PVCs. Once they are deleted as well, it
         * will create the new PVCs. The Pod is not recreated here => that is done by the controller (e.g. StatefulSet).
         *
         * This method expects that the Statefulset or any other controller are already deleted to not interfere with
         * the process.
         *
         * To address these, we:
         *     1. Delete the Pod
         *     2. Wait for the Pod to be actually deleted
         *     3. Delete the PVC
         *     4. Wait for the PVCs to be actually deleted
         *     5. Recreate the PVCs
         *
         * @param podName           Name of the pod which should be deleted
         * @param deletePvcs        The list of PVCs which should be deleted
         * @param createPvcs        The list of PVCs which should be recreated
         *
         * @return
         */
        Future<Void> cleanPodAndPvc(String podName, List<PersistentVolumeClaim> createPvcs, List<PersistentVolumeClaim> deletePvcs) {
            // First we delete the Pod which should be cleaned
            return podOperations.deleteAsync(reconciliation, namespace, podName, true)
                    .compose(ignore -> {
                        // With the pod deleted, we can delete all the PVCs belonging to this pod
                        List<Future> deleteResults = new ArrayList<>(deletePvcs.size());

                        for (PersistentVolumeClaim pvc : deletePvcs)    {
                            String pvcName = pvc.getMetadata().getName();
                            LOGGER.debugCr(reconciliation, "Deleting PVC {} for Pod {} based on {} annotation", pvcName, podName, AbstractScalableResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC);
                            deleteResults.add(pvcOperations.deleteAsync(reconciliation, namespace, pvcName, true));
                        }
                        return CompositeFuture.join(deleteResults);
                    })
                    .compose(ignore -> {
                        // Once everything was deleted, we can recreate the PVCs
                        // The Pod will be recreated later when the controller resource is recreated
                        List<Future> createResults = new ArrayList<>(createPvcs.size());

                        for (PersistentVolumeClaim pvc : createPvcs)    {
                            LOGGER.debugCr(reconciliation, "Reconciling PVC {} for Pod {} after it was deleted and maybe recreated by the pod", pvc.getMetadata().getName(), podName);
                            createResults.add(pvcOperations.reconcile(reconciliation, namespace, pvc.getMetadata().getName(), pvc));
                        }

                        return CompositeFuture.join(createResults);
                    })
                    .map((Void) null);
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
        Future<ReconciliationState> zkPersistentClaimDeletion() {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            Future<List<PersistentVolumeClaim>> futurePvcs = pvcOperations.listAsync(namespace, zkCluster.getSelectorLabels());

            futurePvcs.onComplete(res -> {
                if (res.succeeded() && res.result() != null)    {
                    List<String> maybeDeletePvcs = res.result().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = zkCluster.generatePersistentVolumeClaims().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    persistentClaimDeletion(maybeDeletePvcs, desiredPvcs).onComplete(resultPromise);
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return resultPromise.future();
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

                    persistentClaimDeletion(maybeDeletePvcs, desiredPvcs).onComplete(resultPromise);
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return resultPromise.future();
        }

        /**
         * Internal method for deleting PVCs after scale-downs or disk removal from JBOD storage. It gets list of
         * existing and desired PVCs, diffs them and removes those which should not exist.
         *
         * @param maybeDeletePvcs   List of existing PVCs
         * @param desiredPvcs       List of PVCs which should exist
         * @return
         */
        Future<ReconciliationState> persistentClaimDeletion(List<String> maybeDeletePvcs, List<String> desiredPvcs) {
            List<Future> futures = new ArrayList<>();

            maybeDeletePvcs.removeAll(desiredPvcs);

            for (String pvcName : maybeDeletePvcs)  {
                LOGGER.debugCr(reconciliation, "Considering PVC {} for deletion", pvcName);

                if (Annotations.booleanAnnotation(pvcOperations.get(namespace, pvcName), AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM, false)) {
                    LOGGER.debugCr(reconciliation, "Deleting PVC {}", pvcName);
                    futures.add(pvcOperations.reconcile(reconciliation, namespace, pvcName, null));
                }
            }

            return withVoid(CompositeFuture.all(futures));
        }

        final Future<ReconciliationState> getEntityOperatorDescription() {
            this.entityOperator = EntityOperator.fromCrd(reconciliation, kafkaAssembly, versions);

            if (entityOperator != null) {
                EntityTopicOperator topicOperator = entityOperator.getTopicOperator();
                EntityUserOperator userOperator = entityOperator.getUserOperator();

                return CompositeFuture.join(
                            topicOperator == null ? Future.succeededFuture(null) :
                                Util.metricsAndLogging(reconciliation, configMapOperations, kafkaAssembly.getMetadata().getNamespace(), topicOperator.getLogging(), null),
                            userOperator == null ? Future.succeededFuture(null) :
                                Util.metricsAndLogging(reconciliation, configMapOperations, kafkaAssembly.getMetadata().getNamespace(), userOperator.getLogging(), null))
                        .compose(res -> {
                            MetricsAndLogging toMetricsAndLogging = res.resultAt(0);
                            MetricsAndLogging uoMetricsAndLogging = res.resultAt(1);

                            if (topicOperator != null)  {
                                this.topicOperatorMetricsAndLogsConfigMap = topicOperator.generateMetricsAndLogConfigMap(toMetricsAndLogging);
                            }

                            if (userOperator != null)   {
                                this.userOperatorMetricsAndLogsConfigMap = userOperator.generateMetricsAndLogConfigMap(uoMetricsAndLogging);
                            }

                            this.eoDeployment = entityOperator.generateDeployment(pfa.isOpenshift(), emptyMap(), imagePullPolicy, imagePullSecrets);
                            return Future.succeededFuture(this);
                        });
            } else {
                return Future.succeededFuture(this);
            }
        }

        // Deploy entity operator Role if entity operator is deployed
        Future<ReconciliationState> entityOperatorRole() {
            final Role role;
            if (isEntityOperatorDeployed()) {
                role = entityOperator.generateRole(namespace, namespace);
            } else {
                role = null;
            }

            return withVoid(roleOperations.reconcile(reconciliation,
                    namespace,
                    EntityOperator.getRoleName(name),
                    role));
        }

        // Deploy entity topic operator Role if entity operator is deployed
        Future<ReconciliationState> entityTopicOperatorRole() {
            final String topicWatchedNamespace;
            if (isEntityOperatorDeployed()
                    && entityOperator.getTopicOperator() != null
                    && entityOperator.getTopicOperator().getWatchedNamespace() != null
                    && !entityOperator.getTopicOperator().getWatchedNamespace().isEmpty()) {
                topicWatchedNamespace = entityOperator.getTopicOperator().getWatchedNamespace();
            } else {
                topicWatchedNamespace = namespace;
            }

            final Future<ReconcileResult<Role>> topicWatchedNamespaceFuture;
            if (!namespace.equals(topicWatchedNamespace)) {
                topicWatchedNamespaceFuture = roleOperations.reconcile(reconciliation,
                        topicWatchedNamespace,
                        EntityOperator.getRoleName(name),
                        entityOperator.generateRole(namespace, topicWatchedNamespace));
            } else {
                topicWatchedNamespaceFuture = Future.succeededFuture();
            }

            return withVoid(topicWatchedNamespaceFuture);
        }

        // Deploy entity user operator Role if entity operator is deployed
        Future<ReconciliationState> entityUserOperatorRole() {
            final String userWatchedNamespace;
            if (isEntityOperatorDeployed()
                    && entityOperator.getUserOperator() != null
                    && entityOperator.getUserOperator().getWatchedNamespace() != null
                    && !entityOperator.getUserOperator().getWatchedNamespace().isEmpty()) {
                userWatchedNamespace = entityOperator.getUserOperator().getWatchedNamespace();
            } else {
                userWatchedNamespace = namespace;
            }

            final Future<ReconcileResult<Role>> userWatchedNamespaceFuture;
            if (!namespace.equals(userWatchedNamespace)) {
                userWatchedNamespaceFuture = roleOperations.reconcile(reconciliation,
                        userWatchedNamespace,
                        EntityOperator.getRoleName(name),
                        entityOperator.generateRole(namespace, userWatchedNamespace));
            } else {
                userWatchedNamespaceFuture = Future.succeededFuture();
            }

            return withVoid(userWatchedNamespaceFuture);
        }

        Future<ReconciliationState> entityOperatorServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    EntityOperator.entityOperatorServiceAccountName(name),
                    isEntityOperatorDeployed() ? entityOperator.generateServiceAccount() : null));
        }

        // Check for if the entity operator will be deployed as part of the reconciliation
        // Related resources need to know this to know whether to deploy
        private boolean isEntityOperatorDeployed() {
            return eoDeployment != null;
        }

        Future<ReconciliationState> entityOperatorTopicOpRoleBindingForRole() {
            // Don't deploy Role RoleBinding if the topic operator is not deployed,
            // or if the topic operator needs to watch a different namespace
            if (!isEntityOperatorDeployed()
                    || entityOperator.getTopicOperator() == null) {
                LOGGER.debugCr(reconciliation, "entityOperatorTopicOpRoleBindingForRole not required");
                return withVoid(roleBindingOperations.reconcile(reconciliation,
                        namespace,
                        EntityTopicOperator.roleBindingForRoleName(name),
                        null));
            }


            final String watchedNamespace;

            if (entityOperator.getTopicOperator().getWatchedNamespace() != null
                    && !entityOperator.getTopicOperator().getWatchedNamespace().isEmpty()) {
                watchedNamespace = entityOperator.getTopicOperator().getWatchedNamespace();
            } else {
                watchedNamespace = namespace;
            }

            final Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;

            if (!namespace.equals(watchedNamespace)) {
                watchedNamespaceFuture = roleBindingOperations.reconcile(reconciliation,
                        watchedNamespace,
                        EntityTopicOperator.roleBindingForRoleName(name),
                        entityOperator.getTopicOperator().generateRoleBindingForRole(namespace, watchedNamespace));
            } else {
                watchedNamespaceFuture = Future.succeededFuture();
            }

            // Create role binding for the the UI runs in (it needs to access the CA etc.)
            Future<ReconcileResult<RoleBinding>> ownNamespaceFuture = roleBindingOperations.reconcile(reconciliation,
                    namespace,
                    EntityTopicOperator.roleBindingForRoleName(name),
                    entityOperator.getTopicOperator().generateRoleBindingForRole(namespace, namespace));

            return withVoid(CompositeFuture.join(ownNamespaceFuture, watchedNamespaceFuture));
        }

        Future<ReconciliationState> entityOperatorUserOpRoleBindingForRole() {
            // Don't deploy Role RoleBinding if the user operator is not deployed,
            // or if the user operator needs to watch a different namespace
            if (!isEntityOperatorDeployed()
                    || entityOperator.getUserOperator() == null) {
                LOGGER.debugCr(reconciliation, "entityOperatorUserOpRoleBindingForRole not required");
                return withVoid(roleBindingOperations.reconcile(reconciliation,
                        namespace,
                        EntityUserOperator.roleBindingForRoleName(name),
                        null));
            }


            Future<ReconcileResult<RoleBinding>> ownNamespaceFuture;
            Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;

            String watchedNamespace = namespace;

            if (entityOperator.getUserOperator().getWatchedNamespace() != null
                    && !entityOperator.getUserOperator().getWatchedNamespace().isEmpty()) {
                watchedNamespace = entityOperator.getUserOperator().getWatchedNamespace();
            }

            if (!namespace.equals(watchedNamespace)) {
                watchedNamespaceFuture = roleBindingOperations.reconcile(reconciliation,
                        watchedNamespace,
                        EntityUserOperator.roleBindingForRoleName(name),
                        entityOperator.getUserOperator().generateRoleBindingForRole(namespace, watchedNamespace));
            } else {
                watchedNamespaceFuture = Future.succeededFuture();
            }

            // Create role binding for the the UI runs in (it needs to access the CA etc.)
            ownNamespaceFuture = roleBindingOperations.reconcile(reconciliation,
                    namespace,
                    EntityUserOperator.roleBindingForRoleName(name),
                    entityOperator.getUserOperator().generateRoleBindingForRole(namespace, namespace));


            return withVoid(CompositeFuture.join(ownNamespaceFuture, watchedNamespaceFuture));
        }

        Future<ReconciliationState> entityOperatorTopicOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace,
                    isEntityOperatorDeployed() && entityOperator.getTopicOperator() != null ?
                            entityOperator.getTopicOperator().getAncillaryConfigMapName() : EntityTopicOperator.metricAndLogConfigsName(name),
                    topicOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorUserOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace,
                    isEntityOperatorDeployed() && entityOperator.getUserOperator() != null ?
                            entityOperator.getUserOperator().getAncillaryConfigMapName() : EntityUserOperator.metricAndLogConfigsName(name),
                    userOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorDeployment() {
            if (this.entityOperator != null && isEntityOperatorDeployed()) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.entityOperator.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int clusterCaCertGeneration = getCaCertGeneration(this.clusterCa);

                    Annotations.annotations(eoDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCaCertGeneration));
                    return deploymentOperations.reconcile(reconciliation, namespace, EntityOperator.entityOperatorName(name), eoDeployment);
                }).compose(recon -> {
                    if (recon instanceof ReconcileResult.Noop)   {
                        // Lets check if we need to roll the deployment manually
                        if (existingEntityTopicOperatorCertsChanged || existingEntityUserOperatorCertsChanged) {
                            return entityOperatorRollingUpdate();
                        }
                    }

                    // No need to roll, we patched the deployment (and it will roll it self) or we created a new one
                    return Future.succeededFuture(this);
                });
            } else  {
                return withVoid(deploymentOperations.reconcile(reconciliation, namespace, EntityOperator.entityOperatorName(name), null));
            }
        }

        Future<ReconciliationState> entityOperatorRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(reconciliation, namespace, EntityOperator.entityOperatorName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> entityOperatorReady() {
            if (this.entityOperator != null && isEntityOperatorDeployed()) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.entityOperator.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(reconciliation, namespace, this.entityOperator.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(reconciliation, namespace, this.entityOperator.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        // Clean up the old entity-operator-certificate which is generated in the old releases.
        // Starting from this release, the Topic Operator and User Operator will use new dedicated certificate.
        // Therefore, we need to remove the unused entity-operator-certificate
        Future<ReconciliationState> entityOperatorSecret() {
            return withVoid(secretOperations.reconcile(reconciliation, namespace, EntityOperator.secretName(name), null));
        }

        Future<ReconciliationState> entityTopicOperatorSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(EntityTopicOperator.secretName(name), entityOperator == null || entityOperator.getTopicOperator() == null ? null : entityOperator.getTopicOperator().generateSecret(clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingEntityTopicOperatorCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> entityUserOperatorSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(EntityUserOperator.secretName(name), entityOperator == null || entityOperator.getUserOperator() == null ? null : entityOperator.getUserOperator().generateSecret(clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingEntityUserOperatorCertsChanged = changed;
                        return this;
                    });
        }

        private boolean isPodUpToDate(StatefulSet sts, Pod pod) {
            final int stsGeneration = StatefulSetOperator.getStsGeneration(sts);
            final int podGeneration = StatefulSetOperator.getPodGeneration(pod);
            LOGGER.debugCr(reconciliation, "Rolling update of {}/{}: pod {} has {}={}; sts has {}={}",
                    sts.getMetadata().getNamespace(), sts.getMetadata().getName(), pod.getMetadata().getName(),
                    StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, podGeneration,
                    StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, stsGeneration);
            return stsGeneration == podGeneration;
        }

        /*test*/ final Future<ReconciliationState> getCruiseControlDescription() {
            CruiseControl cruiseControl = CruiseControl.fromCrd(reconciliation, kafkaAssembly, versions, kafkaCluster.getStorage());

            if (cruiseControl != null) {
                return Util.metricsAndLogging(reconciliation, configMapOperations, kafkaAssembly.getMetadata().getNamespace(),
                        cruiseControl.getLogging(), cruiseControl.getMetricsConfigInCm())
                        .compose(metricsAndLogging -> {
                            ConfigMap logAndMetricsConfigMap = cruiseControl.generateMetricsAndLogConfigMap(metricsAndLogging);

                            Map<String, String> annotations = singletonMap(CruiseControl.ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(ANCILLARY_CM_KEY_LOG_CONFIG));

                            this.cruiseControlMetricsAndLogsConfigMap = logAndMetricsConfigMap;
                            this.cruiseControl = cruiseControl;
                            this.ccDeployment = cruiseControl.generateDeployment(pfa.isOpenshift(), annotations, imagePullPolicy, imagePullSecrets);

                            return Future.succeededFuture(this);
                        });
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        Future<ReconciliationState> cruiseControlServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    CruiseControl.cruiseControlServiceAccountName(name),
                    ccDeployment != null ? cruiseControl.generateServiceAccount() : null));
        }

        Future<ReconciliationState> cruiseControlAncillaryCm() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace,
                    ccDeployment != null && cruiseControl != null ?
                            cruiseControl.getAncillaryConfigMapName() : CruiseControl.metricAndLogConfigsName(name),
                    cruiseControlMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> cruiseControlSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(CruiseControl.secretName(name), cruiseControl == null ? null : cruiseControl.generateSecret(kafkaAssembly, clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingCruiseControlCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> cruiseControlApiSecret() {
            if (this.cruiseControl != null) {
                Future<Secret> secretFuture = secretOperations.getAsync(namespace, CruiseControlResources.apiSecretName(name));
                return secretFuture.compose(res -> {
                    if (res == null) {
                        return withVoid(secretOperations.reconcile(reconciliation, namespace, CruiseControlResources.apiSecretName(name),
                                cruiseControl.generateApiSecret()));
                    } else {
                        return withVoid(Future.succeededFuture(this));
                    }
                });

            } else {
                return withVoid(secretOperations.reconcile(reconciliation, namespace, CruiseControlResources.apiSecretName(name), null));
            }
        }

        Future<ReconciliationState> cruiseControlDeployment() {
            if (this.cruiseControl != null && ccDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.cruiseControl.getName());
                return future.compose(dep -> {
                    return deploymentOperations.reconcile(reconciliation, namespace, this.cruiseControl.getName(), ccDeployment);
                }).compose(recon -> {
                    if (recon instanceof ReconcileResult.Noop)   {
                        // Lets check if we need to roll the deployment manually
                        if (existingCruiseControlCertsChanged) {
                            return cruiseControlRollingUpdate();
                        }
                    }

                    // No need to roll, we patched the deployment (and it will roll it self) or we created a new one
                    return Future.succeededFuture(this);
                });
            } else {
                return withVoid(deploymentOperations.reconcile(reconciliation, namespace, CruiseControl.cruiseControlName(name), null));
            }
        }

        Future<ReconciliationState> cruiseControlRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(reconciliation, namespace, CruiseControl.cruiseControlName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> cruiseControlService() {
            return withVoid(serviceOperations.reconcile(reconciliation, namespace, CruiseControl.serviceName(name), cruiseControl != null ? cruiseControl.generateService() : null));
        }

        Future<ReconciliationState> cruiseControlReady() {
            if (this.cruiseControl != null && ccDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.cruiseControl.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(reconciliation, namespace, this.cruiseControl.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(reconciliation, namespace, this.cruiseControl.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> cruiseControlNetPolicy() {
            if (isNetworkPolicyGeneration) {
                return withVoid(networkPolicyOperator.reconcile(reconciliation, namespace, CruiseControl.policyName(name),
                        cruiseControl != null ? cruiseControl.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels) : null));
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        private boolean isPodCaCertUpToDate(Pod pod, Ca ca) {
            final int caCertGeneration = getCaCertGeneration(ca);
            String podAnnotation = getCaCertAnnotation(ca);
            final int podCaCertGeneration =
                    Annotations.intAnnotation(pod, podAnnotation, Ca.INIT_GENERATION);
            return caCertGeneration == podCaCertGeneration;
        }

        private boolean isCustomCertUpToDate(StatefulSet sts, Pod pod, String annotation) {
            final String stsThumbprint = Annotations.stringAnnotation(sts.getSpec().getTemplate(), annotation, "");
            final String podThumbprint = Annotations.stringAnnotation(pod, annotation, "");
            LOGGER.debugCr(reconciliation, "Rolling update of {}/{}: pod {} has {}={}; sts has {}={}",
                    sts.getMetadata().getNamespace(), sts.getMetadata().getName(), pod.getMetadata().getName(),
                    annotation, podThumbprint,
                    annotation, stsThumbprint);
            return podThumbprint.equals(stsThumbprint);
        }

        /**
         * Determines if the Pod needs to be rolled / restarted. And if it does, returns a list of reasons why.
         *
         * @param ctrlResource  Controller resource to which pod belongs
         * @param pod           Pod to restart
         * @param cas           Certificate authorities to be checked for changes
         *
         * @return null or empty if the restart is not needed, reason String otherwise
         */
        private List<String> getReasonsToRestartPod(HasMetadata ctrlResource, Pod pod,
                                                           boolean nodeCertsChange,
                                                           Ca... cas) {
            if (pod == null)    {
                // When the Pod doesn't exist, it doesn't need to be restarted.
                // It will be created with new configuration.
                return new ArrayList<>();
            }

            List<String> reasons = new ArrayList<>(3);

            if (ctrlResource instanceof StatefulSet) {
                StatefulSet sts = (StatefulSet) ctrlResource;

                if (!isPodUpToDate(sts, pod)) {
                    reasons.add("Pod has old generation");
                }

                if (!isCustomCertUpToDate(sts, pod, KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS)) {
                    reasons.add("custom certificate one or more listeners changed");
                }
            } else if (ctrlResource instanceof StrimziPodSet) {
                StrimziPodSet podSet = (StrimziPodSet) ctrlResource;

                if (PodRevision.hasChanged(pod, podSet)) {
                    reasons.add("Pod has old revision");
                }
            }

            for (Ca ca: cas) {
                if (ca.certRenewed()) {
                    reasons.add(ca + " certificate renewal");
                }
                if (ca.certsRemoved()) {
                    reasons.add(ca + " certificate removal");
                }
                if (!isPodCaCertUpToDate(pod, ca)) {
                    reasons.add("Pod has old " + ca + " certificate generation");
                }
            }

            if (fsResizingRestartRequest.contains(pod.getMetadata().getName()))   {
                reasons.add("file system needs to be resized");
            }

            if (nodeCertsChange) {
                reasons.add("server certificates changed");
            }

            if (!reasons.isEmpty()) {
                LOGGER.debugCr(reconciliation, "Rolling pod {} due to {}",
                        pod.getMetadata().getName(), reasons);
            }

            return reasons;
        }

        private boolean isMaintenanceTimeWindowsSatisfied(Supplier<Date> dateSupplier) {
            String currentCron = null;
            try {
                boolean isSatisfiedBy = getMaintenanceTimeWindows() == null || getMaintenanceTimeWindows().isEmpty();
                if (!isSatisfiedBy) {
                    Date date = dateSupplier.get();
                    for (String cron : getMaintenanceTimeWindows()) {
                        currentCron = cron;
                        CronExpression cronExpression = new CronExpression(cron);
                        // the user defines the cron expression in "UTC/GMT" timezone but CO pod
                        // can be running on a different one, so setting it on the cron expression
                        cronExpression.setTimeZone(TimeZone.getTimeZone("GMT"));
                        if (cronExpression.isSatisfiedBy(date)) {
                            isSatisfiedBy = true;
                            break;
                        }
                    }
                }
                return isSatisfiedBy;
            } catch (ParseException e) {
                LOGGER.warnCr(reconciliation, "The provided maintenance time windows list contains {} which is not a valid cron expression", currentCron);
                return false;
            }
        }

        private List<String> getMaintenanceTimeWindows() {
            return kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        }

        private int getCaCertGeneration(Ca ca) {
            return Annotations.intAnnotation(ca.caCertSecret(), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION,
                    Ca.INIT_GENERATION);
        }

        private String getCaCertAnnotation(Ca ca) {
            return ca instanceof ClientsCa ?
                    Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION :
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION;
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
                    labels, ownerRef, isMaintenanceTimeWindowsSatisfied(dateSupplier));

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

        private final Future<ReconciliationState> getKafkaExporterDescription() {
            this.kafkaExporter = KafkaExporter.fromCrd(reconciliation, kafkaAssembly, versions);
            this.exporterDeployment = kafkaExporter.generateDeployment(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> kafkaExporterServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    KafkaExporter.containerServiceAccountName(name),
                    exporterDeployment != null ? kafkaExporter.generateServiceAccount() : null));
        }

        Future<ReconciliationState> kafkaExporterSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(KafkaExporter.secretName(name), kafkaExporter == null ? null : kafkaExporter.generateSecret(clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingKafkaExporterCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> kafkaExporterDeployment() {
            if (this.kafkaExporter != null && this.exporterDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.kafkaExporter.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int caCertGeneration = getCaCertGeneration(this.clusterCa);
                    Annotations.annotations(exporterDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
                    return deploymentOperations.reconcile(reconciliation, namespace, KafkaExporter.kafkaExporterName(name), exporterDeployment);
                })
                .compose(recon -> {
                    if (recon instanceof ReconcileResult.Noop)   {
                        // Lets check if we need to roll the deployment manually
                        if (existingKafkaExporterCertsChanged) {
                            return kafkaExporterRollingUpdate();
                        }
                    }

                    // No need to roll, we patched the deployment (and it will roll it self) or we created a new one
                    return Future.succeededFuture(this);
                });
            } else  {
                return withVoid(deploymentOperations.reconcile(reconciliation, namespace, KafkaExporter.kafkaExporterName(name), null));
            }
        }

        Future<ReconciliationState> kafkaExporterRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(reconciliation, namespace, KafkaExporter.kafkaExporterName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaExporterReady() {
            if (this.kafkaExporter != null && exporterDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.kafkaExporter.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(reconciliation, namespace, this.kafkaExporter.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(reconciliation, namespace, this.kafkaExporter.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> getJmxTransDescription() {
            try {
                int numOfBrokers = kafkaCluster.getReplicas();
                this.jmxTrans = JmxTrans.fromCrd(reconciliation, kafkaAssembly);
                if (this.jmxTrans != null) {
                    this.jmxTransConfigMap = jmxTrans.generateJmxTransConfigMap(kafkaAssembly.getSpec().getJmxTrans(), numOfBrokers);
                    this.jmxTransDeployment = jmxTrans.generateDeployment(imagePullPolicy, imagePullSecrets);
                }

                return Future.succeededFuture(this);
            } catch (Throwable e) {
                return Future.failedFuture(e);
            }
        }

        Future<ReconciliationState> jmxTransConfigMap() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace,
                    JmxTrans.jmxTransConfigName(name),
                    jmxTransConfigMap));
        }


        Future<ReconciliationState> jmxTransServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    JmxTrans.containerServiceAccountName(name),
                    jmxTrans != null ? jmxTrans.generateServiceAccount() : null));
        }

        Future<ReconciliationState> jmxTransDeployment() {
            if (this.jmxTrans != null && this.jmxTransDeployment != null) {
                return deploymentOperations.getAsync(namespace, this.jmxTrans.getName()).compose(dep -> {
                    return configMapOperations.getAsync(namespace, jmxTransConfigMap.getMetadata().getName()).compose(res -> {
                        String resourceVersion = res.getMetadata().getResourceVersion();
                        // getting the current cluster CA generation from the current deployment, if it exists
                        int caCertGeneration = getCaCertGeneration(this.clusterCa);
                        Annotations.annotations(jmxTransDeployment.getSpec().getTemplate()).put(
                                Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
                        Annotations.annotations(jmxTransDeployment.getSpec().getTemplate()).put(
                                JmxTrans.CONFIG_MAP_ANNOTATION_KEY, resourceVersion);
                        return withVoid(deploymentOperations.reconcile(reconciliation, namespace, JmxTrans.jmxTransName(name),
                                jmxTransDeployment));
                    });
                });
            } else {
                return withVoid(deploymentOperations.reconcile(reconciliation, namespace, JmxTrans.jmxTransName(name), null));
            }
        }

        Future<ReconciliationState> jmxTransDeploymentReady() {
            if (this.jmxTrans != null && jmxTransDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace,  this.jmxTrans.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(reconciliation, namespace,  this.jmxTrans.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(reconciliation, namespace, this.jmxTrans.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
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
