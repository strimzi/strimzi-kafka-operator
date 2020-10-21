/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteIngress;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaUpgradeException;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.EntityTopicOperator;
import io.strimzi.operator.cluster.model.EntityUserOperator;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.JmxTrans;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.model.NodeUtils;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.KafkaSpecChecker;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScaler;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronExpression;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG;
import static io.strimzi.operator.cluster.model.AbstractModel.ANNO_STRIMZI_IO_STORAGE;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_FROM_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_TO_VERSION;
import static io.strimzi.operator.cluster.model.KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION;
import static io.strimzi.operator.cluster.model.KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedVersions;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> {
    private static final Logger log = LogManager.getLogger(KafkaAssemblyOperator.class.getName());

    private final long operationTimeoutMs;

    private final ZookeeperSetOperator zkSetOperations;
    private final KafkaSetOperator kafkaSetOperations;
    private final RouteOperator routeOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;
    private final RoleBindingOperator roleBindingOperations;
    private final PodOperator podOperations;
    private final IngressOperator ingressOperations;
    private final StorageClassOperator storageClassOperator;
    private final NodeOperator nodeOperator;
    private final CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> crdOperator;
    private final ZookeeperScalerProvider zkScalerProvider;
    private final AdminClientProvider adminClientProvider;

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
                                 ResourceOperatorSupplier supplier,
                                 ClusterOperatorConfig config) {
        super(vertx, pfa, Kafka.RESOURCE_KIND, certManager, passwordGenerator,
                supplier.kafkaOperator, supplier, config);
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.routeOperations = supplier.routeOperations;
        this.zkSetOperations = supplier.zkSetOperations;
        this.kafkaSetOperations = supplier.kafkaSetOperations;
        this.pvcOperations = supplier.pvcOperations;
        this.deploymentOperations = supplier.deploymentOperations;
        this.roleBindingOperations = supplier.roleBindingOperations;
        this.podOperations = supplier.podOperations;
        this.ingressOperations = supplier.ingressOperations;
        this.storageClassOperator = supplier.storageClassOperations;
        this.crdOperator = supplier.kafkaOperator;
        this.nodeOperator = supplier.nodeOperator;
        this.zkScalerProvider = supplier.zkScalerProvider;
        this.adminClientProvider = supplier.adminClientProvider;
    }

    @Override
    public Future<Void> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
        Promise<Void> createOrUpdatePromise = Promise.promise();

        if (kafkaAssembly.getSpec() == null) {
            log.error("{} spec cannot be null", kafkaAssembly.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }

        ReconciliationState reconcileState = createReconciliationState(reconciliation, kafkaAssembly);
        reconcile(reconcileState).onComplete(reconcileResult -> {
            KafkaStatus status = reconcileState.kafkaStatus;
            Condition readyCondition;

            if (kafkaAssembly.getMetadata().getGeneration() != null)    {
                status.setObservedGeneration(kafkaAssembly.getMetadata().getGeneration());
            }

            if (reconcileResult.succeeded())    {
                readyCondition = new ConditionBuilder()
                        .withLastTransitionTime(ModelUtils.formatTimestamp(dateSupplier()))
                        .withType("Ready")
                        .withStatus("True")
                        .build();
            } else {
                readyCondition = new ConditionBuilder()
                        .withLastTransitionTime(ModelUtils.formatTimestamp(dateSupplier()))
                        .withType("NotReady")
                        .withStatus("True")
                        .withReason(reconcileResult.cause().getClass().getSimpleName())
                        .withMessage(reconcileResult.cause().getMessage())
                        .build();
            }

            status.addCondition(readyCondition);
            reconcileState.updateStatus(status).onComplete(statusResult -> {
                if (statusResult.succeeded())    {
                    log.debug("Status for {} is up to date", kafkaAssembly.getMetadata().getName());
                } else {
                    log.error("Failed to set status for {}", kafkaAssembly.getMetadata().getName());
                }

                // If both features succeeded, createOrUpdate succeeded as well
                // If one or both of them failed, we prefer the reconciliation failure as the main error
                if (reconcileResult.succeeded() && statusResult.succeeded())    {
                    createOrUpdatePromise.complete();
                } else if (reconcileResult.failed())    {
                    createOrUpdatePromise.fail(reconcileResult.cause());
                } else {
                    createOrUpdatePromise.fail(statusResult.cause());
                }
            });
        });

        return createOrUpdatePromise.future();
    }

    Future<Void> reconcile(ReconciliationState reconcileState)  {
        Promise<Void> chainPromise = Promise.promise();

        reconcileState.initialStatus()
                .compose(state -> state.reconcileCas(this::dateSupplier))
                .compose(state -> state.clusterOperatorSecret(this::dateSupplier))
                .compose(state -> state.getKafkaClusterDescription())
                // Roll everything if a new CA is added to the trust store.
                .compose(state -> state.rollingUpdateForNewCaKey())
                .compose(state -> state.getZookeeperDescription())
                .compose(state -> state.zkModelWarnings())
                .compose(state -> state.zkManualPodCleaning())
                .compose(state -> state.zkNetPolicy())
                .compose(state -> state.zkManualRollingUpdate())
                .compose(state -> state.zkVersionChange())
                .compose(state -> state.zookeeperServiceAccount())
                .compose(state -> state.zkPvcs())
                .compose(state -> state.zkService())
                .compose(state -> state.zkHeadlessService())
                .compose(state -> state.zkAncillaryCm())
                .compose(state -> state.zkNodesSecret(this::dateSupplier))
                .compose(state -> state.zkPodDisruptionBudget())
                .compose(state -> state.zkStatefulSet())
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
                .compose(state -> state.kafkaVersionChange())
                .compose(state -> state.kafkaPvcs())
                .compose(state -> state.kafkaInitServiceAccount())
                .compose(state -> state.kafkaInitClusterRoleBinding())
                .compose(state -> state.kafkaScaleDown())
                .compose(state -> state.kafkaService())
                .compose(state -> state.kafkaHeadlessService())
                .compose(state -> state.kafkaExternalBootstrapService())
                .compose(state -> state.kafkaReplicaServices())
                .compose(state -> state.kafkaBootstrapRoute())
                .compose(state -> state.kafkaReplicaRoutes())
                .compose(state -> state.kafkaBootstrapIngress())
                .compose(state -> state.kafkaReplicaIngress())
                .compose(state -> state.kafkaExternalBootstrapServiceReady())
                .compose(state -> state.kafkaReplicaServicesReady())
                .compose(state -> state.kafkaBootstrapRouteReady())
                .compose(state -> state.kafkaReplicaRoutesReady())
                .compose(state -> state.kafkaGenerateCertificates(this::dateSupplier))
                .compose(state -> state.customTlsListenerCertificate())
                .compose(state -> state.customExternalListenerCertificate())
                .compose(state -> state.kafkaAncillaryCm())
                .compose(state -> state.kafkaBrokersSecret())
                .compose(state -> state.kafkaJmxSecret())
                .compose(state -> state.kafkaPodDisruptionBudget())
                .compose(state -> state.kafkaStatefulSet())
                .compose(state -> state.kafkaRollingUpdate())
                .compose(state -> state.kafkaScaleUp())
                .compose(state -> state.kafkaPodsReady())
                .compose(state -> state.kafkaServiceEndpointReady())
                .compose(state -> state.kafkaHeadlessServiceEndpointReady())
                .compose(state -> state.kafkaNodePortExternalListenerStatus())
                .compose(state -> state.kafkaPersistentClaimDeletion())
                .compose(state -> state.kafkaTlsListenerCertificatesToStatus())
                .compose(state -> state.kafkaExternalListenerCertificatesToStatus())

                .compose(state -> state.checkUnsupportedTopicOperator())

                .compose(state -> state.getEntityOperatorDescription())
                .compose(state -> state.entityOperatorServiceAccount())
                .compose(state -> state.entityOperatorTopicOpRoleBinding())
                .compose(state -> state.entityOperatorUserOpRoleBinding())
                .compose(state -> state.entityOperatorTopicOpAncillaryCm())
                .compose(state -> state.entityOperatorUserOpAncillaryCm())
                .compose(state -> state.entityOperatorSecret(this::dateSupplier))
                .compose(state -> state.entityOperatorDeployment())
                .compose(state -> state.entityOperatorReady())

                .compose(state -> state.getCruiseControlDescription())
                .compose(state -> state.cruiseControlNetPolicy())
                .compose(state -> state.cruiseControlServiceAccount())
                .compose(state -> state.cruiseControlAncillaryCm())
                .compose(state -> state.cruiseControlSecret(this::dateSupplier))
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

        /* test */ ClusterCa clusterCa;
        /* test */ ClientsCa clientsCa;

        /* test */ ZookeeperCluster zkCluster;
        private Service zkService;
        private Service zkHeadlessService;
        private ConfigMap zkMetricsAndLogsConfigMap;
        /* test */ ReconcileResult<StatefulSet> zkDiffs;
        private Integer zkCurrentReplicas = null;

        private KafkaCluster kafkaCluster = null;
        private Integer kafkaCurrentReplicas = null;
        /* test */ KafkaStatus kafkaStatus = new KafkaStatus();

        private Service kafkaService;
        private Service kafkaHeadlessService;
        /* test */ ReconcileResult<StatefulSet> kafkaDiffs;
        private Set<String> kafkaExternalBootstrapDnsName = new HashSet<>();
        private Set<String> kafkaExternalAdvertisedHostnames = new TreeSet<>();
        private Set<String> kafkaExternalAdvertisedPorts = new TreeSet<>();
        private Map<Integer, Set<String>> kafkaExternalDnsNames = new HashMap<>();

        private String zkLoggingHash = "";
        private String kafkaLogging = "";
        private String kafkaLoggingAppendersHash = "";
        private String kafkaBrokerConfigurationHash = "";

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
        private boolean existingEntityOperatorCertsChanged = false;
        private boolean existingCruiseControlCertsChanged = false;

        // Custom Listener certificates
        private String tlsListenerCustomCertificate;
        private String tlsListenerCustomCertificateThumbprint;
        private String externalListenerCustomCertificate;
        private String externalListenerCustomCertificateThumbprint;

        // Stores node port of the external bootstrap service for use in KafkaStatus
        /* test */ int externalBootstrapNodePort;

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
                            log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, kafka.getApiVersion());
                            updateStatusPromise.complete();
                        } else {
                            KafkaStatus currentStatus = kafka.getStatus();

                            StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                            if (!ksDiff.isEmpty()) {
                                Kafka resourceWithNewStatus = new KafkaBuilder(kafka).withStatus(desiredStatus).build();

                                crdOperator.updateStatusAsync(resourceWithNewStatus).onComplete(updateRes -> {
                                    if (updateRes.succeeded()) {
                                        log.debug("{}: Completed status update", reconciliation);
                                        updateStatusPromise.complete();
                                    } else {
                                        log.error("{}: Failed to update status", reconciliation, updateRes.cause());
                                        updateStatusPromise.fail(updateRes.cause());
                                    }
                                });
                            } else {
                                log.debug("{}: Status did not change", reconciliation);
                                updateStatusPromise.complete();
                            }
                        }
                    } else {
                        log.error("{}: Current Kafka resource not found", reconciliation);
                        updateStatusPromise.fail("Current Kafka resource not found");
                    }
                } else {
                    log.error("{}: Failed to get the current Kafka resource and its status", reconciliation, getRes.cause());
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
                        log.debug("{}: Setting the initial status for a new resource", reconciliation);

                        Condition deployingCondition = new ConditionBuilder()
                                .withLastTransitionTime(ModelUtils.formatTimestamp(dateSupplier()))
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
                        log.debug("{}: Status is already set. No need to set initial status", reconciliation);
                        initialStatusPromise.complete(this);
                    }
                } else {
                    log.error("{}: Failed to get the current Kafka resource and its status", reconciliation, getRes.cause());
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
            KafkaSpecChecker checker = new KafkaSpecChecker(kafkaAssembly.getSpec(), kafkaCluster, zkCluster);
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
         * The clients CA secret has to have the name determined by {@link KafkaCluster#clientsCaCertSecretName(String)}.
         * Within both the secrets the current certificate is stored under the key {@code ca.crt}
         * and the current key is stored under the key {@code ca.key}.
         */
        Future<ReconciliationState> reconcileCas(Supplier<Date> dateSupplier) {
            Labels selectorLabels = Labels.EMPTY.withStrimziKind(reconciliation.kind()).withStrimziCluster(reconciliation.name());
            Labels caLabels = Labels.generateDefaultLabels(kafkaAssembly, Labels.APPLICATION_NAME, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
            Promise<ReconciliationState> resultPromise = Promise.promise();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        String clusterCaCertName = AbstractModel.clusterCaCertSecretName(name);
                        String clusterCaKeyName = AbstractModel.clusterCaKeySecretName(name);
                        String clientsCaCertName = KafkaCluster.clientsCaCertSecretName(name);
                        String clientsCaKeyName = KafkaCluster.clientsCaKeySecretName(name);
                        Secret clusterCaCertSecret = null;
                        Secret clusterCaKeySecret = null;
                        Secret clientsCaCertSecret = null;
                        Secret clientsCaKeySecret = null;
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

                        this.clusterCa = new ClusterCa(certManager, passwordGenerator, name, clusterCaCertSecret, clusterCaKeySecret,
                                ModelUtils.getCertificateValidity(clusterCaConfig),
                                ModelUtils.getRenewalDays(clusterCaConfig),
                                clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority(),
                                clusterCaConfig != null ? clusterCaConfig.getCertificateExpirationPolicy() : null);
                        clusterCa.createRenewOrReplace(
                                reconciliation.namespace(), reconciliation.name(), caLabels.toMap(),
                                ownerRef, isMaintenanceTimeWindowsSatisfied(dateSupplier));

                        this.clusterCa.initCaSecrets(clusterSecrets);

                        CertificateAuthority clientsCaConfig = kafkaAssembly.getSpec().getClientsCa();

                        // When we are not supposed to generate the CA but it does not exist, we should just throw an error
                        checkCustomCaSecret(clientsCaConfig, clientsCaCertSecret, clientsCaKeySecret, "Clients CA");

                        this.clientsCa = new ClientsCa(certManager, passwordGenerator,
                                clientsCaCertName, clientsCaCertSecret,
                                clientsCaKeyName, clientsCaKeySecret,
                                ModelUtils.getCertificateValidity(clientsCaConfig),
                                ModelUtils.getRenewalDays(clientsCaConfig),
                                clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority(),
                                clientsCaConfig != null ? clientsCaConfig.getCertificateExpirationPolicy() : null);
                        clientsCa.createRenewOrReplace(reconciliation.namespace(), reconciliation.name(),
                                caLabels.toMap(), ownerRef, isMaintenanceTimeWindowsSatisfied(dateSupplier));

                        List<Future> secretReconciliations = new ArrayList<>(2);

                        if (clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority())   {
                            Future clusterSecretReconciliation = secretOperations.reconcile(reconciliation.namespace(), clusterCaCertName, this.clusterCa.caCertSecret())
                                    .compose(ignored -> secretOperations.reconcile(reconciliation.namespace(), clusterCaKeyName, this.clusterCa.caKeySecret()));
                            secretReconciliations.add(clusterSecretReconciliation);
                        }

                        if (clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority())   {
                            Future clientsSecretReconciliation = secretOperations.reconcile(reconciliation.namespace(), clientsCaCertName, this.clientsCa.caCertSecret())
                                .compose(ignored -> secretOperations.reconcile(reconciliation.namespace(), clientsCaKeyName, this.clientsCa.caKeySecret()));
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
        @SuppressWarnings("deprecation")
        Future<ReconciliationState> rollingUpdateForNewCaKey() {
            List<String> reason = new ArrayList<>(4);
            if (this.clusterCa.keyReplaced()) {
                reason.add("trust new cluster CA certificate signed by new key");
            }
            if (this.clientsCa.keyReplaced()) {
                reason.add("trust new clients CA certificate signed by new key");
            }
            if (!reason.isEmpty()) {
                Future<Void> zkRollFuture;
                Function<Pod, List<String>> rollPodAndLogReason = pod -> {
                    log.debug("{}: Rolling Pod {} to {}", reconciliation, pod.getMetadata().getName(), reason);
                    return reason;
                };
                if (this.clusterCa.keyReplaced()) {
                    zkRollFuture = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                        .compose(sts -> zkSetOperations.maybeRollingUpdate(sts, rollPodAndLogReason,
                        clusterCa.caCertSecret(),
                        oldCoSecret));
                } else {
                    zkRollFuture = Future.succeededFuture();
                }
                return zkRollFuture
                        .compose(i -> kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name)))
                        .compose(sts -> new KafkaRoller(vertx, reconciliation, podOperations, 1_000, operationTimeoutMs,
                            () -> new BackOff(250, 2, 10), sts, clusterCa.caCertSecret(), oldCoSecret, adminClientProvider,
                            kafkaCluster.getBrokersConfiguration(), kafkaLogging, kafkaCluster.getKafkaVersion())
                            .rollingRestart(rollPodAndLogReason))
                        .compose(i -> rollDeploymentIfExists(EntityOperator.entityOperatorName(name), reason.toString()))
                        .compose(i -> rollDeploymentIfExists(KafkaExporter.kafkaExporterName(name), reason.toString()))
                        .compose(i -> rollDeploymentIfExists(CruiseControl.cruiseControlName(name), reason.toString()))
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
                            log.debug("{}: Rolling Deployment {} to {}", reconciliation, deploymentName, reasons);
                            return deploymentOperations.rollingUpdate(namespace, deploymentName, operationTimeoutMs);
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }

        @SuppressWarnings("deprecation")
        Future<ReconciliationState> kafkaManualRollingUpdate() {
            Future<StatefulSet> futsts = kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name));
            if (futsts != null) {
                return futsts.compose(sts -> {
                    if (sts != null) {
                        if (Annotations.booleanAnnotation(sts, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE,
                                false, Annotations.ANNO_OP_STRIMZI_IO_MANUAL_ROLLING_UPDATE)) {
                            return maybeRollKafka(sts, pod -> {
                                if (pod == null) {
                                    throw new ConcurrentDeletionException("Unexpectedly pod no longer exists during roll of StatefulSet.");
                                }
                                log.debug("{}: Rolling Kafka pod {} due to manual rolling update",
                                        reconciliation, pod.getMetadata().getName());
                                return singletonList("manual rolling update");
                            });
                        }
                    }
                    return Future.succeededFuture();
                }).map(i -> this);
            }
            return Future.succeededFuture(this);
        }

        @SuppressWarnings("deprecation")
        Future<ReconciliationState> zkManualRollingUpdate() {
            Future<StatefulSet> futsts = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name));
            if (futsts != null) {
                return futsts.compose(sts -> {
                    if (sts != null) {
                        if (Annotations.booleanAnnotation(sts, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE,
                                false, Annotations.ANNO_OP_STRIMZI_IO_MANUAL_ROLLING_UPDATE)) {

                            return zkSetOperations.maybeRollingUpdate(sts, pod -> {

                                log.debug("{}: Rolling Zookeeper pod {} due to manual rolling update",
                                        reconciliation, pod.getMetadata().getName());
                                return singletonList("manual rolling update");
                            });
                        }
                    }
                    return Future.succeededFuture();
                }).map(i -> this);
            }
            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> zkVersionChange() {

            return kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name)).compose(kafkaSts -> {

                    if (kafkaSts == null) {
                        return Future.succeededFuture(this);
                    }

                    KafkaVersionChange versionChange = getKafkaVersionChange(kafkaSts);

                    if (versionChange.isNoop()) {
                        log.debug("Kafka.spec.kafka.version is unchanged therefore no change to Zookeeper is required");
                        return Future.succeededFuture(this);
                    } else {
                        String versionChangeType;
                        if (versionChange.isDowngrade()) {
                            versionChangeType = "downgrade";
                        } else {
                            versionChangeType = "upgrade";
                        }
                        if (versionChange.requiresZookeeperChange()) {
                            log.info("Kafka {} from {} to {} requires Zookeeper {} from {} to {}",
                                    versionChangeType,
                                    versionChange.from().version(),
                                    versionChange.to().version(),
                                    versionChangeType,
                                    versionChange.from().zookeeperVersion(),
                                    versionChange.to().zookeeperVersion());
                        } else {
                            log.info("Kafka {} from {} to {} requires no change in Zookeeper version",
                                    versionChangeType,
                                    versionChange.from().version(),
                                    versionChange.to().version());

                        }
                        // Get the zookeeper image currently set in the Kafka CR or, if that is not set, the image from the target Kafka version
                        String newZkImage = versions.kafkaImage(kafkaAssembly.getSpec().getZookeeper().getImage(), versionChange.to().version());

                        log.debug("Setting new Zookeeper image: " + newZkImage);
                        this.zkCluster.setImage(newZkImage);

                        return Future.succeededFuture(this);
                    }
                }
            );
        }

        /**
         * If the STS exists, complete any pending rolls
         *
         * @return A Future which completes with the current state of the STS, or with null if the STS never existed.
         */
        public Future<Void> waitForQuiescence(StatefulSet sts) {
            if (sts != null) {
                return maybeRollKafka(sts,
                    pod -> {
                        boolean notUpToDate = !isPodUpToDate(sts, pod);
                        List<String> reason = emptyList();
                        if (notUpToDate) {
                            log.debug("Rolling pod {} prior to upgrade", pod.getMetadata().getName());
                            reason = singletonList("upgrade quiescence");
                        }
                        return reason;
                    });
            } else {
                return Future.succeededFuture();
            }
        }

        private KafkaVersionChange getKafkaVersionChange(StatefulSet kafkaSts) {
            // Get the current version of the cluster
            KafkaVersion currentVersion = versions.version(Annotations.annotations(kafkaSts).get(ANNO_STRIMZI_IO_KAFKA_VERSION));

            log.debug("STS {} currently has Kafka version {}", kafkaSts.getMetadata().getName(), currentVersion);

            String fromVersionAnno = Annotations.annotations(kafkaSts).get(ANNO_STRIMZI_IO_FROM_VERSION);
            KafkaVersion fromVersion;
            if (fromVersionAnno != null) { // We're mid version change
                fromVersion = versions.version(fromVersionAnno);
            } else {
                fromVersion = currentVersion;
            }

            log.debug("STS {} is moving from Kafka version {}", kafkaSts.getMetadata().getName(), fromVersion);

            String toVersionAnno = Annotations.annotations(kafkaSts).get(ANNO_STRIMZI_IO_TO_VERSION);
            KafkaVersion toVersion;
            if (toVersionAnno != null) { // We're mid version change
                toVersion = versions.version(toVersionAnno);
            } else {
                toVersion = versions.version(kafkaAssembly.getSpec().getKafka().getVersion());
            }

            log.debug("STS {} is moving to Kafka version {}", kafkaSts.getMetadata().getName(), toVersion);

            KafkaVersionChange versionChange = new KafkaVersionChange(fromVersion, toVersion);

            log.debug("Kafka version change: {}", versionChange);

            return versionChange;
        }

        Future<ReconciliationState> kafkaVersionChange() {
            String kafkaStsName = KafkaCluster.kafkaClusterName(name);
            String configCmName = KafkaCluster.metricAndLogConfigsName(name);

            return CompositeFuture.join(kafkaSetOperations.getAsync(namespace, kafkaStsName), configMapOperations.getAsync(namespace, configCmName))
                    .compose(res -> {
                        if (res.failed())   {
                            return Future.failedFuture(res.cause());
                        }

                        StatefulSet oldSts = res.resultAt(0);
                        ConfigMap oldCm = res.resultAt(1);

                        if (oldSts == null || oldCm == null) {
                            return Future.succeededFuture(this);
                        }

                        KafkaVersionChange versionChange = getKafkaVersionChange(oldSts);

                        // Get the current version of the cluster
                        KafkaVersion currentVersion = versions.version(Annotations.annotations(oldSts).get(ANNO_STRIMZI_IO_KAFKA_VERSION));

                        StatefulSet sts;
                        ConfigMap cm;

                        // When Kafka upgrade is done together with broker upgrade (when `version: X.Y.Z` is missing in
                        // the CRD), the broker configuration file introduced in 0.16.0 might not be there yet (if
                        // upgrading from any version before 0.16.0). We have to detect this and trigger different
                        // upgrade procedure.
                        boolean certificatesHaveToBeUpgraded;
                        if (oldCm.getData().get("server.config") == null)  {
                            certificatesHaveToBeUpgraded = true;
                            cm = getKafkaAncillaryCm();
                            sts = getKafkaStatefulSet();
                        } else {
                            sts = oldSts;
                            cm = oldCm;
                            certificatesHaveToBeUpgraded = false;
                        }

                        if (versionChange.isNoop()) {
                            log.debug("Kafka.spec.kafka.version unchanged");
                            return Future.succeededFuture(this);
                        } else {
                            // Wait until the STS is not being updated (it shouldn't be, but there's no harm in checking)
                            return waitForQuiescence(oldSts).compose(v -> {
                                // Get the image currently set in the Kafka CR or, if that is not set, the image from the version we are changing to.
                                String image = versions.kafkaImage(kafkaAssembly.getSpec().getKafka().getImage(), versionChange.to().version());

                                Future<StatefulSet> f = Future.succeededFuture(sts);
                                Future<?> result;

                                if (versionChange.isUpgrade()) {
                                    if (currentVersion.equals(versionChange.from())) {
                                        f = f.compose(ignored -> kafkaUpgradePhase1(sts, cm, versionChange, image, certificatesHaveToBeUpgraded));
                                    }
                                    result = f.compose(ss2 -> kafkaUpgradePhase2(ss2, cm, versionChange));
                                } else {
                                    // Must be a downgrade
                                    if (currentVersion.equals(versionChange.from())) {
                                        f = f.compose(ignored -> kafkaDowngradePhase1(sts, cm, versionChange));
                                    }
                                    result = f.compose(ignored -> kafkaDowngradePhase2(sts, cm, versionChange, image));
                                }
                                return result.map(this);
                            });
                        }
                    });
        }

        /**
         * <p>Initial upgrade phase.
         * If a message format change is required, check that it's set in the Kafka.spec.kafka.config
         * Set inter.broker.protocol.version if it's not set
         * Perform a rolling update.
         */
        private Future<StatefulSet> kafkaUpgradePhase1(StatefulSet sts, ConfigMap cm, KafkaVersionChange versionChange, String upgradedImage, boolean certificatesHaveToBeUpgraded) {
            log.info("{}: {}, phase 1", reconciliation, versionChange);

            Map<String, String> annotations = Annotations.annotations(sts);
            String config = cm.getData().getOrDefault("server.config", "");
            String oldMessageFormatConfiguration = Arrays.stream(config.split(System.lineSeparator())).filter(line -> line.startsWith(LOG_MESSAGE_FORMAT_VERSION + "=")).findFirst().orElse(null);
            String oldMessageFormat = oldMessageFormatConfiguration == null ? null : oldMessageFormatConfiguration.substring(oldMessageFormatConfiguration.lastIndexOf("=") + 1);

            if (versionChange.requiresMessageFormatChange() && oldMessageFormat == null) {
                // We need to ensure both new and old versions are using the same version (so they agree during the upgrade).
                // If the msg version is given in the CR and it's the same as the current (live) msg version
                // then we're good. If the current live msg version is not given (i.e. the default) and
                // the msg version is given in the CR then we're also good.

                // Force the user to explicitly set the log.message.format.version
                // to match the old version
                return Future.failedFuture(new KafkaUpgradeException(versionChange + " requires a message format change " +
                        "from " + versionChange.from().messageVersion() + " to " + versionChange.to().messageVersion() + ". " +
                        LOG_MESSAGE_FORMAT_VERSION + ": \"" + versionChange.from().messageVersion() + "\"" +
                        " must be explicitly set."));
            }
            // Otherwise both versions use the same message format, so we don't care.

            String lowerVersionProtocolConfiguration = Arrays.stream(config.split(System.lineSeparator())).filter(line -> line.startsWith(INTERBROKER_PROTOCOL_VERSION + "=")).findFirst().orElse(null);
            String lowerVersionProtocol = lowerVersionProtocolConfiguration == null ? null : lowerVersionProtocolConfiguration.substring(lowerVersionProtocolConfiguration.lastIndexOf("=") + 1);

            boolean twoPhase;
            if (lowerVersionProtocol == null) {
                if (!versionChange.requiresProtocolChange()) {
                    // In this case we just need a single rolling update
                    twoPhase = false;
                } else {
                    twoPhase = true;

                    // Set proto version and message version in Kafka config, if they're not already set
                    lowerVersionProtocol = versionChange.from().protocolVersion();
                    log.info("{}: Upgrade: Setting {} to {}", reconciliation, INTERBROKER_PROTOCOL_VERSION, lowerVersionProtocol);
                    config = config + System.lineSeparator() + INTERBROKER_PROTOCOL_VERSION + "=" + lowerVersionProtocol;

                    // Store upgrade state in annotations
                    annotations.put(ANNO_STRIMZI_IO_FROM_VERSION, versionChange.from().version());
                    annotations.put(ANNO_STRIMZI_IO_TO_VERSION, versionChange.to().version());
                }
            } else {
                // There's no need for the next phase of update because the user has
                // inter.broker.protocol.version set explicitly: The CO shouldn't remove it.
                // We're done, so remove the annotations.
                twoPhase = false;
                log.info("{}: Upgrade: Removing annotations {}, {}",
                        reconciliation, ANNO_STRIMZI_IO_FROM_VERSION, ANNO_STRIMZI_IO_TO_VERSION);
                annotations.remove(ANNO_STRIMZI_IO_FROM_VERSION);
                annotations.remove(ANNO_STRIMZI_IO_TO_VERSION);
            }
            log.info("{}: Upgrade: Setting annotation {}={}",
                    reconciliation, ANNO_STRIMZI_IO_KAFKA_VERSION, versionChange.to().version());
            annotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, versionChange.to().version());

            // update the annotations, image and environment
            StatefulSet newSts = new StatefulSetBuilder(sts)
                    .editMetadata()
                        .withAnnotations(annotations)
                    .endMetadata()
                    .editSpec()
                        .editTemplate()
                            .editSpec()
                                .editFirstContainer()
                                    .withImage(upgradedImage)
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                .build();

            ConfigMap newCm = new ConfigMapBuilder(cm).build();
            newCm.getData().put("server.config", config);

            Future<Void> secretUpdateFuture;
            if (certificatesHaveToBeUpgraded) {
                // Advertised hostnames for replication changed, we need to update the certs as well
                secretUpdateFuture = kafkaGenerateCertificates(() -> new Date())
                        .compose(ignore -> secretOperations.reconcile(namespace, KafkaCluster.brokersSecretName(name), kafkaCluster.generateBrokersSecret()))
                        .map(ignore -> (Void) null);
            } else {
                secretUpdateFuture = Future.succeededFuture();
            }

            // patch and rolling upgrade
            String stsName = KafkaCluster.kafkaClusterName(this.name);
            String configCmName = KafkaCluster.metricAndLogConfigsName(this.name);
            log.info("{}: Upgrade: Patch + rolling update of {}", reconciliation, this.name);
            return CompositeFuture.join(kafkaSetOperations.reconcile(namespace, stsName, newSts), configMapOperations.reconcile(namespace, configCmName, newCm), secretUpdateFuture)
                    .compose(result -> {
                        StatefulSet resultSts = null;

                        if (result.resultAt(0) instanceof ReconcileResult && ((ReconcileResult) result.resultAt(0)).resource() instanceof StatefulSet) {
                            resultSts = (StatefulSet) ((ReconcileResult) result.resultAt(0)).resource();
                        }

                        return maybeRollKafka(newSts, pod -> {
                            log.info("{}: Upgrade: Patch + rolling update of {}: Pod {}", reconciliation, stsName, pod.getMetadata().getName());
                            return singletonList("Upgrade phase 1 of " + (twoPhase ? 2 : 1) + ": Patch + rolling update of " + name + ": Pod " + pod.getMetadata().getName());
                        }).map(resultSts);
                    })
                    .compose(ss2 -> {
                        log.info("{}: {}, phase 1 of {} completed: {}", reconciliation, versionChange,
                                twoPhase ? 2 : 1,
                                twoPhase ? "change in " + INTERBROKER_PROTOCOL_VERSION + " requires 2nd phase"
                                        : "no change to " + INTERBROKER_PROTOCOL_VERSION + " because it is explicitly configured"
                        );
                        return Future.succeededFuture(twoPhase ? ss2 : null);
                    });
        }

        /**
         * Final upgrade phase
         * Note: The log.message.format.version is left at the old version.
         * It is a manual action to remove that once the user has updated all their clients.
         */
        private Future<Void> kafkaUpgradePhase2(StatefulSet sts, ConfigMap cm, KafkaVersionChange upgrade) {
            if (sts == null) {
                // It was a one-phase update
                return Future.succeededFuture();
            }
            // Cluster is now using new binaries, but old proto version
            log.info("{}: {}, phase 2", reconciliation, upgrade);
            // Remove the strimzi.io/from-version and strimzi.io/to-version since this is the last phase
            Map<String, String> annotations = Annotations.annotations(sts);
            log.info("{}: Upgrade: Removing annotations {}, {}",
                    reconciliation, ANNO_STRIMZI_IO_FROM_VERSION, ANNO_STRIMZI_IO_TO_VERSION);
            annotations.remove(ANNO_STRIMZI_IO_FROM_VERSION);
            annotations.remove(ANNO_STRIMZI_IO_TO_VERSION);

            // Remove inter.broker.protocol.version (so the new version's default is used)
            String config = cm.getData().getOrDefault("server.config", "");
            StringBuilder newConfigBuilder = new StringBuilder();
            Arrays.stream(config.split(System.lineSeparator())).filter(line -> !line.startsWith(INTERBROKER_PROTOCOL_VERSION + "=")).forEach(line -> newConfigBuilder.append(line + System.lineSeparator()));
            String newConfig = newConfigBuilder.toString();

            log.info("{}: Upgrade: Removing Kafka config {}, will default to {}",
                    reconciliation, INTERBROKER_PROTOCOL_VERSION, upgrade.to().protocolVersion());

            // Update to new proto version and rolling upgrade
            StatefulSet newSts = new StatefulSetBuilder(sts)
                    .editMetadata()
                        .withAnnotations(annotations)
                    .endMetadata()
                    .build();

            ConfigMap newCm = new ConfigMapBuilder(cm).build();
            newCm.getData().put("server.config", newConfig);

            // Reconcile the STS and perform a rolling update of the pods
            String stsName = KafkaCluster.kafkaClusterName(this.name);
            String cmName = KafkaCluster.metricAndLogConfigsName(this.name);
            log.info("{}: Upgrade: Patch + rolling update of {}", reconciliation, stsName);
            return CompositeFuture.join(kafkaSetOperations.reconcile(namespace, stsName, newSts), configMapOperations.reconcile(namespace, cmName, newCm))
                    .compose(ignored -> maybeRollKafka(sts, pod -> {
                        log.info("{}: Upgrade: Patch + rolling update of {}: Pod {}", reconciliation, stsName, pod.getMetadata().getName());
                        return singletonList("Upgrade: Patch + rolling update of " + name + ": Pod " + pod.getMetadata().getName());
                    }))
                    .compose(ignored -> {
                        log.info("{}: {}, phase 2 of 2 completed", reconciliation, upgrade);
                        return Future.succeededFuture();
                    });
        }

        /**
         * <p>Initial downgrade phase.
         * <ol>
         *     <li>Set the log.message.format.version to the old version</li>
         *     <li>Set the inter.broker.protocol.version to the old version</li>
         *     <li>Set the strimzi.io/upgrade-phase=1 (to record progress of the upgrade in case of CO failure)</li>
         *     <li>Reconcile the STS and perform a rolling update of the pods</li>
         * </ol>
         */
        private Future<StatefulSet> kafkaDowngradePhase1(StatefulSet sts, ConfigMap cm, KafkaVersionChange versionChange) {
            log.info("{}: {}, phase 1", reconciliation, versionChange);

            Map<String, String> annotations = Annotations.annotations(sts);
            String config = cm.getData().getOrDefault("server.config", "");
            String newConfig;
            String oldMessageFormatConfiguration = Arrays.stream(config.split(System.lineSeparator())).filter(line -> line.startsWith(LOG_MESSAGE_FORMAT_VERSION + "=")).findFirst().orElse(null);
            String oldMessageFormat = oldMessageFormatConfiguration == null ? null : oldMessageFormatConfiguration.substring(oldMessageFormatConfiguration.lastIndexOf("=") + 1);

            // Force the user to explicitly set log.message.format.version
            // (Controller shouldn't break clients)
            if (oldMessageFormat == null || !oldMessageFormat.equals(versionChange.to().messageVersion())) {
                return Future.failedFuture(new KafkaUpgradeException(
                        String.format("Cannot downgrade Kafka cluster %s in namespace %s to version %s " +
                                        "because the current cluster is configured with %s=%s. " +
                                        "Downgraded brokers would not be able to understand existing " +
                                        "messages with the message version %s. ",
                                name, namespace, versionChange.to(),
                                LOG_MESSAGE_FORMAT_VERSION, oldMessageFormat,
                                oldMessageFormat)));
            }

            String lowerVersionProtocolConfiguration = Arrays.stream(config.split(System.lineSeparator())).filter(line -> line.startsWith(INTERBROKER_PROTOCOL_VERSION + "=")).findFirst().orElse(null);
            String lowerVersionProtocol = lowerVersionProtocolConfiguration == null ? null : lowerVersionProtocolConfiguration.substring(lowerVersionProtocolConfiguration.lastIndexOf("=") + 1);

            String phases;
            if (lowerVersionProtocol == null || compareDottedVersions(lowerVersionProtocol, versionChange.to().protocolVersion()) > 0) {
                phases = "2 (change in " + INTERBROKER_PROTOCOL_VERSION + " requires 2nd phase)";
                // Set proto version and message version in Kafka config, if they're not already set
                String newLowerVersionProtocol = lowerVersionProtocol == null ? versionChange.to().protocolVersion() : lowerVersionProtocol;
                log.info("{}: Downgrade: Setting {} to {}", reconciliation, INTERBROKER_PROTOCOL_VERSION, newLowerVersionProtocol);

                if (config.contains(INTERBROKER_PROTOCOL_VERSION + "=")) {
                    StringBuilder newConfigBuilder = new StringBuilder();
                    Arrays.stream(config.split(System.lineSeparator())).forEach(line -> {
                        if (line.startsWith(INTERBROKER_PROTOCOL_VERSION + "=")) {
                            newConfigBuilder.append(INTERBROKER_PROTOCOL_VERSION + "=" + newLowerVersionProtocol + System.lineSeparator());
                        } else {
                            newConfigBuilder.append(line + System.lineSeparator());
                        }
                    });
                    newConfig = newConfigBuilder.toString();
                } else {
                    newConfig = config + System.lineSeparator() + INTERBROKER_PROTOCOL_VERSION + "=" + newLowerVersionProtocol;
                }

                // Store upgrade state in annotations
                annotations.put(ANNO_STRIMZI_IO_FROM_VERSION, versionChange.from().version());
                annotations.put(ANNO_STRIMZI_IO_TO_VERSION, versionChange.to().version());
            } else {
                // In this case there's no need for this phase of update, because the both old and new
                // brokers speaking protocol of the lower version.
                phases = "2 (1st phase skips rolling update)";
                log.info("{}: {}, phase 1 of {} completed", reconciliation, versionChange, phases);
                return Future.succeededFuture(sts);
            }

            // update the annotations, image and environment
            StatefulSet newSts = new StatefulSetBuilder(sts)
                    .editMetadata()
                        .withAnnotations(annotations)
                    .endMetadata()
                    .build();

            ConfigMap newCm = new ConfigMapBuilder(cm).build();
            newCm.getData().put("server.config", newConfig);

            // patch and rolling upgrade
            String stsName = KafkaCluster.kafkaClusterName(this.name);
            String cmName = KafkaCluster.metricAndLogConfigsName(this.name);
            log.info("{}: Downgrade: Patch + rolling update of {}", reconciliation, stsName);
            return CompositeFuture.join(kafkaSetOperations.reconcile(namespace, stsName, newSts), configMapOperations.reconcile(namespace, cmName, newCm))
                    .compose(result -> {
                        StatefulSet resultSts = null;

                        if (result.resultAt(0) instanceof ReconcileResult && ((ReconcileResult) result.resultAt(0)).resource() instanceof StatefulSet) {
                            resultSts = (StatefulSet) ((ReconcileResult) result.resultAt(0)).resource();
                        }

                        Function<Pod, List<String>> fn = pod -> {
                            log.info("{}: Downgrade: Patch + rolling update of {}: Pod {}", reconciliation, stsName, pod.getMetadata().getName());
                            return singletonList("Downgrade phase 1 of " + phases + ": Patch + rolling update of " + name + ": Pod " + pod.getMetadata().getName());
                        };
                        return maybeRollKafka(sts, fn).map(resultSts);
                    })
                    .compose(ss2 -> {
                        log.info("{}: {}, phase 1 of {} completed", reconciliation, versionChange, phases);
                        return Future.succeededFuture(ss2);
                    });
        }

        protected CompositeFuture adminClientSecrets() {
            Future<Secret> clusterCaCertSecretFuture = secretOperations.getAsync(
                namespace, KafkaResources.clusterCaCertificateSecretName(name)).compose(secret -> {
                    if (secret == null) {
                        return Future.failedFuture(Util.missingSecretException(namespace, KafkaCluster.clusterCaCertSecretName(name)));
                    } else {
                        return Future.succeededFuture(secret);
                    }
                });
            Future<Secret> coKeySecretFuture = secretOperations.getAsync(
                namespace, ClusterOperator.secretName(name)).compose(secret -> {
                    if (secret == null) {
                        return Future.failedFuture(Util.missingSecretException(namespace, ClusterOperator.secretName(name)));
                    } else {
                        return Future.succeededFuture(secret);
                    }
                });
            return CompositeFuture.join(clusterCaCertSecretFuture, coKeySecretFuture);
        }

        /**
         *
         * @param sts Kafka statefullset
         * @param podNeedsRestart this function serves as a predicate whether to roll pod or not
         * @return succeeded future if kafka pod was rolled and is ready
         */
        Future<Void> maybeRollKafka(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart) {
            return adminClientSecrets()
                .compose(compositeFuture -> new KafkaRoller(vertx, reconciliation, podOperations, 1_000, operationTimeoutMs,
                    () -> new BackOff(250, 2, 10), sts, compositeFuture.resultAt(0), compositeFuture.resultAt(1), adminClientProvider,
                        kafkaCluster.getBrokersConfiguration(), kafkaLogging, kafkaCluster.getKafkaVersion())
                    .rollingRestart(podNeedsRestart));
        }

        /**
         * <p>Final downgrade phase
         * <ol>
         *     <li>Update the strimzi.io/kafka-version to the new version</li>
         *     <li>Remove the strimzi.io/from-kafka-version since this is the last phase</li>
         *     <li>Remove the strimzi.io/to-kafka-version since this is the last phase</li>
         *     <li>Remove inter.broker.protocol.version (so the new version's default is used)</li>
         *     <li>Update the image in the STS</li>
         *     <li>Reconcile the STS and perform a rolling update of the pods</li>
         * </ol>
         */
        private Future<Void> kafkaDowngradePhase2(StatefulSet sts, ConfigMap cm, KafkaVersionChange versionChange, String downgradedImage) {
            log.info("{}: {}, phase 2", reconciliation, versionChange);
            // Remove the strimzi.io/from-version and strimzi.io/to-version since this is the last phase

            Map<String, String> annotations = Annotations.annotations(sts);

            log.info("{}: Upgrade: Removing annotations {}, {}",
                    reconciliation, ANNO_STRIMZI_IO_FROM_VERSION, ANNO_STRIMZI_IO_TO_VERSION);
            annotations.remove(ANNO_STRIMZI_IO_FROM_VERSION);
            annotations.remove(ANNO_STRIMZI_IO_TO_VERSION);
            annotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, versionChange.to().version());

            // Remove inter.broker.protocol.version (so the new version's default is used)
            String config = cm.getData().getOrDefault("server.config", "");
            log.info("{}: Upgrade: Removing Kafka config {}, will default to {}",
                    reconciliation, INTERBROKER_PROTOCOL_VERSION, versionChange.to().protocolVersion());

            StatefulSet newSts = new StatefulSetBuilder(sts)
                    .editMetadata()
                        .withAnnotations(annotations)
                    .endMetadata()
                    .build();

            StringBuilder newConfigBuilder = new StringBuilder();
            Arrays.stream(config.split(System.lineSeparator())).filter(line -> !line.startsWith(INTERBROKER_PROTOCOL_VERSION + "=")).forEach(line -> newConfigBuilder.append(line + System.lineSeparator()));
            String newConfig = newConfigBuilder.toString();

            ConfigMap newCm = new ConfigMapBuilder(cm).build();
            newCm.getData().put("server.config", newConfig);

            // Reconcile the STS and perform a rolling update of the pods
            String stsName = KafkaCluster.kafkaClusterName(this.name);
            String cmName = KafkaCluster.metricAndLogConfigsName(this.name);
            log.info("{}: Upgrade: Patch + rolling update of {}", reconciliation, stsName);
            return CompositeFuture.join(kafkaSetOperations.reconcile(namespace, stsName, newSts), configMapOperations.reconcile(namespace, cmName, newCm))
                    .compose(ignored -> maybeRollKafka(sts, pod -> {
                        log.info("{}: Upgrade: Patch + rolling update of {}: Pod {}", reconciliation, stsName, pod.getMetadata().getName());
                        return singletonList("Upgrade phase 2 of 2: Patch + rolling update of " + name + ": Pod " + pod.getMetadata().getName());
                    }))
                    .compose(ignored -> {
                        log.info("{}: {}, phase 2 of 2 completed", reconciliation, versionChange);
                        return Future.succeededFuture();
                    });
        }

        Future<ReconciliationState> getZookeeperDescription() {
            return zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                    .compose(sts -> {
                        Storage oldStorage = getOldStorage(sts);

                        if (sts != null && sts.getSpec() != null)   {
                            this.zkCurrentReplicas = sts.getSpec().getReplicas();
                        }

                        this.zkCluster = ZookeeperCluster.fromCrd(kafkaAssembly, versions, oldStorage, zkCurrentReplicas != null ? zkCurrentReplicas : 0);
                        this.zkService = zkCluster.generateService();
                        this.zkHeadlessService = zkCluster.generateHeadlessService();

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

                        if (zkCluster.getLogging() instanceof  ExternalLogging) {
                            return configMapOperations.getAsync(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) zkCluster.getLogging()).getName());
                        } else {
                            return Future.succeededFuture(null);
                        }
                    }).compose(cm -> {
                        ConfigMap logAndMetricsConfigMap = zkCluster.generateConfigurationConfigMap(cm);
                        this.zkMetricsAndLogsConfigMap = zkCluster.generateConfigurationConfigMap(logAndMetricsConfigMap);

                        String loggingConfiguration = zkMetricsAndLogsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
                        this.zkLoggingHash = Util.stringHash(loggingConfiguration);

                        return Future.succeededFuture(this);
                    });
        }

        Future<ReconciliationState> withZkDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.zkDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> withVoid(Future<?> r) {
            return r.map(this);
        }

        Future<ReconciliationState> zookeeperServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(namespace,
                    ZookeeperCluster.containerServiceAccountName(zkCluster.getCluster()),
                    zkCluster.generateServiceAccount()));
        }

        Future<ReconciliationState> zkService() {
            return withVoid(serviceOperations.reconcile(namespace, zkCluster.getServiceName(), zkService));
        }

        Future<ReconciliationState> zkHeadlessService() {
            return withVoid(serviceOperations.reconcile(namespace, zkCluster.getHeadlessServiceName(), zkHeadlessService));
        }

        Future<ReconciliationState> zkAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace, zkCluster.getAncillaryConfigMapName(), zkMetricsAndLogsConfigMap));
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
                    .compose(oldSecret -> secretOperations.reconcile(namespace, secretName, secret)
                            .map(res -> {
                                if (res instanceof ReconcileResult.Patched) {
                                    // The secret is patched and some changes to the existing certificates actually occured
                                    return ModelUtils.doExistingCertificatesDiffer(oldSecret, res.resource());
                                }

                                return false;
                            })
                    );
        }

        Future<ReconciliationState> zkNodesSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(ZookeeperCluster.nodesSecretName(name), zkCluster.generateNodesSecret(clusterCa, kafkaAssembly, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingZookeeperCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> zkNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(namespace, ZookeeperCluster.policyName(name), zkCluster.generateNetworkPolicy(pfa.isNamespaceAndPodSelectorNetworkPolicySupported())));
        }

        Future<ReconciliationState> zkPodDisruptionBudget() {
            return withVoid(podDisruptionBudgetOperator.reconcile(namespace, zkCluster.getName(), zkCluster.generatePodDisruptionBudget()));
        }

        Future<ReconciliationState> zkStatefulSet() {
            StatefulSet zkSts = zkCluster.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            Annotations.annotations(zkSts.getSpec().getTemplate()).put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(getCaCertGeneration(this.clusterCa)));
            Annotations.annotations(zkSts.getSpec().getTemplate()).put(Annotations.ANNO_STRIMZI_LOGGING_HASH, zkLoggingHash);
            return withZkDiff(zkSetOperations.reconcile(namespace, zkCluster.getName(), zkSts));
        }

        Future<ReconciliationState> zkRollingUpdate() {
            // Scale-down and Scale-up might have change the STS. we should get a fresh one.
            return zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                    .compose(sts -> zkSetOperations.maybeRollingUpdate(sts,
                        pod -> getReasonsToRestartPod(zkDiffs.resource(), pod, existingZookeeperCertsChanged, this.clusterCa)))
                    .map(this);
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
            Future<Secret> clusterCaCertSecretFuture = secretOperations.getAsync(namespace, KafkaResources.clusterCaCertificateSecretName(name));
            Future<Secret> coKeySecretFuture = secretOperations.getAsync(namespace, ClusterOperator.secretName(name));

            return CompositeFuture.join(clusterCaCertSecretFuture, coKeySecretFuture)
                    .compose(compositeFuture -> {
                        // Handle Cluster CA for connecting to Zoo
                        Secret clusterCaCertSecret = compositeFuture.resultAt(0);
                        if (clusterCaCertSecret == null) {
                            return Future.failedFuture(Util.missingSecretException(namespace, KafkaCluster.clusterCaKeySecretName(name)));
                        }

                        // Handle CO key for connecting to Zoo
                        Secret coKeySecret = compositeFuture.resultAt(1);
                        if (coKeySecret == null) {
                            return Future.failedFuture(Util.missingSecretException(namespace, ClusterOperator.secretName(name)));
                        }

                        Function<Integer, String> zkNodeAddress = (Integer i) -> ModelUtils.podDnsNameWithoutClusterDomain(
                                namespace,
                                KafkaResources.zookeeperHeadlessServiceName(name),
                                zkCluster.getPodName(i));

                        ZookeeperScaler zkScaler = zkScalerProvider.createZookeeperScaler(vertx, zkConnectionString(connectToReplicas, zkNodeAddress), zkNodeAddress, clusterCaCertSecret, coKeySecret, operationTimeoutMs);

                        return Future.succeededFuture(zkScaler);
                    });
        }

        Future<ReconciliationState> zkScalingUp() {
            int desired = zkCluster.getReplicas();

            if (zkCurrentReplicas != null
                    && zkCurrentReplicas < desired) {
                log.info("{}: Scaling Zookeeper up from {} to {} replicas", reconciliation, zkCurrentReplicas, desired);

                return zkScaler(zkCurrentReplicas)
                        .compose(zkScaler -> {
                            Promise<ReconciliationState> scalingPromise = Promise.promise();

                            zkScalingUpByOne(zkScaler, zkCurrentReplicas, desired)
                                    .onComplete(res -> {
                                        zkScaler.close();

                                        if (res.succeeded())    {
                                            scalingPromise.complete(res.result());
                                        } else {
                                            log.warn("{}: Failed to scale Zookeeper", reconciliation, res.cause());
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
                return zkSetOperations.scaleUp(namespace, zkCluster.getName(), current + 1)
                        .compose(ignore -> podOperations.readiness(namespace, zkCluster.getPodName(current), 1_000, operationTimeoutMs))
                        .compose(ignore -> zkScaler.scale(current + 1))
                        .compose(ignore -> zkScalingUpByOne(zkScaler, current + 1, desired));
            } else {
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScalingDown() {
            int desired = zkCluster.getReplicas();

            if (zkCurrentReplicas != null
                    && zkCurrentReplicas > desired) {
                // With scaling
                log.info("{}: Scaling Zookeeper down from {} to {} replicas", reconciliation, zkCurrentReplicas, desired);

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
                                            log.warn("{}: Failed to scale Zookeeper", reconciliation, res.cause());
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
                        .compose(ignore -> zkSetOperations.scaleDown(namespace, zkCluster.getName(), current - 1))
                        .compose(ignore -> zkScalingDownByOne(zkScaler, current - 1, desired));
            } else {
                return Future.succeededFuture(this);
            }
        }


        Future<ReconciliationState> zkScalingCheck() {
            // No scaling, but we should check the configuration
            // This can cover any previous failures in the Zookeeper reconfiguration
            log.debug("{}: Verifying that Zookeeper is configured to run with {} replicas", reconciliation, zkCurrentReplicas);

            // No need to check for pod readiness since we run right after the readiness check
            return zkScaler(zkCluster.getReplicas())
                    .compose(zkScaler -> {
                        Promise<ReconciliationState> scalingPromise = Promise.promise();

                        zkScaler.scale(zkCluster.getReplicas()).onComplete(res -> {
                            zkScaler.close();

                            if (res.succeeded())    {
                                scalingPromise.complete(this);
                            } else {
                                log.warn("{}: Failed to verify Zookeeper configuration", res.cause());
                                scalingPromise.fail(res.cause());
                            }
                        });

                        return scalingPromise.future();
                    });
        }

        Future<ReconciliationState> zkServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(namespace, zkService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> zkHeadlessServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(namespace, zkHeadlessService, 1_000, operationTimeoutMs));
        }

        /*test*/ Future<ReconciliationState> getKafkaClusterDescription() {
            return kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name))
                    .compose(sts -> {
                        Storage oldStorage = getOldStorage(sts);

                        kafkaCurrentReplicas = 0;
                        if (sts != null && sts.getSpec() != null)   {
                            kafkaCurrentReplicas = sts.getSpec().getReplicas();
                        }

                        this.kafkaCluster = KafkaCluster.fromCrd(kafkaAssembly, versions, oldStorage, kafkaCurrentReplicas);
                        this.kafkaService = kafkaCluster.generateService();
                        this.kafkaHeadlessService = kafkaCluster.generateHeadlessService();
                        return Future.succeededFuture(this);
                    });
        }

        Future<ReconciliationState> withKafkaDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.kafkaDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> kafkaInitServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(namespace,
                    KafkaCluster.initContainerServiceAccountName(kafkaCluster.getCluster()),
                    kafkaCluster.generateServiceAccount()));
        }

        Future<ReconciliationState> kafkaInitClusterRoleBinding() {
            ClusterRoleBinding desired = kafkaCluster.generateClusterRoleBinding(namespace);
            Future<ReconcileResult<ClusterRoleBinding>> fut = clusterRoleBindingOperations.reconcile(
                    KafkaCluster.initContainerClusterRoleBindingName(namespace, name), desired);

            Promise replacementPromise = Promise.promise();

            fut.onComplete(res -> {
                if (res.failed()) {
                    if (desired == null && res.cause() != null && res.cause().getMessage() != null &&
                            res.cause().getMessage().contains("Message: Forbidden!")) {
                        log.debug("Ignoring forbidden access to ClusterRoleBindings which seems not needed while Kafka rack awareness is disabled.");
                        replacementPromise.complete();
                    } else {
                        replacementPromise.fail(res.cause());
                    }
                } else {
                    replacementPromise.complete();
                }
            });

            return withVoid(replacementPromise.future());
        }

        Future<ReconciliationState> kafkaScaleDown() {
            return withVoid(kafkaSetOperations.scaleDown(namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
        }

        Future<ReconciliationState> kafkaService() {
            Future<ReconcileResult<Service>> serviceFuture = serviceOperations.reconcile(namespace, kafkaCluster.getServiceName(), kafkaService);

            return withVoid(serviceFuture.map(res -> {
                setPlainAndTlsListenerStatus();
                return res;
            }));
        }

        Future<ReconciliationState> kafkaHeadlessService() {
            return withVoid(serviceOperations.reconcile(namespace, kafkaCluster.getHeadlessServiceName(), kafkaHeadlessService));
        }

        Future<ReconciliationState> kafkaExternalBootstrapService() {
            return withVoid(serviceOperations.reconcile(namespace, KafkaCluster.externalBootstrapServiceName(name), kafkaCluster.generateExternalBootstrapService()));
        }

        Future<ReconciliationState> kafkaReplicaServices() {
            int replicas = kafkaCluster.getReplicas();
            List<Future> serviceFutures = new ArrayList<>(replicas);

            for (int i = 0; i < replicas; i++) {
                serviceFutures.add(serviceOperations.reconcile(namespace, KafkaCluster.externalServiceName(name, i), kafkaCluster.generateExternalService(i)));
            }

            return withVoid(CompositeFuture.join(serviceFutures));
        }

        Future<ReconciliationState> kafkaBootstrapRoute() {
            Route route = kafkaCluster.generateExternalBootstrapRoute();

            if (pfa.hasRoutes()) {
                return withVoid(routeOperations.reconcile(namespace, KafkaCluster.serviceName(name), route));
            } else if (route != null) {
                log.warn("{}: The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster {} using routes is not possible.", reconciliation, name);
                return withVoid(Future.failedFuture("The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster " + name + " using routes is not possible."));
            }

            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> kafkaReplicaRoutes() {
            int replicas = kafkaCluster.getReplicas();
            List<Future> routeFutures = new ArrayList<>(replicas);

            for (int i = 0; i < replicas; i++) {
                Route route = kafkaCluster.generateExternalRoute(i);

                if (pfa.hasRoutes()) {
                    routeFutures.add(routeOperations.reconcile(namespace, KafkaCluster.externalServiceName(name, i), route));
                } else if (route != null) {
                    log.warn("{}: The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster {} using routes is not possible.", reconciliation, name);
                    return withVoid(Future.failedFuture("The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster " + name + " using routes is not possible."));
                }
            }

            return withVoid(CompositeFuture.join(routeFutures));
        }

        Future<ReconciliationState> kafkaBootstrapIngress() {
            if (kafkaCluster.isExposedWithIngress()) {
                Ingress ingress = kafkaCluster.generateExternalBootstrapIngress();

                if (kafkaCluster.getExternalListenerBootstrapOverride() != null && kafkaCluster.getExternalListenerBootstrapOverride().getAddress() != null) {
                    log.debug("{}: Adding address {} from overrides to certificate DNS names", reconciliation, kafkaCluster.getExternalListenerBootstrapOverride().getAddress());
                    this.kafkaExternalBootstrapDnsName.add(kafkaCluster.getExternalListenerBootstrapOverride().getAddress());
                }

                this.kafkaExternalBootstrapDnsName.add(ingress.getSpec().getRules().get(0).getHost());

                return withVoid(ingressOperations.reconcile(namespace, KafkaCluster.serviceName(name), ingress));
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        Future<ReconciliationState> kafkaReplicaIngress() {
            if (kafkaCluster.isExposedWithIngress()) {
                int replicas = kafkaCluster.getReplicas();
                List<Future> routeFutures = new ArrayList<>(replicas);

                for (int i = 0; i < replicas; i++) {
                    Ingress ingress = kafkaCluster.generateExternalIngress(i);

                    Set<String> dnsNames = new HashSet<>();

                    String dnsOverride = kafkaCluster.getExternalServiceAdvertisedHostOverride(i);
                    if (dnsOverride != null)    {
                        dnsNames.add(dnsOverride);
                    }

                    String host = ingress.getSpec().getRules().get(0).getHost();
                    dnsNames.add(host);

                    this.kafkaExternalDnsNames.put(i, dnsNames);
                    this.kafkaExternalAdvertisedHostnames.add(kafkaCluster.getExternalAdvertisedHostname(i, host));
                    this.kafkaExternalAdvertisedPorts.add(kafkaCluster.getExternalAdvertisedPort(i, "443"));

                    routeFutures.add(ingressOperations.reconcile(namespace, KafkaCluster.externalServiceName(name, i), ingress));
                }

                return withVoid(CompositeFuture.join(routeFutures));
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        Future<ReconciliationState> kafkaExternalBootstrapServiceReady() {
            if (!kafkaCluster.isExposedWithLoadBalancer() && !kafkaCluster.isExposedWithNodePort()) {
                return withVoid(Future.succeededFuture());
            }

            if (kafkaCluster.getExternalListenerBootstrapOverride() != null && kafkaCluster.getExternalListenerBootstrapOverride().getAddress() != null)    {
                log.trace("{}: Adding address {} from overrides to certificate DNS names", reconciliation, kafkaCluster.getExternalListenerBootstrapOverride().getAddress());
                this.kafkaExternalBootstrapDnsName.add(kafkaCluster.getExternalListenerBootstrapOverride().getAddress());
            }

            Promise blockingPromise = Promise.promise();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    String serviceName = KafkaCluster.externalBootstrapServiceName(name);
                    Future<Void> address = null;

                    if (kafkaCluster.isExposedWithNodePort()) {
                        address = serviceOperations.hasNodePort(namespace, serviceName, 1_000, operationTimeoutMs);
                    } else {
                        address = serviceOperations.hasIngressAddress(namespace, serviceName, 1_000, operationTimeoutMs);
                    }

                    address.onComplete(res -> {
                        if (res.succeeded()) {
                            if (kafkaCluster.isExposedWithLoadBalancer()) {
                                String bootstrapAddress = null;

                                if (serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null) {
                                    bootstrapAddress = serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                } else {
                                    bootstrapAddress = serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                }

                                if (log.isTraceEnabled()) {
                                    log.trace("{}: Found address {} for Service {}", reconciliation, bootstrapAddress, serviceName);
                                }

                                this.kafkaExternalBootstrapDnsName.add(bootstrapAddress);

                                setExternalListenerStatus(new ListenerAddressBuilder()
                                        .withHost(bootstrapAddress)
                                        .withPort(kafkaCluster.getLoadbalancerPort())
                                        .build());
                            } else if (kafkaCluster.isExposedWithNodePort()) {
                                ServiceSpec sts = serviceOperations.get(namespace, serviceName).getSpec();
                                externalBootstrapNodePort = sts.getPorts().get(0).getNodePort();
                            }
                            future.complete();
                        } else {
                            if (kafkaCluster.isExposedWithNodePort()) {
                                log.warn("{}: Node port was not assigned for Service {}.", reconciliation, serviceName);
                                future.fail("Node port was not assigned for Service " + serviceName + ".");
                            } else {
                                log.warn("{}: No loadbalancer address found in the Status section of Service {} resource. Loadbalancer was probably not provisioned.", reconciliation, serviceName);
                                future.fail("No loadbalancer address found in the Status section of Service " + serviceName + " resource. Loadbalancer was probably not provisioned.");
                            }
                        }
                    });
                }, res -> {
                    if (res.succeeded()) {
                        blockingPromise.complete();
                    } else {
                        blockingPromise.fail(res.cause());
                    }
                });

            return withVoid(blockingPromise.future());
        }

        Future<ReconciliationState> kafkaNodePortExternalListenerStatus() {
            List<Node> nodes = new ArrayList<>();

            if (kafkaCluster.isExposedWithNodePort())   {
                return nodeOperator.listAsync(Labels.EMPTY)
                        .compose(result -> {
                            nodes.addAll(result);
                            return podOperations.listAsync(namespace, kafkaCluster.getSelectorLabels());
                        })
                        .map(pods -> {
                            Set<ListenerAddress> statusAddresses = new HashSet<>();

                            for (Pod broker : pods) {
                                String podName = broker.getMetadata().getName();
                                Integer podIndex = getPodIndexFromPodName(podName);

                                if (kafkaCluster.getExternalServiceAdvertisedHostOverride(podIndex) != null)    {
                                    ListenerAddress address = new ListenerAddressBuilder()
                                            .withHost(kafkaCluster.getExternalServiceAdvertisedHostOverride(podIndex))
                                            .withPort(externalBootstrapNodePort)
                                            .build();

                                    statusAddresses.add(address);
                                } else if (broker.getStatus() != null && broker.getStatus().getHostIP() != null) {
                                    String hostIP = broker.getStatus().getHostIP();
                                    Node podNode = nodes.stream().filter(node -> {
                                        if (node.getStatus() != null && node.getStatus().getAddresses() != null)    {
                                            return null != node.getStatus().getAddresses().stream().filter(address -> hostIP.equals(address.getAddress())).findFirst().orElse(null);
                                        } else {
                                            return false;
                                        }
                                    }).findFirst().orElse(null);

                                    if (podNode != null) {
                                        ListenerAddress address = new ListenerAddressBuilder()
                                                .withHost(NodeUtils.findAddress(podNode.getStatus().getAddresses(), kafkaCluster.getPreferredNodeAddressType()))
                                                .withPort(externalBootstrapNodePort)
                                                .build();

                                        statusAddresses.add(address);
                                    }

                                } else {
                                    continue;
                                }
                            }

                            setExternalListenerStatus(statusAddresses.toArray(new ListenerAddress[statusAddresses.size()]));

                            return this;
                        });
            } else {
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> kafkaReplicaServicesReady() {
            if (!kafkaCluster.isExposedWithLoadBalancer() && !kafkaCluster.isExposedWithNodePort()) {
                return withVoid(Future.succeededFuture());
            }

            Promise blockingPromise = Promise.promise();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    int replicas = kafkaCluster.getReplicas();
                    List<Future> serviceFutures = new ArrayList<>(replicas);

                    for (int i = 0; i < replicas; i++) {
                        String serviceName = KafkaCluster.externalServiceName(name, i);
                        Promise servicePromise = Promise.promise();

                        Future<Void> address = null;
                        Set<String> dnsNames = new HashSet<>();

                        String dnsOverride = kafkaCluster.getExternalServiceAdvertisedHostOverride(i);
                        if (dnsOverride != null)    {
                            dnsNames.add(dnsOverride);
                        }

                        if (kafkaCluster.isExposedWithNodePort()) {
                            address = serviceOperations.hasNodePort(namespace, serviceName, 1_000, operationTimeoutMs);
                        } else {
                            address = serviceOperations.hasIngressAddress(namespace, serviceName, 1_000, operationTimeoutMs);
                        }

                        int podNumber = i;

                        address.onComplete(res -> {
                            if (res.succeeded()) {
                                if (kafkaCluster.isExposedWithLoadBalancer()) {
                                    // Get the advertised URL
                                    String serviceAddress = null;

                                    if (serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null) {
                                        serviceAddress = serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                    } else {
                                        serviceAddress = serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                    }

                                    if (log.isTraceEnabled()) {
                                        log.trace("{}: Found address {} for Service {}", reconciliation, serviceAddress, serviceName);
                                    }

                                    this.kafkaExternalAdvertisedHostnames.add(kafkaCluster.getExternalAdvertisedHostname(podNumber, serviceAddress));
                                    this.kafkaExternalAdvertisedPorts.add(kafkaCluster.getExternalAdvertisedPort(podNumber, "9094"));

                                    // Collect the DNS names for certificates
                                    for (LoadBalancerIngress ingress : serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress())    {
                                        if (ingress.getHostname() != null) {
                                            dnsNames.add(ingress.getHostname());
                                        } else {
                                            dnsNames.add(ingress.getIp());
                                        }
                                    }
                                } else if (kafkaCluster.isExposedWithNodePort()) {
                                    // Get the advertised URL
                                    String port = serviceOperations.get(namespace, serviceName).getSpec().getPorts()
                                        .get(0).getNodePort().toString();

                                    if (log.isTraceEnabled()) {
                                        log.trace("{}: Found port {} for Service {}", reconciliation, port, serviceName);
                                    }

                                    // For node ports, when the override is not set, we don't pass any advertised hostname
                                    String advertisedHostname = kafkaCluster.getExternalAdvertisedHostname(podNumber, null);
                                    if (advertisedHostname != null) {
                                        this.kafkaExternalAdvertisedHostnames.add(advertisedHostname);
                                    }

                                    this.kafkaExternalAdvertisedPorts.add(kafkaCluster.getExternalAdvertisedPort(podNumber, port));
                                }

                                this.kafkaExternalDnsNames.put(podNumber, dnsNames);

                                servicePromise.complete();
                            } else {
                                if (kafkaCluster.isExposedWithNodePort()) {
                                    log.warn("{}: Node port was not assigned for Service {}.", reconciliation, serviceName);
                                    servicePromise.fail("Node port was not assigned for Service " + serviceName + ".");
                                } else {
                                    log.warn("{}: No loadbalancer address found in the Status section of Service {} resource. Loadbalancer was probably not provisioned.", reconciliation, serviceName);
                                    servicePromise.fail("No loadbalancer address found in the Status section of Service " + serviceName + " resource. Loadbalancer was probably not provisioned.");
                                }
                            }
                        });

                        serviceFutures.add(servicePromise.future());
                    }

                    CompositeFuture.join(serviceFutures).onComplete(res -> {
                        if (res.succeeded()) {
                            future.complete();
                        } else {
                            future.fail(res.cause());
                        }
                    });
                }, res -> {
                    if (res.succeeded()) {
                        blockingPromise.complete();
                    } else {
                        blockingPromise.fail(res.cause());
                    }
                });

            return withVoid(blockingPromise.future());
        }

        Future<ReconciliationState> kafkaBootstrapRouteReady() {
            if (routeOperations == null || !kafkaCluster.isExposedWithRoute()) {
                return withVoid(Future.succeededFuture());
            }

            if (kafkaCluster.getExternalListenerBootstrapOverride() != null && kafkaCluster.getExternalListenerBootstrapOverride().getAddress() != null)    {
                log.trace("{}: Adding address {} from overrides to certificate DNS names", reconciliation, kafkaCluster.getExternalListenerBootstrapOverride().getAddress());
                this.kafkaExternalBootstrapDnsName.add(kafkaCluster.getExternalListenerBootstrapOverride().getAddress());
            }

            Promise blockingPromise = Promise.promise();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    String routeName = KafkaCluster.serviceName(name);
                    Future<Void> address = routeOperations.hasAddress(namespace, routeName, 1_000, operationTimeoutMs);

                    address.onComplete(res -> {
                        if (res.succeeded()) {
                            String bootstrapAddress = routeOperations.get(namespace, routeName).getStatus().getIngress().get(0).getHost();
                            this.kafkaExternalBootstrapDnsName.add(bootstrapAddress);

                            setExternalListenerStatus(new ListenerAddressBuilder()
                                    .withHost(bootstrapAddress)
                                    .withPort(kafkaCluster.getRoutePort())
                                    .build());

                            if (log.isTraceEnabled()) {
                                log.trace("{}: Found address {} for Route {}", reconciliation, bootstrapAddress, routeName);
                            }

                            future.complete();
                        } else {
                            log.warn("{}: No route address found in the Status section of Route {} resource. Route was probably not provisioned by the OpenShift router.", reconciliation, routeName);
                            future.fail("No route address found in the Status section of Route " + routeName + " resource. Route was probably not provisioned by the OpenShift router.");
                        }
                    });
                }, res -> {
                    if (res.succeeded()) {
                        blockingPromise.complete();
                    } else {
                        blockingPromise.fail(res.cause());
                    }
                });

            return withVoid(blockingPromise.future());
        }

        Future<ReconciliationState> kafkaReplicaRoutesReady() {
            if (routeOperations == null || !kafkaCluster.isExposedWithRoute()) {
                return withVoid(Future.succeededFuture());
            }

            Promise blockingPromise = Promise.promise();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    int replicas = kafkaCluster.getReplicas();
                    List<Future> routeFutures = new ArrayList<>(replicas);

                    for (int i = 0; i < replicas; i++) {
                        String routeName = KafkaCluster.externalServiceName(name, i);
                        Promise routePromise = Promise.promise();
                        Future<Void> address = routeOperations.hasAddress(namespace, routeName, 1_000, operationTimeoutMs);
                        int podNumber = i;

                        Set<String> dnsNames = new HashSet<>();

                        String dnsOverride = kafkaCluster.getExternalServiceAdvertisedHostOverride(i);
                        if (dnsOverride != null)    {
                            dnsNames.add(dnsOverride);
                        }

                        address.onComplete(res -> {
                            if (res.succeeded()) {
                                Route route = routeOperations.get(namespace, routeName);

                                // Get the advertised URL
                                String routeAddress = route.getStatus().getIngress().get(0).getHost();
                                this.kafkaExternalAdvertisedHostnames.add(kafkaCluster.getExternalAdvertisedHostname(podNumber, routeAddress));
                                this.kafkaExternalAdvertisedPorts.add(kafkaCluster.getExternalAdvertisedPort(podNumber, "443"));

                                if (log.isTraceEnabled()) {
                                    log.trace("{}: Found address {} for Route {}", reconciliation, routeAddress, routeName);
                                }

                                // Collect the DNS names for certificates
                                for (RouteIngress ingress : route.getStatus().getIngress()) {
                                    dnsNames.add(ingress.getHost());
                                }

                                this.kafkaExternalDnsNames.put(podNumber, dnsNames);

                                routePromise.complete();
                            } else {
                                log.warn("{}: No route address found in the Status section of Route {} resource. Route was probably not provisioned by the OpenShift router.", reconciliation, routeName);
                                routePromise.fail("No route address found in the Status section of Route " + routeName + " resource. Route was probably not provisioned by the OpenShift router.");
                            }
                        });

                        routeFutures.add(routePromise.future());
                    }

                    CompositeFuture.join(routeFutures).onComplete(res -> {
                        if (res.succeeded()) {
                            future.complete();
                        } else {
                            future.fail(res.cause());
                        }
                    });
                }, res -> {
                    if (res.succeeded()) {
                        blockingPromise.complete();
                    } else {
                        blockingPromise.fail(res.cause());
                    }
                });

            return withVoid(blockingPromise.future());
        }

        Future<ReconciliationState> kafkaGenerateCertificates(Supplier<Date> dateSupplier) {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        kafkaCluster.generateCertificates(kafkaAssembly,
                                clusterCa, kafkaExternalBootstrapDnsName, kafkaExternalDnsNames,
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

        Future<ReconciliationState> customTlsListenerCertificate() {
            CertAndKeySecretSource customCertSecret = kafkaCluster.getSecretSourceTls();

            return getCustomCertificateSecret(customCertSecret)
                    .compose(secret -> {
                        if (secret != null)  {
                            byte[] publicKeyBytes = Base64.getDecoder().decode(secret.getData().get(customCertSecret.getCertificate()));
                            tlsListenerCustomCertificate = new String(publicKeyBytes, StandardCharsets.US_ASCII);
                            tlsListenerCustomCertificateThumbprint = getCertificateThumbprint(secret, customCertSecret);

                            return Future.succeededFuture(this);
                        } else {
                            return Future.succeededFuture(this);
                        }
                    });
        }

        Future<ReconciliationState> customExternalListenerCertificate() {
            CertAndKeySecretSource customCertSecret = kafkaCluster.getSecretSourceExternal();

            return getCustomCertificateSecret(customCertSecret)
                    .compose(secret -> {
                        if (secret != null)  {
                            byte[] publicKeyBytes = Base64.getDecoder().decode(secret.getData().get(customCertSecret.getCertificate()));
                            externalListenerCustomCertificate = new String(publicKeyBytes, StandardCharsets.US_ASCII);
                            externalListenerCustomCertificateThumbprint = getCertificateThumbprint(secret, customCertSecret);

                            return Future.succeededFuture(this);
                        } else {
                            return Future.succeededFuture(this);
                        }
                    });
        }

        Future<Secret> getCustomCertificateSecret(CertAndKeySecretSource customCertSecret)  {
            Promise<Secret> certificatePromise = Promise.promise();
            if (customCertSecret != null)   {
                secretOperations.getAsync(namespace, customCertSecret.getSecretName())
                        .onComplete(result -> {
                            if (result.succeeded()) {
                                Secret certSecret = result.result();
                                if (certSecret != null) {
                                    if (!certSecret.getData().containsKey(customCertSecret.getCertificate())) {
                                        certificatePromise.fail(new InvalidResourceException("Secret " + customCertSecret.getSecretName() + " does not contain certificate under the key " + customCertSecret.getCertificate() + "."));
                                    } else if (!certSecret.getData().containsKey(customCertSecret.getKey())) {
                                        certificatePromise.fail(new InvalidResourceException("Secret " + customCertSecret.getSecretName() + " does not contain custom certificate private key under the key " + customCertSecret.getKey() + "."));
                                    } else  {
                                        certificatePromise.complete(certSecret);
                                    }
                                } else {
                                    certificatePromise.fail(new InvalidResourceException("Secret " + customCertSecret.getSecretName() + " with custom TLS certificate does not exist."));
                                }
                            } else {
                                certificatePromise.fail(new NoSuchResourceException("Failed to get secret " + customCertSecret.getSecretName() + " with custom TLS certificate."));
                            }
                        });
            } else {
                certificatePromise.complete(null);
            }

            return certificatePromise.future();
        }

        String getCertificateThumbprint(Secret certSecret, CertAndKeySecretSource customCertSecret)   {
            try {
                X509Certificate cert = Ca.cert(certSecret, customCertSecret.getCertificate());
                byte[] signature = MessageDigest.getInstance("SHA-256").digest(cert.getEncoded());
                return Base64.getEncoder().encodeToString(signature);
            } catch (NoSuchAlgorithmException | CertificateEncodingException e) {
                throw new RuntimeException("Failed to get certificate signature of " + customCertSecret.getCertificate() + " from Secret " + certSecret.getMetadata().getName(), e);
            }
        }

        ConfigMap getKafkaAncillaryCm()    {
            ConfigMap loggingCm = null;

            if (kafkaCluster.getLogging() instanceof ExternalLogging) {
                loggingCm = configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) kafkaCluster.getLogging()).getName());
            }

            ConfigMap brokerCm = kafkaCluster.generateAncillaryConfigMap(loggingCm, kafkaExternalAdvertisedHostnames, kafkaExternalAdvertisedPorts);

            // if BROKER_ADVERTISED_HOSTNAMES_FILENAME or BROKER_ADVERTISED_PORTS_FILENAME changes, compute a hash and put it into annotation
            String brokerConfiguration = brokerCm.getData().getOrDefault(KafkaCluster.BROKER_ADVERTISED_HOSTNAMES_FILENAME, "");
            brokerConfiguration += brokerCm.getData().getOrDefault(KafkaCluster.BROKER_ADVERTISED_PORTS_FILENAME, "");

            this.kafkaBrokerConfigurationHash = Util.stringHash(brokerConfiguration);
            KafkaConfiguration kc = KafkaConfiguration.unvalidated(kafkaCluster.getBrokersConfiguration());
            this.kafkaBrokerConfigurationHash += Util.stringHash(kc.unknownConfigsWithValues(kafkaCluster.getKafkaVersion()).toString());

            String loggingConfiguration = brokerCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
            this.kafkaLogging = loggingConfiguration;
            this.kafkaLoggingAppendersHash = Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(loggingConfiguration));

            return brokerCm;
        }

        Future<ReconciliationState> kafkaAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace, kafkaCluster.getAncillaryConfigMapName(), getKafkaAncillaryCm()));
        }

        Future<ReconciliationState> kafkaBrokersSecret() {
            return updateCertificateSecretWithDiff(KafkaCluster.brokersSecretName(name), kafkaCluster.generateBrokersSecret())
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
                        return withVoid(secretOperations.reconcile(namespace, KafkaCluster.jmxSecretName(name),
                                kafkaCluster.generateJmxSecret()));
                    }
                    return withVoid(Future.succeededFuture(this));
                });

            }
            return withVoid(secretOperations.reconcile(namespace, KafkaCluster.jmxSecretName(name), null));
        }

        Future<ReconciliationState> kafkaNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(namespace, KafkaCluster.policyName(name), kafkaCluster.generateNetworkPolicy(pfa.isNamespaceAndPodSelectorNetworkPolicySupported())));
        }

        Future<ReconciliationState> kafkaPodDisruptionBudget() {
            return withVoid(podDisruptionBudgetOperator.reconcile(namespace, kafkaCluster.getName(), kafkaCluster.generatePodDisruptionBudget()));
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
                        } else if (currentPvc.getStatus().getConditions().stream().filter(cond -> "Resizing".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))).findFirst().orElse(null) != null)  {
                            // The PVC is Bound but it is already resizing => Nothing to do, we should let it resize
                            log.debug("{}: The PVC {} is resizing, nothing to do", reconciliation, desiredPvc.getMetadata().getName());
                            resultPromise.complete();
                        } else if (currentPvc.getStatus().getConditions().stream().filter(cond -> "FileSystemResizePending".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))).findFirst().orElse(null) != null)  {
                            // The PVC is Bound and resized but waiting for FS resizing => We need to restart the pod which is using it
                            String podName = cluster.getPodName(getPodIndexFromPvcName(desiredPvc.getMetadata().getName()));
                            fsResizingRestartRequest.add(podName);
                            log.info("{}: The PVC {} is waiting for file system resizing and the pod {} needs to be restarted.", reconciliation, desiredPvc.getMetadata().getName(), podName);
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

            pvcOperations.reconcile(namespace, desired.getMetadata().getName(), desired).onComplete(pvcRes -> {
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
                            log.warn("{}: Storage Class {} not found. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", reconciliation, storageClassName, desired.getMetadata().getName());
                            resultPromise.complete();
                        } else if (sc.getAllowVolumeExpansion() == null || !sc.getAllowVolumeExpansion())    {
                            // Resizing not suported in SC => do nothing
                            log.warn("{}: Storage Class {} does not support resizing of volumes. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", reconciliation, storageClassName, desired.getMetadata().getName());
                            resultPromise.complete();
                        } else  {
                            // Resizing supported by SC => We can reconcile the PVC to have it resized
                            log.info("{}: Resizing PVC {} from {} to {}.", reconciliation, desired.getMetadata().getName(), current.getStatus().getCapacity().get("storage").getAmount(), desired.getSpec().getResources().getRequests().get("storage").getAmount());
                            pvcOperations.reconcile(namespace, desired.getMetadata().getName(), desired).onComplete(pvcRes -> {
                                if (pvcRes.succeeded()) {
                                    resultPromise.complete();
                                } else {
                                    resultPromise.fail(pvcRes.cause());
                                }
                            });
                        }
                    } else {
                        log.error("{}: Storage Class {} not found. PVC {} cannot be resized.", reconciliation, storageClassName, desired.getMetadata().getName(), scRes.cause());
                        resultPromise.fail(scRes.cause());
                    }
                });
            } else {
                log.warn("{}: PVC {} does not use any Storage Class and cannot be resized. Reconciliation will proceed without reconciling this PVC.", reconciliation, desired.getMetadata().getName());
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

        StatefulSet getKafkaStatefulSet()   {
            StatefulSet kafkaSts = kafkaCluster.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            PodTemplateSpec template = kafkaSts.getSpec().getTemplate();

            // Annotations with CA generations to help with rolling updates when CA changes
            Annotations.annotations(template).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION,
                    String.valueOf(getCaCertGeneration(this.clusterCa)));
            Annotations.annotations(template).put(
                    Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION,
                    String.valueOf(getCaCertGeneration(this.clientsCa)));

            Annotations.annotations(template).put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, kafkaLoggingAppendersHash);
            Annotations.annotations(template).put(KafkaCluster.ANNO_STRIMZI_BROKER_CONFIGURATION_HASH, kafkaBrokerConfigurationHash);

            // Annotations with custom cert thumbprints to help with rolling updates when they change
            if (tlsListenerCustomCertificateThumbprint != null) {
                Annotations.annotations(template).put(
                        KafkaCluster.ANNO_STRIMZI_CUSTOM_CERT_THUMBPRINT_TLS_LISTENER,
                        tlsListenerCustomCertificateThumbprint);
            }

            if (externalListenerCustomCertificateThumbprint != null) {
                Annotations.annotations(template).put(
                        KafkaCluster.ANNO_STRIMZI_CUSTOM_CERT_THUMBPRINT_EXTERNAL_LISTENER,
                        externalListenerCustomCertificateThumbprint);
            }

            return kafkaSts;
        }

        Future<ReconciliationState> kafkaStatefulSet() {
            return withKafkaDiff(kafkaSetOperations.reconcile(namespace, kafkaCluster.getName(), getKafkaStatefulSet()));
        }

        Future<ReconciliationState> kafkaRollingUpdate() {
            return withVoid(maybeRollKafka(kafkaDiffs.resource(), pod ->
                    getReasonsToRestartPod(kafkaDiffs.resource(), pod, existingKafkaCertsChanged, this.clusterCa, this.clientsCa)));
        }

        Future<ReconciliationState> kafkaScaleUp() {
            return withVoid(kafkaSetOperations.scaleUp(namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
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
                log.debug("{}: Checking readiness of pod {}.", reconciliation, model.getPodName(i));
                podFutures.add(podOperations.readiness(namespace, model.getPodName(i), 1_000, operationTimeoutMs));
            }

            return withVoid(CompositeFuture.join(podFutures));
        }

        Future<ReconciliationState> kafkaServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(namespace, kafkaService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaHeadlessServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(namespace, kafkaHeadlessService, 1_000, operationTimeoutMs));
        }

        /**
         * Will check all Zookeeper pods whether the user requested the pod and PVC deletion through an annotation
         *
         * @return
         */
        Future<ReconciliationState> zkManualPodCleaning() {
            String stsName = ZookeeperCluster.zookeeperClusterName(name);
            Future<StatefulSet> futureSts = zkSetOperations.getAsync(namespace, stsName);
            Promise<Void> resultPromise = Promise.promise();

            futureSts.onComplete(res -> {
                if (res.succeeded())    {
                    List<PersistentVolumeClaim> desiredPvcs = zkCluster.generatePersistentVolumeClaims();
                    Future<List<PersistentVolumeClaim>> existingPvcsFuture = pvcOperations.listAsync(namespace, zkCluster.getSelectorLabels());

                    maybeCleanPodAndPvc(zkSetOperations, res.result(), desiredPvcs, existingPvcsFuture).onComplete(resultPromise);
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return withVoid(resultPromise.future());
        }

        /**
         * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
         *
         * @return
         */
        Future<ReconciliationState> kafkaManualPodCleaning() {
            String stsName = KafkaCluster.kafkaClusterName(name);
            Future<StatefulSet> futureSts = kafkaSetOperations.getAsync(namespace, stsName);
            Promise<Void> resultPromise = Promise.promise();

            futureSts.onComplete(res -> {
                if (res.succeeded())    {
                    StatefulSet sts = res.result();

                    // The storage can change when the JBOD volumes are added / removed etc.
                    // At this point, the STS has not been updated yet. So we use the old storage configuration to get the old PVCs.
                    // This is needed because the restarted pod will be created from old statefulset with old storage configuration.
                    List<PersistentVolumeClaim> desiredPvcs = kafkaCluster.generatePersistentVolumeClaims(getOldStorage(sts));

                    Future<List<PersistentVolumeClaim>> existingPvcsFuture = pvcOperations.listAsync(namespace, kafkaCluster.getSelectorLabels());

                    maybeCleanPodAndPvc(kafkaSetOperations, sts, desiredPvcs, existingPvcsFuture).onComplete(resultPromise);
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return withVoid(resultPromise.future());
        }

        /**
         * Internal method for checking Pods and PVCs for deletion. It goes through the pods and checks the annotations
         * to decide whether they should be deleted. For the first pod marked to be deleted, it will gather the existing
         * and desired PVCs to be able to delete and recreate them.
         *
         * This method exits with the first Pod it finds where deletion is required. The deletion is done asynchronously
         * so if we deleted multiple pods at the same time we would either need to sync the deletions of different pods
         * to not happen in parallel or have a risk that there will be several pods deleted at the same time (which can
         * affect availability). If multiple pods are marked for deletion, the next one will be deleted in the next loop.
         *
         * @param stsOperator           StatefulSet Operator for managing stateful sets
         * @param sts                   StatefulSet which owns the pods which should be checked for deletion
         * @param desiredPvcs           The list of PVCs which should exist
         * @param existingPvcsFuture    Future which will return a list of PVCs which actually exist
         * @return
         */
        @SuppressWarnings("deprecation")
        Future<Void> maybeCleanPodAndPvc(StatefulSetOperator stsOperator, StatefulSet sts, List<PersistentVolumeClaim> desiredPvcs, Future<List<PersistentVolumeClaim>> existingPvcsFuture)  {
            if (sts != null) {
                log.debug("{}: Considering manual cleaning of Pods for StatefulSet {}", reconciliation, sts.getMetadata().getName());

                String stsName = sts.getMetadata().getName();

                for (int i = 0; i < sts.getSpec().getReplicas(); i++) {
                    String podName = stsName + "-" + i;
                    Pod pod = podOperations.get(namespace, podName);

                    if (pod != null) {
                        if (Annotations.booleanAnnotation(pod, AbstractScalableResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC,
                                false, AbstractScalableResourceOperator.ANNO_OP_STRIMZI_IO_DELETE_POD_AND_PVC)) {
                            log.debug("{}: Pod and PVCs for {} should be deleted based on annotation", reconciliation, podName);

                            return existingPvcsFuture
                                    .compose(existingPvcs -> {
                                        List<PersistentVolumeClaim> deletePvcs;

                                        if (existingPvcs != null) {
                                            deletePvcs = existingPvcs
                                                    .stream()
                                                    .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                                                    .collect(Collectors.toList());
                                        } else {
                                            deletePvcs = new ArrayList<>(0);
                                        }

                                        List<PersistentVolumeClaim> createPvcs = desiredPvcs
                                                .stream()
                                                .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                                                .collect(Collectors.toList());

                                        return cleanPodAndPvc(stsOperator, sts, podName, deletePvcs, createPvcs);
                                    });
                        }
                    }
                }
            }

            return Future.succeededFuture();
        }

        /**
         * This is an internal method which actually executes the deletion of the Pod and PVC. This is a non-trivial
         * since the PVC and the Pod are tightly coupled and one cannot be deleted without the other. Also, the
         * StatefulSet controller will recreate the deleted Pod and make it hard to recreate the PVCs manually.
         *
         * To address these, we:
         *     1. Delete the STS in non-cascading delete
         *     2. Delete the Pod
         *     3. Delete the PVC
         *     4. Wait for the Pod to be actually deleted
         *     5. Wait for the PVCs to be actually deleted
         *     6. Recreate the PVCs
         *     7. Recreate the STS (which will in turn recreate the Pod)
         *     5. Wait for Pod readiness
         *
         * @param stsOperator       StatefulSet Operator for managing stateful sets
         * @param sts               The current StatefulSet to which the cleaned pod belongs
         * @param podName           Name of the pod which should be deleted
         * @param deletePvcs        The list of PVCs which should be deleted
         * @param createPvcs        The list of PVCs which should be recreated
         * @return
         */
        Future<Void> cleanPodAndPvc(StatefulSetOperator stsOperator, StatefulSet sts, String podName, List<PersistentVolumeClaim> deletePvcs, List<PersistentVolumeClaim> createPvcs) {
            long pollingIntervalMs = 1_000;
            long timeoutMs = operationTimeoutMs;

            // We start by deleting the StatefulSet so that it doesn't interfere with the pod deletion process
            // The deletion has to be non-cascading so that the other pods are not affected
            Future<Void> fut = stsOperator.deleteAsync(namespace, sts.getMetadata().getName(), false)
                    .compose(ignored -> {
                        // After the StatefulSet is deleted, we can delete the pod which was marked for deletion
                        return podOperations.reconcile(namespace, podName, null);
                    })
                    .compose(ignored -> {
                        // With the pod deleting, we can delete all the PVCs belonging to this pod
                        List<Future> deleteResults = new ArrayList<>(deletePvcs.size());

                        for (PersistentVolumeClaim pvc : deletePvcs)    {
                            String pvcName = pvc.getMetadata().getName();
                            log.debug("{}: Deleting PVC {} for Pod {} based on {} annotation", reconciliation, pvcName, podName, AbstractScalableResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC);
                            deleteResults.add(pvcOperations.reconcile(namespace, pvcName, null));
                        }
                        return CompositeFuture.join(deleteResults);
                    })
                    .compose(ignored -> {
                        // The pod deletion just triggers it asynchronously
                        // We have to wait for the pod to be actually deleted
                        log.debug("{}: Checking if Pod {} has been deleted", reconciliation, podName);

                        Future<Void> waitForDeletion = podOperations.waitFor(namespace, podName, "deleted", pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                            Pod deletion = podOperations.get(namespace, podName);
                            log.trace("Checking if Pod {} in namespace {} has been deleted or recreated", podName, namespace);
                            return deletion == null;
                        });

                        return waitForDeletion;
                    })
                    .compose(ignored -> {
                        // Once the pod is deleted, the PVCs should delete as well
                        // Faked PVCs on Minishift etc. might delete while the pod is running, real PVCs will not
                        List<Future> waitForDeletionResults = new ArrayList<>(deletePvcs.size());

                        for (PersistentVolumeClaim pvc : deletePvcs)    {
                            String pvcName = pvc.getMetadata().getName();
                            String uid = pvc.getMetadata().getUid();

                            log.debug("{}: Checking if PVC {} for Pod {} has been deleted", reconciliation, pvcName, podName);

                            Future<Void> waitForDeletion = pvcOperations.waitFor(namespace, pvcName, "deleted", pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                                PersistentVolumeClaim deletion = pvcOperations.get(namespace, pvcName);
                                log.trace("Checking if {} {} in namespace {} has been deleted", pvc.getKind(), pvcName, namespace);
                                return deletion == null || (deletion.getMetadata() != null && !uid.equals(deletion.getMetadata().getUid()));
                            });

                            waitForDeletionResults.add(waitForDeletion);
                        }

                        return CompositeFuture.join(waitForDeletionResults);
                    })
                    .compose(ignored -> {
                        // Once everything was deleted, we can start recreating it.
                        // First we recreate the PVCs
                        List<Future> createResults = new ArrayList<>(createPvcs.size());

                        for (PersistentVolumeClaim pvc : createPvcs)    {
                            log.debug("{}: Reconciling PVC {} for Pod {} after it was deleted and maybe recreated by the pod", reconciliation, pvc.getMetadata().getName(), podName);
                            createResults.add(pvcOperations.reconcile(namespace, pvc.getMetadata().getName(), pvc));
                        }

                        return CompositeFuture.join(createResults);
                    })
                    .compose(ignored -> {
                        // After the PVCs we recreate the StatefulSet which will in turn recreate the missing Pod
                        // We cannot use the new StatefulSet here because there might have been some other changes for
                        // which we are not yet ready - e.g. changes to off-cluster access etc. which might not work
                        // without the CO creating some other infrastructure.
                        // Therefore we use the old STS and just remove some things such as Status, ResourceVersion, UID
                        // or self link. These will be recreated by Kubernetes after it is created.
                        sts.getMetadata().setResourceVersion(null);
                        sts.getMetadata().setSelfLink(null);
                        sts.getMetadata().setUid(null);
                        sts.setStatus(null);

                        return stsOperator.reconcile(namespace, sts.getMetadata().getName(), sts);
                    })
                    .compose(ignored -> podOperations.readiness(namespace, podName, pollingIntervalMs, timeoutMs));

            return fut;
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
                log.debug("{}: Considering PVC {} for deletion", reconciliation, pvcName);

                if (Annotations.booleanAnnotation(pvcOperations.get(namespace, pvcName), AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM, false)) {
                    log.debug("{}: Deleting PVC {}", reconciliation, pvcName);
                    futures.add(pvcOperations.reconcile(namespace, pvcName, null));
                }
            }

            return withVoid(CompositeFuture.all(futures));
        }

        @SuppressWarnings("deprecation")
        Future<ReconciliationState> checkUnsupportedTopicOperator() {
            if (kafkaAssembly.getSpec().getTopicOperator() != null) {
                kafkaStatus.addCondition(StatusUtils.buildWarningCondition("TopicOperator",
                        "Kafka.spec.topicOperator is not supported anymore. " +
                                "Topic operator should be configured at path spec.entityOperator.topicOperator."));
            }

            return Future.succeededFuture(this);
        }

        private final Future<ReconciliationState> getEntityOperatorDescription() {
            this.entityOperator = EntityOperator.fromCrd(kafkaAssembly, versions);

            if (entityOperator != null) {
                EntityTopicOperator topicOperator = entityOperator.getTopicOperator();
                EntityUserOperator userOperator = entityOperator.getUserOperator();

                Future<ConfigMap> futToConfigMap;

                if (topicOperator != null && topicOperator.getLogging() instanceof ExternalLogging)  {
                    futToConfigMap = configMapOperations.getAsync(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) topicOperator.getLogging()).getName());
                } else {
                    futToConfigMap = Future.succeededFuture(null);
                }

                Future<ConfigMap> futUoConfigMap;

                if (userOperator != null && userOperator.getLogging() instanceof ExternalLogging)  {
                    futUoConfigMap = configMapOperations.getAsync(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) userOperator.getLogging()).getName());
                } else {
                    futUoConfigMap = Future.succeededFuture(null);
                }

                return CompositeFuture.join(futToConfigMap, futUoConfigMap)
                        .compose(res -> {
                            ConfigMap toCm = res.resultAt(0);
                            ConfigMap uoCm = res.resultAt(1);

                            if (topicOperator != null)  {
                                this.topicOperatorMetricsAndLogsConfigMap = topicOperator.generateMetricsAndLogConfigMap(toCm);
                            }

                            if (userOperator != null)   {
                                this.userOperatorMetricsAndLogsConfigMap = userOperator.generateMetricsAndLogConfigMap(uoCm);
                            }

                            this.eoDeployment = entityOperator.generateDeployment(pfa.isOpenshift(), Collections.emptyMap(), imagePullPolicy, imagePullSecrets);
                            return Future.succeededFuture(this);
                        });
            } else {
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> entityOperatorServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(namespace,
                    EntityOperator.entityOperatorServiceAccountName(name),
                    eoDeployment != null ? entityOperator.generateServiceAccount() : null));
        }

        Future<ReconciliationState> entityOperatorTopicOpRoleBinding() {
            if (eoDeployment != null && entityOperator.getTopicOperator() != null) {
                String watchedNamespace = namespace;

                if (entityOperator.getTopicOperator().getWatchedNamespace() != null
                        && !entityOperator.getTopicOperator().getWatchedNamespace().isEmpty()) {
                    watchedNamespace = entityOperator.getTopicOperator().getWatchedNamespace();
                }

                return withVoid(roleBindingOperations.reconcile(
                        watchedNamespace,
                        EntityTopicOperator.roleBindingName(name),
                        entityOperator.getTopicOperator().generateRoleBinding(namespace, watchedNamespace)));
            } else  {
                return withVoid(roleBindingOperations.reconcile(namespace, EntityTopicOperator.roleBindingName(name), null));
            }
        }

        Future<ReconciliationState> entityOperatorUserOpRoleBinding() {
            if (eoDeployment != null && entityOperator.getUserOperator() != null) {
                Future<ReconcileResult<RoleBinding>> ownNamespaceFuture;
                Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;

                String watchedNamespace = namespace;

                if (entityOperator.getUserOperator().getWatchedNamespace() != null
                        && !entityOperator.getUserOperator().getWatchedNamespace().isEmpty()) {
                    watchedNamespace = entityOperator.getUserOperator().getWatchedNamespace();
                }

                if (!namespace.equals(watchedNamespace)) {
                    watchedNamespaceFuture = roleBindingOperations.reconcile(watchedNamespace, EntityUserOperator.roleBindingName(name), entityOperator.getUserOperator().generateRoleBinding(namespace, watchedNamespace));
                } else {
                    watchedNamespaceFuture = Future.succeededFuture();
                }

                // Create role binding for the the UI runs in (it needs to access the CA etc.)
                ownNamespaceFuture = roleBindingOperations.reconcile(namespace, EntityUserOperator.roleBindingName(name), entityOperator.getUserOperator().generateRoleBinding(namespace, namespace));


                return withVoid(CompositeFuture.join(ownNamespaceFuture, watchedNamespaceFuture));
            } else {
                return withVoid(roleBindingOperations.reconcile(namespace, EntityUserOperator.roleBindingName(name), null));
            }
        }

        Future<ReconciliationState> entityOperatorTopicOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    eoDeployment != null && entityOperator.getTopicOperator() != null ?
                            entityOperator.getTopicOperator().getAncillaryConfigMapName() : EntityTopicOperator.metricAndLogConfigsName(name),
                    topicOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorUserOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    eoDeployment != null && entityOperator.getUserOperator() != null ?
                            entityOperator.getUserOperator().getAncillaryConfigMapName() : EntityUserOperator.metricAndLogConfigsName(name),
                    userOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorDeployment() {
            if (this.entityOperator != null && eoDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.entityOperator.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int clusterCaCertGeneration = getCaCertGeneration(this.clusterCa);
                    int clientsCaCertGeneration = getCaCertGeneration(this.clientsCa);

                    Annotations.annotations(eoDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCaCertGeneration));
                    Annotations.annotations(eoDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, String.valueOf(clientsCaCertGeneration));
                    return deploymentOperations.reconcile(namespace, EntityOperator.entityOperatorName(name), eoDeployment);
                }).compose(recon -> {
                    if (recon instanceof ReconcileResult.Noop)   {
                        // Lets check if we need to roll the deployment manually
                        if (existingEntityOperatorCertsChanged) {
                            return entityOperatorRollingUpdate();
                        }
                    }

                    // No need to roll, we patched the deployment (and it will roll it self) or we created a new one
                    return Future.succeededFuture(this);
                });
            } else  {
                return withVoid(deploymentOperations.reconcile(namespace, EntityOperator.entityOperatorName(name), null));
            }
        }

        Future<ReconciliationState> entityOperatorRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(namespace, EntityOperator.entityOperatorName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> entityOperatorReady() {
            if (this.entityOperator != null && eoDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.entityOperator.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(namespace, this.entityOperator.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(namespace, this.entityOperator.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> entityOperatorSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(EntityOperator.secretName(name), entityOperator == null ? null : entityOperator.generateSecret(clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingEntityOperatorCertsChanged = changed;
                        return this;
                    });
        }

        private boolean isPodUpToDate(StatefulSet sts, Pod pod) {
            final int stsGeneration = StatefulSetOperator.getStsGeneration(sts);
            final int podGeneration = StatefulSetOperator.getPodGeneration(pod);
            log.debug("Rolling update of {}/{}: pod {} has {}={}; sts has {}={}",
                    sts.getMetadata().getNamespace(), sts.getMetadata().getName(), pod.getMetadata().getName(),
                    StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, podGeneration,
                    StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, stsGeneration);
            return stsGeneration == podGeneration;
        }

        private final Future<ReconciliationState> getCruiseControlDescription() {
            CruiseControl cruiseControl = CruiseControl.fromCrd(kafkaAssembly, versions);
            if (cruiseControl != null) {
                ConfigMap logAndMetricsConfigMap = cruiseControl.generateMetricsAndLogConfigMap(
                        cruiseControl.getLogging() instanceof ExternalLogging ?
                                configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) cruiseControl.getLogging()).getName()) :
                                null);
                Map<String, String> annotations = singletonMap(CruiseControl.ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(ANCILLARY_CM_KEY_LOG_CONFIG));

                this.cruiseControlMetricsAndLogsConfigMap = logAndMetricsConfigMap;
                this.cruiseControl = cruiseControl;

                this.ccDeployment = cruiseControl.generateDeployment(pfa.isOpenshift(), annotations, imagePullPolicy, imagePullSecrets);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> cruiseControlServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(namespace,
                    CruiseControl.cruiseControlServiceAccountName(name),
                    ccDeployment != null ? cruiseControl.generateServiceAccount() : null));
        }

        Future<ReconciliationState> cruiseControlAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    ccDeployment != null && cruiseControl != null ?
                            cruiseControl.getAncillaryConfigMapName() : CruiseControl.metricAndLogConfigsName(name),
                    cruiseControlMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> cruiseControlSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(CruiseControl.secretName(name), cruiseControl == null ? null : cruiseControl.generateSecret(clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingCruiseControlCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> cruiseControlDeployment() {
            if (this.cruiseControl != null && ccDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.cruiseControl.getName());
                return future.compose(dep -> {
                    return deploymentOperations.reconcile(namespace, this.cruiseControl.getName(), ccDeployment);
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
                return withVoid(deploymentOperations.reconcile(namespace, CruiseControl.cruiseControlName(name), null));
            }
        }

        Future<ReconciliationState> cruiseControlRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(namespace, CruiseControl.cruiseControlName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> cruiseControlService() {
            return withVoid(serviceOperations.reconcile(namespace, CruiseControl.cruiseControlServiceName(name), cruiseControl != null ? cruiseControl.generateService() : null));
        }

        Future<ReconciliationState> cruiseControlReady() {
            if (this.cruiseControl != null && ccDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.cruiseControl.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(namespace, this.cruiseControl.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(namespace, this.cruiseControl.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> cruiseControlNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(namespace, CruiseControl.policyName(name),
                    cruiseControl != null ? cruiseControl.generateNetworkPolicy(pfa.isNamespaceAndPodSelectorNetworkPolicySupported()) : null));
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
            log.debug("Rolling update of {}/{}: pod {} has {}={}; sts has {}={}",
                    sts.getMetadata().getNamespace(), sts.getMetadata().getName(), pod.getMetadata().getName(),
                    annotation, podThumbprint,
                    annotation, stsThumbprint);
            return podThumbprint.equals(stsThumbprint);
        }

        /**
         * @param sts Stateful set to which pod belongs
         * @param pod Pod to restart
         * @param cas Certificate authorities to be checked for changes
         * @return null or empty if the restart is not needed, reason String otherwise
         */
        private List<String> getReasonsToRestartPod(StatefulSet sts, Pod pod,
                                                           boolean nodeCertsChange,
                                                           Ca... cas) {
            if (pod == null)    {
                // When the Pod doesn't exist, it doesn't need to be restarted.
                // It will be created with new configuration.
                return null;
            }

            boolean isPodUpToDate = isPodUpToDate(sts, pod);
            boolean isCustomCertTlsListenerUpToDate = isCustomCertUpToDate(sts, pod, KafkaCluster.ANNO_STRIMZI_CUSTOM_CERT_THUMBPRINT_TLS_LISTENER);
            boolean isCustomCertExternalListenerUpToDate = isCustomCertUpToDate(sts, pod, KafkaCluster.ANNO_STRIMZI_CUSTOM_CERT_THUMBPRINT_EXTERNAL_LISTENER);

            List<String> reasons = new ArrayList<>(3);

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
            if (!isPodUpToDate) {
                reasons.add("Pod has old generation");
            }
            if (fsResizingRestartRequest.contains(pod.getMetadata().getName()))   {
                reasons.add("file system needs to be resized");
            }
            if (!isCustomCertTlsListenerUpToDate) {
                reasons.add("custom certificate on the TLS listener changes");
            }
            if (!isCustomCertExternalListenerUpToDate) {
                reasons.add("custom certificate on the external listener changes");
            }
            if (nodeCertsChange) {
                reasons.add("server certificates changed");
            }
            if (!reasons.isEmpty()) {
                log.debug("{}: Rolling pod {} due to {}",
                        reconciliation, pod.getMetadata().getName(), reasons);
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
                log.warn("The provided maintenance time windows list contains {} which is not a valid cron expression", currentCron);
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

            Secret secret = ModelUtils.buildSecret(clusterCa, clusterCa.clusterOperatorSecret(), namespace,
                    ClusterOperator.secretName(name), "cluster-operator", "cluster-operator",
                    labels, ownerRef, isMaintenanceTimeWindowsSatisfied(dateSupplier));

            return withVoid(secretOperations.reconcile(namespace, ClusterOperator.secretName(name),
                    secret));
        }

        private Storage getOldStorage(StatefulSet sts)  {
            Storage storage = null;

            if (sts != null)    {
                String jsonStorage = Annotations.stringAnnotation(sts, ANNO_STRIMZI_IO_STORAGE, null);

                if (jsonStorage != null)    {
                    storage = ModelUtils.decodeStorageFromJson(jsonStorage);
                }
            }

            return storage;
        }

        void setExternalListenerStatus(ListenerAddress... addresses)   {
            KafkaListeners listeners = kafkaCluster.getListeners();

            if (listeners != null)  {
                if (listeners.getExternal() != null)   {
                    ListenerStatus ls = new ListenerStatusBuilder()
                            .withNewType("external")
                            .withAddresses(addresses)
                            .build();

                    addListenerStatus(ls);
                }
            }
        }

        void setPlainAndTlsListenerStatus()   {
            KafkaListeners listeners = kafkaCluster.getListeners();

            if (listeners != null)  {
                if (listeners.getPlain() != null)   {
                    ListenerStatus ls = new ListenerStatusBuilder()
                            .withNewType("plain")
                            .withAddresses(new ListenerAddressBuilder()
                                    .withHost(getInternalServiceHostname(kafkaService.getMetadata().getName()))
                                    .withPort(kafkaCluster.getClientPort())
                                    .build())
                            .build();

                    addListenerStatus(ls);
                }

                if (listeners.getTls() != null) {
                    ListenerStatus ls = new ListenerStatusBuilder()
                            .withNewType("tls")
                            .withAddresses(new ListenerAddressBuilder()
                                    .withHost(getInternalServiceHostname(kafkaService.getMetadata().getName()))
                                    .withPort(kafkaCluster.getClientTlsPort())
                                    .build())
                            .build();

                    addListenerStatus(ls);
                }
            }
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

        Future<ReconciliationState> kafkaTlsListenerCertificatesToStatus() {
            if (kafkaCluster.getListeners() != null
                    && kafkaCluster.getListeners().getTls() != null) {
                addCertificateToListener("tls", tlsListenerCustomCertificate);
            }

            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> kafkaExternalListenerCertificatesToStatus() {
            if (kafkaCluster.isExposedWithTls())    {
                addCertificateToListener("external", externalListenerCustomCertificate);
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
                ListenerStatus listener = listeners.stream().filter(listenerType -> type.equals(listenerType.getType())).findFirst().orElse(null);

                if (listener != null) {
                    listener.setCertificates(singletonList(certificate));
                }
            }
        }

        String getInternalServiceHostname(String serviceName)    {
            return ModelUtils.serviceDnsNameWithoutClusterDomain(namespace, serviceName);
        }

        private final Future<ReconciliationState> getKafkaExporterDescription() {
            this.kafkaExporter = KafkaExporter.fromCrd(kafkaAssembly, versions);
            this.exporterDeployment = kafkaExporter.generateDeployment(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> kafkaExporterServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(namespace,
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
                    return deploymentOperations.reconcile(namespace, KafkaExporter.kafkaExporterName(name), exporterDeployment);
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
                return withVoid(deploymentOperations.reconcile(namespace, KafkaExporter.kafkaExporterName(name), null));
            }
        }

        Future<ReconciliationState> kafkaExporterRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(namespace, KafkaExporter.kafkaExporterName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaExporterReady() {
            if (this.kafkaExporter != null && exporterDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.kafkaExporter.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(namespace, this.kafkaExporter.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(namespace, this.kafkaExporter.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> getJmxTransDescription() {
            try {
                int numOfBrokers = kafkaCluster.getReplicas();
                this.jmxTrans = JmxTrans.fromCrd(kafkaAssembly, versions);
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
            return withVoid(configMapOperations.reconcile(namespace,
                    JmxTrans.jmxTransConfigName(name),
                    jmxTransConfigMap));
        }


        Future<ReconciliationState> jmxTransServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(namespace,
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
                        return withVoid(deploymentOperations.reconcile(namespace, JmxTrans.jmxTransName(name),
                                jmxTransDeployment));
                    });
                });
            } else {
                return withVoid(deploymentOperations.reconcile(namespace, JmxTrans.jmxTransName(name), null));
            }
        }

        Future<ReconciliationState> jmxTransDeploymentReady() {
            if (this.jmxTrans != null && jmxTransDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace,  this.jmxTrans.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(namespace,  this.jmxTrans.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(namespace, this.jmxTrans.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

    }

    /* test */ Date dateSupplier() {
        return new Date();
    }

}
