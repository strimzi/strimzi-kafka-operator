/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
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
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
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
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.EntityTopicOperator;
import io.strimzi.operator.cluster.model.EntityUserOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaUpgrade;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronExpression;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;
import static io.strimzi.operator.cluster.model.AbstractModel.ANNO_STRIMZI_IO_STORAGE;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_FROM_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_TO_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ENV_VAR_KAFKA_CONFIGURATION;
import static io.strimzi.operator.cluster.model.KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION;
import static io.strimzi.operator.cluster.model.KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedVersions;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> {
    private static final Logger log = LogManager.getLogger(KafkaAssemblyOperator.class.getName());

    public static final String ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE = Annotations.STRIMZI_DOMAIN + "/manual-rolling-update";
    @Deprecated
    public static final String ANNO_OP_STRIMZI_IO_MANUAL_ROLLING_UPDATE = "operator.strimzi.io/manual-rolling-update";

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
    private final CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> crdOperator;

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
    }

    @Override
    public Future<Void> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
        Future<Void> createOrUpdateFuture = Future.future();

        if (kafkaAssembly.getSpec() == null) {
            log.error("{} spec cannot be null", kafkaAssembly.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }

        ReconciliationState reconcileState = createReconciliationState(reconciliation, kafkaAssembly);
        reconcile(reconcileState).setHandler(reconcileResult -> {
            KafkaStatus status = reconcileState.kafkaStatus;
            Condition readyCondition;

            if (kafkaAssembly.getMetadata().getGeneration() != null)    {
                status.setObservedGeneration(kafkaAssembly.getMetadata().getGeneration());
            }

            if (reconcileResult.succeeded())    {
                readyCondition = new ConditionBuilder()
                        .withLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dateSupplier()))
                        .withType("Ready")
                        .withStatus("True")
                        .build();
            } else {
                readyCondition = new ConditionBuilder()
                        .withLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dateSupplier()))
                        .withType("NotReady")
                        .withStatus("True")
                        .withReason(reconcileResult.cause().getClass().getSimpleName())
                        .withMessage(reconcileResult.cause().getMessage())
                        .build();
            }

            status.setConditions(Collections.singletonList(readyCondition));
            reconcileState.updateStatus(status).setHandler(statusResult -> {
                if (statusResult.succeeded())    {
                    log.debug("Status for {} is up to date", kafkaAssembly.getMetadata().getName());
                } else {
                    log.error("Failed to set status for {}", kafkaAssembly.getMetadata().getName());
                }

                // If both features succeeded, createOrUpdate succeeded as well
                // If one or both of them failed, we prefer the reconciliation failure as the main error
                if (reconcileResult.succeeded() && statusResult.succeeded())    {
                    createOrUpdateFuture.complete();
                } else if (reconcileResult.failed())    {
                    createOrUpdateFuture.fail(reconcileResult.cause());
                } else {
                    createOrUpdateFuture.fail(statusResult.cause());
                }
            });
        });

        return createOrUpdateFuture;
    }

    Future<Void> reconcile(ReconciliationState reconcileState)  {
        Future<Void> chainFuture = Future.future();

        reconcileState.reconcileCas(this::dateSupplier)
                .compose(state -> state.clusterOperatorSecret())
                // Roll everything if a new CA is added to the trust store.
                .compose(state -> state.rollingUpdateForNewCaKey())
                .compose(state -> state.getZookeeperDescription())
                .compose(state -> state.zkManualPodCleaning())
                .compose(state -> state.zkNetPolicy())
                .compose(state -> state.zkManualRollingUpdate())
                .compose(state -> state.zookeeperServiceAccount())
                .compose(state -> state.zkPvcs())
                .compose(state -> state.zkScaleUpStep())
                .compose(state -> state.zkScaleDown())
                .compose(state -> state.zkService())
                .compose(state -> state.zkHeadlessService())
                .compose(state -> state.zkAncillaryCm())
                .compose(state -> state.zkNodesSecret())
                .compose(state -> state.zkPodDisruptionBudget())
                .compose(state -> state.zkStatefulSet())
                .compose(state -> state.zkScaleUp())
                .compose(state -> state.zkRollingUpdate())
                .compose(state -> state.zkPodsReady())
                .compose(state -> state.zkServiceEndpointReadiness())
                .compose(state -> state.zkHeadlessServiceEndpointReadiness())
                .compose(state -> state.zkPersistentClaimDeletion())

                .compose(state -> state.getKafkaClusterDescription())
                .compose(state -> state.kafkaManualPodCleaning())
                .compose(state -> state.kafkaNetPolicy())
                .compose(state -> state.kafkaManualRollingUpdate())
                .compose(state -> state.kafkaUpgrade())
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
                .compose(state -> state.kafkaGenerateCertificates())
                .compose(state -> state.kafkaAncillaryCm())
                .compose(state -> state.kafkaBrokersSecret())
                .compose(state -> state.kafkaPodDisruptionBudget())
                .compose(state -> state.kafkaStatefulSet())
                .compose(state -> state.kafkaRollingUpdate())
                .compose(state -> state.kafkaScaleUp())
                .compose(state -> state.kafkaPodsReady())
                .compose(state -> state.kafkaServiceEndpointReady())
                .compose(state -> state.kafkaHeadlessServiceEndpointReady())
                .compose(state -> state.kafkaPersistentClaimDeletion())

                .compose(state -> state.getTopicOperatorDescription())
                .compose(state -> state.topicOperatorServiceAccount())
                .compose(state -> state.topicOperatorRoleBinding())
                .compose(state -> state.topicOperatorAncillaryCm())
                .compose(state -> state.topicOperatorSecret())
                .compose(state -> state.topicOperatorDeployment())

                .compose(state -> state.getEntityOperatorDescription())
                .compose(state -> state.entityOperatorServiceAccount())
                .compose(state -> state.entityOperatorTopicOpRoleBinding())
                .compose(state -> state.entityOperatorUserOpRoleBinding())
                .compose(state -> state.entityOperatorTopicOpAncillaryCm())
                .compose(state -> state.entityOperatorUserOpAncillaryCm())
                .compose(state -> state.entityOperatorSecret())
                .compose(state -> state.entityOperatorDeployment())
                .compose(state -> state.entityOperatorReady())

                .compose(state -> state.getKafkaExporterDescription())
                .compose(state -> state.kafkaExporterServiceAccount())
                .compose(state -> state.kafkaExporterSecret())
                .compose(state -> state.kafkaExporterDeployment())
                .compose(state -> state.kafkaExporterService())
                .compose(state -> state.kafkaExporterReady())

                .compose(state -> chainFuture.complete(), chainFuture);

        return chainFuture;
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

        private ZookeeperCluster zkCluster;
        private Service zkService;
        private Service zkHeadlessService;
        private ConfigMap zkMetricsAndLogsConfigMap;
        /* test */ ReconcileResult<StatefulSet> zkDiffs;
        private boolean zkAncillaryCmChange;

        private KafkaCluster kafkaCluster = null;
        /* test */ KafkaStatus kafkaStatus = new KafkaStatus();

        private Service kafkaService;
        private Service kafkaHeadlessService;
        private ConfigMap kafkaMetricsAndLogsConfigMap;
        /* test */ ReconcileResult<StatefulSet> kafkaDiffs;
        private Set<String> kafkaExternalBootstrapDnsName = new HashSet<>();
        private Set<String> kafkaExternalAddresses = new HashSet<>();
        private Map<Integer, Set<String>> kafkaExternalDnsNames = new HashMap<>();
        private boolean kafkaAncillaryCmChange;

        @SuppressWarnings("deprecation")
        /* test */ io.strimzi.operator.cluster.model.TopicOperator topicOperator;
        /* test */ Deployment toDeployment = null;
        private ConfigMap toMetricsAndLogsConfigMap = null;

        /* test */ EntityOperator entityOperator;
        /* test */ Deployment eoDeployment = null;
        private ConfigMap topicOperatorMetricsAndLogsConfigMap = null;
        private ConfigMap userOperatorMetricsAndLogsConfigMap;
        private Secret oldCoSecret;

        /* test */ KafkaExporter kafkaExporter;
        /* test */ Deployment exporterDeployment = null;

        /* test */ Set<String> fsResizingRestartRequest = new HashSet<>();

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
            Future<Void> updateStatusFuture = Future.future();

            crdOperator.getAsync(namespace, name).setHandler(getRes -> {
                if (getRes.succeeded())    {
                    Kafka kafka = getRes.result();

                    if (kafka != null) {
                        if ("kafka.strimzi.io/v1alpha1".equals(kafka.getApiVersion()))   {
                            log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, kafka.getApiVersion());
                            updateStatusFuture.complete();
                        } else {
                            KafkaStatus currentStatus = kafka.getStatus();

                            StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                            if (!ksDiff.isEmpty()) {
                                Kafka resourceWithNewStatus = new KafkaBuilder(kafka).withStatus(desiredStatus).build();

                                crdOperator.updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
                                    if (updateRes.succeeded()) {
                                        log.debug("{}: Completed status update", reconciliation);
                                        updateStatusFuture.complete();
                                    } else {
                                        log.error("{}: Failed to update status", reconciliation, updateRes.cause());
                                        updateStatusFuture.fail(updateRes.cause());
                                    }
                                });
                            } else {
                                log.debug("{}: Status did not change", reconciliation);
                                updateStatusFuture.complete();
                            }
                        }
                    } else {
                        log.error("{}: Current Kafka resource not found", reconciliation);
                        updateStatusFuture.fail("Current Kafka resource not found");
                    }
                } else {
                    log.error("{}: Failed to get the current Kafka resource and its status", reconciliation, getRes.cause());
                    updateStatusFuture.fail(getRes.cause());
                }
            });

            return updateStatusFuture;
        }

        /**
         * Asynchronously reconciles the cluster and clients CA secrets.
         * The cluster CA secret has to have the name determined by {@link AbstractModel#clusterCaCertSecretName(String)}.
         * The clients CA secret has to have the name determined by {@link KafkaCluster#clientsCaCertSecretName(String)}.
         * Within both the secrets the current certificate is stored under the key {@code ca.crt}
         * and the current key is stored under the key {@code ca.key}.
         */
        Future<ReconciliationState> reconcileCas(Supplier<Date> dateSupplier) {
            Labels selectorLabels = Labels.EMPTY.withKind(reconciliation.kind()).withCluster(reconciliation.name());
            Labels caLabels = Labels.fromResource(kafkaAssembly)
                    .withKind(reconciliation.kind())
                    .withCluster(reconciliation.name())
                    .withKubernetesName()
                    .withKubernetesInstance(reconciliation.name())
                    .withKubernetesManagedBy(AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
            Future<ReconciliationState> result = Future.future();
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

                        CompositeFuture.join(secretReconciliations).setHandler(res -> {
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
                result
            );
            return result;
        }

        /**
         * Utility method for checking the Secret existence when custom CA is used. The custom CA is configured but the
         * secrets do not exist, it will throw InvalifConfigurationException.
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
                String reasons = reason.stream().collect(Collectors.joining(", "));
                Future<Void> zkRollFuture;
                Predicate<Pod> rollPodAndLogReason = pod -> {
                    log.debug("{}: Rolling Pod {} to {}", reconciliation, pod.getMetadata().getName(), reasons);
                    return true;
                };
                if (this.clusterCa.keyReplaced()) {
                    zkRollFuture = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                        .compose(ss -> zkSetOperations.maybeRollingUpdate(ss, rollPodAndLogReason,
                        clusterCa.caCertSecret(),
                        oldCoSecret));
                } else {
                    zkRollFuture = Future.succeededFuture();
                }
                return zkRollFuture
                        .compose(i -> kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name)))
                        .compose(ss -> kafkaSetOperations.maybeRollingUpdate(ss, rollPodAndLogReason,
                                clusterCa.caCertSecret(),
                                oldCoSecret))
                        .compose(i -> deploymentOperations.getAsync(namespace, io.strimzi.operator.cluster.model.TopicOperator.topicOperatorName(name)))
                        .compose(dep -> {
                            if (dep != null) {
                                log.debug("{}: Rolling Deployment {} to {}", reconciliation, io.strimzi.operator.cluster.model.TopicOperator.topicOperatorName(name), reasons);
                                return deploymentOperations.rollingUpdate(namespace, io.strimzi.operator.cluster.model.TopicOperator.topicOperatorName(name), operationTimeoutMs);
                            } else {
                                return Future.succeededFuture();
                            }
                        })
                        .compose(i -> deploymentOperations.getAsync(namespace, EntityOperator.entityOperatorName(name)))
                        .compose(dep -> {
                            if (dep != null) {
                                log.debug("{}: Rolling Deployment {} to {}", reconciliation, EntityOperator.entityOperatorName(name), reasons);
                                return deploymentOperations.rollingUpdate(namespace, EntityOperator.entityOperatorName(name), operationTimeoutMs);
                            } else {
                                return Future.succeededFuture();
                            }
                        })
                        .map(i -> this);
            } else {
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> kafkaManualRollingUpdate() {
            Future<StatefulSet> futss = kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name));
            if (futss != null) {
                return futss.compose(ss -> {
                    if (ss != null) {
                        if (Annotations.booleanAnnotation(ss, ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE,
                                false, ANNO_OP_STRIMZI_IO_MANUAL_ROLLING_UPDATE)) {
                            return kafkaSetOperations.maybeRollingUpdate(ss, pod -> {

                                log.debug("{}: Rolling Kafka pod {} due to manual rolling update",
                                        reconciliation, pod.getMetadata().getName());
                                return true;
                            });
                        }
                    }
                    return Future.succeededFuture();
                }).map(i -> this);
            }
            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> zkManualRollingUpdate() {
            Future<StatefulSet> futss = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name));
            if (futss != null) {
                return futss.compose(ss -> {
                    if (ss != null) {
                        if (Annotations.booleanAnnotation(ss, ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE,
                                false, ANNO_OP_STRIMZI_IO_MANUAL_ROLLING_UPDATE)) {

                            return zkSetOperations.maybeRollingUpdate(ss, pod -> {

                                log.debug("{}: Rolling Zookeeper pod {} due to manual rolling update",
                                        reconciliation, pod.getMetadata().getName());
                                return true;
                            });
                        }
                    }
                    return Future.succeededFuture();
                }).map(i -> this);
            }
            return Future.succeededFuture(this);
        }

        /**
         * If the SS exists, complete any pending rolls
         *
         * @return A Future which completes with the current state of the SS, or with null if the SS never existed.
         */
        public Future<Void> waitForQuiescence(StatefulSet ss) {
            if (ss != null) {
                return kafkaSetOperations.maybeRollingUpdate(ss,
                    pod -> {
                        boolean notUpToDate = !isPodUpToDate(ss, pod);
                        if (notUpToDate) {
                            log.debug("Rolling pod {} prior to upgrade", pod.getMetadata().getName());
                        }
                        return notUpToDate;
                    });
            } else {
                return Future.succeededFuture();
            }
        }

        Future<ReconciliationState> kafkaUpgrade() {
            String kafkaSsName = KafkaCluster.kafkaClusterName(name);
            return kafkaSetOperations.getAsync(namespace, kafkaSsName).compose(
                ss -> {
                    if (ss == null) {
                        return Future.succeededFuture(this);
                    }
                    log.debug("Does SS {} need to be upgraded?", ss.getMetadata().getName());
                    // Get the current version of the cluster
                    // Strimzi 0.8.x and 0.9.0 didn't set the annotation, so if it's absent we know it must be 2.0.0
                    KafkaVersion currentVersion = versions.version(Annotations.annotations(ss).getOrDefault(ANNO_STRIMZI_IO_KAFKA_VERSION, "2.0.0"));
                    log.debug("SS {} has current version {}", ss.getMetadata().getName(), currentVersion);
                    String fromVersionAnno = Annotations.annotations(ss).get(ANNO_STRIMZI_IO_FROM_VERSION);
                    KafkaVersion fromVersion;
                    if (fromVersionAnno != null) { // We're mid-upgrade
                        fromVersion = versions.version(fromVersionAnno);
                    } else {
                        fromVersion = currentVersion;
                    }
                    log.debug("SS {} is from version {}", ss.getMetadata().getName(), fromVersion);
                    String toVersionAnno = Annotations.annotations(ss).get(ANNO_STRIMZI_IO_TO_VERSION);
                    KafkaVersion toVersion;
                    if (toVersionAnno != null) { // We're mid-upgrade
                        toVersion = versions.version(toVersionAnno);
                    } else {
                        toVersion = versions.version(kafkaAssembly.getSpec().getKafka().getVersion());
                    }
                    log.debug("SS {} is to version {}", ss.getMetadata().getName(), toVersion);
                    KafkaUpgrade upgrade = new KafkaUpgrade(fromVersion, toVersion);
                    log.debug("Kafka upgrade {}", upgrade);
                    if (upgrade.isNoop()) {
                        log.debug("Kafka.spec.kafka.version unchanged");
                        return Future.succeededFuture(this);
                    } else {
                        // Wait until the SS is not being updated (it shouldn't be, but there's no harm in checking)
                        return waitForQuiescence(ss).compose(v -> {
                            String image = versions.kafkaImage(kafkaAssembly.getSpec().getKafka().getImage(), toVersion.version());
                            Future<StatefulSet> f = Future.succeededFuture(ss);
                            Future<?> result;
                            if (upgrade.isUpgrade()) {
                                if (currentVersion.equals(fromVersion)) {
                                    f = f.compose(ignored -> kafkaUpgradePhase1(ss, upgrade, image));
                                }
                                result = f.compose(ss2 -> kafkaUpgradePhase2(ss2, upgrade));
                            } else {
                                if (currentVersion.equals(fromVersion)) {
                                    f = f.compose(ignored -> kafkaDowngradePhase1(ss, upgrade));
                                }
                                result = f.compose(ignored -> kafkaDowngradePhase2(ss, upgrade, image));
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
        private Future<StatefulSet> kafkaUpgradePhase1(StatefulSet ss, KafkaUpgrade upgrade, String upgradedImage) {
            log.info("{}: {}, phase 1", reconciliation, upgrade);

            Map<String, String> annotations = Annotations.annotations(ss);
            Map<String, String> env = ModelUtils.getKafkaContainerEnv(ss);
            String string = env.getOrDefault(ENV_VAR_KAFKA_CONFIGURATION, "");
            log.debug("Current config {}", string);
            KafkaConfiguration currentKafkaConfig = KafkaConfiguration.unvalidated(string);
            String oldMessageFormat = currentKafkaConfig.getConfigOption(LOG_MESSAGE_FORMAT_VERSION);
            if (upgrade.requiresMessageFormatChange() &&
                    oldMessageFormat == null) {
                // We need to ensure both new and old versions are using the same version (so they agree during the upgrade).
                // If the msg version is given in the CR and it's the same as the current (live) msg version
                // then we're good. If the current live msg version is not given (i.e. the default) and
                // the msg version is given in the CR then we're also good.

                // Force the user to explicitly set the log.message.format.version
                // to match the old version
                return Future.failedFuture(new KafkaUpgradeException(upgrade + " requires a message format change " +
                        "from " + upgrade.from().messageVersion() + " to " + upgrade.to().messageVersion() + ". " +
                        "You must explicitly set " +
                        LOG_MESSAGE_FORMAT_VERSION + ": \"" + upgrade.from().messageVersion() + "\"" +
                        " in Kafka.spec.kafka.config to perform the upgrade. " +
                        "Then you can upgrade client applications. " +
                        "And finally you can remove " + LOG_MESSAGE_FORMAT_VERSION +
                        " from Kafka.spec.kafka.config"));
            }
            // Otherwise both versions use the same message format, so we don't care.

            String lowerVersionProtocol = currentKafkaConfig.getConfigOption(INTERBROKER_PROTOCOL_VERSION);
            boolean twoPhase;
            if (lowerVersionProtocol == null) {
                if (!upgrade.requiresProtocolChange()) {
                    // In this case we just need a single rolling update
                    twoPhase = false;
                } else {
                    twoPhase = true;
                    // Set proto version and message version in Kafka config, if they're not already set
                    lowerVersionProtocol = currentKafkaConfig.getConfigOption(INTERBROKER_PROTOCOL_VERSION, upgrade.from().protocolVersion());
                    log.info("{}: Upgrade: Setting {} to {}", reconciliation, INTERBROKER_PROTOCOL_VERSION, lowerVersionProtocol);
                    currentKafkaConfig.setConfigOption(INTERBROKER_PROTOCOL_VERSION, lowerVersionProtocol);
                    env.put(ENV_VAR_KAFKA_CONFIGURATION, currentKafkaConfig.getConfiguration());
                    // Store upgrade state in annotations
                    annotations.put(ANNO_STRIMZI_IO_FROM_VERSION, upgrade.from().version());
                    annotations.put(ANNO_STRIMZI_IO_TO_VERSION, upgrade.to().version());
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
                    reconciliation, ANNO_STRIMZI_IO_KAFKA_VERSION, upgrade.to().version());
            annotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, upgrade.to().version());
            // update the annotations, image and environment
            StatefulSet newSs = new StatefulSetBuilder(ss)
                    .editMetadata()
                        .withAnnotations(annotations)
                    .endMetadata()
                    .editSpec()
                        .editTemplate()
                            .editSpec()
                                .editFirstContainer()
                                    .withImage(upgradedImage)
                                    .withEnv(ModelUtils.envAsList(env))
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                .build();

            // patch and rolling upgrade
            String name = KafkaCluster.kafkaClusterName(this.name);
            log.info("{}: Upgrade: Patch + rolling update of {}", reconciliation, name);
            return kafkaSetOperations.reconcile(namespace, name, newSs)
                    .compose(result -> kafkaSetOperations.maybeRollingUpdate(ss, pod -> {
                        log.info("{}: Upgrade: Patch + rolling update of {}: Pod {}", reconciliation, name, pod.getMetadata().getName());
                        return true;
                    }).map(result.resource()))
                    .compose(ss2 -> {
                        log.info("{}: {}, phase 1 of {} completed: {}", reconciliation, upgrade,
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
        private Future<Void> kafkaUpgradePhase2(StatefulSet ss, KafkaUpgrade upgrade) {
            if (ss == null) {
                // It was a one-phase update
                return Future.succeededFuture();
            }
            // Cluster is now using new binaries, but old proto version
            log.info("{}: {}, phase 2", reconciliation, upgrade);
            // Remove the strimzi.io/from-version and strimzi.io/to-version since this is the last phase
            Map<String, String> annotations = Annotations.annotations(ss);
            log.info("{}: Upgrade: Removing annotations {}, {}",
                    reconciliation, ANNO_STRIMZI_IO_FROM_VERSION, ANNO_STRIMZI_IO_TO_VERSION);
            annotations.remove(ANNO_STRIMZI_IO_FROM_VERSION);
            annotations.remove(ANNO_STRIMZI_IO_TO_VERSION);

            // Remove inter.broker.protocol.version (so the new version's default is used)
            Map<String, String> env = ModelUtils.getKafkaContainerEnv(ss);
            KafkaConfiguration currentKafkaConfig = KafkaConfiguration.unvalidated(env.get(ENV_VAR_KAFKA_CONFIGURATION));

            log.info("{}: Upgrade: Removing Kafka config {}, will default to {}",
                    reconciliation, INTERBROKER_PROTOCOL_VERSION, upgrade.to().protocolVersion());
            currentKafkaConfig.removeConfigOption(INTERBROKER_PROTOCOL_VERSION);
            env.put(ENV_VAR_KAFKA_CONFIGURATION, currentKafkaConfig.getConfiguration());

            // Update to new proto version and rolling upgrade
            currentKafkaConfig.removeConfigOption(INTERBROKER_PROTOCOL_VERSION);

            StatefulSet newSs = new StatefulSetBuilder(ss)
                    .editMetadata()
                        .withAnnotations(annotations)
                    .endMetadata()
                    .editSpec()
                        .editTemplate()
                            .editSpec()
                                .editFirstContainer()
                                    .withEnv(ModelUtils.envAsList(env))
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build();

            // Reconcile the SS and perform a rolling update of the pods
            log.info("{}: Upgrade: Patch + rolling update of {}", reconciliation, name);
            return kafkaSetOperations.reconcile(namespace, KafkaCluster.kafkaClusterName(name), newSs)
                    .compose(ignored -> kafkaSetOperations.maybeRollingUpdate(ss, pod -> {
                        log.info("{}: Upgrade: Patch + rolling update of {}: Pod {}", reconciliation, name, pod.getMetadata().getName());
                        return true;
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
         *     <li>Reconcile the SS and perform a rolling update of the pods</li>
         * </ol>
         */
        private Future<StatefulSet> kafkaDowngradePhase1(StatefulSet ss, KafkaUpgrade upgrade) {
            log.info("{}: {}, phase 1", reconciliation, upgrade);

            Map<String, String> annotations = Annotations.annotations(ss);
            Map<String, String> env = ModelUtils.getKafkaContainerEnv(ss);
            KafkaConfiguration currentKafkaConfig = KafkaConfiguration.unvalidated(env.getOrDefault(ENV_VAR_KAFKA_CONFIGURATION, ""));

            String oldMessageFormat = currentKafkaConfig.getConfigOption(LOG_MESSAGE_FORMAT_VERSION);
            // Force the user to explicitly set log.message.format.version
            // (Controller shouldn't break clients)
            if (oldMessageFormat == null || !oldMessageFormat.equals(upgrade.to().messageVersion())) {
                return Future.failedFuture(new KafkaUpgradeException(
                        String.format("Cannot downgrade Kafka cluster %s in namespace %s to version %s " +
                                        "because the current cluster is configured with %s=%s. " +
                                        "Downgraded brokers would not be able to understand existing " +
                                        "messages with the message version %s. ",
                                name, namespace, upgrade.to(),
                                LOG_MESSAGE_FORMAT_VERSION, oldMessageFormat,
                                oldMessageFormat)));
            }

            String lowerVersionProtocol = currentKafkaConfig.getConfigOption(INTERBROKER_PROTOCOL_VERSION);
            String phases;
            if (lowerVersionProtocol == null
                    || compareDottedVersions(lowerVersionProtocol, upgrade.to().protocolVersion()) > 0) {
                phases = "2 (change in " + INTERBROKER_PROTOCOL_VERSION + " requires 2nd phase)";
                // Set proto version and message version in Kafka config, if they're not already set
                lowerVersionProtocol = currentKafkaConfig.getConfigOption(INTERBROKER_PROTOCOL_VERSION, upgrade.to().protocolVersion());
                log.info("{}: Downgrade: Setting {} to {}", reconciliation, INTERBROKER_PROTOCOL_VERSION, lowerVersionProtocol);
                currentKafkaConfig.setConfigOption(INTERBROKER_PROTOCOL_VERSION, lowerVersionProtocol);
                env.put(ENV_VAR_KAFKA_CONFIGURATION, currentKafkaConfig.getConfiguration());
                // Store upgrade state in annotations
                annotations.put(ANNO_STRIMZI_IO_FROM_VERSION, upgrade.from().version());
                annotations.put(ANNO_STRIMZI_IO_TO_VERSION, upgrade.to().version());
            } else {
                // In this case there's no need for this phase of update, because the both old and new
                // brokers speaking protocol of the lower version.
                phases = "2 (1st phase skips rolling update)";
                log.info("{}: {}, phase 1 of {} completed", reconciliation, upgrade, phases);
                return Future.succeededFuture(ss);
            }

            // update the annotations, image and environment
            StatefulSet newSs = new StatefulSetBuilder(ss)
                    .editMetadata()
                        .withAnnotations(annotations)
                    .endMetadata()
                    .editSpec()
                        .editTemplate()
                            .editSpec()
                                .editFirstContainer()
                                    .withEnv(ModelUtils.envAsList(env))
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build();

            // patch and rolling upgrade
            String name = KafkaCluster.kafkaClusterName(this.name);
            log.info("{}: Downgrade: Patch + rolling update of {}", reconciliation, name);
            return kafkaSetOperations.reconcile(namespace, name, newSs)
                    .compose(result -> kafkaSetOperations.maybeRollingUpdate(ss, pod -> {
                        log.info("{}: Downgrade: Patch + rolling update of {}: Pod {}", reconciliation, name, pod.getMetadata().getName());
                        return true;
                    }).map(result.resource()))
                    .compose(ss2 -> {
                        log.info("{}: {}, phase 1 of {} completed", reconciliation, upgrade, phases);
                        return Future.succeededFuture(ss2);
                    });
        }

        /**
         * <p>Final downgrade phase
         * <ol>
         *     <li>Update the strimzi.io/kafka-version to the new version</li>
         *     <li>Remove the strimzi.io/from-kafka-version since this is the last phase</li>
         *     <li>Remove the strimzi.io/to-kafka-version since this is the last phase</li>
         *     <li>Remove inter.broker.protocol.version (so the new version's default is used)</li>
         *     <li>Update the image in the SS</li>
         *     <li>Reconcile the SS and perform a rolling update of the pods</li>
         * </ol>
         */
        private Future<Void> kafkaDowngradePhase2(StatefulSet ss, KafkaUpgrade downgrade, String downgradedImage) {
            log.info("{}: {}, phase 2", reconciliation, downgrade);
            // Remove the strimzi.io/from-version and strimzi.io/to-version since this is the last phase

            Map<String, String> annotations = Annotations.annotations(ss);

            log.info("{}: Upgrade: Removing annotations {}, {}",
                    reconciliation, ANNO_STRIMZI_IO_FROM_VERSION, ANNO_STRIMZI_IO_TO_VERSION);
            annotations.remove(ANNO_STRIMZI_IO_FROM_VERSION);
            annotations.remove(ANNO_STRIMZI_IO_TO_VERSION);
            annotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, downgrade.to().version());

            // Remove inter.broker.protocol.version (so the new version's default is used)
            Map<String, String> env = ModelUtils.getKafkaContainerEnv(ss);
            KafkaConfiguration currentKafkaConfig = KafkaConfiguration.unvalidated(env.getOrDefault(ENV_VAR_KAFKA_CONFIGURATION, ""));
            log.info("{}: Upgrade: Removing Kafka config {}, will default to {}",
                    reconciliation, INTERBROKER_PROTOCOL_VERSION, downgrade.to().protocolVersion());
            currentKafkaConfig.removeConfigOption(INTERBROKER_PROTOCOL_VERSION);
            env.put(ENV_VAR_KAFKA_CONFIGURATION, currentKafkaConfig.getConfiguration());

            StatefulSet newSs = new StatefulSetBuilder(ss)
                    .editMetadata()
                        .withAnnotations(annotations)
                    .endMetadata()
                    .editSpec()
                        .editTemplate()
                            .editSpec()
                                .editFirstContainer()
                                    .withImage(downgradedImage)
                                    .withEnv(ModelUtils.envAsList(env))
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build();

            // Reconcile the SS and perform a rolling update of the pods
            log.info("{}: Upgrade: Patch + rolling update of {}", reconciliation, name);
            return kafkaSetOperations.reconcile(namespace, KafkaCluster.kafkaClusterName(name), newSs)
                    .compose(ignored -> kafkaSetOperations.maybeRollingUpdate(ss, pod -> {
                        log.info("{}: Upgrade: Patch + rolling update of {}: Pod {}", reconciliation, name, pod.getMetadata().getName());
                        return true;
                    }))
                    .compose(ignored -> {
                        log.info("{}: {}, phase 2 of 2 completed", reconciliation, downgrade);
                        return Future.succeededFuture();
                    });
        }

        Future<ReconciliationState> getZookeeperDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        StatefulSet sts = zkSetOperations.get(namespace, ZookeeperCluster.zookeeperClusterName(name));
                        Storage oldStorage = getOldStorage(sts);

                        this.zkCluster = ZookeeperCluster.fromCrd(kafkaAssembly, versions, oldStorage);

                        ConfigMap logAndMetricsConfigMap = zkCluster.generateMetricsAndLogConfigMap(zkCluster.getLogging() instanceof ExternalLogging ?
                                configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) zkCluster.getLogging()).getName()) :
                                null);

                        this.zkService = zkCluster.generateService();
                        this.zkHeadlessService = zkCluster.generateHeadlessService();
                        this.zkMetricsAndLogsConfigMap = zkCluster.generateMetricsAndLogConfigMap(logAndMetricsConfigMap);

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete((ReconciliationState) res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );

            return fut;
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

        Future<ReconciliationState> zkScaleDown() {
            return withVoid(zkSetOperations.scaleDown(namespace, zkCluster.getName(), zkCluster.getReplicas()));
        }

        Future<ReconciliationState> zkService() {
            return withVoid(serviceOperations.reconcile(namespace, zkCluster.getServiceName(), zkService));
        }

        Future<ReconciliationState> zkHeadlessService() {
            return withVoid(serviceOperations.reconcile(namespace, zkCluster.getHeadlessServiceName(), zkHeadlessService));
        }

        Future<ReconciliationState> getReconciliationStateOfConfigMap(AbstractModel cluster, ConfigMap configMap, BiFunction<Boolean, Future<ReconcileResult<ConfigMap>>, Future<ReconciliationState>> function) {
            Future<ReconciliationState> result = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<Boolean>executeBlocking(
                future -> {
                    ConfigMap current = configMapOperations.get(namespace, cluster.getAncillaryConfigName());
                    boolean onlyMetricsSettingChanged = onlyMetricsSettingChanged(current, configMap);
                    future.complete(onlyMetricsSettingChanged);
                }, res -> {
                    if (res.succeeded()) {
                        boolean onlyMetricsSettingChanged = res.result();
                        function.apply(onlyMetricsSettingChanged, configMapOperations.reconcile(namespace, cluster.getAncillaryConfigName(), configMap)).setHandler(res2 -> {
                            if (res2.succeeded()) {
                                result.complete(res2.result());
                            } else {
                                result.fail(res2.cause());
                            }
                        });
                    } else {
                        result.fail(res.cause());
                    }
                });
            return result;
        }

        Future<ReconciliationState> zkAncillaryCm() {
            return getReconciliationStateOfConfigMap(zkCluster, zkMetricsAndLogsConfigMap, this::withZkAncillaryCmChanged);
        }

        Future<ReconciliationState> zkNodesSecret() {
            return withVoid(secretOperations.reconcile(namespace, ZookeeperCluster.nodesSecretName(name),
                    zkCluster.generateNodesSecret(clusterCa, kafkaAssembly)));
        }

        Future<ReconciliationState> zkNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(namespace, ZookeeperCluster.policyName(name), zkCluster.generateNetworkPolicy(pfa.isNamespaceAndPodSelectorNetworkPolicySupported())));
        }

        Future<ReconciliationState> zkPodDisruptionBudget() {
            return withVoid(podDisruptionBudgetOperator.reconcile(namespace, zkCluster.getName(), zkCluster.generatePodDisruptionBudget()));
        }

        Future<ReconciliationState> zkStatefulSet() {
            StatefulSet zkSs = zkCluster.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            Annotations.annotations(zkSs.getSpec().getTemplate()).put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(getCaCertGeneration(this.clusterCa)));
            return withZkDiff(zkSetOperations.reconcile(namespace, zkCluster.getName(), zkSs));
        }

        Future<ReconciliationState> zkRollingUpdate() {
            return withVoid(zkSetOperations.maybeRollingUpdate(zkDiffs.resource(), pod ->
                isPodToRestart(zkDiffs.resource(), pod, zkAncillaryCmChange, this.clusterCa)
            ));
        }

        /* test */ void setZkAncillaryCmChange(boolean zkAncillaryCmChange) {
            this.zkAncillaryCmChange = zkAncillaryCmChange;
        }

        /* test */ void setKafkaAncillaryCmChange(boolean kafkaAncillaryCmChange) {
            this.kafkaAncillaryCmChange = kafkaAncillaryCmChange;
        }

        /**
         * Scale up is divided by scaling up Zookeeper cluster in steps.
         * Scaling up from N to M (N > 0 and M>N) replicas is done in M-N steps.
         * Each step performs scale up by one replica and full rolling update of Zookeeper cluster.
         * This approach ensures a valid configuration of each Zk pod.
         * Together with modified `maybeRollingUpdate` the quorum is not lost after the scale up operation is performed.
         * There is one special case of scaling from standalone (single one) Zookeeper pod.
         * In this case quorum cannot be preserved.
         */
        Future<ReconciliationState> zkScaleUpStep() {
            Future<StatefulSet> futss = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name));
            return withVoid(futss.map(ss -> ss == null ? Integer.valueOf(0) : ss.getSpec().getReplicas())
                    .compose(currentReplicas -> {
                        if (currentReplicas > 0 && zkCluster.getReplicas() > currentReplicas) {
                            zkCluster.setReplicas(currentReplicas + 1);
                        }
                        Future<Integer> result = Future.succeededFuture(zkCluster.getReplicas() + 1);
                        return result;
                    }));
        }

        Future<ReconciliationState> zkScaleUp() {
            return withVoid(zkSetOperations.scaleUp(namespace, zkCluster.getName(), zkCluster.getReplicas()));
        }

        Future<ReconciliationState> zkServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(namespace, zkService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> zkHeadlessServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(namespace, zkHeadlessService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> withZkAncillaryCmChanged(boolean onlyMetricsSettingChanged, Future<ReconcileResult<ConfigMap>> r) {
            return r.map(rr -> {
                if (onlyMetricsSettingChanged) {
                    log.debug("Only metrics setting changed - not triggering rolling update");
                    this.zkAncillaryCmChange = false;
                } else {
                    this.zkAncillaryCmChange = rr instanceof ReconcileResult.Patched;
                }
                return this;
            });
        }

        private Future<ReconciliationState> getKafkaClusterDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        StatefulSet sts = kafkaSetOperations.get(namespace, KafkaCluster.kafkaClusterName(name));
                        Storage oldStorage = getOldStorage(sts);

                        this.kafkaCluster = KafkaCluster.fromCrd(kafkaAssembly, versions, oldStorage);

                        ConfigMap logAndMetricsConfigMap = kafkaCluster.generateMetricsAndLogConfigMap(
                                kafkaCluster.getLogging() instanceof ExternalLogging ?
                                        configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) kafkaCluster.getLogging()).getName()) :
                                        null);
                        this.kafkaService = kafkaCluster.generateService();
                        this.kafkaHeadlessService = kafkaCluster.generateHeadlessService();
                        this.kafkaMetricsAndLogsConfigMap = logAndMetricsConfigMap;

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete(res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );
            return fut;
        }

        Future<ReconciliationState> withKafkaDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.kafkaDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> withKafkaAncillaryCmChanged(boolean onlyMetricsSettingChanged, Future<ReconcileResult<ConfigMap>> r) {
            return r.map(rr -> {
                if (onlyMetricsSettingChanged) {
                    log.debug("Only metrics setting changed - not triggering rolling update");
                    this.kafkaAncillaryCmChange = false;
                } else {
                    this.kafkaAncillaryCmChange = rr instanceof ReconcileResult.Patched;
                }
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

            Future replacementFut = Future.future();

            fut.setHandler(res -> {
                if (res.failed()) {
                    if (desired == null && res.cause() != null && res.cause().getMessage() != null &&
                            res.cause().getMessage().contains("Message: Forbidden!")) {
                        log.debug("Ignoring forbidden access to ClusterRoleBindings which seems not needed while Kafka rack awareness is disabled.");
                        replacementFut.complete();
                    } else {
                        replacementFut.fail(res.cause());
                    }
                } else {
                    replacementFut.complete();
                }
            });

            return withVoid(replacementFut);
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
                    this.kafkaExternalAddresses.add(kafkaCluster.getExternalAdvertisedUrl(i, host, "443"));

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

            Future blockingFuture = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    String serviceName = KafkaCluster.externalBootstrapServiceName(name);
                    Future<Void> address = null;

                    if (kafkaCluster.isExposedWithNodePort()) {
                        address = serviceOperations.hasNodePort(namespace, serviceName, 1_000, operationTimeoutMs);
                    } else {
                        address = serviceOperations.hasIngressAddress(namespace, serviceName, 1_000, operationTimeoutMs);
                    }

                    address.setHandler(res -> {
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
                                ServiceSpec ss = serviceOperations.get(namespace, serviceName).getSpec();
                                Integer nodePort = ss.getPorts().get(0).getNodePort();

                                setExternalListenerStatus(new ListenerAddressBuilder()
                                        .withHost("<AnyNodeAddress>")
                                        .withPort(nodePort)
                                        .build());
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
                        blockingFuture.complete();
                    } else {
                        blockingFuture.fail(res.cause());
                    }
                });

            return withVoid(blockingFuture);
        }

        Future<ReconciliationState> kafkaReplicaServicesReady() {
            if (!kafkaCluster.isExposedWithLoadBalancer() && !kafkaCluster.isExposedWithNodePort()) {
                return withVoid(Future.succeededFuture());
            }

            Future blockingFuture = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    int replicas = kafkaCluster.getReplicas();
                    List<Future> serviceFutures = new ArrayList<>(replicas);

                    for (int i = 0; i < replicas; i++) {
                        String serviceName = KafkaCluster.externalServiceName(name, i);
                        Future serviceFuture = Future.future();

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

                        address.setHandler(res -> {
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

                                    this.kafkaExternalAddresses.add(kafkaCluster.getExternalAdvertisedUrl(podNumber, serviceAddress, "9094"));

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

                                    this.kafkaExternalAddresses.add(kafkaCluster.getExternalAdvertisedUrl(podNumber, "", port));
                                }

                                this.kafkaExternalDnsNames.put(podNumber, dnsNames);

                                serviceFuture.complete();
                            } else {
                                if (kafkaCluster.isExposedWithNodePort()) {
                                    log.warn("{}: Node port was not assigned for Service {}.", reconciliation, serviceName);
                                    serviceFuture.fail("Node port was not assigned for Service " + serviceName + ".");
                                } else {
                                    log.warn("{}: No loadbalancer address found in the Status section of Service {} resource. Loadbalancer was probably not provisioned.", reconciliation, serviceName);
                                    serviceFuture.fail("No loadbalancer address found in the Status section of Service " + serviceName + " resource. Loadbalancer was probably not provisioned.");
                                }
                            }
                        });

                        serviceFutures.add(serviceFuture);
                    }

                    CompositeFuture.join(serviceFutures).setHandler(res -> {
                        if (res.succeeded()) {
                            future.complete();
                        } else {
                            future.fail(res.cause());
                        }
                    });
                }, res -> {
                    if (res.succeeded()) {
                        blockingFuture.complete();
                    } else {
                        blockingFuture.fail(res.cause());
                    }
                });

            return withVoid(blockingFuture);
        }

        Future<ReconciliationState> kafkaBootstrapRouteReady() {
            if (routeOperations == null || !kafkaCluster.isExposedWithRoute()) {
                return withVoid(Future.succeededFuture());
            }

            if (kafkaCluster.getExternalListenerBootstrapOverride() != null && kafkaCluster.getExternalListenerBootstrapOverride().getAddress() != null)    {
                log.trace("{}: Adding address {} from overrides to certificate DNS names", reconciliation, kafkaCluster.getExternalListenerBootstrapOverride().getAddress());
                this.kafkaExternalBootstrapDnsName.add(kafkaCluster.getExternalListenerBootstrapOverride().getAddress());
            }

            Future blockingFuture = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    String routeName = KafkaCluster.serviceName(name);
                    //Future future = Future.future();
                    Future<Void> address = routeOperations.hasAddress(namespace, routeName, 1_000, operationTimeoutMs);

                    address.setHandler(res -> {
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
                        blockingFuture.complete();
                    } else {
                        blockingFuture.fail(res.cause());
                    }
                });

            return withVoid(blockingFuture);
        }

        Future<ReconciliationState> kafkaReplicaRoutesReady() {
            if (routeOperations == null || !kafkaCluster.isExposedWithRoute()) {
                return withVoid(Future.succeededFuture());
            }

            Future blockingFuture = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    int replicas = kafkaCluster.getReplicas();
                    List<Future> routeFutures = new ArrayList<>(replicas);

                    for (int i = 0; i < replicas; i++) {
                        String routeName = KafkaCluster.externalServiceName(name, i);
                        Future routeFuture = Future.future();
                        Future<Void> address = routeOperations.hasAddress(namespace, routeName, 1_000, operationTimeoutMs);
                        int podNumber = i;

                        Set<String> dnsNames = new HashSet<>();

                        String dnsOverride = kafkaCluster.getExternalServiceAdvertisedHostOverride(i);
                        if (dnsOverride != null)    {
                            dnsNames.add(dnsOverride);
                        }

                        address.setHandler(res -> {
                            if (res.succeeded()) {
                                Route route = routeOperations.get(namespace, routeName);

                                // Get the advertised URL
                                String routeAddress = route.getStatus().getIngress().get(0).getHost();
                                this.kafkaExternalAddresses.add(kafkaCluster.getExternalAdvertisedUrl(podNumber, routeAddress, "443"));

                                if (log.isTraceEnabled()) {
                                    log.trace("{}: Found address {} for Route {}", reconciliation, routeAddress, routeName);
                                }

                                // Collect the DNS names for certificates
                                for (RouteIngress ingress : route.getStatus().getIngress()) {
                                    dnsNames.add(ingress.getHost());
                                }

                                this.kafkaExternalDnsNames.put(podNumber, dnsNames);

                                routeFuture.complete();
                            } else {
                                log.warn("{}: No route address found in the Status section of Route {} resource. Route was probably not provisioned by the OpenShift router.", reconciliation, routeName);
                                routeFuture.fail("No route address found in the Status section of Route " + routeName + " resource. Route was probably not provisioned by the OpenShift router.");
                            }
                        });

                        routeFutures.add(routeFuture);
                    }

                    CompositeFuture.join(routeFutures).setHandler(res -> {
                        if (res.succeeded()) {
                            future.complete();
                        } else {
                            future.fail(res.cause());
                        }
                    });
                }, res -> {
                    if (res.succeeded()) {
                        blockingFuture.complete();
                    } else {
                        blockingFuture.fail(res.cause());
                    }
                });

            return withVoid(blockingFuture);
        }

        Future<ReconciliationState> kafkaGenerateCertificates() {
            Future<ReconciliationState> result = Future.future();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        kafkaCluster.generateCertificates(kafkaAssembly,
                                clusterCa, kafkaExternalBootstrapDnsName, kafkaExternalDnsNames);
                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                },
                true,
                result);
            return result;
        }

        Future<ReconciliationState> kafkaAncillaryCm() {
            return getReconciliationStateOfConfigMap(kafkaCluster, kafkaMetricsAndLogsConfigMap, this::withKafkaAncillaryCmChanged);
        }

        Future<ReconciliationState> kafkaBrokersSecret() {
            return withVoid(secretOperations.reconcile(namespace, KafkaCluster.brokersSecretName(name), kafkaCluster.generateBrokersSecret()));
        }

        Future<ReconciliationState> kafkaNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(namespace, KafkaCluster.policyName(name), kafkaCluster.generateNetworkPolicy()));
        }

        Future<ReconciliationState> kafkaPodDisruptionBudget() {
            return withVoid(podDisruptionBudgetOperator.reconcile(namespace, kafkaCluster.getName(), kafkaCluster.generatePodDisruptionBudget()));
        }

        int getPodIndexFromPvcName(String pvcName)  {
            return Integer.parseInt(pvcName.substring(pvcName.lastIndexOf("-") + 1));
        }

        Future<ReconciliationState> maybeResizeReconcilePvcs(List<PersistentVolumeClaim> pvcs, AbstractModel cluster) {
            List<Future> futures = new ArrayList<>(pvcs.size());

            for (PersistentVolumeClaim desiredPvc : pvcs)  {
                Future<Void> result = Future.future();

                pvcOperations.getAsync(namespace, desiredPvc.getMetadata().getName()).setHandler(res -> {
                    if (res.succeeded())    {
                        PersistentVolumeClaim currentPvc = res.result();

                        if (currentPvc == null || currentPvc.getStatus() == null || !"Bound".equals(currentPvc.getStatus().getPhase())) {
                            // This branch handles the following conditions:
                            // * The PVC doesn't exist yet, we should create it
                            // * The PVC is not Bound and we should reconcile it
                            reconcilePvc(desiredPvc).setHandler(result);
                        } else if (currentPvc.getStatus().getConditions().stream().filter(cond -> "Resizing".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))).findFirst().orElse(null) != null)  {
                            // The PVC is Bound but it is already resizing => Nothing to do, we should let it resize
                            log.debug("{}: The PVC {} is resizing, nothing to do", reconciliation, desiredPvc.getMetadata().getName());
                            result.complete();
                        } else if (currentPvc.getStatus().getConditions().stream().filter(cond -> "FileSystemResizePending".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))).findFirst().orElse(null) != null)  {
                            // The PVC is Bound and resized but waiting for FS resizing => We need to restart the pod which is using it
                            String podName = cluster.getPodName(getPodIndexFromPvcName(desiredPvc.getMetadata().getName()));
                            fsResizingRestartRequest.add(podName);
                            log.info("{}: The PVC {} is waiting for file system resizing and the pod {} needs to be restarted.", reconciliation, desiredPvc.getMetadata().getName(), podName);
                            result.complete();
                        } else {
                            // The PVC is Bound and resizing is not in progress => We should check if the SC supports resizing and check if size changed
                            Quantity currentSize = currentPvc.getSpec().getResources().getRequests().get("storage");
                            Quantity desiredSize = desiredPvc.getSpec().getResources().getRequests().get("storage");

                            if (!desiredSize.equals(currentSize))   {
                                // The sizes are different => we should resize (shrinking will be handled in StorageDiff, so we do not need to check that)
                                resizePvc(currentPvc, desiredPvc).setHandler(result);
                            } else  {
                                // size didn't changed, just reconcile
                                reconcilePvc(desiredPvc).setHandler(result);
                            }
                        }
                    } else {
                        result.fail(res.cause());
                    }
                });

                futures.add(result);
            }

            return withVoid(CompositeFuture.all(futures));
        }

        Future<Void> reconcilePvc(PersistentVolumeClaim desired)  {
            Future<Void> result = Future.future();

            pvcOperations.reconcile(namespace, desired.getMetadata().getName(), desired).setHandler(pvcRes -> {
                if (pvcRes.succeeded()) {
                    result.complete();
                } else {
                    result.fail(pvcRes.cause());
                }
            });

            return result;
        }

        Future<Void> resizePvc(PersistentVolumeClaim current, PersistentVolumeClaim desired)  {
            Future<Void> result = Future.future();

            String storageClassName = current.getSpec().getStorageClassName();

            if (storageClassName != null && !storageClassName.isEmpty()) {
                storageClassOperator.getAsync(storageClassName).setHandler(scRes -> {
                    if (scRes.succeeded()) {
                        StorageClass sc = scRes.result();

                        if (sc == null) {
                            log.warn("{}: Storage Class {} not found. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", reconciliation, storageClassName, desired.getMetadata().getName());
                            result.complete();
                        } else if (sc.getAllowVolumeExpansion() == null || !sc.getAllowVolumeExpansion())    {
                            // Resizing not suported in SC => do nothing
                            log.warn("{}: Storage Class {} does not support resizing of volumes. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", reconciliation, storageClassName, desired.getMetadata().getName());
                            result.complete();
                        } else  {
                            // Resizing supported by SC => We can reconcile the PVC to have it resized
                            log.info("{}: Resizing PVC {} from {} to {}.", reconciliation, desired.getMetadata().getName(), current.getStatus().getCapacity().get("storage").getAmount(), desired.getSpec().getResources().getRequests().get("storage").getAmount());
                            pvcOperations.reconcile(namespace, desired.getMetadata().getName(), desired).setHandler(pvcRes -> {
                                if (pvcRes.succeeded()) {
                                    result.complete();
                                } else {
                                    result.fail(pvcRes.cause());
                                }
                            });
                        }
                    } else {
                        log.error("{}: Storage Class {} not found. PVC {} cannot be resized.", reconciliation, storageClassName, desired.getMetadata().getName(), scRes.cause());
                        result.fail(scRes.cause());
                    }
                });
            } else {
                log.warn("{}: PVC {} does not use any Storage Class and cannot be resized. Reconciliation will proceed without reconciling this PVC.", reconciliation, desired.getMetadata().getName());
                result.complete();
            }

            return result;
        }

        Future<ReconciliationState> zkPvcs() {
            List<PersistentVolumeClaim> pvcs = zkCluster.generatePersistentVolumeClaims();

            return maybeResizeReconcilePvcs(pvcs, zkCluster);
        }

        Future<ReconciliationState> kafkaPvcs() {
            List<PersistentVolumeClaim> pvcs = kafkaCluster.generatePersistentVolumeClaims(kafkaCluster.getStorage());

            return maybeResizeReconcilePvcs(pvcs, kafkaCluster);
        }

        Future<ReconciliationState> kafkaStatefulSet() {
            kafkaCluster.setExternalAddresses(kafkaExternalAddresses);
            StatefulSet kafkaSs = kafkaCluster.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            PodTemplateSpec template = kafkaSs.getSpec().getTemplate();
            Annotations.annotations(template).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION,
                    String.valueOf(getCaCertGeneration(this.clusterCa)));
            Annotations.annotations(template).put(
                    Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION,
                    String.valueOf(getCaCertGeneration(this.clientsCa)));
            return withKafkaDiff(kafkaSetOperations.reconcile(namespace, kafkaCluster.getName(), kafkaSs));
        }

        Future<ReconciliationState> kafkaRollingUpdate() {
            return withVoid(kafkaSetOperations.maybeRollingUpdate(kafkaDiffs.resource(), pod ->
                isPodToRestart(kafkaDiffs.resource(), pod, kafkaAncillaryCmChange, this.clusterCa, this.clientsCa)
            ));
        }

        Future<ReconciliationState> kafkaScaleUp() {
            return withVoid(kafkaSetOperations.scaleUp(namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
        }

        Future<ReconciliationState> zkPodsReady() {
            return podsReady(zkCluster);
        }

        Future<ReconciliationState> kafkaPodsReady() {
            return podsReady(kafkaCluster);
        }

        Future<ReconciliationState> podsReady(AbstractModel model) {
            int replicas = model.getReplicas();
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
            Future<Void> resultFuture = Future.future();

            futureSts.setHandler(res -> {
                if (res.succeeded())    {
                    List<PersistentVolumeClaim> desiredPvcs = zkCluster.generatePersistentVolumeClaims();
                    Future<List<PersistentVolumeClaim>> existingPvcsFuture = pvcOperations.listAsync(namespace, Labels.fromMap(zkCluster.getSelectorLabels()));

                    maybeCleanPodAndPvc(zkSetOperations, res.result(), desiredPvcs, existingPvcsFuture).setHandler(resultFuture);
                } else {
                    resultFuture.fail(res.cause());
                }
            });

            return withVoid(resultFuture);
        }

        /**
         * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
         *
         * @return
         */
        Future<ReconciliationState> kafkaManualPodCleaning() {
            String stsName = KafkaCluster.kafkaClusterName(name);
            Future<StatefulSet> futureSts = kafkaSetOperations.getAsync(namespace, stsName);
            Future<Void> resultFuture = Future.future();

            futureSts.setHandler(res -> {
                if (res.succeeded())    {
                    StatefulSet sts = res.result();

                    // The storage can change when the JBOD volumes are added / removed etc.
                    // At this point, the STS has not been updated yet. So we use the old storage configuration to get the old PVCs.
                    // This is needed because the restarted pod will be created from old statefulset with old storage configuration.
                    List<PersistentVolumeClaim> desiredPvcs = kafkaCluster.generatePersistentVolumeClaims(getOldStorage(sts));

                    Future<List<PersistentVolumeClaim>> existingPvcsFuture = pvcOperations.listAsync(namespace, Labels.fromMap(kafkaCluster.getSelectorLabels()));

                    maybeCleanPodAndPvc(kafkaSetOperations, sts, desiredPvcs, existingPvcsFuture).setHandler(resultFuture);
                } else {
                    resultFuture.fail(res.cause());
                }
            });

            return withVoid(resultFuture);
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
                                            deletePvcs = new ArrayList<>();
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

                        Future<Void> waitForDeletion = podOperations.waitFor(namespace, podName, pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
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

                            Future<Void> waitForDeletion = pvcOperations.waitFor(namespace, pvcName, pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
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
            Future<ReconciliationState> futureResult = Future.future();
            Future<List<PersistentVolumeClaim>> futurePvcs = pvcOperations.listAsync(namespace, Labels.fromMap(zkCluster.getSelectorLabels()));

            futurePvcs.setHandler(res -> {
                if (res.succeeded() && res.result() != null)    {
                    List<String> maybeDeletePvcs = res.result().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = zkCluster.generatePersistentVolumeClaims().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    persistentClaimDeletion(maybeDeletePvcs, desiredPvcs).setHandler(futureResult);
                } else {
                    futureResult.fail(res.cause());
                }
            });

            return futureResult;
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
            Future<ReconciliationState> futureResult = Future.future();
            Future<List<PersistentVolumeClaim>> futurePvcs = pvcOperations.listAsync(namespace, Labels.fromMap(kafkaCluster.getSelectorLabels()));

            futurePvcs.setHandler(res -> {
                if (res.succeeded() && res.result() != null)    {
                    List<String> maybeDeletePvcs = res.result().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = kafkaCluster.generatePersistentVolumeClaims(kafkaCluster.getStorage()).stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    persistentClaimDeletion(maybeDeletePvcs, desiredPvcs).setHandler(futureResult);
                } else {
                    futureResult.fail(res.cause());
                }
            });

            return futureResult;
        }

        /**
         * Internal method for deleting PVCs after scale-downs or disk removal from JBOD storage. It gets list of
         * existing and desired PVCs, diffs them and removes those wioch should not exist.
         *
         * @param maybeDeletePvcs   List of existing PVCs
         * @param desiredPvcs       List of PVCs whcih should exist
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
        private final Future<ReconciliationState> getTopicOperatorDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        this.topicOperator = io.strimzi.operator.cluster.model.TopicOperator.fromCrd(kafkaAssembly, versions);

                        if (topicOperator != null) {
                            ConfigMap logAndMetricsConfigMap = topicOperator.generateMetricsAndLogConfigMap(
                                    topicOperator.getLogging() instanceof ExternalLogging ?
                                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) topicOperator.getLogging()).getName()) :
                                            null);
                            this.toDeployment = topicOperator.generateDeployment(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
                            this.toMetricsAndLogsConfigMap = logAndMetricsConfigMap;
                            Annotations.annotations(this.toDeployment.getSpec().getTemplate()).put(
                                    io.strimzi.operator.cluster.model.TopicOperator.ANNO_STRIMZI_IO_LOGGING,
                                    this.toMetricsAndLogsConfigMap.getData().get("log4j2.properties"));
                        } else {
                            this.toDeployment = null;
                            this.toMetricsAndLogsConfigMap = null;
                        }

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete(res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );
            return fut;
        }

        @SuppressWarnings("deprecation")
        Future<ReconciliationState> topicOperatorServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(namespace,
                    io.strimzi.operator.cluster.model.TopicOperator.topicOperatorServiceAccountName(name),
                    toDeployment != null ? topicOperator.generateServiceAccount() : null));
        }

        @SuppressWarnings("deprecation")
        Future<ReconciliationState> topicOperatorRoleBinding() {
            if (topicOperator != null) {
                String watchedNamespace = namespace;

                if (topicOperator.getWatchedNamespace() != null
                        && !topicOperator.getWatchedNamespace().isEmpty()) {
                    watchedNamespace = topicOperator.getWatchedNamespace();
                }

                return withVoid(roleBindingOperations.reconcile(watchedNamespace, io.strimzi.operator.cluster.model.TopicOperator.roleBindingName(name), topicOperator.generateRoleBinding(namespace, watchedNamespace)));
            } else {
                return withVoid(roleBindingOperations.reconcile(namespace, io.strimzi.operator.cluster.model.TopicOperator.roleBindingName(name), null));
            }
        }

        @SuppressWarnings("deprecation")
        Future<ReconciliationState> topicOperatorAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    toDeployment != null ? topicOperator.getAncillaryConfigName() : io.strimzi.operator.cluster.model.TopicOperator.metricAndLogConfigsName(name),
                    toMetricsAndLogsConfigMap));
        }

        @SuppressWarnings("deprecation")
        Future<ReconciliationState> topicOperatorDeployment() {
            if (this.topicOperator != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.topicOperator.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int caCertGeneration = getCaCertGeneration(this.clusterCa);
                    Annotations.annotations(toDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
                    return withVoid(deploymentOperations.reconcile(namespace, io.strimzi.operator.cluster.model.TopicOperator.topicOperatorName(name), toDeployment));
                }).map(i -> this);
            } else  {
                return withVoid(deploymentOperations.reconcile(namespace, io.strimzi.operator.cluster.model.TopicOperator.topicOperatorName(name), null));
            }
        }

        @SuppressWarnings("deprecation")
        Future<ReconciliationState> topicOperatorSecret() {
            return withVoid(secretOperations.reconcile(namespace, io.strimzi.operator.cluster.model.TopicOperator.secretName(name), topicOperator == null ? null : topicOperator.generateSecret(clusterCa)));
        }

        @SuppressWarnings("deprecation")
        private final Future<ReconciliationState> getEntityOperatorDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        EntityOperator entityOperator = EntityOperator.fromCrd(kafkaAssembly, versions);

                        if (entityOperator != null) {
                            EntityTopicOperator topicOperator = entityOperator.getTopicOperator();
                            EntityUserOperator userOperator = entityOperator.getUserOperator();

                            ConfigMap topicOperatorLogAndMetricsConfigMap = topicOperator != null ?
                                    topicOperator.generateMetricsAndLogConfigMap(topicOperator.getLogging() instanceof ExternalLogging ?
                                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) topicOperator.getLogging()).getName()) :
                                            null) : null;

                            ConfigMap userOperatorLogAndMetricsConfigMap = userOperator != null ?
                                    userOperator.generateMetricsAndLogConfigMap(userOperator.getLogging() instanceof ExternalLogging ?
                                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) userOperator.getLogging()).getName()) :
                                            null) : null;

                            String configAnnotation = "";

                            if (topicOperatorLogAndMetricsConfigMap != null)    {
                                configAnnotation += topicOperatorLogAndMetricsConfigMap.getData().get("log4j2.properties");
                            }

                            if (userOperatorLogAndMetricsConfigMap != null)    {
                                configAnnotation += userOperatorLogAndMetricsConfigMap.getData().get("log4j2.properties");
                            }

                            Map<String, String> annotations = new HashMap<>();
                            annotations.put(io.strimzi.operator.cluster.model.TopicOperator.ANNO_STRIMZI_IO_LOGGING, configAnnotation);

                            this.entityOperator = entityOperator;
                            this.eoDeployment = entityOperator.generateDeployment(pfa.isOpenshift(), annotations, imagePullPolicy, imagePullSecrets);
                            this.topicOperatorMetricsAndLogsConfigMap = topicOperatorLogAndMetricsConfigMap;
                            this.userOperatorMetricsAndLogsConfigMap = userOperatorLogAndMetricsConfigMap;
                        }

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete(res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );
            return fut;
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
                            entityOperator.getTopicOperator().getAncillaryConfigName() : EntityTopicOperator.metricAndLogConfigsName(name),
                    topicOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorUserOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    eoDeployment != null && entityOperator.getUserOperator() != null ?
                            entityOperator.getUserOperator().getAncillaryConfigName() : EntityUserOperator.metricAndLogConfigsName(name),
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
                    return withVoid(deploymentOperations.reconcile(namespace, EntityOperator.entityOperatorName(name), eoDeployment));
                }).map(i -> this);
            } else  {
                return withVoid(deploymentOperations.reconcile(namespace, EntityOperator.entityOperatorName(name), null));
            }
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

        Future<ReconciliationState> entityOperatorSecret() {
            return withVoid(secretOperations.reconcile(namespace, EntityOperator.secretName(name),
                    entityOperator == null ? null : entityOperator.generateSecret(clusterCa)));
        }

        private boolean isPodUpToDate(StatefulSet ss, Pod pod) {
            final int ssGeneration = StatefulSetOperator.getSsGeneration(ss);
            final int podGeneration = StatefulSetOperator.getPodGeneration(pod);
            log.debug("Rolling update of {}/{}: pod {} has {}={}; ss has {}={}",
                    ss.getMetadata().getNamespace(), ss.getMetadata().getName(), pod.getMetadata().getName(),
                    StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, podGeneration,
                    StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, ssGeneration);
            return ssGeneration == podGeneration;
        }

        private boolean isPodCaCertUpToDate(Pod pod, Ca ca) {
            final int caCertGeneration = getCaCertGeneration(ca);
            String podAnnotation = getCaCertAnnotation(ca);
            final int podCaCertGeneration =
                    Annotations.intAnnotation(pod, podAnnotation, Ca.INIT_GENERATION);
            return caCertGeneration == podCaCertGeneration;
        }

        private boolean isPodToRestart(StatefulSet ss, Pod pod, boolean isAncillaryCmChange, Ca... cas) {
            boolean isPodUpToDate = isPodUpToDate(ss, pod);
            boolean isPodCaCertUpToDate = true;
            boolean isCaCertsChanged = false;
            boolean isFsResizeNeeded = false;

            for (Ca ca: cas) {
                isCaCertsChanged |= ca.certRenewed() || ca.certsRemoved();
                isPodCaCertUpToDate &= isPodCaCertUpToDate(pod, ca);
            }

            boolean isPodToRestart = !isPodUpToDate || isAncillaryCmChange;
            isPodToRestart |= isCaCertsChanged;
            isPodToRestart |= !isPodCaCertUpToDate;

            if (fsResizingRestartRequest.contains(pod.getMetadata().getName())) {
                isFsResizeNeeded = true;
            }
            isPodToRestart |= isFsResizeNeeded;

            if (log.isDebugEnabled()) {
                List<String> reasons = new ArrayList<>();
                for (Ca ca: cas) {
                    if (ca.certRenewed()) {
                        reasons.add(ca + " certificate renewal");
                    }
                    if (ca.certsRemoved()) {
                        reasons.add(ca + " certificate removal");
                    }
                    if (ca.certChanged()) {
                        reasons.add(ca + " certificate metadata changed");
                    }
                    if (!isPodCaCertUpToDate(pod, ca)) {
                        reasons.add("Pod has old " + ca + " certificate generation");
                    }
                }
                if (isAncillaryCmChange) {
                    reasons.add("ancillary CM change");
                }
                if (!isPodUpToDate) {
                    reasons.add("Pod has old generation");
                }
                if (isFsResizeNeeded)   {
                    reasons.add("file system needs to be resized");
                }
                if (!reasons.isEmpty()) {
                    if (isPodToRestart) {
                        log.debug("{}: Rolling pod {} due to {}",
                                reconciliation, pod.getMetadata().getName(), reasons);
                    }
                }
            }
            return isPodToRestart;
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

        Future<ReconciliationState> clusterOperatorSecret() {
            oldCoSecret = clusterCa.clusterOperatorSecret();

            Labels labels = Labels.fromResource(kafkaAssembly)
                    .withKind(reconciliation.kind())
                    .withCluster(reconciliation.name())
                    .withKubernetesName()
                    .withKubernetesInstance(reconciliation.name())
                    .withKubernetesManagedBy(AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);

            OwnerReference ownerRef = new OwnerReferenceBuilder()
                    .withApiVersion(kafkaAssembly.getApiVersion())
                    .withKind(kafkaAssembly.getKind())
                    .withName(kafkaAssembly.getMetadata().getName())
                    .withUid(kafkaAssembly.getMetadata().getUid())
                    .withBlockOwnerDeletion(false)
                    .withController(false)
                    .build();

            Secret secret = ModelUtils.buildSecret(clusterCa, clusterCa.clusterOperatorSecret(), namespace, ClusterOperator.secretName(name), "cluster-operator", "cluster-operator", labels, ownerRef);

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

        String getInternalServiceHostname(String serviceName)    {
            return serviceName + "." + namespace + ".svc";
        }

        private final Future<ReconciliationState> getKafkaExporterDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        this.kafkaExporter = KafkaExporter.fromCrd(kafkaAssembly, versions);
                        this.exporterDeployment = kafkaExporter.generateDeployment(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete(res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );
            return fut;
        }

        Future<ReconciliationState> kafkaExporterServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(namespace,
                    KafkaExporter.containerServiceAccountName(name),
                    exporterDeployment != null ? kafkaExporter.generateServiceAccount() : null));
        }

        Future<ReconciliationState> kafkaExporterSecret() {
            return withVoid(secretOperations.reconcile(namespace, KafkaExporter.secretName(name), kafkaExporter.generateSecret(clusterCa)));
        }

        Future<ReconciliationState> kafkaExporterDeployment() {
            if (this.kafkaExporter != null && this.exporterDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.kafkaExporter.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int caCertGeneration = getCaCertGeneration(this.clusterCa);
                    Annotations.annotations(exporterDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
                    return withVoid(deploymentOperations.reconcile(namespace, KafkaExporter.kafkaExporterName(name), exporterDeployment));
                }).map(i -> this);
            } else  {
                return withVoid(deploymentOperations.reconcile(namespace, KafkaExporter.kafkaExporterName(name), null));
            }
        }

        Future<ReconciliationState> kafkaExporterService() {
            return withVoid(serviceOperations.reconcile(namespace, this.kafkaExporter.getServiceName(), this.kafkaExporter.generateService()));
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

    }

    private Date dateSupplier() {
        return new Date();
    }

    /**
     * @param current Current ConfigMap
     * @param desired Desired ConfigMap
     * @return Returns true if only metrics settings has been changed
     */
    public boolean onlyMetricsSettingChanged(ConfigMap current, ConfigMap desired) {
        if ((current == null && desired != null) || (current != null && desired == null)) {
            // Metrics were added or deleted. We want rolling update
            return false;
        }
        JsonNode diff = JsonDiff.asJson(patchMapper().valueToTree(current), patchMapper().valueToTree(desired));
        boolean onlyMetricsSettingChanged = false;
        for (JsonNode d : diff) {
            if (d.get("path").asText().equals("/data/metrics-config.yml") && d.get("op").asText().equals("replace")) {
                onlyMetricsSettingChanged = true;
            }
        }
        return onlyMetricsSettingChanged && diff.size() == 1;
    }
}
