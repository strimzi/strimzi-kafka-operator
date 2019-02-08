/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.JbodStorage;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.SingleVolumeStorage;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.KafkaUpgradeException;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.EntityTopicOperator;
import io.strimzi.operator.cluster.model.EntityUserOperator;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaUpgrade;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronExpression;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_FROM_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_TO_VERSION;
import static io.strimzi.operator.cluster.model.KafkaCluster.ENV_VAR_KAFKA_CONFIGURATION;
import static io.strimzi.operator.cluster.model.KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION;
import static io.strimzi.operator.cluster.model.KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedVersions;
import static io.strimzi.operator.cluster.model.TopicOperator.ANNO_STRIMZI_IO_LOGGING;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaAssemblyList, DoneableKafka, Resource<Kafka, DoneableKafka>> {
    private static final Logger log = LogManager.getLogger(KafkaAssemblyOperator.class.getName());

    public static final String ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE = Annotations.STRIMZI_DOMAIN + "/manual-rolling-update";
    @Deprecated
    public static final String ANNO_OP_STRIMZI_IO_MANUAL_ROLLING_UPDATE = "operator.strimzi.io/manual-rolling-update";

    private final long operationTimeoutMs;

    private final ZookeeperSetOperator zkSetOperations;
    private final KafkaSetOperator kafkaSetOperations;
    private final ServiceOperator serviceOperations;
    private final RouteOperator routeOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;
    private final ConfigMapOperator configMapOperations;
    private final ServiceAccountOperator serviceAccountOperator;
    private final RoleBindingOperator roleBindingOperator;
    private final ClusterRoleBindingOperator clusterRoleBindingOperator;

    private final KafkaVersion.Lookup versions;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     */
    public KafkaAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                 long operationTimeoutMs,
                                 CertManager certManager,
                                 ResourceOperatorSupplier supplier,
                                 KafkaVersion.Lookup versions,
                                 ImagePullPolicy imagePullPolicy) {
        super(vertx, isOpenShift, ResourceType.KAFKA, certManager, supplier.kafkaOperator, supplier.secretOperations, supplier.networkPolicyOperator, supplier.podDisruptionBudgetOperator, imagePullPolicy);
        this.operationTimeoutMs = operationTimeoutMs;
        this.serviceOperations = supplier.serviceOperations;
        this.routeOperations = supplier.routeOperations;
        this.zkSetOperations = supplier.zkSetOperations;
        this.kafkaSetOperations = supplier.kafkaSetOperations;
        this.configMapOperations = supplier.configMapOperations;
        this.pvcOperations = supplier.pvcOperations;
        this.deploymentOperations = supplier.deploymentOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperator;
        this.roleBindingOperator = supplier.roleBindingOperator;
        this.clusterRoleBindingOperator = supplier.clusterRoleBindingOperator;
        this.versions = versions;
    }

    @Override
    public Future<Void> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
        Future<Void> chainFuture = Future.future();
        if (kafkaAssembly.getSpec() == null) {
            log.error("{} spec cannot be null", kafkaAssembly.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }
        createReconciliationState(reconciliation, kafkaAssembly)
                .reconcileCas()
                .compose(state -> state.clusterOperatorSecret())
                // Roll everything if a new CA is added to the trust store.
                .compose(state -> state.rollingUpdateForNewCaKey())
                .compose(state -> state.zkManualPodCleaning())
                .compose(state -> state.zkManualRollingUpdate())
                .compose(state -> state.getZookeeperDescription())
                .compose(state -> state.zkScaleUpStep())
                .compose(state -> state.zkScaleDown())
                .compose(state -> state.zkService())
                .compose(state -> state.zkHeadlessService())
                .compose(state -> state.zkAncillaryCm())
                .compose(state -> state.zkNodesSecret())
                .compose(state -> state.zkNetPolicy())
                .compose(state -> state.zkPodDisruptionBudget())
                .compose(state -> state.zkStatefulSet())
                .compose(state -> state.zkScaleUp())
                .compose(state -> state.zkRollingUpdate(this::dateSupplier))
                .compose(state -> state.zkServiceEndpointReadiness())
                .compose(state -> state.zkHeadlessServiceEndpointReadiness())
                .compose(state -> state.zkPersistentClaimDeletion())
                .compose(state -> state.kafkaUpgrade())
                .compose(state -> state.kafkaManualPodCleaning())
                .compose(state -> state.kafkaManualRollingUpdate())
                .compose(state -> state.getKafkaClusterDescription())
                .compose(state -> state.kafkaInitServiceAccount())
                .compose(state -> state.kafkaInitClusterRoleBinding())
                .compose(state -> state.kafkaScaleDown())
                .compose(state -> state.kafkaService())
                .compose(state -> state.kafkaHeadlessService())
                .compose(state -> state.kafkaExternalBootstrapService())
                .compose(state -> state.kafkaReplicaServices())
                .compose(state -> state.kafkaBootstrapRoute())
                .compose(state -> state.kafkaReplicaRoutes())
                .compose(state -> state.kafkaExternalBootstrapServiceReady())
                .compose(state -> state.kafkaReplicaServicesReady())
                .compose(state -> state.kafkaBootstrapRouteReady())
                .compose(state -> state.kafkaReplicaRoutesReady())
                .compose(state -> state.kafkaGenerateCertificates())
                .compose(state -> state.kafkaAncillaryCm())
                .compose(state -> state.kafkaBrokersSecret())
                .compose(state -> state.kafkaNetPolicy())
                .compose(state -> state.kafkaPodDisruptionBudget())
                .compose(state -> state.kafkaStatefulSet())
                .compose(state -> state.kafkaRollingUpdate(this::dateSupplier))
                .compose(state -> state.kafkaScaleUp())
                .compose(state -> state.kafkaServiceEndpointReady())
                .compose(state -> state.kafkaHeadlessServiceEndpointReady())
                .compose(state -> state.kafkaPersistentClaimDeletion())

                .compose(state -> state.getTopicOperatorDescription())
                .compose(state -> state.topicOperatorServiceAccount())
                .compose(state -> state.topicOperatorRoleBinding())
                .compose(state -> state.topicOperatorAncillaryCm())
                .compose(state -> state.topicOperatorSecret())
                .compose(state -> state.topicOperatorDeployment(this::dateSupplier))

                .compose(state -> state.getEntityOperatorDescription())
                .compose(state -> state.entityOperatorServiceAccount())
                .compose(state -> state.entityOperatorTopicOpRoleBinding())
                .compose(state -> state.entityOperatorUserOpRoleBinding())
                .compose(state -> state.entityOperatorTopicOpAncillaryCm())
                .compose(state -> state.entityOperatorUserOpAncillaryCm())
                .compose(state -> state.entityOperatorSecret())
                .compose(state -> state.entityOperatorDeployment(this::dateSupplier))

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
        private Service kafkaService;
        private Service kafkaHeadlessService;
        private ConfigMap kafkaMetricsAndLogsConfigMap;
        /* test */ ReconcileResult<StatefulSet> kafkaDiffs;
        private String kafkaExternalBootstrapDnsName = null;
        private SortedMap<Integer, String> kafkaExternalAddresses = new TreeMap<>();
        private SortedMap<Integer, String> kafkaExternalDnsNames = new TreeMap<>();
        private boolean kafkaAncillaryCmChange;

        /* test */ TopicOperator topicOperator;
        /* test */ Deployment toDeployment = null;
        private ConfigMap toMetricsAndLogsConfigMap = null;

        /* test */ EntityOperator entityOperator;
        /* test */ Deployment eoDeployment = null;
        private ConfigMap topicOperatorMetricsAndLogsConfigMap = null;
        private ConfigMap userOperatorMetricsAndLogsConfigMap;

        ReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            this.reconciliation = reconciliation;
            this.kafkaAssembly = kafkaAssembly;
            this.namespace = kafkaAssembly.getMetadata().getNamespace();
            this.name = kafkaAssembly.getMetadata().getName();
        }

        /**
         * Asynchronously reconciles the cluster and clients CA secrets.
         * The cluster CA secret has to have the name determined by {@link AbstractModel#clusterCaCertSecretName(String)}.
         * The clients CA secret has to have the name determined by {@link KafkaCluster#clientsCaCertSecretName(String)}.
         * Within both the secrets the current certificate is stored under the key {@code ca.crt}
         * and the current key is stored under the key {@code ca.key}.
         */
        Future<ReconciliationState> reconcileCas() {
            Labels selectorLabels = Labels.EMPTY.withKind(reconciliation.type().toString()).withCluster(reconciliation.name());
            Labels caLabels = Labels.userLabels(kafkaAssembly.getMetadata().getLabels()).withKind(reconciliation.type().toString()).withCluster(reconciliation.name());
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
                        this.clusterCa = new ClusterCa(certManager, name, clusterCaCertSecret, clusterCaKeySecret,
                                ModelUtils.getCertificateValidity(clusterCaConfig),
                                ModelUtils.getRenewalDays(clusterCaConfig),
                                clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority(),
                                clusterCaConfig != null ? clusterCaConfig.getCertificateExpirationPolicy() : null);
                        clusterCa.createRenewOrReplace(
                                reconciliation.namespace(), reconciliation.name(), caLabels.toMap(),
                                ownerRef);

                        this.clusterCa.initCaSecrets(clusterSecrets);

                        CertificateAuthority clientsCaConfig = kafkaAssembly.getSpec().getClientsCa();
                        this.clientsCa = new ClientsCa(certManager,
                                clientsCaCertName, clientsCaCertSecret,
                                clientsCaKeyName, clientsCaKeySecret,
                                ModelUtils.getCertificateValidity(clientsCaConfig),
                                ModelUtils.getRenewalDays(clientsCaConfig),
                                clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority(),
                                clientsCaConfig != null ? clientsCaConfig.getCertificateExpirationPolicy() : null);
                        clientsCa.createRenewOrReplace(reconciliation.namespace(), reconciliation.name(),
                                caLabels.toMap(), ownerRef);

                        secretOperations.reconcile(reconciliation.namespace(), clusterCaCertName, this.clusterCa.caCertSecret())
                                .compose(ignored -> secretOperations.reconcile(reconciliation.namespace(), clusterCaKeyName, this.clusterCa.caKeySecret()))
                                .compose(ignored -> secretOperations.reconcile(reconciliation.namespace(), clientsCaCertName, this.clientsCa.caCertSecret()))
                                .compose(ignored -> secretOperations.reconcile(reconciliation.namespace(), clientsCaKeyName, this.clientsCa.caKeySecret()))
                                .compose(ignored -> {
                                    future.complete(this);
                                }, future);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                result.completer()
            );
            return result;
        }

        /**
         * Perform a rolling update of the cluster so that CA certificates get added to their truststores,
         * or expired CA certificates get removed from their truststores.
         * Note this is only necessary when the CA certificate has changed due to a new CA key.
         * It is not necessary when the CA certificate is replace while retaining the existing key.
         */
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
                return zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                        .compose(ss -> {
                            return zkSetOperations.maybeRollingUpdate(ss, pod -> {
                                log.debug("{}: Rolling Pod {} to {}", reconciliation, pod.getMetadata().getName(), reasons);
                                return true;
                            });
                        })
                        .compose(i -> kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name)))
                        .compose(ss -> {
                            return kafkaSetOperations.maybeRollingUpdate(ss, pod -> {
                                log.debug("{}: Rolling Pod {} to {}", reconciliation, pod.getMetadata().getName(), reasons);
                                return true;
                            });
                        })
                        .compose(i -> deploymentOperations.getAsync(namespace, TopicOperator.topicOperatorName(name)))
                        .compose(dep -> {
                            if (dep != null) {
                                log.debug("{}: Rolling Deployment {} to {}", reconciliation, TopicOperator.topicOperatorName(name), reasons);
                                return deploymentOperations.rollingUpdate(namespace, TopicOperator.topicOperatorName(name), operationTimeoutMs);
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

                                log.debug("{}: Rolling Zookeeper pod {} to manual rolling update",
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
        public Future<StatefulSet> waitForQuiescence(String namespace, String statefulSetName) {
            return kafkaSetOperations.getAsync(namespace, statefulSetName).compose(ss -> {
                if (ss != null) {
                    return kafkaSetOperations.maybeRollingUpdate(ss,
                        pod -> {
                            boolean notUpToDate = !isPodUpToDate(ss, pod);
                            if (notUpToDate) {
                                log.debug("Rolling pod {} prior to upgrade", pod.getMetadata().getName());
                            }
                            return notUpToDate;
                        }).map(ignored -> ss);
                } else {
                    return Future.succeededFuture(ss);
                }
            });
        }

        Future<ReconciliationState> kafkaUpgrade() {
            // Wait until the SS is not being updated (it shouldn't be, but there's no harm in checking)
            String kafkaSsName = KafkaCluster.kafkaClusterName(name);
            return waitForQuiescence(namespace, kafkaSsName).compose(
                ss -> {
                    if (ss == null) {
                        return Future.succeededFuture(this);
                    }
                    log.debug("Does SS {} need to be upgraded?", ss.getMetadata().getName());
                    Future<?> result;
                    // Get the current version of the cluster
                    KafkaVersion currentVersion = versions.version(Annotations.annotations(ss).get(ANNO_STRIMZI_IO_KAFKA_VERSION));
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
                        result = Future.succeededFuture();
                    } else {
                        String image = versions.kafkaImage(kafkaAssembly.getSpec().getKafka().getImage(), toVersion.version());
                        Future<StatefulSet> f = Future.succeededFuture(ss);
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

                    }
                    return result.map(this);
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
                        this.zkCluster = ZookeeperCluster.fromCrd(kafkaAssembly, versions);

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
            return withVoid(networkPolicyOperator.reconcile(namespace, ZookeeperCluster.policyName(name), zkCluster.generateNetworkPolicy()));
        }

        Future<ReconciliationState> zkPodDisruptionBudget() {
            return withVoid(podDisruptionBudgetOperator.reconcile(namespace, zkCluster.getName(), zkCluster.generatePodDisruptionBudget()));
        }

        Future<ReconciliationState> zkStatefulSet() {
            StatefulSet zkSs = zkCluster.generateStatefulSet(isOpenShift, imagePullPolicy);
            Annotations.annotations(zkSs.getSpec().getTemplate()).put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(getCaCertGeneration(this.clusterCa)));
            return withZkDiff(zkSetOperations.reconcile(namespace, zkCluster.getName(), zkSs));
        }

        Future<ReconciliationState> zkRollingUpdate(Supplier<Date> dateSupplier) {
            return withVoid(zkSetOperations.maybeRollingUpdate(zkDiffs.resource(), pod ->
                isPodToRestart(zkDiffs.resource(), pod, zkAncillaryCmChange, dateSupplier, this.clusterCa)
            ));
        }

        /**
         * Scale up is divided by scaling up Zookeeper cluster in steps.
         * Scaling up from N to M (N > 0 and M>N) replicas is done in M-N steps.
         * Each step performs scale up by one replica and full tolling update of Zookeeper cluster.
         * This approach ensures a valid configuration of each Zk pod.
         * Together with modified `maybeRollingUpdate` the quorum is not lost after the scale up operation is performed.
         * There is one special case of scaling from standalone (single one) Zookeeper pod.
         * In this case quorum cannot be preserved.
         */
        Future<ReconciliationState> zkScaleUpStep() {
            Future<StatefulSet> futss = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name));
            return withVoid(futss.map(ss -> ss == null ? 0 : ss.getSpec().getReplicas())
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

        Future<ReconciliationState> zkManualPodCleaning() {
            String reason = "manual pod cleaning";
            Future<StatefulSet> futss = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name));
            if (futss != null) {
                return futss.compose(ss -> {
                    if (ss != null) {
                        log.debug("{}: Cleaning Pods for StatefulSet {} to {}", reconciliation, ss.getMetadata().getName(), reason);
                        return zkSetOperations.maybeDeletePodAndPvc(ss);
                    }
                    return Future.succeededFuture();
                }).map(i -> this);
            }
            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> zkPersistentClaimDeletion() {
            return persistentClaimDeletion(zkCluster.getStorage(), zkCluster.getReplicas(),
                (storage, i) -> AbstractModel.VOLUME_NAME + "-" + ZookeeperCluster.zookeeperClusterName(reconciliation.name()) + "-" + i);
        }

        private Future<ReconciliationState> getKafkaClusterDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        this.kafkaCluster = KafkaCluster.fromCrd(kafkaAssembly, versions);

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
            return withVoid(serviceAccountOperator.reconcile(namespace,
                    KafkaCluster.initContainerServiceAccountName(kafkaCluster.getCluster()),
                    kafkaCluster.generateInitContainerServiceAccount()));
        }

        Future<ReconciliationState> kafkaInitClusterRoleBinding() {
            ClusterRoleBindingOperator.ClusterRoleBinding desired = kafkaCluster.generateClusterRoleBinding(namespace);
            Future<Void> fut = clusterRoleBindingOperator.reconcile(
                    KafkaCluster.initContainerClusterRoleBindingName(namespace, name),
                    desired);

            Future replacementFut = Future.future();

            fut.setHandler(res -> {
                if (res.failed()) {
                    if (desired == null && res.cause().getMessage().contains("403: Forbidden")) {
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
            return withVoid(serviceOperations.reconcile(namespace, kafkaCluster.getServiceName(), kafkaService));
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

            if (routeOperations != null) {
                return withVoid(routeOperations.reconcile(namespace, KafkaCluster.serviceName(name), route));
            } else if (route != null) {
                log.warn("{}: Exposing Kafka cluster {} using OpenShift Routes is available only on OpenShift", reconciliation, name);
                return withVoid(Future.failedFuture("Exposing Kafka cluster " + name + " using OpenShift Routes is available only on OpenShift"));
            }

            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> kafkaReplicaRoutes() {
            int replicas = kafkaCluster.getReplicas();
            List<Future> routeFutures = new ArrayList<>(replicas);

            for (int i = 0; i < replicas; i++) {
                Route route = kafkaCluster.generateExternalRoute(i);

                if (routeOperations != null) {
                    routeFutures.add(routeOperations.reconcile(namespace, KafkaCluster.externalServiceName(name, i), route));
                } else if (route != null) {
                    log.warn("{}: Exposing Kafka cluster {} using OpenShift Routes is available only on OpenShift", reconciliation, name);
                    return withVoid(Future.failedFuture("Exposing Kafka cluster " + name + " using OpenShift Routes is available only on OpenShift"));
                }
            }

            return withVoid(CompositeFuture.join(routeFutures));
        }

        Future<ReconciliationState> kafkaExternalBootstrapServiceReady() {
            if (!kafkaCluster.isExposedWithLoadBalancer() && !kafkaCluster.isExposedWithNodePort()) {
                return withVoid(Future.succeededFuture());
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
                            String bootstrapAddress = null;

                            if (kafkaCluster.isExposedWithLoadBalancer()) {
                                if (serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null) {
                                    bootstrapAddress = serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                } else {
                                    bootstrapAddress = serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                }

                                this.kafkaExternalBootstrapDnsName = bootstrapAddress;
                            } else if (kafkaCluster.isExposedWithNodePort()) {
                                bootstrapAddress = serviceOperations.get(namespace, serviceName).getSpec().getPorts().get(0).getNodePort().toString();
                            }

                            if (log.isTraceEnabled()) {
                                log.trace("{}: Found address {} for Service {}", reconciliation, bootstrapAddress, serviceName);
                            }

                            future.complete();
                        } else {
                            log.warn("{}: No address found for Service {}", reconciliation, serviceName);
                            future.fail("No address found for Service " + serviceName);
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
                    List<Future> routeFutures = new ArrayList<>(replicas);

                    for (int i = 0; i < replicas; i++) {
                        String serviceName = KafkaCluster.externalServiceName(name, i);
                        Future routeFuture = Future.future();

                        Future<Void> address = null;

                        if (kafkaCluster.isExposedWithNodePort()) {
                            address = serviceOperations.hasNodePort(namespace, serviceName, 1_000, operationTimeoutMs);
                        } else {
                            address = serviceOperations.hasIngressAddress(namespace, serviceName, 1_000, operationTimeoutMs);
                        }

                        int podNumber = i;

                        address.setHandler(res -> {
                            if (res.succeeded()) {
                                String serviceAddress = null;
                                if (kafkaCluster.isExposedWithLoadBalancer()) {
                                    if (serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null) {
                                        serviceAddress = serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                    } else {
                                        serviceAddress = serviceOperations.get(namespace, serviceName).getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                    }

                                    if (kafkaCluster.isExposedWithTls()) {
                                        this.kafkaExternalDnsNames.put(podNumber, serviceAddress);
                                    }
                                } else if (kafkaCluster.isExposedWithNodePort()) {
                                    serviceAddress = serviceOperations.get(namespace, serviceName).getSpec().getPorts()
                                        .get(0).getNodePort().toString();
                                    kafkaCluster.getExternalNodePortServiceAddressOverride(podNumber)
                                        .ifPresent(host -> this.kafkaExternalDnsNames.put(podNumber, host));
                                }

                                this.kafkaExternalAddresses.put(podNumber, serviceAddress);

                                if (log.isTraceEnabled()) {
                                    log.trace("{}: Found address {} for Service {}", reconciliation, serviceAddress, serviceName);
                                }

                                routeFuture.complete();
                            } else {
                                log.warn("{}: No address found for Service {}", reconciliation, serviceName);
                                routeFuture.fail("No address found for Service " + serviceName);
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

        Future<ReconciliationState> kafkaBootstrapRouteReady() {
            if (routeOperations == null || !kafkaCluster.isExposedWithRoute()) {
                return withVoid(Future.succeededFuture());
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
                            this.kafkaExternalBootstrapDnsName = bootstrapAddress;

                            if (log.isTraceEnabled()) {
                                log.trace("{}: Found address {} for Route {}", reconciliation, bootstrapAddress, routeName);
                            }

                            future.complete();
                        } else {
                            log.warn("{}: No address found for Route {}", reconciliation, routeName);
                            future.fail("No address found for Route " + routeName);
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

                        address.setHandler(res -> {
                            if (res.succeeded()) {
                                String routeAddress = routeOperations.get(namespace, routeName).getStatus().getIngress().get(0).getHost();
                                this.kafkaExternalAddresses.put(podNumber, routeAddress);
                                this.kafkaExternalDnsNames.put(podNumber, routeAddress);

                                if (log.isTraceEnabled()) {
                                    log.trace("{}: Found address {} for Route {}", reconciliation, routeAddress, routeName);
                                }

                                routeFuture.complete();
                            } else {
                                log.warn("{}: No address found for Route {}", reconciliation, routeName);
                                routeFuture.fail("No address found for Route " + routeName);
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
                        if (kafkaCluster.isExposedWithNodePort()) {
                            kafkaCluster.generateCertificates(kafkaAssembly,
                                    clusterCa, null, Collections.EMPTY_MAP);
                        } else {
                            kafkaCluster.generateCertificates(kafkaAssembly,
                                    clusterCa, kafkaExternalBootstrapDnsName, kafkaExternalDnsNames);
                        }
                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                },
                true,
                result.completer());
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

        Future<ReconciliationState> kafkaStatefulSet() {
            kafkaCluster.setExternalAddresses(kafkaExternalAddresses);
            StatefulSet kafkaSs = kafkaCluster.generateStatefulSet(isOpenShift, imagePullPolicy);
            PodTemplateSpec template = kafkaSs.getSpec().getTemplate();
            Annotations.annotations(template).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION,
                    String.valueOf(getCaCertGeneration(this.clusterCa)));
            Annotations.annotations(template).put(
                    Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION,
                    String.valueOf(getCaCertGeneration(this.clientsCa)));
            return withKafkaDiff(kafkaSetOperations.reconcile(namespace, kafkaCluster.getName(), kafkaSs));
        }

        Future<ReconciliationState> kafkaRollingUpdate(Supplier<Date> dateSupplier) {
            return withVoid(kafkaSetOperations.maybeRollingUpdate(kafkaDiffs.resource(), pod ->
                isPodToRestart(kafkaDiffs.resource(), pod, kafkaAncillaryCmChange, dateSupplier, this.clusterCa, this.clientsCa)
            ));
        }

        Future<ReconciliationState> kafkaScaleUp() {
            return withVoid(kafkaSetOperations.scaleUp(namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
        }

        Future<ReconciliationState> kafkaServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(namespace, kafkaService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaHeadlessServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(namespace, kafkaHeadlessService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaManualPodCleaning() {
            String reason = "manual pod cleaning";
            Future<StatefulSet> futss = kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name));
            if (futss != null) {
                return futss.compose(ss -> {
                    if (ss != null) {
                        log.debug("{}: Cleaning Pods for StatefulSet {} to {}", reconciliation, ss.getMetadata().getName(), reason);
                        return kafkaSetOperations.maybeDeletePodAndPvc(ss);
                    }
                    return Future.succeededFuture();
                }).map(i -> this);
            }
            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> kafkaPersistentClaimDeletion() {
            return persistentClaimDeletion(kafkaCluster.getStorage(), kafkaCluster.getReplicas(),
                (storage, i) -> {
                    String name = ModelUtils.getVolumePrefix(storage.getId());
                    return name + "-" + KafkaCluster.kafkaClusterName(reconciliation.name()) + "-" + i;
                });
        }

        private final Future<ReconciliationState> getTopicOperatorDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        this.topicOperator = TopicOperator.fromCrd(kafkaAssembly);

                        if (topicOperator != null) {
                            ConfigMap logAndMetricsConfigMap = topicOperator.generateMetricsAndLogConfigMap(
                                    topicOperator.getLogging() instanceof ExternalLogging ?
                                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) topicOperator.getLogging()).getName()) :
                                            null);
                            this.toDeployment = topicOperator.generateDeployment(isOpenShift, imagePullPolicy);
                            this.toMetricsAndLogsConfigMap = logAndMetricsConfigMap;
                            Annotations.annotations(this.toDeployment.getSpec().getTemplate()).put(
                                    ANNO_STRIMZI_IO_LOGGING,
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

        Future<ReconciliationState> topicOperatorServiceAccount() {
            return withVoid(serviceAccountOperator.reconcile(namespace,
                    TopicOperator.topicOperatorServiceAccountName(name),
                    toDeployment != null ? topicOperator.generateServiceAccount() : null));
        }

        Future<ReconciliationState> topicOperatorRoleBinding() {
            String watchedNamespace = topicOperator != null ? topicOperator.getWatchedNamespace() : null;
            return withVoid(roleBindingOperator.reconcile(
                    watchedNamespace != null && !watchedNamespace.isEmpty() ?
                            watchedNamespace : namespace,
                    TopicOperator.roleBindingName(name),
                    toDeployment != null ? topicOperator.generateRoleBinding(namespace) : null));
        }

        Future<ReconciliationState> topicOperatorAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    toDeployment != null ? topicOperator.getAncillaryConfigName() : TopicOperator.metricAndLogConfigsName(name),
                    toMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> topicOperatorDeployment(Supplier<Date> dateSupplier) {
            if (this.topicOperator != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.topicOperator.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int caCertGeneration = getDeploymentCaCertGeneration(dep, this.clusterCa);
                    // if maintenance windows are satisfied, the cluster CA generation could be changed
                    // and EO needs a rolling update updating the related annotation
                    boolean isSatisfiedBy = isMaintenanceTimeWindowsSatisfied(dateSupplier);
                    if (isSatisfiedBy) {
                        caCertGeneration = getCaCertGeneration(this.clusterCa);
                    }
                    Annotations.annotations(toDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
                    return withVoid(deploymentOperations.reconcile(namespace, TopicOperator.topicOperatorName(name), toDeployment));
                }).map(i -> this);
            } else  {
                return withVoid(deploymentOperations.reconcile(namespace, TopicOperator.topicOperatorName(name), null));
            }
        }

        Future<ReconciliationState> topicOperatorSecret() {
            return withVoid(secretOperations.reconcile(namespace, TopicOperator.secretName(name), topicOperator == null ? null : topicOperator.generateSecret(clusterCa)));
        }

        private final Future<ReconciliationState> getEntityOperatorDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        EntityOperator entityOperator = EntityOperator.fromCrd(kafkaAssembly);

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

                            Map<String, String> annotations = new HashMap();
                            annotations.put(ANNO_STRIMZI_IO_LOGGING, topicOperatorLogAndMetricsConfigMap.getData().get("log4j2.properties") + userOperatorLogAndMetricsConfigMap.getData().get("log4j2.properties"));

                            this.entityOperator = entityOperator;
                            this.eoDeployment = entityOperator.generateDeployment(isOpenShift, annotations, imagePullPolicy);
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
            return withVoid(serviceAccountOperator.reconcile(namespace,
                    EntityOperator.entityOperatorServiceAccountName(name),
                    eoDeployment != null ? entityOperator.generateServiceAccount() : null));
        }

        Future<ReconciliationState> entityOperatorTopicOpRoleBinding() {
            String watchedNamespace = entityOperator != null && entityOperator.getTopicOperator() != null ?
                    entityOperator.getTopicOperator().getWatchedNamespace() : null;
            return withVoid(roleBindingOperator.reconcile(
                    watchedNamespace != null && !watchedNamespace.isEmpty() ?
                            watchedNamespace : namespace,
                    EntityTopicOperator.roleBindingName(name),
                    eoDeployment != null && entityOperator.getTopicOperator() != null ?
                            entityOperator.getTopicOperator().generateRoleBinding(namespace) : null));
        }

        Future<ReconciliationState> entityOperatorUserOpRoleBinding() {
            String watchedNamespace = entityOperator != null && entityOperator.getUserOperator() != null ?
                    entityOperator.getUserOperator().getWatchedNamespace() : null;
            return withVoid(roleBindingOperator.reconcile(
                    watchedNamespace != null && !watchedNamespace.isEmpty() ?
                            watchedNamespace : namespace,
                    EntityUserOperator.roleBindingName(name),
                    eoDeployment != null && entityOperator.getUserOperator() != null ?
                            entityOperator.getUserOperator().generateRoleBinding(namespace) : null));
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

        Future<ReconciliationState> entityOperatorDeployment(Supplier<Date> dateSupplier) {
            if (this.entityOperator != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.entityOperator.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int clusterCaCertGeneration = getDeploymentCaCertGeneration(dep, this.clusterCa);
                    int clientsCaCertGeneration = getDeploymentCaCertGeneration(dep, this.clientsCa);
                    // if maintenance windows are satisfied, the cluster CA generation could be changed
                    // and EO needs a rolling update updating the related annotation
                    boolean isSatisfiedBy = isMaintenanceTimeWindowsSatisfied(dateSupplier);
                    if (isSatisfiedBy) {
                        clusterCaCertGeneration = getCaCertGeneration(this.clusterCa);
                        clientsCaCertGeneration = getCaCertGeneration(this.clientsCa);
                    }
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

        private boolean isPodToRestart(StatefulSet ss, Pod pod, boolean isAncillaryCmChange, Supplier<Date> dateSupplier, Ca... cas) {
            boolean isPodUpToDate = isPodUpToDate(ss, pod);
            boolean isPodCaCertUpToDate = true;
            boolean isCaCertsChanged = false;
            for (Ca ca: cas) {
                isCaCertsChanged |= ca.certRenewed() || ca.certsRemoved();
                isPodCaCertUpToDate &= isPodCaCertUpToDate(pod, ca);
            }

            boolean isPodToRestart = !isPodUpToDate || !isPodCaCertUpToDate || isAncillaryCmChange || isCaCertsChanged;
            boolean isSatisfiedBy = true;
            // it makes sense to check maintenance windows if pod restarting is needed
            if (isPodToRestart) {
                isSatisfiedBy = isMaintenanceTimeWindowsSatisfied(dateSupplier);
            }

            if (log.isDebugEnabled()) {
                List<String> reasons = new ArrayList<>();
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
                if (isAncillaryCmChange) {
                    reasons.add("ancillary CM change");
                }
                if (!isPodUpToDate) {
                    reasons.add("Pod has old generation");
                }
                if (!reasons.isEmpty()) {
                    if (isSatisfiedBy) {
                        log.debug("{}: Rolling pod {} due to {}",
                                reconciliation, pod.getMetadata().getName(), reasons);
                    } else {
                        log.debug("{}: Potential pod {} rolling due to {} but maintenance time windows not satisfied",
                                reconciliation, pod.getMetadata().getName(), reasons);
                    }
                }
            }
            return isSatisfiedBy && isPodToRestart;
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

        private int getDeploymentCaCertGeneration(Deployment dep, Ca ca) {
            int caCertGeneration = 0;
            if (dep != null) {
                caCertGeneration =
                        Annotations.intAnnotation(
                                dep.getSpec().getTemplate(), getCaCertAnnotation(ca), 0);
            }
            return caCertGeneration;
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

        private PersistentVolumeClaim annotateDeleteClaim(String namespace, String pvcName, boolean isDeleteClaim) {
            PersistentVolumeClaim pvc = pvcOperations.get(namespace, pvcName);
            // this is called during a reconcile even when user is trying to change from ephemeral to persistent which
            // is not allowed, so the PVC doesn't exist
            if (pvc != null) {
                Annotations.annotations(pvc).put(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(isDeleteClaim));
            }
            return pvc;
        }

        Future<ReconciliationState> clusterOperatorSecret() {
            Labels labels = Labels.userLabels(kafkaAssembly.getMetadata().getLabels()).withKind(reconciliation.type().toString()).withCluster(reconciliation.name());

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

        private Future<ReconciliationState> persistentClaimDeletion(Storage storage, int replicas, BiFunction<PersistentClaimStorage, Integer, String> pvcName) {
            if (storage instanceof PersistentClaimStorage) {
                for (int i = 0; i < replicas; i++) {
                    PersistentVolumeClaim pvc = annotateDeleteClaim(reconciliation.namespace(),
                            pvcName.apply((PersistentClaimStorage) storage, i),
                            ((PersistentClaimStorage) storage).isDeleteClaim());
                    if (pvc != null) {
                        pvcOperations.reconcile(namespace, pvc.getMetadata().getName(), pvc);
                    }
                }
            } else if (storage instanceof JbodStorage) {
                JbodStorage jbodStorage = (JbodStorage) storage;
                for (int i = 0; i < replicas; i++) {
                    for (SingleVolumeStorage volume : jbodStorage.getVolumes()) {
                        if (volume instanceof PersistentClaimStorage) {
                            PersistentVolumeClaim pvc = annotateDeleteClaim(reconciliation.namespace(),
                                    pvcName.apply((PersistentClaimStorage) volume, i),
                                    ((PersistentClaimStorage) volume).isDeleteClaim());
                            if (pvc != null) {
                                pvcOperations.reconcile(namespace, pvc.getMetadata().getName(), pvc);
                            }
                        }
                    }
                }
            }
            return Future.succeededFuture(this);
        }
    }

    private final Future<CompositeFuture> deleteKafka(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        String kafkaSsName = KafkaCluster.kafkaClusterName(name);

        Labels pvcSelector = Labels.forCluster(name).withKind(Kafka.RESOURCE_KIND).withName(kafkaSsName);
        return deletePersistentVolumeClaim(namespace, pvcSelector);
    }

    private final Future<CompositeFuture> deleteZk(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        String zkSsName = ZookeeperCluster.zookeeperClusterName(name);

        Labels pvcSelector = Labels.forCluster(name).withKind(Kafka.RESOURCE_KIND).withName(zkSsName);
        return deletePersistentVolumeClaim(namespace, pvcSelector);
    }

    @Override
    protected Future<Void> delete(Reconciliation reconciliation) {
        return deleteKafka(reconciliation)
                .compose(i -> deleteZk(reconciliation))
                .map((Void) null);
    }

    @Override
    protected List<HasMetadata> getResources(String namespace, Labels selector) {
        // TODO: Search for PVCs!
        return Collections.EMPTY_LIST;
    }

    private Date dateSupplier() {
        return new Date();
    }

    /**
     * Delete Persistent Volume Claims in the specified {@code namespace} and having the
     * labels described by {@code pvcSelector} if the related {@link AbstractModel#ANNO_STRIMZI_IO_DELETE_CLAIM}
     * annotation is
     *
     * @param namespace namespace where the Persistent Volume Claims to delete are
     * @param pvcSelector labels to select the Persistent Volume Claims to delete
     * @return
     */
    private Future<CompositeFuture> deletePersistentVolumeClaim(String namespace, Labels pvcSelector) {
        List<PersistentVolumeClaim> pvcs = pvcOperations.list(namespace, pvcSelector);
        List<Future> result = new ArrayList<>();

        for (PersistentVolumeClaim pvc: pvcs) {
            if (Annotations.booleanAnnotation(pvc, AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM,
                    false, AbstractModel.ANNO_CO_STRIMZI_IO_DELETE_CLAIM)) {
                log.debug("Delete selected PVCs with labels", pvcSelector);
                result.add(pvcOperations.reconcile(namespace, pvc.getMetadata().getName(), null));
            }
        }
        return CompositeFuture.join(result);
    }
}