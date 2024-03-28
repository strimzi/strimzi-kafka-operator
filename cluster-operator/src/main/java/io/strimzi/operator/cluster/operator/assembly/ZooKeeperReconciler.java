/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZooKeeperRoller;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScaler;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StorageClassOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.TlsIdentitySet;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.auth.TlsPkcs12Identity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_SERVER_CERT_HASH;
import static java.util.Collections.singletonList;

/**
 * Class used for reconciliation of ZooKeeper. This class contains both the steps of the ZooKeeper
 * reconciliation pipeline and is also used to store the state between them.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class ZooKeeperReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZooKeeperReconciler.class.getName());

    private final Reconciliation reconciliation;
    private final Vertx vertx;
    private final long operationTimeoutMs;
    private final ZookeeperCluster zk;
    private final KafkaVersionChange versionChange;
    private final ClusterCa clusterCa;
    private final List<String> maintenanceWindows;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final boolean isNetworkPolicyGeneration;
    private final PlatformFeaturesAvailability pfa;
    private final int adminSessionTimeoutMs;
    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;

    private final StatefulSetOperator stsOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final ServiceOperator serviceOperator;
    private final PvcOperator pvcOperator;
    private final StorageClassOperator storageClassOperator;
    private final ConfigMapOperator configMapOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    private final PodOperator podOperator;

    private final ZookeeperScalerProvider zooScalerProvider;
    private final ZookeeperLeaderFinder zooLeaderFinder;

    private final Integer currentReplicas;

    private final Set<String> fsResizingRestartRequest = new HashSet<>();
    private ReconcileResult<StrimziPodSet> podSetDiff;
    private final Map<Integer, String> zkCertificateHash = new HashMap<>();

    private String loggingHash = "";
    private TlsPemIdentity tlsPemIdentity;
    private TlsPkcs12Identity tlsPkcs12Identity;

    private final boolean isKRaftMigrationRollback;

    /**
     * Constructs the ZooKeeper reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param vertx                     Vert.x instance
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param pfa                       PlatformFeaturesAvailability describing the environment we run in
     * @param kafkaAssembly             The Kafka custom resource
     * @param versionChange             Description of Kafka upgrade / downgrade state
     * @param currentReplicas           The current number of replicas
     * @param oldStorage                The storage configuration of the current cluster (null if it does not exist yet)
     * @param clusterCa                 The Cluster CA instance
     * @param isKRaftMigrationRollback  If a KRaft migration rollback is going on
     */
    public ZooKeeperReconciler(
            Reconciliation reconciliation,
            Vertx vertx,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            PlatformFeaturesAvailability pfa,
            Kafka kafkaAssembly,
            KafkaVersionChange versionChange,
            Storage oldStorage,
            int currentReplicas,
            ClusterCa clusterCa,
            boolean isKRaftMigrationRollback
    ) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.zk = ZookeeperCluster.fromCrd(reconciliation, kafkaAssembly, config.versions(), oldStorage, currentReplicas, supplier.sharedEnvironmentProvider);
        this.versionChange = versionChange;
        this.currentReplicas = currentReplicas;
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.pfa = pfa;
        this.adminSessionTimeoutMs = config.getZkAdminSessionTimeoutMs();
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.isKRaftMigrationRollback = isKRaftMigrationRollback;

        this.stsOperator = supplier.stsOperations;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.serviceOperator = supplier.serviceOperations;
        this.pvcOperator = supplier.pvcOperations;
        this.storageClassOperator = supplier.storageClassOperations;
        this.configMapOperator = supplier.configMapOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.podOperator = supplier.podOperations;

        this.zooScalerProvider = supplier.zkScalerProvider;
        this.zooLeaderFinder = supplier.zookeeperLeaderFinder;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param kafkaStatus   The Kafka Status class for adding conditions to it during the reconciliation
     * @param clock         The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                      That time is used for checking maintenance windows
     *
     * @return              Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
        return modelWarnings(kafkaStatus)
                .compose(i -> initClientAuthenticationCertificates())
                .compose(i -> jmxSecret())
                .compose(i -> manualPodCleaning())
                .compose(i -> networkPolicy())
                .compose(i -> manualRollingUpdate())
                .compose(i -> logVersionChange())
                .compose(i -> serviceAccount())
                .compose(i -> pvcs(kafkaStatus))
                .compose(i -> service())
                .compose(i -> headlessService())
                .compose(i -> certificateSecret(clock))
                .compose(i -> loggingAndMetricsConfigMap())
                .compose(i -> podDisruptionBudget())
                .compose(i -> migrateFromStatefulSetToPodSet())
                .compose(i -> podSet())
                .compose(i -> scaleDown())
                .compose(i -> rollingUpdate())
                .compose(i -> podsReady())
                .compose(i -> scaleUp())
                .compose(i -> scalingCheck())
                .compose(i -> serviceEndpointsReady())
                .compose(i -> headlessServiceEndpointsReady())
                .compose(i -> deletePersistentClaims())
                .compose(i -> maybeDeleteControllerZnode());
    }

    /**
     * Takes the warning conditions from the Model and adds them in the KafkaStatus
     *
     * @param kafkaStatus   The Kafka Status where the warning conditions will be added
     *
     * @return              Completes when the warnings are added to the status object
     */
    protected Future<Void> modelWarnings(KafkaStatus kafkaStatus) {
        kafkaStatus.addConditions(zk.getWarningConditions());
        return Future.succeededFuture();
    }

    /**
     * Initialize the TrustSet, PemAuthIdentity and Pkcs12AuthIdentity to be used by TLS clients during reconciliation
     *
     * @return Completes when the TrustSet, PemAuthIdentity and Pkcs12AuthIdentity have been created and stored in records
     */
    protected Future<Void> initClientAuthenticationCertificates() {
        return Future.join(
                ReconcilerUtils.clusterCaPemTrustSet(reconciliation, secretOperator),
                ReconcilerUtils.coClientAuthIdentity(reconciliation, secretOperator)
        ).onSuccess(result -> {
            PemTrustSet pemTrustSet = result.resultAt(0);
            TlsIdentitySet tlsIdentitySet = result.resultAt(1);
            this.tlsPemIdentity = new TlsPemIdentity(pemTrustSet, tlsIdentitySet.pemAuthIdentity());
            this.tlsPkcs12Identity = new TlsPkcs12Identity(pemTrustSet, tlsIdentitySet.pkcs12AuthIdentity());
        }).mapEmpty();
    }

    /**
     * Manages the secret with JMX credentials when JMX is enabled
     *
     * @return  Completes when the JMX secret is successfully created or updated
     */
    protected Future<Void> jmxSecret() {
        return ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperator, zk);
    }

    /**
     * Will check all Zookeeper pods whether the user requested the pod and PVC deletion through an annotation
     *
     * @return  Completes when the manual pod cleaning is done
     */
    protected Future<Void> manualPodCleaning() {
        return new ManualPodCleaner(
                reconciliation,
                zk.getSelectorLabels(),
                strimziPodSetOperator,
                podOperator,
                pvcOperator
        ).maybeManualPodCleaning();
    }

    /**
     * Manages the network policy protecting the ZooKeeper cluster
     *
     * @return  Completes when the network policy is successfully created or updated
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperNetworkPolicyName(reconciliation.name()), zk.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels))
                    .map((Void) null);
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Does manual rolling update of Zoo pods based on an annotation on the StrimziPodSet or on the Pods. Annotation
     * on StrimziPodSet level triggers rolling update of all pods. Annotation on pods triggers rolling update only of
     * the selected pods. If the annotation is present on both StrimziPodSet and one or more pods, only one rolling
     * update of all pods occurs.
     *
     * @return  Future with the result of the rolling update
     */
    protected Future<Void> manualRollingUpdate() {
        return strimziPodSetOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()))
                .compose(podSet -> {
                    if (podSet != null
                            && Annotations.booleanAnnotation(podSet, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                        // User trigger rolling update of the whole cluster
                        return maybeRollZooKeeper(pod -> {
                            LOGGER.debugCr(reconciliation, "Rolling Zookeeper pod {} due to manual rolling update", pod.getMetadata().getName());
                            return singletonList("manual rolling update");
                        }, this.tlsPemIdentity);
                    } else {
                        // The StrimziPodSet does not exist or is not annotated
                        // But maybe the individual pods are annotated to restart only some of them.
                        return manualPodRollingUpdate();
                    }
                });
    }

    /**
     * Does rolling update of Zoo pods based on the annotation on Pod level
     *
     * @return  Future with the result of the rolling update
     */
    private Future<Void> manualPodRollingUpdate() {
        return podOperator.listAsync(reconciliation.namespace(), zk.getSelectorLabels())
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
                        }, this.tlsPemIdentity);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Logs any changes to the ZooKeeper version which will be done during the reconciliation. This method only logs
     * them, it doesn't actually change the version.
     *
     * @return  Completes when the upgrade / downgrade information is logged
     */
    private Future<Void> logVersionChange() {
        int versionCompare = versionChange.from().compareTo(versionChange.to());

        if (versionCompare == 0) {
            LOGGER.debugCr(reconciliation, "Kafka.spec.kafka.version is unchanged therefore no change to Zookeeper is required");
        } else {
            String versionChangeType;

            if (versionCompare > 0) {
                versionChangeType = "downgrade";
            } else {
                versionChangeType = "upgrade";
            }

            if (versionChange.from().zookeeperVersion().equals(versionChange.to().zookeeperVersion())) {
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
        }

        return Future.succeededFuture();
    }

    /**
     * Manages the ZooKeeper service account
     *
     * @return  Completes when the service account was successfully created or updated
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), zk.generateServiceAccount())
                .map((Void) null);
    }

    /**
     * Manages the PVCs needed by the ZooKeeper cluster. This method only creates or updates the PVCs. Deletion of PVCs
     * after scale-down happens only at the end of the reconciliation when they are not used anymore.
     *
     * @param kafkaStatus   Status of the Kafka custom resource where warnings about any issues with resizing will be added
     *
     * @return  Completes when the PVCs were successfully created or updated
     */
    protected Future<Void> pvcs(KafkaStatus kafkaStatus) {
        List<PersistentVolumeClaim> pvcs = zk.generatePersistentVolumeClaims();

        return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                .resizeAndReconcilePvcs(kafkaStatus, podIndex -> KafkaResources.zookeeperPodName(reconciliation.name(), podIndex), pvcs)
                .compose(podsToRestart -> {
                    fsResizingRestartRequest.addAll(podsToRestart);
                    return Future.succeededFuture();
                });
    }

    /**
     * Manages the regular CLusterIP service used by ZooKeeper clients
     *
     * @return  Completes when the service was successfully created or updated
     */
    protected Future<Void> service() {
        return serviceOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperServiceName(reconciliation.name()), zk.generateService())
                .map((Void) null);
    }

    /**
     * Manages the headless service
     *
     * @return  Completes when the service was successfully created or updated
     */
    protected Future<Void> headlessService() {
        return serviceOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperHeadlessServiceName(reconciliation.name()), zk.generateHeadlessService())
                .map((Void) null);
    }

    /**
     * Manages the Secret with the node certificates used by the ZooKeeper nodes.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Completes when the Secret was successfully created or updated
     */
    protected Future<Void> certificateSecret(Clock clock) {
        return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperSecretName(reconciliation.name()))
                .compose(oldSecret -> {
                    return secretOperator
                            .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperSecretName(reconciliation.name()),
                                    zk.generateCertificatesSecret(clusterCa, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant())))
                            .compose(patchResult -> {
                                if (patchResult != null) {
                                    for (int podNum = 0; podNum < zk.getReplicas(); podNum++) {
                                        var podName = KafkaResources.zookeeperPodName(reconciliation.name(), podNum);
                                        zkCertificateHash.put(
                                                podNum,
                                                CertUtils.getCertificateThumbprint(patchResult.resource(),
                                                        Ca.SecretEntry.CRT.asKey(podName)
                                                ));
                                    }
                                }

                                return Future.succeededFuture();
                            });
                });
    }

    /**
     * Manages the ConfigMap with logging and metrics configuration.
     *
     * @return  Completes when the ConfigMap was successfully created or updated
     */
    protected Future<Void> loggingAndMetricsConfigMap() {
        return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, zk.logging(), zk.metrics())
                .compose(metricsAndLogging -> {
                    ConfigMap logAndMetricsConfigMap = zk.generateConfigurationConfigMap(metricsAndLogging);

                    loggingHash = Util.hashStub(logAndMetricsConfigMap.getData().get(zk.logging().configMapKey()));

                    return configMapOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperMetricsAndLogConfigMapName(reconciliation.name()), logAndMetricsConfigMap)
                            .map((Void) null);
                });
    }

    /**
     * Manages the PodDisruptionBudgets on Kubernetes clusters which support v1 version of PDBs
     *
     * @return  Completes when the PDB was successfully created or updated
     */
    protected Future<Void> podDisruptionBudget() {
        return podDisruptionBudgetOperator
                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), zk.generatePodDisruptionBudget())
                .map((Void) null);
    }

    /**
     * Helps with the migration from StatefulSets to StrimziPodSets when the cluster is switching between them. When the
     * switch happens, it deletes the old StatefulSet. It should happen before the new PodSet is created to
     * allow the controller hand-off.
     *
     * @return          Future which completes when the StatefulSet is deleted or does not need to be deleted
     */
    protected Future<Void> migrateFromStatefulSetToPodSet() {
        // Delete the StatefulSet if it exists
        return stsOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()))
                .compose(sts -> {
                    if (sts != null)    {
                        return stsOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), false);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Create or update the StrimziPodSet for the ZooKeeper cluster with the default number of pods. When PodSets are
     * disabled, it will try to delete the old PodSet. That means either the number of pods the pod set had before or
     * the number of pods based on the Kafka CR if this is a new cluster. Scale-up and scale-down are down separately.
     *
     * @return  Future which completes when the PodSet is created, updated or deleted
     */
    protected Future<Void> podSet() {
        return podSet(currentReplicas > 0 ? currentReplicas : zk.getReplicas());
    }

    /**
     * Create the StrimziPodSet for the ZooKeeper cluster with a specific number of pods. This is used directly
     * during scale-ups or scale-downs.
     *
     * @param replicas  Number of replicas which the PodSet should use
     *
     * @return          Future which completes when the PodSet is created or updated
     */
    private Future<Void> podSet(int replicas) {
        StrimziPodSet zkPodSet = zk.generatePodSet(replicas, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, this::zkPodSetPodAnnotations);
        return strimziPodSetOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()), zkPodSet)
                .compose(rr -> {
                    podSetDiff = rr;
                    return Future.succeededFuture();
                });
    }

    /**
     * Prepares annotations for ZooKeeper pods within a StrimziPodSet.
     *
     * @param podNum Number of the ZooKeeper pod, the annotations of which are being prepared.
     * @return Map with Pod annotations
     */
    public Map<String, String> zkPodSetPodAnnotations(int podNum) {
        Map<String, String> podAnnotations = new LinkedHashMap<>((int) Math.ceil(podNum / 0.75));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(this.clusterCa.caCertGeneration()));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(this.clusterCa.caKeyGeneration()));
        podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_HASH, loggingHash);
        podAnnotations.put(ANNO_STRIMZI_SERVER_CERT_HASH, zkCertificateHash.get(podNum));
        return podAnnotations;
    }

    /**
     * Prepares the Zookeeper connectionString
     * The format is host1:port1,host2:port2,...
     *
     * Used by the Zookeeper Admin client for scaling.
     *
     * @param connectToReplicas     Number of replicas from the ZK STS which should be used
     *
     * @return                      The generated Zookeeper connection string
     */
    private String zkConnectionString(int connectToReplicas, Function<Integer, String> zkNodeAddress)  {
        // Prepare Zoo connection string. We want to connect only to nodes which existed before
        // scaling and will exist after it is finished
        List<String> zooNodes = new ArrayList<>(connectToReplicas);

        for (int i = 0; i < connectToReplicas; i++)   {
            zooNodes.add(String.format("%s:%d", zkNodeAddress.apply(i), ZookeeperCluster.CLIENT_TLS_PORT));
        }

        return  String.join(",", zooNodes);
    }

    /**
     * Helper method for getting the required secrets with certificates and creating the ZookeeperScaler instance
     * for the given cluster. The ZookeeperScaler instance created by this method should be closed manually after
     * it is not used anymore.
     *
     * @param connectToReplicas     Number of pods from the Zookeeper STS which the scaler should use
     *
     * @return                      Zookeeper scaler instance.
     */
    private Future<ZookeeperScaler> zkScaler(int connectToReplicas)  {
        Function<Integer, String> zkNodeAddress = (Integer i) ->
                DnsNameGenerator.podDnsNameWithoutClusterDomain(reconciliation.namespace(), KafkaResources.zookeeperHeadlessServiceName(reconciliation.name()), KafkaResources.zookeeperPodName(reconciliation.name(), i));

        ZookeeperScaler zkScaler = zooScalerProvider
                .createZookeeperScaler(
                        reconciliation,
                        vertx,
                        zkConnectionString(connectToReplicas, zkNodeAddress),
                        zkNodeAddress,
                        this.tlsPkcs12Identity,
                        operationTimeoutMs,
                        adminSessionTimeoutMs
                );

        return Future.succeededFuture(zkScaler);
    }

    /**
     * General method which orchestrates ZooKeeper scale-down from N to M pods. This relies on other methods which scale
     * the pods one by one.
     *
     * @return  Future which completes ZooKeeper scale-down is complete
     */
    protected Future<Void> scaleDown() {
        int desired = zk.getReplicas();

        if (currentReplicas > desired) {
            // With scaling
            LOGGER.infoCr(reconciliation, "Scaling Zookeeper down from {} to {} replicas", currentReplicas, desired);

            // No need to check for pod readiness since we run right after the readiness check
            return zkScaler(desired)
                    .compose(zkScaler -> {
                        Promise<Void> scalingPromise = Promise.promise();

                        scaleDownByOne(zkScaler, currentReplicas, desired)
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
            return Future.succeededFuture();
        }
    }

    /**
     * Scales-down ZooKeeper by one node. To not break the quorum when scaling down, we always remove the pod from the
     * quorum and only then remove the Pod.
     *
     * @return  Future which completes new pod is removed
     */
    private Future<Void> scaleDownByOne(ZookeeperScaler zkScaler, int current, int desired) {
        if (current > desired) {
            return ReconcilerUtils
                    .podsReady(
                            reconciliation,
                            podOperator,
                            operationTimeoutMs,
                            IntStream.rangeClosed(0, current - 1).mapToObj(i -> KafkaResources.zookeeperPodName(reconciliation.name(), i)).collect(Collectors.toList())
                    )
                    .compose(i -> zkScaler.scale(current - 1))
                    .compose(i -> scaleDownPodSet(current - 1))
                    .compose(i -> scaleDownByOne(zkScaler, current - 1, desired));
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Scales-down the ZooKeeper PodSet, depending on what is used. This method only updates the PodSet, it does not
     * handle the pods or ZooKeeper configuration.
     *
     * @return  Future which completes when PodSet is scaled-down.
     */
    private Future<Void> scaleDownPodSet(int desiredScale)   {
        return podSet(desiredScale)
                // We wait for the pod to be deleted, otherwise it might disrupt the rolling update
                .compose(ignore -> podOperator.waitFor(
                        reconciliation,
                        reconciliation.namespace(),
                        KafkaResources.zookeeperPodName(reconciliation.name(), desiredScale),
                        "to be deleted",
                        1_000L,
                        operationTimeoutMs,
                        (podNamespace, podName) -> podOperator.get(podNamespace, podName) == null)
                );
    }

    /**
     * General method for rolling update of the ZooKeeper cluster.
     *
     * @return  Future which completes when any of the ZooKeeper pods which need rolling is rolled
     */
    protected Future<Void> rollingUpdate() {
        return maybeRollZooKeeper(pod ->
                ReconcilerUtils.reasonsToRestartPod(
                        reconciliation,
                        podSetDiff.resource(),
                        pod,
                        fsResizingRestartRequest,
                        ReconcilerUtils.trackedServerCertChanged(pod, zkCertificateHash),
                        clusterCa)
                        .getAllReasonNotes(),
                        this.tlsPemIdentity
        );
    }

    /**
     * Checks if the ZooKeeper cluster needs rolling and if it does, it will roll it.
     *
     * @param podNeedsRestart       Function to determine if the ZooKeeper pod needs to be restarted
     * @param coTlsPemIdentity      Trust set and identity for TLS client authentication for connecting to ZooKeeper
     *
     * @return                      Future which completes when any of the ZooKeeper pods which need rolling is rolled
     */
    /* test */ Future<Void> maybeRollZooKeeper(Function<Pod, List<String>> podNeedsRestart, TlsPemIdentity coTlsPemIdentity) {
        return new ZooKeeperRoller(podOperator, zooLeaderFinder, operationTimeoutMs)
                .maybeRollingUpdate(
                        reconciliation,
                        currentReplicas > 0 && currentReplicas < zk.getReplicas() ? currentReplicas : zk.getReplicas(),
                        zk.getSelectorLabels(),
                        podNeedsRestart,
                        coTlsPemIdentity
                );
    }

    /**
     * Checks whether the ZooKeeper pods are ready and if not, waits for them to get ready
     *
     * @return  Future which completes when all ZooKeeper pods are ready
     */
    protected Future<Void> podsReady() {
        return ReconcilerUtils
                .podsReady(
                        reconciliation,
                        podOperator,
                        operationTimeoutMs,
                        IntStream
                                .range(0, currentReplicas > 0 && currentReplicas < zk.getReplicas() ? currentReplicas : zk.getReplicas())
                                .mapToObj(i -> KafkaResources.zookeeperPodName(reconciliation.name(), i))
                                .collect(Collectors.toList())
                );
    }

    /**
     * General method which orchestrates ZooKeeper scale-up from N to M pods. This relies on other methods which scale
     * the pods one by one.
     *
     * @return  Future which completes when the ZooKeeper scale-up is complete
     */
    protected Future<Void> scaleUp() {
        int desired = zk.getReplicas();

        if (currentReplicas > 0 && currentReplicas < desired) {
            LOGGER.infoCr(reconciliation, "Scaling Zookeeper up from {} to {} replicas", currentReplicas, desired);

            return zkScaler(currentReplicas)
                    .compose(zkScaler -> {
                        Promise<Void> scalingPromise = Promise.promise();

                        scaleUpByOne(zkScaler, currentReplicas, desired)
                                .onComplete(res -> {
                                    zkScaler.close();

                                    if (res.succeeded())    {
                                        scalingPromise.complete();
                                    } else {
                                        LOGGER.warnCr(reconciliation, "Failed to scale Zookeeper", res.cause());
                                        scalingPromise.fail(res.cause());
                                    }
                                });

                        return scalingPromise.future();
                    });
        } else {
            // No scaling up => do nothing
            return Future.succeededFuture();
        }
    }

    /**
     * Scales-up ZooKeeper by one node. To not break the quorum when scaling up, we always add only one new pod at once,
     * wait for it to get ready and reconfigure the ZooKeeper quorum to include this pod.
     *
     * @return  Future which completes new pod is created and added to the quorum
     */
    private Future<Void> scaleUpByOne(ZookeeperScaler zkScaler, int current, int desired) {
        if (current < desired) {
            return zkScaleUpPodSet(current + 1)
                    .compose(ignore -> podOperator.readiness(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperPodName(reconciliation.name(), current), 1_000, operationTimeoutMs))
                    .compose(ignore -> zkScaler.scale(current + 1))
                    .compose(ignore -> scaleUpByOne(zkScaler, current + 1, desired));
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Scales-up the ZooKeeper PodSet. This method only updates the PodSet, it does not handle the pods or ZooKeeper
     * configuration.
     *
     * @return  Future which completes when the PodSet is scaled-up.
     */
    private Future<Void> zkScaleUpPodSet(int desiredScale)   {
        return podSet(desiredScale);
    }

    /**
     * Checks that the ZooKeeper cluster is configured for the correct number of nodes. This method is used to recover
     * from any scaling which previously failed (e.g. added a new ZooKeeper pod but didn't manage to reconfigure the
     * quorum configuration). It also serves as a good test if the ZooKeeper cluster formed or not.
     *
     * @return  Future which completes when the ZooKeeper quorum is configured for the current number of nodes
     */
    protected Future<Void> scalingCheck() {
        // No scaling, but we should check the configuration
        // This can cover any previous failures in the Zookeeper reconfiguration
        LOGGER.debugCr(reconciliation, "Verifying that Zookeeper is configured to run with {} replicas", zk.getReplicas());

        // No need to check for pod readiness since we run right after the readiness check
        return zkScaler(zk.getReplicas())
                .compose(zkScaler -> {
                    Promise<Void> scalingPromise = Promise.promise();

                    zkScaler.scale(zk.getReplicas()).onComplete(res -> {
                        zkScaler.close();

                        if (res.succeeded())    {
                            scalingPromise.complete();
                        } else {
                            LOGGER.warnCr(reconciliation, "Failed to verify Zookeeper configuration", res.cause());
                            scalingPromise.fail(res.cause());
                        }
                    });

                    return scalingPromise.future();
                });
    }

    /**
     * Waits for readiness of the endpoints of the clients service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> serviceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Waits for readiness of the endpoints of the headless service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> headlessServiceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperHeadlessServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Deletion of PVCs after the cluster is deleted is handled by owner reference and garbage collection. However,
     * this would not help after scale-downs. Therefore, we check if there are any PVCs which should not be present
     * and delete them when they are.
     *
     * This should be called only after the StrimziPodSet reconciliation, rolling update and scale-down when the PVCs
     * are not used any more by the pods.
     *
     * @return  Future which completes when the PVCs which should be deleted are deleted
     */
    protected Future<Void> deletePersistentClaims() {
        return pvcOperator.listAsync(reconciliation.namespace(), zk.getSelectorLabels())
                .compose(pvcs -> {
                    List<String> maybeDeletePvcs = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = zk.generatePersistentVolumeClaims().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                            .deletePersistentClaims(maybeDeletePvcs, desiredPvcs);
                });
    }

    /**
     * Defers to the Kafka metadata state manager to determine if there is a KRaft migration rollback ongoing and in such case,
     * it will delete the /controller znode to allow brokers to elect a new controller among them, now that KRaft
     * controllers are out of the picture.
     *
     * @return  Completes when the possible /controller znode deletion is done or no deletion is required
     */
    protected Future<Void> maybeDeleteControllerZnode() {
        return this.isKRaftMigrationRollback ? deleteControllerZnode() : Future.succeededFuture();
    }

    /**
     * Deletes the /controller znode to allow brokers to elect a new controller among them, now that KRaft
     * controllers are out of the picture.
     *
     * @return  Completes when the /controller znode deletion is done
     */
    protected Future<Void> deleteControllerZnode() {
        // migration rollback process ongoing
        String zkConnectionString = KafkaResources.zookeeperServiceName(reconciliation.name()) + ":" + ZookeeperCluster.CLIENT_TLS_PORT;
        KRaftMigrationUtils.deleteZooKeeperControllerZnode(
                reconciliation,
                this.tlsPkcs12Identity,
                operationTimeoutMs,
                zkConnectionString
        );
        return Future.succeededFuture();
    }
}
