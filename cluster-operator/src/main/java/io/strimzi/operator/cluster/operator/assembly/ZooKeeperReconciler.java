/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZooKeeperRoller;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScaler;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    private final FeatureGates featureGates;
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
    private final PodDisruptionBudgetV1Beta1Operator podDisruptionBudgetV1Beta1Operator;
    private final PodOperator podOperator;

    private final ZookeeperScalerProvider zooScalerProvider;
    private final ZookeeperLeaderFinder zooLeaderFinder;

    private final Integer currentReplicas;

    private final Set<String> fsResizingRestartRequest = new HashSet<>();
    private ReconcileResult<StatefulSet> statefulSetDiff;
    private ReconcileResult<StrimziPodSet> podSetDiff;
    private boolean existingCertsChanged = false;
    private String loggingHash = "";

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
            ClusterCa clusterCa
    ) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.zk = ZookeeperCluster.fromCrd(reconciliation, kafkaAssembly, config.versions(), oldStorage, currentReplicas);
        this.versionChange = versionChange;
        this.currentReplicas = currentReplicas;
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.pfa = pfa;
        this.featureGates = config.featureGates();
        this.adminSessionTimeoutMs = config.getZkAdminSessionTimeoutMs();
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();

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
        this.podDisruptionBudgetV1Beta1Operator = supplier.podDisruptionBudgetV1Beta1Operator;
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
                .compose(i -> jmxSecret())
                .compose(i -> manualPodCleaning())
                .compose(i -> networkPolicy())
                .compose(i -> manualRollingUpdate())
                .compose(i -> logVersionChange())
                .compose(i -> serviceAccount())
                .compose(i -> pvcs())
                .compose(i -> service())
                .compose(i -> headlessService())
                .compose(i -> certificateSecret(clock))
                .compose(i -> loggingAndMetricsConfigMap())
                .compose(i -> podDisruptionBudget())
                .compose(i -> podDisruptionBudgetV1Beta1())
                .compose(i -> migrateFromStatefulSetToPodSet())
                .compose(i -> migrateFromPodSetToStatefulSet())
                .compose(i -> statefulSet())
                .compose(i -> podSet())
                .compose(i -> scaleDown())
                .compose(i -> rollingUpdate())
                .compose(i -> podsReady())
                .compose(i -> scaleUp())
                .compose(i -> scalingCheck())
                .compose(i -> serviceEndpointsReady())
                .compose(i -> headlessServiceEndpointsReady())
                .compose(i -> deletePersistentClaims());
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
     * Manages the secret with JMX credentials when JMX is enabled
     *
     * @return  Completes when the JMX secret is successfully created or updated
     */
    protected Future<Void> jmxSecret() {
        return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperJmxSecretName(reconciliation.name()))
                .compose(currentJmxSecret -> {
                    Secret desiredJmxSecret = zk.generateJmxSecret(currentJmxSecret);

                    if (desiredJmxSecret != null)  {
                        // Desired secret is not null => should be updated
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperJmxSecretName(reconciliation.name()), desiredJmxSecret)
                                .map((Void) null);
                    } else if (currentJmxSecret != null)    {
                        // Desired secret is null but current is not => we should delete the secret
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperJmxSecretName(reconciliation.name()), null)
                                .map((Void) null);
                    } else {
                        // Both current and desired secret are null => nothing to do
                        return Future.succeededFuture();
                    }

                });
    }

    /**
     * Will check all Zookeeper pods whether the user requested the pod and PVC deletion through an annotation
     *
     * @return  Completes when the manual pod cleaning is done
     */
    protected Future<Void> manualPodCleaning() {
        return new ManualPodCleaner(
                reconciliation,
                KafkaResources.zookeeperStatefulSetName(reconciliation.name()),
                zk.getSelectorLabels(),
                operationTimeoutMs,
                featureGates.useStrimziPodSetsEnabled(),
                stsOperator,
                strimziPodSetOperator,
                podOperator,
                pvcOperator
        ).maybeManualPodCleaning(zk.generatePersistentVolumeClaims());
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
     * Does manual rolling update of Zoo pods based on an annotation on the StatefulSet or on the Pods. Annotation
     * on StatefulSet level triggers rolling update of all pods. Annotation on pods triggers rolling update only of
     * the selected pods. If the annotation is present on both StatefulSet and one or more pods, only one rolling
     * update of all pods occurs.
     *
     * @return  Future with the result of the rolling update
     */
    protected Future<Void> manualRollingUpdate() {
        Future<HasMetadata> futureController;
        if (featureGates.useStrimziPodSetsEnabled())   {
            futureController = strimziPodSetOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()))
                    .map(podSet -> podSet); // The .map(...) is required to convert to HasMetadata
        } else {
            futureController = stsOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()))
                    .map(sts -> sts); // The .map(...) is required to convert to HasMetadata
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
                        });
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
        }

        return Future.succeededFuture();
    }

    /**
     * Manages the ZooKeeper service account
     *
     * @return  Completes when the service account was successfully created or updated
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), zk.generateServiceAccount())
                .map((Void) null);
    }

    /**
     * Manages the PVCs needed by the ZooKeeper cluster. This method only creates or updates the PVCs. Deletion of PVCs
     * after scale-down happens only at the end of the reconciliation when they are not used anymore.
     *
     * @return  Completes when the PVCs were successfully created or updated
     */
    protected Future<Void> pvcs() {
        List<PersistentVolumeClaim> pvcs = zk.generatePersistentVolumeClaims();

        return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                .resizeAndReconcilePvcs(podIndex -> KafkaResources.zookeeperPodName(reconciliation.name(), podIndex), pvcs)
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
                                if (patchResult instanceof ReconcileResult.Patched) {
                                    // The secret is patched and some changes to the existing certificates actually occurred
                                    existingCertsChanged = ModelUtils.doExistingCertificatesDiffer(oldSecret, patchResult.resource());
                                } else {
                                    existingCertsChanged = false;
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
        return Util.metricsAndLogging(reconciliation, configMapOperator, reconciliation.namespace(), zk.getLogging(), zk.getMetricsConfigInCm())
                .compose(metricsAndLogging -> {
                    ConfigMap logAndMetricsConfigMap = zk.generateConfigurationConfigMap(metricsAndLogging);

                    loggingHash = Util.hashStub(logAndMetricsConfigMap.getData().get(zk.getAncillaryConfigMapKeyLogConfig()));

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
        if (!pfa.hasPodDisruptionBudgetV1()) {
            return Future.succeededFuture();
        } else {
            return podDisruptionBudgetOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), zk.generatePodDisruptionBudget(featureGates.useStrimziPodSetsEnabled()))
                    .map((Void) null);
        }
    }

    /**
     * Manages the PodDisruptionBudgets on Kubernetes clusters which support only v1beta1 version of PDBs
     *
     * @return  Completes when the PDB was successfully created or updated
     */
    protected Future<Void> podDisruptionBudgetV1Beta1() {
        if (pfa.hasPodDisruptionBudgetV1()) {
            return Future.succeededFuture();
        } else {
            return podDisruptionBudgetV1Beta1Operator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), zk.generatePodDisruptionBudgetV1Beta1(featureGates.useStrimziPodSetsEnabled()))
                    .map((Void) null);
        }
    }

    /**
     * Create or update the StatefulSet for the ZooKeeper cluster.
     *
     * @return  Future which completes when the StatefulSet is created, updated or deleted
     */
    protected Future<Void> statefulSet() {
        if (!featureGates.useStrimziPodSetsEnabled())   {
            // StatefulSets are enabled => make sure the StatefulSet exists with the right settings
            StatefulSet zkSts = zk.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            Annotations.annotations(zkSts.getSpec().getTemplate()).put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(ModelUtils.caCertGeneration(this.clusterCa)));
            Annotations.annotations(zkSts.getSpec().getTemplate()).put(Annotations.ANNO_STRIMZI_LOGGING_HASH, loggingHash);
            return stsOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), zkSts)
                    .compose(rr -> {
                        statefulSetDiff = rr;
                        return Future.succeededFuture();
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Helps with the migration from StatefulSets to StrimziPodSets when the cluster is switching between them. When the
     * switch happens, it deletes the old StatefulSet. It should happen before the new PodSet is created to
     * allow the controller hand-off.
     *
     * @return          Future which completes when the StatefulSet is deleted or does not need to be deleted
     */
    protected Future<Void> migrateFromStatefulSetToPodSet() {
        if (featureGates.useStrimziPodSetsEnabled())   {
            // StatefulSets are disabled => delete the StatefulSet if it exists
            return stsOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()))
                    .compose(sts -> {
                        if (sts != null)    {
                            return stsOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), false);
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        } else {
            return Future.succeededFuture();
        }
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
        if (featureGates.useStrimziPodSetsEnabled())   {
            // StrimziPodSets are enabled => make sure the StrimziPodSet exists
            Map<String, String> podAnnotations = new LinkedHashMap<>(2);
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(ModelUtils.caCertGeneration(this.clusterCa)));
            podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_HASH, loggingHash);

            StrimziPodSet zkPodSet = zk.generatePodSet(replicas, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, podAnnotations);
            return strimziPodSetOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), zkPodSet)
                    .compose(rr -> {
                        podSetDiff = rr;
                        return Future.succeededFuture();
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Helps with the migration from StrimziPodSets to StatefulSets when the cluster is switching between them. When the
     * switch happens, it deletes the old StrimziPodSet. It needs to happen before the StatefulSet is created to allow
     * the controller hand-off (STS will not accept pods with another controller in owner references).
     *
     * @return          Future which completes when the PodSet is deleted or does not need to be deleted
     */
    protected Future<Void> migrateFromPodSetToStatefulSet() {
        if (!featureGates.useStrimziPodSetsEnabled())   {
            // StrimziPodSets are disabled => delete the StrimziPodSet if it exists
            return strimziPodSetOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()))
                    .compose(podSet -> {
                        if (podSet != null)    {
                            return strimziPodSetOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), false);
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        } else {
            return Future.succeededFuture();
        }
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
        return ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
                .compose(compositeFuture -> {
                    Secret clusterCaCertSecret = compositeFuture.resultAt(0);
                    Secret coKeySecret = compositeFuture.resultAt(1);

                    Function<Integer, String> zkNodeAddress = (Integer i) ->
                            DnsNameGenerator.podDnsNameWithoutClusterDomain(reconciliation.namespace(), KafkaResources.zookeeperHeadlessServiceName(reconciliation.name()), KafkaResources.zookeeperPodName(reconciliation.name(), i));

                    ZookeeperScaler zkScaler = zooScalerProvider
                            .createZookeeperScaler(
                                    reconciliation,
                                    vertx,
                                    zkConnectionString(connectToReplicas, zkNodeAddress),
                                    zkNodeAddress,
                                    clusterCaCertSecret,
                                    coKeySecret,
                                    operationTimeoutMs,
                                    adminSessionTimeoutMs
                            );

                    return Future.succeededFuture(zkScaler);
                });
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
                    .compose(i -> scaleDownStatefulSetOrPodSet(current - 1))
                    .compose(i -> scaleDownByOne(zkScaler, current - 1, desired));
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Scales-down the ZooKeeper StatefulSet or PodSet, depending on what is used. This method only updates the
     * StatefulSet or PodSet, it does not handle the pods or ZooKeeper configuration.
     *
     * @return  Future which completes when StatefulSet or PodSet are scaled-down.
     */
    private Future<Void> scaleDownStatefulSetOrPodSet(int desiredScale)   {
        if (featureGates.useStrimziPodSetsEnabled())   {
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
        } else {
            return stsOperator.scaleDown(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), desiredScale)
                    .map((Void) null);
        }
    }

    /**
     * General method for rolling update of the ZooKeeper cluster. This method handles the whether StatefulSets or
     * PodSets are being used and calls other methods to execute the rolling update if needed.
     *
     * @return  Future which completes when any of the ZooKeeper pods which need rolling is rolled
     */
    protected Future<Void> rollingUpdate() {
        if (featureGates.useStrimziPodSetsEnabled())   {
            return maybeRollZooKeeper(pod -> ReconcilerUtils.reasonsToRestartPod(reconciliation, podSetDiff.resource(), pod, fsResizingRestartRequest, existingCertsChanged, clusterCa).getAllReasonNotes());
        } else {
            return maybeRollZooKeeper(pod -> ReconcilerUtils.reasonsToRestartPod(reconciliation, statefulSetDiff.resource(), pod, fsResizingRestartRequest, existingCertsChanged, clusterCa).getAllReasonNotes());
        }
    }

    /**
     * Checks if the ZooKeeper cluster needs rolling and if it does, it will roll it.
     *
     * @param podNeedsRestart   Function to determine if the ZooKeeper pod needs to be restarted
     *
     * @return                  Future which completes when any of the ZooKeeper pods which need rolling is rolled
     */
    /* test */ Future<Void> maybeRollZooKeeper(Function<Pod, List<String>> podNeedsRestart) {
        return ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
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
     * @return                      Future which completes when any of the ZooKeeper pods which need rolling is rolled
     */
    private Future<Void> maybeRollZooKeeper(Function<Pod, List<String>> podNeedsRestart, Secret clusterCaCertSecret, Secret coKeySecret) {
        return new ZooKeeperRoller(podOperator, zooLeaderFinder, operationTimeoutMs)
                .maybeRollingUpdate(reconciliation, zk.getSelectorLabels(), podNeedsRestart, clusterCaCertSecret, coKeySecret);
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
     * @return  Future which completes ZooKeeper scale-up is complete
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
            return zkScaleUpStatefulSetOrPodSet(current + 1)
                    .compose(ignore -> podOperator.readiness(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperPodName(reconciliation.name(), current), 1_000, operationTimeoutMs))
                    .compose(ignore -> zkScaler.scale(current + 1))
                    .compose(ignore -> scaleUpByOne(zkScaler, current + 1, desired));
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Scales-up the ZooKeeper StatefulSet or PodSet, depending on what is used. This method only updates the
     * StatefulSet or PodSet, it does not handle the pods or ZooKeeper configuration.
     *
     * @return  Future which completes when StatefulSet or PodSet are scaled-up.
     */
    private Future<Void> zkScaleUpStatefulSetOrPodSet(int desiredScale)   {
        if (featureGates.useStrimziPodSetsEnabled())   {
            return podSet(desiredScale);
        } else {
            return stsOperator.scaleUp(reconciliation, reconciliation.namespace(), KafkaResources.zookeeperStatefulSetName(reconciliation.name()), desiredScale)
                    .map((Void) null);
        }
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
     * This should be called only after the StatefulSet reconciliation, rolling update and scale-down when the PVCs
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
}
