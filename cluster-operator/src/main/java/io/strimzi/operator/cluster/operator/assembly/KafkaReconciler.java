/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.StorageDiff;
import io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;

/**
 * Class used for reconciliation of Kafka. This class contains both the steps of the Kafka
 * reconciliation pipeline (although the steps for listener reconciliation are outsourced to the KafkaListenerReconciler)
 * and is also used to store the state between them.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class KafkaReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaReconciler.class.getName());

    /* test */ final Reconciliation reconciliation;
    private final Vertx vertx;
    private final long operationTimeoutMs;
    /* test */ final KafkaCluster kafka;
    private final Storage oldStorage;
    private final ClusterCa clusterCa;
    private final ClientsCa clientsCa;
    private final List<String> maintenanceWindows;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final boolean isNetworkPolicyGeneration;
    /* test */ final PlatformFeaturesAvailability pfa;
    private final FeatureGates featureGates;
    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;

    private final StatefulSetOperator stsOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    /* test */ final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    /* test */ final ServiceOperator serviceOperator;
    private final PvcOperator pvcOperator;
    private final StorageClassOperator storageClassOperator;
    private final ConfigMapOperator configMapOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    private final PodDisruptionBudgetV1Beta1Operator podDisruptionBudgetV1Beta1Operator;
    private final PodOperator podOperator;
    private final ClusterRoleBindingOperator clusterRoleBindingOperator;
    /* test */ final RouteOperator routeOperator;
    /* test */ final IngressOperator ingressOperator;
    private final NodeOperator nodeOperator;

    private final KubernetesRestartEventPublisher eventsPublisher;

    private final AdminClientProvider adminClientProvider;

    private final int currentReplicas;

    private final Set<String> fsResizingRestartRequest = new HashSet<>();
    private ReconcileResult<StatefulSet> statefulSetDiff;
    private ReconcileResult<StrimziPodSet> podSetDiff;
    private boolean existingCertsChanged = false;
    private String logging = "";
    private String loggingHash = "";
    private final Map<Integer, String> brokerConfigurationHash = new HashMap<>();
    private final Map<Integer, String> kafkaServerCertificateHash = new HashMap<>();
    @SuppressFBWarnings(value = "SS_SHOULD_BE_STATIC", justification = "Field cannot be static in inner class in Java 11")
    private final int sharedConfigurationId = -1; // "Fake" broker ID used to indicate hash stored for all brokers when shared configuration is used

    // Result of the listener reconciliation with the listener details
    /* test */ KafkaListenersReconciler.ReconciliationResult listenerReconciliationResults;

    /**
     * Constructs the Kafka reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param kafkaCr                   The Kafka custom resource
     * @param oldStorage                The storage configuration of the current cluster (null if it does not exist yet)
     * @param currentReplicas           The current number of replicas
     * @param clusterCa                 The Cluster CA instance
     * @param clientsCa                 The Clients CA instance
     * @param versionChange             Description of Kafka upgrade / downgrade state
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param pfa                       PlatformFeaturesAvailability describing the environment we run in
     * @param vertx                     Vert.x instance
     */
    public KafkaReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            Storage oldStorage,
            int currentReplicas,
            ClusterCa clusterCa,
            ClientsCa clientsCa,
            KafkaVersionChange versionChange,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            PlatformFeaturesAvailability pfa,
            Vertx vertx
    ) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.kafka = KafkaCluster.fromCrd(reconciliation, kafkaCr, config.versions(), oldStorage, currentReplicas, config.featureGates().useKRaftEnabled());

        // We set the user-configured inter.broker.protocol.version if needed (when not set by the user)
        if (versionChange.interBrokerProtocolVersion() != null) {
            this.kafka.setInterBrokerProtocolVersion(versionChange.interBrokerProtocolVersion());
        }

        // We set the user-configured log.message.format.version if needed (when not set by the user)
        if (versionChange.logMessageFormatVersion() != null) {
            this.kafka.setLogMessageFormatVersion(versionChange.logMessageFormatVersion());
        }

        this.oldStorage = oldStorage;
        this.currentReplicas = currentReplicas;
        this.clusterCa = clusterCa;
        this.clientsCa = clientsCa;
        this.maintenanceWindows = kafkaCr.getSpec().getMaintenanceTimeWindows();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.pfa = pfa;
        this.featureGates = config.featureGates();
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
        this.clusterRoleBindingOperator = supplier.clusterRoleBindingOperator;
        this.routeOperator = supplier.routeOperations;
        this.ingressOperator = supplier.ingressOperations;
        this.nodeOperator = supplier.nodeOperator;
        this.eventsPublisher = supplier.restartEventsPublisher;

        this.adminClientProvider = supplier.adminClientProvider;
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
                .compose(i -> manualPodCleaning())
                .compose(i -> networkPolicy())
                .compose(i -> manualRollingUpdate())
                .compose(i -> pvcs())
                .compose(i -> serviceAccount())
                .compose(i -> initClusterRoleBinding())
                .compose(i -> scaleDown())
                .compose(i -> listeners())
                .compose(i -> certificateSecret(clock))
                .compose(i -> brokerConfigurationConfigMaps())
                .compose(i -> jmxSecret())
                .compose(i -> podDisruptionBudget())
                .compose(i -> podDisruptionBudgetV1Beta1())
                .compose(i -> migrateFromStatefulSetToPodSet())
                .compose(i -> migrateFromPodSetToStatefulSet())
                .compose(i -> statefulSet())
                .compose(i -> podSet())
                .compose(i -> rollToAddOrRemoveVolumes())
                .compose(i -> rollingUpdate())
                .compose(i -> scaleUp())
                .compose(i -> podsReady())
                .compose(i -> serviceEndpointsReady())
                .compose(i -> headlessServiceEndpointsReady())
                .compose(i -> clusterId(kafkaStatus))
                .compose(i -> deletePersistentClaims())
                .compose(i -> brokerConfigurationConfigMapsCleanup())
                // This has to run after all possible rolling updates which might move the pods to different nodes
                .compose(i -> nodePortExternalListenerStatus())
                .compose(i -> addListenersToKafkaStatus(kafkaStatus));
    }

    /**
     * Takes the warning conditions from the Model and adds them in the KafkaStatus
     *
     * @param kafkaStatus   The Kafka Status where the warning conditions will be added
     *
     * @return              Completes when the warnings are added to the status object
     */
    protected Future<Void> modelWarnings(KafkaStatus kafkaStatus) {
        List<Condition> conditions = kafka.getWarningConditions();

        if (!featureGates.useStrimziPodSetsEnabled())   {
            conditions.add(StatusUtils.buildWarningCondition("StatefulSetRemoval",
                    "Support for StatefulSets will be removed in Strimzi 0.35. You should consider migrating to StrimziPodSets."));
            LOGGER.warnCr(reconciliation, "Support for StatefulSets will be removed in Strimzi 0.35. You should consider migrating to StrimziPodSets.");
        }

        kafkaStatus.addConditions(conditions);

        return Future.succeededFuture();
    }

    /**
     * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
     *
     * @return  Completes when the manual pod cleaning is done
     */
    protected Future<Void> manualPodCleaning() {
        return new ManualPodCleaner(
                reconciliation,
                KafkaResources.kafkaStatefulSetName(reconciliation.name()),
                kafka.getSelectorLabels(),
                operationTimeoutMs,
                featureGates.useStrimziPodSetsEnabled(),
                stsOperator,
                strimziPodSetOperator,
                podOperator,
                pvcOperator
        ).maybeManualPodCleaning(kafka.generatePersistentVolumeClaims(oldStorage));
    }

    /**
     * Manages the network policy protecting the Kafka cluster
     *
     * @return  Completes when the network policy is successfully created or updated
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaNetworkPolicyName(reconciliation.name()), kafka.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels))
                    .map((Void) null);
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Does manual rolling update of Kafka pods based on an annotation on the StatefulSet or on the Pods. Annotation
     * on StatefulSet level triggers rolling update of all pods. Annotation on pods triggers rolling update only of
     * the selected pods. If the annotation is present on both StatefulSet and one or more pods, only one rolling
     * update of all pods occurs.
     *
     * @return  Future with the result of the rolling update
     */
    protected Future<Void> manualRollingUpdate() {
        Future<HasMetadata> futureController;
        if (featureGates.useStrimziPodSetsEnabled())   {
            futureController = strimziPodSetOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()))
                    .map(podSet -> podSet);  // The .map(...) is required to convert to HasMetadata
        } else {
            futureController = stsOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()))
                    .map(sts -> sts);  // The .map(...) is required to convert to HasMetadata
        }

        return futureController.compose(controller -> {
            if (controller != null) {
                if (Annotations.booleanAnnotation(controller, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                    // User trigger rolling update of the whole cluster
                    return maybeRollKafka(kafka.getReplicas(), pod -> {
                        if (pod == null) {
                            throw new ConcurrentDeletionException("Unexpectedly pod no longer exists during roll of StatefulSet.");
                        }
                        LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation",
                                pod.getMetadata().getName());
                        return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                    }, Map.of(), Map.of(), false);
                } else {
                    // The controller is not annotated to roll all pods.
                    // We should check if maybe the individual pods are annotated to restart only some of them.
                    return kafkaManualPodRollingUpdate();
                }
            } else {
                // Controller does not exist => nothing to roll
                return Future.succeededFuture();
            }
        });
    }

    /**
     * Does rolling update of Kafka pods based on the annotation on Pod level
     *
     * @return  Future with the result of the rolling update
     */
    protected Future<Void> kafkaManualPodRollingUpdate() {
        return podOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(pods -> {
                    List<String> podsToRoll = new ArrayList<>(0);

                    for (Pod pod : pods)    {
                        if (Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            podsToRoll.add(pod.getMetadata().getName());
                        }
                    }

                    if (!podsToRoll.isEmpty())  {
                        return maybeRollKafka(kafka.getReplicas(), pod -> {
                            if (pod != null && podsToRoll.contains(pod.getMetadata().getName())) {
                                LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation on a pod", pod.getMetadata().getName());
                                return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                            } else {
                                return RestartReasons.empty();
                            }
                        }, Map.of(), Map.of(), false);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Rolls Kafka pods if needed
     *
     * @param replicas                  Number of Kafka replicas which should be considered for rolling
     * @param podNeedsRestart           Function which serves as a predicate whether to roll pod or not
     * @param kafkaAdvertisedHostnames  Map with advertised hostnames required to generate the per-broker configuration
     * @param kafkaAdvertisedPorts      Map with advertised ports required to generate the per-broker configuration
     * @param allowReconfiguration      Defines whether the rolling update should also attempt to do dynamic reconfiguration or not
     *
     * @return  Future which completes when the rolling is complete
     */
    protected Future<Void> maybeRollKafka(
            int replicas,
            Function<Pod, RestartReasons> podNeedsRestart,
            Map<Integer, Map<String, String>> kafkaAdvertisedHostnames,
            Map<Integer, Map<String, String>> kafkaAdvertisedPorts,
            boolean allowReconfiguration
    ) {
        return ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
                .compose(compositeFuture ->
                        new KafkaRoller(
                                reconciliation,
                                vertx,
                                podOperator,
                                1_000,
                                operationTimeoutMs,
                                () -> new BackOff(250, 2, 10),
                                KafkaCluster.generatePodList(reconciliation.name(), replicas),
                                compositeFuture.resultAt(0),
                                compositeFuture.resultAt(1),
                                adminClientProvider,
                                brokerId -> {
                                    if (featureGates.useStrimziPodSetsEnabled()) {
                                        return kafka.generatePerBrokerBrokerConfiguration(brokerId, kafkaAdvertisedHostnames, kafkaAdvertisedPorts);
                                    } else {
                                        return kafka.generateSharedBrokerConfiguration();
                                    }
                                },
                                logging,
                                kafka.getKafkaVersion(),
                                allowReconfiguration,
                                eventsPublisher

                        ).rollingRestart(podNeedsRestart));
    }

    /**
     * Manages the PVCs needed by the Kafka cluster. This method only creates or updates the PVCs. Deletion of PVCs
     * after scale-down happens only at the end of the reconciliation when they are not used anymore.
     *
     * @return  Completes when the PVCs were successfully created or updated
     */
    protected Future<Void> pvcs() {
        List<PersistentVolumeClaim> pvcs = kafka.generatePersistentVolumeClaims(kafka.getStorage());

        return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                .resizeAndReconcilePvcs(podIndex -> KafkaResources.kafkaPodName(reconciliation.name(), podIndex), pvcs)
                .compose(podsToRestart -> {
                    fsResizingRestartRequest.addAll(podsToRestart);
                    return Future.succeededFuture();
                });
    }

    /**
     * Manages the Kafka service account
     *
     * @return  Completes when the service account was successfully created or updated
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafka.generateServiceAccount())
                .map((Void) null);
    }

    /**
     * Manages the Kafka cluster role binding. When the desired Cluster Role Binding is null, and we get an RBAC error,
     * we ignore it. This is to allow users to run the operator only inside a namespace when no features requiring
     * Cluster Role Bindings are needed.
     *
     * @return  Completes when the Cluster Role Binding was successfully created or updated
     */
    protected Future<Void> initClusterRoleBinding() {
        ClusterRoleBinding desired = kafka.generateClusterRoleBinding(reconciliation.namespace());

        return ReconcilerUtils.withIgnoreRbacError(
                reconciliation,
                clusterRoleBindingOperator
                        .reconcile(
                                reconciliation,
                                KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()),
                                desired
                        ),
                desired
        ).map((Void) null);
    }

    /**
     * Scales down the Kafka cluster if needed. Kafka scale-down is done in one go.
     *
     * @return  Future which completes when the scale-down is finished
     */
    protected Future<Void> scaleDown() {
        if (currentReplicas != 0
                && currentReplicas > kafka.getReplicas()) {
            // The previous (current) number of replicas is bigger than desired => we should scale-down
            LOGGER.infoCr(reconciliation, "Scaling Kafka down from {} to {} replicas", currentReplicas, kafka.getReplicas());

            if (featureGates.useStrimziPodSetsEnabled())   {
                Set<String> desiredPodNames = new HashSet<>(kafka.getReplicas());
                for (int i = 0; i < kafka.getReplicas(); i++) {
                    desiredPodNames.add(kafka.getPodName(i));
                }

                return strimziPodSetOperator.getAsync(reconciliation.namespace(), kafka.getComponentName())
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

                                return strimziPodSetOperator
                                        .reconcile(reconciliation, reconciliation.namespace(), kafka.getComponentName(), scaledDownPodSet)
                                        .map((Void) null);
                            }
                        });
            } else {
                return stsOperator
                        .scaleDown(reconciliation, reconciliation.namespace(), kafka.getComponentName(), kafka.getReplicas())
                        .map((Void) null);
            }
        } else {
            // Previous replica count is unknown (because the PodSet / StatefulSet did not exist) or is smaller or equal to
            // desired replicas => no need to scale-down
            return Future.succeededFuture();
        }
    }

    /**
     * Utility method to create the Kafka Listener reconciler. It can be also use to inject mocked reconciler during
     * tests.
     *
     * @return  KafkaListenerReconciler instance
     */
    protected KafkaListenersReconciler listenerReconciler()   {
        return new KafkaListenersReconciler(
                reconciliation,
                kafka,
                clusterCa,
                pfa,
                operationTimeoutMs,
                secretOperator,
                serviceOperator,
                routeOperator,
                ingressOperator
        );
    }

    /**
     * Reconciles listeners of this Kafka cluster
     *
     * @return  Future which completes when listeners are reconciled
     */
    protected Future<Void> listeners()    {
        return listenerReconciler()
                .reconcile()
                .compose(result -> {
                    listenerReconciliationResults = result;
                    return Future.succeededFuture();
                });
    }

    /**
     * Generates and creates the ConfigMap with shared configuration for Kafka brokers used in StatefulSets.
     *
     * @param metricsAndLogging     Metrics and Logging configuration
     *
     * @return  Future which completes when the Kafka Configuration is prepared
     */
    protected Future<Void> sharedKafkaConfiguration(MetricsAndLogging metricsAndLogging) {
        ConfigMap sharedCm = kafka.generateSharedConfigurationConfigMap(metricsAndLogging, listenerReconciliationResults.advertisedHostnames, listenerReconciliationResults.advertisedPorts);

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
        this.brokerConfigurationHash.put(sharedConfigurationId, Util.hashStub(brokerConfiguration + kc.unknownConfigsWithValues(kafka.getKafkaVersion()).toString()));

        // This is used during Kafka rolling updates -> we have to store it for later
        this.logging = sharedCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
        this.loggingHash = Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging));

        return configMapOperator
                .reconcile(reconciliation, reconciliation.namespace(), kafka.getAncillaryConfigMapName(), sharedCm)
                .map((Void) null);
    }

    /**
     * Utility method to extract pod index number from pod name
     *
     * @param podName   Name of the pod
     *
     * @return          Index of the pod
     */
    private static int getPodIndexFromPodName(String podName)  {
        return Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
    }

    /**
     * Generates and creates the ConfigMaps with per-broker configuration for Kafka brokers used in PodSets. It will
     * also delete the ConfigMaps for any scaled-down brokers (scale down is done before this is called in the
     * reconciliation)
     *
     * @param metricsAndLogging     Metrics and Logging configuration
     *
     * @return  Future which completes when the Kafka Configuration is prepared
     */
    protected Future<Void> perBrokerKafkaConfiguration(MetricsAndLogging metricsAndLogging) {
        return configMapOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(existingConfigMaps -> {
                    // This is used during Kafka rolling updates -> we have to store it for later
                    this.logging = kafka.loggingConfiguration(metricsAndLogging.getLoggingCm());
                    this.loggingHash = Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging));

                    List<ConfigMap> desiredConfigMaps = kafka.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, listenerReconciliationResults.advertisedHostnames, listenerReconciliationResults.advertisedPorts);
                    @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                    List<Future> ops = new ArrayList<>(existingConfigMaps.size() + kafka.getReplicas());

                    // Delete all existing ConfigMaps which are not desired and are not the shared config map
                    List<String> desiredNames = new ArrayList<>(desiredConfigMaps.size() + 1);
                    desiredNames.add(kafka.getAncillaryConfigMapName()); // We do not want to delete the shared ConfigMap, so we add it here
                    desiredNames.addAll(desiredConfigMaps.stream().map(cm -> cm.getMetadata().getName()).toList());

                    for (ConfigMap cm : existingConfigMaps) {
                        // We delete the cms not on the desired names list
                        if (!desiredNames.contains(cm.getMetadata().getName())) {
                            ops.add(configMapOperator.deleteAsync(reconciliation, reconciliation.namespace(), cm.getMetadata().getName(), true));
                        }
                    }

                    // Create / update the desired config maps
                    for (ConfigMap cm : desiredConfigMaps) {
                        String cmName = cm.getMetadata().getName();
                        int brokerId = getPodIndexFromPodName(cmName);

                        // The advertised hostname and port might change. If they change, we need to roll the pods.
                        // Here we collect their hash to trigger the rolling update. For per-broker configuration,
                        // we need just the advertised hostnames / ports for given broker.
                        String brokerConfiguration = listenerReconciliationResults.advertisedHostnames
                                .get(brokerId)
                                .entrySet()
                                .stream()
                                .map(kv -> kv.getKey() + "://" + kv.getValue())
                                .sorted()
                                .collect(Collectors.joining(" "));
                        brokerConfiguration += listenerReconciliationResults.advertisedPorts
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
                        this.brokerConfigurationHash.put(brokerId, Util.hashStub(brokerConfiguration + kc.unknownConfigsWithValues(kafka.getKafkaVersion()).toString()));

                        ops.add(configMapOperator.reconcile(reconciliation, reconciliation.namespace(), cmName, cm));
                    }

                    return CompositeFuture
                            .join(ops)
                            .map((Void) null);
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
     * @return  Future which completes when the Config Map(s) with configuration are created or updated
     */
    protected Future<Void> brokerConfigurationConfigMaps() {
        return Util.metricsAndLogging(reconciliation, configMapOperator, reconciliation.namespace(), kafka.getLogging(), kafka.getMetricsConfigInCm())
                .compose(metricsAndLoggingCm -> {
                    if (featureGates.useStrimziPodSetsEnabled())    {
                        return perBrokerKafkaConfiguration(metricsAndLoggingCm);
                    } else {
                        return sharedKafkaConfiguration(metricsAndLoggingCm);
                    }
                });
    }

    /**
     * Manages the Secret with the node certificates used by the Kafka brokers.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Completes when the Secret was successfully created or updated
     */
    protected Future<Void> certificateSecret(Clock clock) {
        return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaSecretName(reconciliation.name()))
                .compose(oldSecret -> {
                    return secretOperator
                            .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaSecretName(reconciliation.name()),
                                    kafka.generateCertificatesSecret(clusterCa, clientsCa, listenerReconciliationResults.bootstrapDnsNames, listenerReconciliationResults.brokerDnsNames, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant())))
                            .compose(patchResult -> {
                                if (patchResult != null) {
                                    existingCertsChanged = false;
                                    if (patchResult instanceof ReconcileResult.Patched) {
                                        // The secret is patched and some changes to the existing certificates actually occurred
                                        existingCertsChanged = ModelUtils.doExistingCertificatesDiffer(oldSecret, patchResult.resource());
                                    }
                                    IntStream.range(0, kafka.getReplicas())
                                            .forEach(brokerId -> {
                                                var podName = KafkaResources.kafkaPodName(reconciliation.name(), brokerId);
                                                kafkaServerCertificateHash.put(
                                                        brokerId,
                                                        CertUtils.getCertificateThumbprint(patchResult.resource(),
                                                                Ca.secretEntryNameForPod(podName, Ca.SecretEntry.CRT)
                                                        ));
                                            });
                                }
                                return Future.succeededFuture();
                            });
                });
    }

    /**
     * Manages the secret with JMX credentials when JMX is enabled
     *
     * @return  Completes when the JMX secret is successfully created or updated
     */
    protected Future<Void> jmxSecret() {
        return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaJmxSecretName(reconciliation.name()))
                .compose(currentJmxSecret -> {
                    Secret desiredJmxSecret = kafka.generateJmxSecret(currentJmxSecret);

                    if (desiredJmxSecret != null)  {
                        // Desired secret is not null => should be updated
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaJmxSecretName(reconciliation.name()), desiredJmxSecret)
                                .map((Void) null);
                    } else if (currentJmxSecret != null)    {
                        // Desired secret is null but current is not => we should delete the secret
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaJmxSecretName(reconciliation.name()), null)
                                .map((Void) null);
                    } else {
                        // Both current and desired secret are null => nothing to do
                        return Future.succeededFuture();
                    }

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
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafka.generatePodDisruptionBudget(featureGates.useStrimziPodSetsEnabled()))
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
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafka.generatePodDisruptionBudgetV1Beta1(featureGates.useStrimziPodSetsEnabled()))
                    .map((Void) null);
        }
    }

    /**
     * Prepares annotations for Kafka pods within a StrimziPodSet which are known only in the KafkaAssemblyOperator level.
     * These are later passed to KafkaCluster where there are used when creating the Pod definitions.
     *
     * @param brokerId ID of the broker, the annotations of which are being prepared.
     * @return Map with Pod annotations
     */
    private Map<String, String> podSetPodAnnotations(int brokerId) {
        Map<String, String> podAnnotations = commonKafkaPodAnnotations(brokerId);

        // Annotation of broker certificate hash
        podAnnotations.put(KafkaCluster.ANNO_STRIMZI_SERVER_CERT_HASH, kafkaServerCertificateHash.get(brokerId));

        return podAnnotations;
    }

    /**
     * Prepares annotations for Kafka pods within a StatefulSet which are known only in the KafkaAssemblyOperator level.
     * These are later passed to KafkaCluster where there are used when creating the Pod definitions.
     *
     * @return Map with Pod annotations
     */
    private Map<String, String> statefulSetPodAnnotations() {
        Map<String, String> podAnnotations = commonKafkaPodAnnotations(sharedConfigurationId);

        // Storage annotation on Pods is used only for StatefulSets
        podAnnotations.put(Annotations.ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(kafka.getStorage()));

        return podAnnotations;
    }

    /**
     * Prepares the Kafka pods' annotations that are common between StatefulSets and StrimziPodSets.
     *
     * @param brokerId ID of the broker, the annotations of which are being prepared.
     * @return Map with Pod annotations
     */
    private Map<String, String> commonKafkaPodAnnotations(int brokerId) {
        Map<String, String> podAnnotations = new LinkedHashMap<>(9);
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(ModelUtils.caCertGeneration(this.clusterCa)));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, String.valueOf(ModelUtils.caCertGeneration(this.clientsCa)));
        podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, loggingHash);
        podAnnotations.put(KafkaCluster.ANNO_STRIMZI_BROKER_CONFIGURATION_HASH, brokerConfigurationHash.get(brokerId));
        podAnnotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, kafka.getKafkaVersion().version());
        podAnnotations.put(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, kafka.getLogMessageFormatVersion());
        podAnnotations.put(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, kafka.getInterBrokerProtocolVersion());

        // Annotations with custom cert thumbprints to help with rolling updates when they change
        if (!listenerReconciliationResults.customListenerCertificateThumbprints.isEmpty()) {
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, listenerReconciliationResults.customListenerCertificateThumbprints.toString());
        }

        return podAnnotations;
    }

    /**
     * Create or update the StatefulSet for the Kafka cluster.
     *
     * @return  Future which completes when the StatefulSet is created, updated or deleted
     */
    protected Future<Void> statefulSet() {
        if (!featureGates.useStrimziPodSetsEnabled()) {
            // StatefulSets are enabled => make sure the StatefulSet exists with the right settings
            StatefulSet kafkaSts = kafka.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, statefulSetPodAnnotations());
            return stsOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafkaSts)
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
            return stsOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()))
                    .compose(sts -> {
                        if (sts != null)    {
                            return stsOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), false);
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Create or update the StrimziPodSet for the Kafka cluster. If set, it uses the old replica count since scaling-up
     * happens only later in a separate step.
     *
     * @return  Future which completes when the PodSet is created, updated or deleted
     */
    protected Future<Void> podSet() {
        if (featureGates.useStrimziPodSetsEnabled())   {
            // PodSets are enabled => create/update the StrimziPodSet for Kafka
            int replicas;

            if (currentReplicas != 0
                    && currentReplicas < kafka.getReplicas())  {
                // If there is previous replica count & it is smaller than the desired replica count, we use the
                // previous one because the scale-up will happen only later during the reconciliation
                replicas = currentReplicas;
            } else {
                // If there is no previous number of replicas (because the PodSet did not exist) or if the
                // previous replicas are bigger than desired replicas we use desired replicas (scale-down already
                // happened)
                replicas = kafka.getReplicas();
            }

            StrimziPodSet kafkaPodSet = kafka.generatePodSet(replicas, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, this::podSetPodAnnotations);
            return strimziPodSetOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafkaPodSet)
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
            // PodSets are disabled => delete the StrimziPodSet for Kafka
            return strimziPodSetOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()))
                    .compose(podSet -> {
                        if (podSet != null)    {
                            return strimziPodSetOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), false);
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Checks if any Kafka broker needs rolling update to add or remove JBOD volumes. If it does, we trigger a
     * sequential rolling update because the pods need to be rolled in sequence to add or remove volumes.
     *
     * @return  Future which completes when all added or removed JBOD volumes are handled
     */
    protected Future<Void> rollToAddOrRemoveVolumes() {
        Storage storage = kafka.getStorage();

        // We do the special rolling update only when:
        //   * JBOD storage is actually used as storage
        //   * and StatefulSets are used
        // StrimziPodSets do not need special rolling update, they can add / remove volumes during regular rolling updates
        if (storage instanceof JbodStorage jbodStorage
                && !featureGates.useStrimziPodSetsEnabled()) {
            return kafkaRollToAddOrRemoveVolumesInStatefulSet(jbodStorage);
        } else {
            return Future.succeededFuture();
        }
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
    private RestartReasons needsRestartBecauseAddedOrRemovedJbodVolumes(Pod pod, JbodStorage desiredStorage, int currentReplicas, int desiredReplicas)  {
        if (pod != null
                && pod.getMetadata() != null) {
            String jsonStorage = Annotations.stringAnnotation(pod, Annotations.ANNO_STRIMZI_IO_STORAGE, null);

            if (jsonStorage != null) {
                Storage currentStorage = ModelUtils.decodeStorageFromJson(jsonStorage);

                if (new StorageDiff(reconciliation, currentStorage, desiredStorage, currentReplicas, desiredReplicas).isVolumesAddedOrRemoved())    {
                    return RestartReasons.of(RestartReason.JBOD_VOLUMES_CHANGED);
                }
            }
        }

        return RestartReasons.empty();
    }

    /**
     * Normally, the rolling update of Kafka brokers is done in the sequence best for Kafka (controller is rolled
     * last etc.). This method does a rolling update of Kafka brokers in sequence based on the pod index number.
     * This is needed in some special situation such as storage configuration changes.
     *
     * This method is using the regular rolling update mechanism. In order to achieve the sequential rolling
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
     * @return  Future which completes when the rolling update of all brokers in sequence is complete
     */
    protected Future<Void> maybeRollKafkaInSequence(StatefulSet sts, Function<Pod, RestartReasons> podNeedsRestart, int nextPod, int lastPod) {
        if (nextPod <= lastPod)  {
            final int podToRoll = nextPod;

            return maybeRollKafka(sts.getSpec().getReplicas(), pod -> {
                if (pod != null && pod.getMetadata().getName().endsWith("-" + podToRoll))    {
                    return podNeedsRestart.apply(pod);
                } else {
                    return RestartReasons.empty();
                }
            }, Map.of(), Map.of(), false)
                    .compose(ignore -> maybeRollKafkaInSequence(sts, podNeedsRestart, nextPod + 1, lastPod));
        } else {
            // All pods checked for sequential RU => nothing more to do
            return Future.succeededFuture();
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
    protected Future<Void> kafkaRollToAddOrRemoveVolumesInStatefulSet(JbodStorage jbodStorage) {
        // We first check if any broker actually needs the rolling update. Only if at least one of them needs it,
        // we trigger it. This check helps to not go through the rolling update if not needed.
        return podOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(pods -> {
                    for (Pod pod : pods) {
                        if (needsRestartBecauseAddedOrRemovedJbodVolumes(pod, jbodStorage, currentReplicas, kafka.getReplicas()).shouldRestart())   {
                            // At least one broker needs rolling update => we can trigger it without checking the other brokers
                            LOGGER.debugCr(reconciliation, "Kafka brokers needs rolling update to add or remove JBOD volumes");

                            return stsOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()))
                                    .compose(sts -> {
                                        if (sts != null) {
                                            int lastPodIndex = Math.min(currentReplicas, kafka.getReplicas()) - 1;
                                            return maybeRollKafkaInSequence(sts, podToCheck -> needsRestartBecauseAddedOrRemovedJbodVolumes(podToCheck, jbodStorage, currentReplicas, kafka.getReplicas()), 0, lastPodIndex);
                                        } else {
                                            // STS does not exist => nothing to roll
                                            return Future.succeededFuture();
                                        }
                                    });
                        }
                    }

                    LOGGER.debugCr(reconciliation, "No rolling update of Kafka brokers due to added or removed JBOD volumes is needed");
                    return Future.succeededFuture();
                });
    }

    /**
     * Roles the Kafka brokers (if needed). This method handles the whether StatefulSets or PodSets are being used and
     * calls other methods to execute the rolling update if needed.
     *
     * @return  Future which completes when any of the Kafka pods which need rolling is rolled
     */
    protected Future<Void> rollingUpdate() {
        if (featureGates.useStrimziPodSetsEnabled())   {
            return maybeRollKafka(
                    podSetDiff.resource().getSpec().getPods().size(),
                    pod -> ReconcilerUtils.reasonsToRestartPod(reconciliation, podSetDiff.resource(), pod, fsResizingRestartRequest, existingCertsChanged, clusterCa, clientsCa),
                    listenerReconciliationResults.advertisedHostnames,
                    listenerReconciliationResults.advertisedPorts,
                    true
            );
        } else {
            return maybeRollKafka(
                    statefulSetDiff.resource().getSpec().getReplicas(),
                    pod -> ReconcilerUtils.reasonsToRestartPod(reconciliation, statefulSetDiff.resource(), pod, fsResizingRestartRequest, existingCertsChanged, clusterCa, clientsCa),
                    listenerReconciliationResults.advertisedHostnames,
                    listenerReconciliationResults.advertisedPorts,
                    true
            );
        }
    }

    /**
     * Scales-up the Kafka cluster if needed. When scaling uo the Kafka cluster, all new brokers are created in one go.
     * This method does not wait for the readiness of the new pods, it just scales the controller resource.
     *
     * @return  Future which completes when the StrimziPodSet or StatefulSet were scaled up
     */
    protected Future<Void> scaleUp() {
        if (currentReplicas != 0
                && currentReplicas < kafka.getReplicas()) {
            // The previous number of replicas is known and is smaller than desired number of replicas
            //   => we need to do scale-up
            LOGGER.infoCr(reconciliation, "Scaling Kafka up from {} to {} replicas", currentReplicas, kafka.getReplicas());

            if (featureGates.useStrimziPodSetsEnabled()) {
                StrimziPodSet kafkaPodSet = kafka.generatePodSet(kafka.getReplicas(), pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, this::podSetPodAnnotations);
                return strimziPodSetOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafkaPodSet)
                        .map((Void) null);
            } else {
                return stsOperator.scaleUp(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafka.getReplicas())
                        .map((Void) null);
            }
        } else {
            // Previous number of replicas is not known (because the PodSet os StatefulSet  did not exist) or is
            // bigger than desired replicas => nothing to do
            // (if the previous replica count was not known, the desired count was already used when patching
            // the pod set, so no need to do it again)
            return Future.succeededFuture();
        }
    }

    /**
     * Checks whether the Kafka pods are ready and if not, waits for them to get ready
     *
     * @return  Future which completes when all Kafka pods are ready
     */
    protected Future<Void> podsReady() {
        return ReconcilerUtils
                .podsReady(
                        reconciliation,
                        podOperator,
                        operationTimeoutMs,
                        IntStream
                                .range(0, kafka.getReplicas())
                                .mapToObj(i -> KafkaResources.kafkaPodName(reconciliation.name(), i))
                                .collect(Collectors.toList())
                );
    }

    /**
     * Waits for readiness of the endpoints of the clients service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> serviceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.bootstrapServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Waits for readiness of the endpoints of the headless service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> headlessServiceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Get the Cluster ID of the Kafka cluster
     *
     * @return  Future which completes when the Cluster ID is retrieved and set in the status
     */
    protected Future<Void> clusterId(KafkaStatus kafkaStatus) {
        return ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
                .compose(compositeFuture -> {
                    LOGGER.debugCr(reconciliation, "Attempt to get clusterId");
                    Promise<Void> resultPromise = Promise.promise();
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                            future -> {
                                Admin kafkaAdmin = null;

                                try {
                                    String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
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

                                future.complete();
                            },
                            true,
                            resultPromise);
                    return resultPromise.future();
                });
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
        return pvcOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(pvcs -> {
                    List<String> maybeDeletePvcs = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = kafka.generatePersistentVolumeClaims(kafka.getStorage()).stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                            .deletePersistentClaims(maybeDeletePvcs, desiredPvcs);
                });
    }

    /**
     * Deletes the ConfigMap with shared Kafka configuration. This needs to be done when switching to per-broker
     * configuration / PodSets.
     *
     * @return  Future which returns when the shared configuration config map is deleted
     */
    protected Future<Void> sharedKafkaConfigurationCleanup() {
        return configMapOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name()), true);
    }

    /**
     * Deletes all ConfigMaps with per-broker Kafka configuration. This needs to be done when switching to the
     * shared configuration / StatefulSets
     *
     * @return  Future which completes when all the unnecessary per-broker config maps are deleted
     */
    protected Future<Void> perBrokerKafkaConfigurationCleanup() {
        return configMapOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(existingConfigMaps -> {
                    @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                    List<Future> ops = new ArrayList<>(existingConfigMaps.size()); // We will need at most the deletion of existing configmaps + update of new one => hence the + 1

                    // Delete all existing apart from the shared one
                    for (ConfigMap cm : existingConfigMaps) {
                        // We delete the cm if it is not getAncillaryConfigMapName
                        if (!KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name()).equals(cm.getMetadata().getName())) {
                            ops.add(configMapOperator.deleteAsync(reconciliation, reconciliation.namespace(), cm.getMetadata().getName(), true));
                        }
                    }

                    return CompositeFuture
                            .join(ops)
                            .map((Void) null);
                });
    }

    /**
     * This method is used to clean up the configuration Config Maps. This might be needed when switching between
     * StatefulSets / shared configuration and PodSets / per-broker configuration. This is done in a separate step
     * at the end of the reconciliation to avoid problems if the reconciliation fails (the deleted config map would
     * not allow pods to be restarted etc.)
     *
     * @return  Future which completes when all the unnecessary configuration config maps are deleted
     */
    protected Future<Void> brokerConfigurationConfigMapsCleanup() {
        if (featureGates.useStrimziPodSetsEnabled())    {
            return sharedKafkaConfigurationCleanup();
        } else {
            return perBrokerKafkaConfigurationCleanup();
        }
    }

    /**
     * Creates the status for NodePort listeners. This can be done only now at the end and not when the other listener
     * types are done because it requires the Kafka brokers to be scheduled and running to collect their node addresses.
     * Without that, we do not know on which node would they be running.
     *
     * @return  Future which completes when the Listener status is created for all node port listeners
     */
    protected Future<Void> nodePortExternalListenerStatus() {
        List<Node> allNodes = new ArrayList<>();

        if (!ListenersUtils.nodePortListeners(kafka.getListeners()).isEmpty())   {
            return nodeOperator.listAsync(Labels.EMPTY)
                    .compose(result -> {
                        allNodes.addAll(result);
                        return podOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels());
                    })
                    .map(pods -> {
                        Map<Integer, Node> brokerNodes = new HashMap<>(kafka.getReplicas());

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

                        for (GenericKafkaListener listener : ListenersUtils.nodePortListeners(kafka.getListeners())) {
                            // Set is used to ensure each node/port is listed only once. It is later converted to List.
                            Set<ListenerAddress> statusAddresses = new HashSet<>(brokerNodes.size());

                            for (Map.Entry<Integer, Node> entry : brokerNodes.entrySet())   {
                                String advertisedHost = ListenersUtils.brokerAdvertisedHost(listener, entry.getKey());
                                ListenerAddress address;

                                if (advertisedHost != null)    {
                                    address = new ListenerAddressBuilder()
                                            .withHost(advertisedHost)
                                            .withPort(listenerReconciliationResults.bootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                            .build();
                                } else {
                                    address = new ListenerAddressBuilder()
                                            .withHost(NodeUtils.findAddress(entry.getValue().getStatus().getAddresses(), ListenersUtils.preferredNodeAddressType(listener)))
                                            .withPort(listenerReconciliationResults.bootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                            .build();
                                }

                                statusAddresses.add(address);
                            }

                            ListenerStatus ls = listenerReconciliationResults.listenerStatuses
                                    .stream()
                                    .filter(listenerStatus -> listener.getName().equals(listenerStatus.getName()))
                                    .findFirst()
                                    .orElseThrow(() -> new RuntimeException("Status for listener " + listener.getName() + " not found"));
                            ls.setAddresses(new ArrayList<>(statusAddresses));
                        }

                        return null;
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    // Adds prepared Listener Statuses to the Kafka Status instance
    protected Future<Void> addListenersToKafkaStatus(KafkaStatus kafkaStatus) {
        kafkaStatus.setListeners(listenerReconciliationResults.listenerStatuses);
        return Future.succeededFuture();
    }

    /**
     * This is used to get the Kafka Storage configuration actually used by the Kafka brokers. The storage configuration
     * is needed by Cruise Control, but due to the possibility of illegal storage changes which will be reverted it
     * cannot be taken from the Kafka custom resource directly.
     *
     * @return  The storage configuration used by the Kafka brokers
     */
    public Storage kafkaStorage()   {
        return kafka.getStorage();
    }
}
