/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSetStatus;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.PodRevision;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.InformerUtils;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.metrics.ControllerMetricsHolder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.model.StatusUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * StrimziPodSet controller is responsible for managing the StrimziPodSets and the pods which belong to them
 */
public class StrimziPodSetController implements Runnable {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(StrimziPodSetController.class);

    private static final long DEFAULT_RESYNC_PERIOD_MS = 5 * 60 * 1_000L; // 5 minutes by default
    private static final LabelSelector POD_LABEL_SELECTOR = new LabelSelectorBuilder()
            .withMatchExpressions(new LabelSelectorRequirement(Labels.STRIMZI_KIND_LABEL, "Exists", null))
            .build();

    private final Thread controllerThread;

    private volatile boolean stop = false;

    private final PodOperator podOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final ControllerMetricsHolder metrics;
    private final LabelSelector crSelector;
    private final String watchedNamespace;

    private final BlockingQueue<SimplifiedReconciliation> workQueue;
    private final SharedIndexInformer<Pod> podInformer;
    private final SharedIndexInformer<StrimziPodSet> strimziPodSetInformer;
    private final SharedIndexInformer<Kafka> kafkaInformer;
    private final SharedIndexInformer<KafkaConnect> kafkaConnectInformer;
    private final SharedIndexInformer<KafkaMirrorMaker2> kafkaMirrorMaker2Informer;
    private final Lister<Pod> podLister;
    private final Lister<StrimziPodSet> strimziPodSetLister;
    private final Lister<Kafka> kafkaLister;
    private final Lister<KafkaConnect> kafkaConnectLister;
    private final Lister<KafkaMirrorMaker2> kafkaMirrorMaker2Lister;

    /**
     * Creates the StrimziPodSet controller. The controller should normally exist once per operator for cluster-wide mode
     * or once per namespace for namespaced mode.
     *
     * @param watchedNamespace              Namespace which should be watched. Use * for all namespaces.
     * @param crSelectorLabels              Selector labels for custom resource managed by this operator instance. This is used
     *                                      to check that the pods belong to a Kafka cluster matching these labels.
     * @param kafkaOperator                 Kafka Operator for getting the Kafka custom resources
     * @param kafkaConnectOperator          KafkaConnect Operator for getting the KafkaConnect custom resources
     * @param kafkaMirrorMaker2Operator     KafkaMirrorMaker2 Operator for getting the KafkaMirrorMaker2 custom resources
     * @param strimziPodSetOperator         StrimziPodSet Operator used to manage the StrimziPodSet resources - get them, update
     *                                      their status etc.
     * @param podOperator                   Pod operator for managing pods
     * @param metricsProvider               Metrics provider
     * @param podSetControllerWorkQueueSize Indicates the size of the StrimziPodSetController work queue
     */
    public StrimziPodSetController(
            String watchedNamespace,
            Labels crSelectorLabels,
            CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator,
            CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> kafkaConnectOperator,
            CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> kafkaMirrorMaker2Operator,
            StrimziPodSetOperator strimziPodSetOperator,
            PodOperator podOperator,
            MetricsProvider metricsProvider,
            int podSetControllerWorkQueueSize
    ) {
        this.podOperator = podOperator;
        this.strimziPodSetOperator = strimziPodSetOperator;
        this.crSelector = (crSelectorLabels == null || crSelectorLabels.toMap().isEmpty()) ? null : new LabelSelector(null, crSelectorLabels.toMap());
        this.watchedNamespace = watchedNamespace;
        this.workQueue = new ArrayBlockingQueue<>(podSetControllerWorkQueueSize);

        // Set up the metrics holder
        this.metrics = new ControllerMetricsHolder("StrimziPodSet", crSelectorLabels != null ? crSelectorLabels : Labels.EMPTY, metricsProvider);

        // Kafka, KafkaConnect and KafkaMirrorMaker2 informers and listers are used to get the CRs quickly.
        // This is needed for verification of the CR selector labels.
        this.kafkaInformer = kafkaOperator.informer(watchedNamespace, (crSelectorLabels == null) ? Map.of() : crSelectorLabels.toMap(), DEFAULT_RESYNC_PERIOD_MS);
        this.kafkaLister = new Lister<>(kafkaInformer.getIndexer());
        this.kafkaConnectInformer = kafkaConnectOperator.informer(watchedNamespace, (crSelectorLabels == null) ? Map.of() : crSelectorLabels.toMap(), DEFAULT_RESYNC_PERIOD_MS);
        this.kafkaConnectLister = new Lister<>(kafkaConnectInformer.getIndexer());
        this.kafkaMirrorMaker2Informer = kafkaMirrorMaker2Operator.informer(watchedNamespace, (crSelectorLabels == null) ? Map.of() : crSelectorLabels.toMap(), DEFAULT_RESYNC_PERIOD_MS);
        this.kafkaMirrorMaker2Lister = new Lister<>(kafkaMirrorMaker2Informer.getIndexer());

        // StrimziPodSet informer and lister is used to get events about StrimziPodSet and get StrimziPodSet quickly
        this.strimziPodSetInformer = strimziPodSetOperator.informer(watchedNamespace, DEFAULT_RESYNC_PERIOD_MS);
        this.strimziPodSetLister = new Lister<>(strimziPodSetInformer.getIndexer());

        // Pod informer and lister is used to get events about pods and get pods quickly
        this.podInformer = podOperator.informer(watchedNamespace, POD_LABEL_SELECTOR, DEFAULT_RESYNC_PERIOD_MS);
        this.podLister = new Lister<>(podInformer.getIndexer());

        this.controllerThread = new Thread(this, "StrimziPodSetController");
    }

    protected ControllerMetricsHolder metrics()   {
        return metrics;
    }

    protected boolean isSynced() {
        return podInformer.hasSynced()
                && strimziPodSetInformer.hasSynced()
                && kafkaInformer.hasSynced()
                && kafkaConnectInformer.hasSynced()
                && kafkaMirrorMaker2Informer.hasSynced();
    }

    protected void startController() {
        strimziPodSetInformer.addEventHandler(new PodSetEventHandler());
        strimziPodSetInformer.exceptionHandler((isStarted, throwable) -> InformerUtils.loggingExceptionHandler("StrimziPodSet", isStarted, throwable));

        podInformer.addEventHandler(new PodEventHandler());
        podInformer.exceptionHandler((isStarted, throwable) -> InformerUtils.loggingExceptionHandler("Pod", isStarted, throwable));

        kafkaInformer.exceptionHandler((isStarted, throwable) -> InformerUtils.loggingExceptionHandler("Kafka", isStarted, throwable));
        kafkaConnectInformer.exceptionHandler((isStarted, throwable) -> InformerUtils.loggingExceptionHandler("KafkaConnect", isStarted, throwable));
        kafkaMirrorMaker2Informer.exceptionHandler((isStarted, throwable) -> InformerUtils.loggingExceptionHandler("KafkaMirrorMaker2", isStarted, throwable));

        strimziPodSetInformer.start();
        podInformer.start();
        kafkaInformer.start();
        kafkaConnectInformer.start();
        kafkaMirrorMaker2Informer.start();

        strimziPodSetInformer.stopped().whenComplete((v, t) -> InformerUtils.stoppedInformerHandler("StrimziPodSet", t, stop));
        podInformer.stopped().whenComplete((v, t) -> InformerUtils.stoppedInformerHandler("Pod", t, stop));
        kafkaInformer.stopped().whenComplete((v, t) -> InformerUtils.stoppedInformerHandler("Kafka", t, stop));
        kafkaConnectInformer.stopped().whenComplete((v, t) -> InformerUtils.stoppedInformerHandler("KafkaConnect", t, stop));
        kafkaMirrorMaker2Informer.stopped().whenComplete((v, t) -> InformerUtils.stoppedInformerHandler("KafkaMirrorMaker2", t, stop));
    }

    protected void stopController() {
        InformerUtils.stopAll(5_000L, strimziPodSetInformer, podInformer, kafkaInformer, kafkaConnectInformer, kafkaMirrorMaker2Informer);
    }

    /**
     * Checks if the StrimziPodSet which should be enqueued matches the CR selector. If it does, it will enqueue the
     * reconciliation. This is used to enqueue reconciliations based on StrimziPodSet events.
     *
     * @param podSet    StrimziPodSet which should be checked and possibly enqueued
     * @param action    The action from the event which triggered this
     */
    private void enqueueStrimziPodSet(StrimziPodSet podSet, String action)   {
        LOGGER.debugOp("StrimziPodSet {} in namespace {} was {}", podSet.getMetadata().getName(), podSet.getMetadata().getNamespace(), action);

        if (matchesCrSelector(podSet)) {
            enqueue(new SimplifiedReconciliation(podSet.getMetadata().getNamespace(), podSet.getMetadata().getName()));
        } else {
            LOGGER.debugOp("StrimziPodSet {} in namespace {} was {} but does not belong to a Kafka cluster managed by this operator", podSet.getMetadata().getName(), podSet.getMetadata().getNamespace(), action);
        }
    }

    /**
     * Checks if the Pod which should be enqueued belongs to a StrimziPodSet this controller manages and whether the
     * Kafka cluster which owns it matches the CR selector. If it does, it will enqueue the reconciliation. This is used
     * to enqueue reconciliations based on Pod events.
     *
     * Note: The reconciliation is enqueued per StrimziPodSet to which the pod belongs and not based on the Pod itself.
     *
     * @param pod      Pod which should be checked and possibly enqueued
     * @param action   The action from the event which triggered this
     */
    private void enqueuePod(Pod pod, String action) {
        LOGGER.debugOp("Pod {} in namespace {} was {}", pod.getMetadata().getName(), pod.getMetadata().getNamespace(), action);

        StrimziPodSet parentPodSet = findParentPodSetForPod(pod);

        if (parentPodSet != null) {
            if (matchesCrSelector(parentPodSet)) {
                enqueue(new SimplifiedReconciliation(parentPodSet.getMetadata().getNamespace(), parentPodSet.getMetadata().getName()));
            } else {
                LOGGER.debugOp("Pod {} in namespace {} was {} but does not belong to a cluster managed by this operator", pod.getMetadata().getName(), pod.getMetadata().getNamespace(), action);
            }
        } else {
            LOGGER.debugOp("Pod {} in namespace {} which was {} does not seem to be controlled by any StrimziPodSet and will be ignored", pod.getMetadata().getName(), pod.getMetadata().getNamespace(), action);
        }
    }

    /**
     * Finds parent StrimziPodSet of a Pod. It first tries to do it through an owner reference. If that does not
     * succeed, it tries that by matching the selector labels of the StrimziPodSet against the Pod. This is needed to
     * work around label changes introduced by node pools.
     *
     * @param pod   Pod for which we want to find the StrimziPodSet
     *
     * @return  The parent StrimziPodSet (or null if not found)
     */
    private StrimziPodSet findParentPodSetForPod(Pod pod) {
        StrimziPodSet parentPodSet = findParentPodSetForPodByOwnerReference(pod);

        if (parentPodSet == null)   {
            LOGGER.debugOp("Did not found parent StrimziPodSet for Pod {} in namespace {} based on owner reference. Will try selector labels next.", pod.getMetadata().getName(), pod.getMetadata().getNamespace());
            parentPodSet = findParentPodSetForPodByLabels(pod);
        }

        return parentPodSet;
    }

    /**
     * Finds the parent StrimziPodSet of the Pod from the parameter by matching the StrimziPodSet selector labels
     * against the Pod.
     *
     * @param pod   Pod for which we want to find the StrimziPodSet
     *
     * @return  The parent StrimziPodSet (or null if not found)
     */
    private StrimziPodSet findParentPodSetForPodByLabels(Pod pod)   {
        return strimziPodSetLister
                .namespace(pod.getMetadata().getNamespace())
                .list()
                .stream()
                .filter(podSet -> podSet.getSpec() != null
                        && Util.matchesSelector(podSet.getSpec().getSelector(), pod))
                .findFirst().orElse(null);
    }

    /**
     * Finds the parent StrimziPodSet of the Pod from the parameter based on the OwnerReference from the Pod.
     *
     * @param pod   Pod for which we want to find the StrimziPodSet
     *
     * @return  The parent StrimziPodSet (or null if not found)
     */
    private StrimziPodSet findParentPodSetForPodByOwnerReference(Pod pod)   {
        OwnerReference owner = pod.getMetadata().getOwnerReferences()
                .stream().filter(or -> StrimziPodSet.RESOURCE_KIND.equals(or.getKind()))
                .findFirst()
                .orElse(null);

        if (owner == null)    {
            // There is no owner reference to a PodSet => we cannot find the parent StrimziPodSet based on it
            return null;
        } else {
            // We have owner reference => we find the StrimziPodSet based on it
            return strimziPodSetLister
                    .namespace(pod.getMetadata().getNamespace())
                    .list()
                    .stream()
                    .filter(podSet -> podSet.getMetadata().getName().equals(owner.getName()))
                    .findFirst()
                    .orElse(null);
        }
    }

    /**
     * Utility method which tries to find the Kafka cluster to which this StrimziPodSet belongs and checks whether the CR
     * selector labels match or not. The controller handles only Kafka clusters matching the CR selector labels. Other
     * clusters are ignored
     *
     * @param podSet    StrimziPodSet which should be checked
     *
     * @return          True if the StrimziPodSet's Kafka cluster matches the selector labels
     */
    private boolean matchesCrSelector(StrimziPodSet podSet)    {
        if (podSet.getMetadata().getLabels() != null
                && podSet.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL) != null
                && podSet.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL) != null) {
            // We find the matching custom resource and check the CR selector
            HasMetadata cr = findCustomResource(podSet);

            if (cr != null
                    && Util.matchesSelector(crSelector, cr)) {
                return true;
            } else {
                LOGGER.debugOp("StrimziPodSet {} in namespace {} does not belong to a custom resource matching the selector", podSet.getMetadata().getName(), podSet.getMetadata().getNamespace());
                return false;
            }
        } else {
            LOGGER.warnOp("Invalid event received: StrimziPodSet was without the required {} and {} labels", Labels.STRIMZI_KIND_LABEL, Labels.STRIMZI_CLUSTER_LABEL);
            return false;
        }
    }

    private HasMetadata findCustomResource(StrimziPodSet podSet)    {
        String customResourceName = podSet.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        HasMetadata cr = null;

        switch (podSet.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL)) {
            case Kafka.RESOURCE_KIND -> cr = kafkaLister.namespace(podSet.getMetadata().getNamespace()).get(customResourceName);
            case KafkaConnect.RESOURCE_KIND -> cr = kafkaConnectLister.namespace(podSet.getMetadata().getNamespace()).get(customResourceName);
            case KafkaMirrorMaker2.RESOURCE_KIND -> cr = kafkaMirrorMaker2Lister.namespace(podSet.getMetadata().getNamespace()).get(customResourceName);
            default -> LOGGER.warnOp("StrimziPodSet {} belongs to unsupported custom resource kind {}", podSet.getMetadata().getName(), podSet.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL));
        }

        return cr;
    }

    /**
     * Checks whether the StrimziPodSet is being deleted or not. This is needed to handle non-cascading deletions.
     *
     * @param podSet    StrimziPodSet which needs to be checked whether it is being deleted
     *
     * @return          True if the PodSet is being deleted. False otherwise.
     */
    private boolean isDeleting(StrimziPodSet podSet)    {
        return podSet.getMetadata().getDeletionTimestamp() != null;
    }

    /**
     * The main reconciliation logic which handles the reconciliations.
     *
     * @param reconciliation    Reconciliation identifier used for logging
     */
    private void reconcile(Reconciliation reconciliation)    {
        metrics().reconciliationsCounter(reconciliation.namespace()).increment(); // Increase the reconciliation counter
        Timer.Sample reconciliationTimerSample = Timer.start(metrics().metricsProvider().meterRegistry()); // Start the reconciliation timer

        try {
            String name = reconciliation.name();
            String namespace = reconciliation.namespace();
            StrimziPodSet podSet = strimziPodSetLister.namespace(namespace).get(name);

            if (podSet == null) {
                LOGGER.debugCr(reconciliation, "StrimziPodSet is null => nothing to do");
                metrics.successfulReconciliationsCounter(reconciliation.namespace()).increment();
            } else if (!matchesCrSelector(podSet)) {
                LOGGER.debugCr(reconciliation, "StrimziPodSet doesn't match the selector => nothing to do");
                metrics.successfulReconciliationsCounter(reconciliation.namespace()).increment();
            } else if (isDeleting(podSet)) {
                // When the PodSet is deleted, the pod deletion is done by Kubernetes Garbage Collection. When the PodSet
                // deletion is non-cascading, Kubernetes will remove the owner references. In order to avoid setting the
                // owner reference again, we need to check if the PodSet is being deleted and if it is, we leave it to
                // Kubernetes.
                LOGGER.infoCr(reconciliation, "StrimziPodSet is deleting => nothing to do");
                metrics.successfulReconciliationsCounter(reconciliation.namespace()).increment();
            } else {
                LOGGER.infoCr(reconciliation, "StrimziPodSet will be reconciled");

                StrimziPodSetStatus status = new StrimziPodSetStatus();
                status.setObservedGeneration(podSet.getMetadata().getGeneration());

                try {
                    // This has to:
                    // 1) Create missing pods
                    // 2) Modify changed pods if needed (patch owner reference)
                    // 3) Delete scaled down pods

                    // Will be used later to find out if any pod needs to be deleted
                    Set<String> desiredPods = new HashSet<>(podSet.getSpec().getPods().size());
                    PodCounter podCounter = new PodCounter();
                    podCounter.pods = podSet.getSpec().getPods().size();

                    for (Map<String, Object> desiredPod : podSet.getSpec().getPods()) {
                        Pod pod = PodSetUtils.mapToPod(desiredPod);
                        desiredPods.add(pod.getMetadata().getName());

                        maybeCreateOrPatchPod(reconciliation, pod, ModelUtils.createOwnerReference(podSet, true), podCounter);
                    }

                    // Check if any pods needs to be deleted
                    removeDeletedPods(reconciliation, podSet.getSpec().getSelector(), desiredPods, podCounter);

                    status.setPods(podCounter.pods);
                    status.setReadyPods(podCounter.readyPods);
                    status.setCurrentPods(podCounter.currentPods);
                    metrics.successfulReconciliationsCounter(reconciliation.namespace()).increment();
                } catch (Exception e) {
                    LOGGER.errorCr(reconciliation, "StrimziPodSet {} in namespace {} reconciliation failed", reconciliation.name(), reconciliation.namespace(), e);
                    status.addCondition(StatusUtils.buildConditionFromException("Error", "true", e));
                    metrics.failedReconciliationsCounter(reconciliation.namespace()).increment();
                } finally {
                    maybeUpdateStatus(reconciliation, podSet, status);
                    LOGGER.infoCr(reconciliation, "reconciled");
                }
            }
        } finally   {
            // Tasks after reconciliation
            reconciliationTimerSample.stop(metrics().reconciliationsTimer(reconciliation.namespace())); // Stop the reconciliation timer
        }
    }

    /**
     * Updates the status of the StrimziPodSet. The status will be updated only when it changed since last time.
     *
     * @param reconciliation    Reconciliation in which this is executed
     * @param podSet            Original pod set with the current status
     * @param desiredStatus     The desired status which should be set if it differs
     */
    private void maybeUpdateStatus(Reconciliation reconciliation, StrimziPodSet podSet, StrimziPodSetStatus desiredStatus) {
        if (!new StatusDiff(podSet.getStatus(), desiredStatus).isEmpty())  {
            try {
                LOGGER.debugCr(reconciliation, "Updating status of StrimziPodSet {} in namespace {}", reconciliation.name(), reconciliation.namespace());
                StrimziPodSet latestPodSet = strimziPodSetLister.namespace(reconciliation.namespace()).get(reconciliation.name());
                if (latestPodSet != null) {
                    StrimziPodSet updatedPodSet = new StrimziPodSetBuilder(latestPodSet)
                            .withStatus(desiredStatus)
                            .build();

                    strimziPodSetOperator.client().inNamespace(reconciliation.namespace()).resource(updatedPodSet).updateStatus();
                }
            } catch (KubernetesClientException e)   {
                if (e.getCode() == 409) {
                    LOGGER.debugCr(reconciliation, "StrimziPodSet {} in namespace {} changed while trying to update status", reconciliation.name(), reconciliation.namespace());
                } else if (e.getCode() == 404) {
                    LOGGER.debugCr(reconciliation, "StrimziPodSet {} in namespace {} was deleted while trying to update status", reconciliation.name(), reconciliation.namespace());
                } else {
                    LOGGER.errorCr(reconciliation, "Failed to update status of StrimziPodSet {} in namespace {}", reconciliation.name(), reconciliation.namespace(), e);
                }
            }
        }
    }

    /**
     * Creates missing pod defined in the StrimziPodSet. If the pod already exists, it checks the owner reference and if
     * needed adds it to the Pod.
     *
     * @param reconciliation    Reconciliation in which this is executed
     * @param pod               Pod which should be checked and created if needed
     * @param owner             The OwnerReference which should be set to the pod
     * @param podCounter        Pod Counter used to count pods for the status
     */
    private void maybeCreateOrPatchPod(Reconciliation reconciliation, Pod pod, OwnerReference owner, PodCounter podCounter)    {
        Pod currentPod = podLister.namespace(reconciliation.namespace()).get(pod.getMetadata().getName());

        if (currentPod == null) {
            // Pod does not exist => we create it
            LOGGER.debugCr(reconciliation, "Creating pod {} in namespace {}", pod.getMetadata().getName(), reconciliation.namespace());
            pod.getMetadata().setOwnerReferences(List.of(owner));
            podOperator.client().inNamespace(reconciliation.namespace()).resource(pod).create();
        } else {
            if (PodSetUtils.isInTerminalState(currentPod))  {
                // The Pods might reach a terminal state of Succeeded or Failed in some situations such as node failures
                // The controller detects these states and deletes such pods. Another reconciliation triggered by the
                // deletion will recreate it.
                LOGGER.debugCr(reconciliation, "Pod {} in namespace {} reached terminal phase {} => deleting it", currentPod.getMetadata().getName(), reconciliation.namespace(), currentPod.getStatus().getPhase());
                podOperator.client().inNamespace(reconciliation.namespace()).resource(currentPod).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
            } else if (ModelUtils.hasOwnerReference(currentPod, owner))    {
                LOGGER.debugCr(reconciliation, "Pod {} in namespace {} already exists => nothing to do right now", pod.getMetadata().getName(), reconciliation.namespace());
            } else  {
                LOGGER.debugCr(reconciliation, "Pod {} in namespace {} is missing owner reference => patching it", currentPod.getMetadata().getName(), reconciliation.namespace());
                Pod podWithOwnerReference = new PodBuilder(currentPod).build();

                if (podWithOwnerReference.getMetadata().getOwnerReferences() != null)   {
                    podWithOwnerReference.getMetadata().getOwnerReferences().add(owner);
                } else {
                    podWithOwnerReference.getMetadata().setOwnerReferences(List.of(owner));
                }

                podOperator.client().inNamespace(reconciliation.namespace()).withName(pod.getMetadata().getName()).patch(PatchContext.of(PatchType.JSON), podWithOwnerReference);
            }

            if (Readiness.isPodReady(currentPod))   {
                podCounter.readyPods++;
            }

            if (!PodRevision.hasChanged(currentPod, pod))    {
                podCounter.currentPods++;
            }

            // TODO: Add patching of exiting pods => to be done in the future to handle selected changes to the Pods
            //  which might not require rolling updates
        }
    }

    /**
     * Removes the pods which were removed from the StrimziPodSet but which match the selector.
     *
     * @param reconciliation    Reconciliation in which this is executed
     * @param selector          LabelSelector to match the pods belonging to this StrimziPodSet
     * @param desiredPodNames   Collection with names of the pods which are still desired
     * @param podCounter        Pod Counter used to count pods for the status
     */
    private void removeDeletedPods(Reconciliation reconciliation, LabelSelector selector, Collection<String> desiredPodNames, PodCounter podCounter) {
        Set<String> toBeDeleted = podLister
                .namespace(reconciliation.namespace())
                .list()
                .stream()
                .filter(pod -> Util.matchesSelector(selector, pod))
                .map(pod -> pod.getMetadata().getName())
                .collect(Collectors.toSet());
        toBeDeleted.removeAll(desiredPodNames);

        for (String podName : toBeDeleted)  {
            LOGGER.debugCr(reconciliation, "Deleting pod {} in namespace {}", podName, reconciliation.namespace());
            podOperator.client().inNamespace(reconciliation.namespace()).withName(podName).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
            podCounter.pods++;
        }
    }

    /**
     * Enqueues the next reconciliation. It checks whether another reconciliation for the same resource is already in
     * the queue and enqueues the new event only if it is not there yet.
     *
     * @param reconciliation    Reconciliation identifier
     */
    private void enqueue(SimplifiedReconciliation reconciliation)    {
        if (!workQueue.contains(reconciliation)) {
            LOGGER.debugOp("Enqueueing StrimziPodSet {} in namespace {}", reconciliation.name, reconciliation.name);
            workQueue.add(reconciliation);
        } else {
            metrics().alreadyEnqueuedReconciliationsCounter(reconciliation.namespace).increment(); // Increase the metrics counter
            LOGGER.debugOp("StrimziPodSet {} in namespace {} is already enqueued => ignoring", reconciliation.name, reconciliation.name);
        }
    }

    /**
     * The run loop of the controller thread. It picks reconciliations from the work queue and executes them.
     */
    @Override
    public void run() {
        LOGGER.infoOp("Starting StrimziPodSet controller for namespace {}", watchedNamespace);
        startController();

        LOGGER.infoOp("Waiting for informers to sync");
        while (!stop && !isSynced())   {
            // Nothing to do => just loop
        }

        LOGGER.infoOp("Informers are in-sync");

        while (!stop) {
            try {
                LOGGER.debugOp("Waiting for next event from work queue");
                Reconciliation reconciliation = workQueue.take().toReconciliation();
                reconcile(reconciliation);
            } catch (InterruptedException e)    {
                LOGGER.debugOp("StrimziPodSet Controller was interrupted", e);
            } catch (Exception e)   {
                LOGGER.warnOp("StrimziPodSet reconciliation failed", e);
            }
        }

        LOGGER.infoOp("Stopping StrimziPodSet controller");

        stopController();
    }

    /**
     * Starts the controller: this method creates a new thread in which the controller will run
     */
    public void start()  {
        LOGGER.infoOp("Starting the StrimziPodSet controller");
        controllerThread.start();
    }

    /**
     * Stops the controller: this method sets the stop flag and interrupt the run loop
     */
    public void stop()  {
        LOGGER.infoOp("Requesting the StrimziPodSet controller to stop");
        this.stop = true;
        controllerThread.interrupt();
        try {
            controllerThread.join();
        } catch (InterruptedException e)    {
            LOGGER.warnOp("Interrupted while waiting for the StrimziPodSet controller thread to stop");
        }
        LOGGER.infoOp("StrimziPodSet controller stopped");
    }

    /**
     * Helper class to track the pod counts during reconciliation and to pass through different methods. This is used to
     * count the numbers for the StrimziPodSet status subresource.
     */
    static class PodCounter    {
        int pods = 0;
        int readyPods = 0;
        int currentPods = 0;
    }

    /**
     * Helper class to track the pod counts during reconciliation and to pass through different methods. This simplified
     * class is used initially instead of the regular Reconciliation class. It also has a custom equals implementation
     * to detect if the same resource is already enqueued or not. It doesn't yet request the reconciliation ID. Not
     * issuing the reconciliation ID right away makes the IDs more linear and means they are not requested unless a
     * reconciliation really starts.
     */
    static class SimplifiedReconciliation    {
        private final String namespace;
        private final String name;

        public SimplifiedReconciliation(String namespace, String name) {
            this.namespace = namespace;
            this.name = name;
        }

        /**
         * Converts the simplified reconciliation to a proper reconciliation
         *
         * @return  Reconciliation object
         */
        public Reconciliation toReconciliation()    {
            return new Reconciliation("watch", "StrimziPodSet", namespace, name);
        }

        /**
         * Compares two SimplifiedReconciliation objects. This is used to avoid having the same resource queued multiple
         * times.
         *
         * @param o     SimplifiedReconciliation to be compared
         *
         * @return      True if the objects equal, false otherwise
         */
        @Override
        public boolean equals(Object o) {
            if (this == o)  {
                return true;
            } else if (o == null || getClass() != o.getClass())   {
                return false;
            } else {
                SimplifiedReconciliation reconciliation = (SimplifiedReconciliation) o;

                return this.name.equals(reconciliation.name)
                        && this.namespace.equals(reconciliation.namespace);
            }
        }

        /**
         * Generates the hashcode based on the name and namespace hash codes.
         *
         * @return  The hashcode of this object
         */
        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
            return result;
        }
    }

    /**
     * Event handler used in the StrimziPodSet informer which decides what to do with the incoming events.
     */
    private class PodSetEventHandler implements ResourceEventHandler<StrimziPodSet> {
        @Override
        public void onAdd(StrimziPodSet podSet) {
            if (matchesCrSelector(podSet)) {
                metrics.resourceCounter(podSet.getMetadata().getNamespace()).incrementAndGet();
            }

            enqueueStrimziPodSet(podSet, "ADDED");
        }

        @Override
        public void onUpdate(StrimziPodSet oldPodSet, StrimziPodSet newPodSet) {
            enqueueStrimziPodSet(newPodSet, "MODIFIED");
        }

        @Override
        public void onDelete(StrimziPodSet podSet, boolean deletedFinalStateUnknown) {
            if (matchesCrSelector(podSet)) {
                metrics.resourceCounter(podSet.getMetadata().getNamespace()).decrementAndGet();
            }

            LOGGER.debugOp("StrimziPodSet {} in namespace {} was {}", podSet.getMetadata().getName(), podSet.getMetadata().getNamespace(), "DELETED");
            // Nothing to do => garbage collection should take care of things
        }
    }

    /**
     * Event handler used in the Pod informer which decides what to do with the incoming events.
     */
    private class PodEventHandler implements ResourceEventHandler<Pod> {
        @Override
        public void onAdd(Pod pod) {
            enqueuePod(pod, "ADDED");
        }

        @Override
        public void onUpdate(Pod oldPod, Pod newPod) {
            enqueuePod(newPod, "MODIFIED");
        }

        @Override
        public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
            enqueuePod(pod, "DELETED");
        }
    }
}
