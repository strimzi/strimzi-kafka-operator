/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaClusterTemplate;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolTemplate;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.model.nodepools.NodeIdAssignment;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.StatusUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Kafka pool model
 */
public class KafkaPool extends AbstractModel {
    protected static final String COMPONENT_TYPE = "kafka";

    /**
     * Name of the Kafka pool
     */
    protected final String poolName;

    /**
     * Assigment of Node IDs
     */
    /* test */ final NodeIdAssignment idAssignment;

    /**
     * Storage configuration
     */
    protected Storage storage;

    /**
     * Process roles the nodes in this pool will take. This field is set in the fromCrd method, here it is only
     * set to null to avoid spotbugs complains. For KRaft based cluster, the nodes in this pool might be brokers,
     * controllers or both.
     */
    protected Set<ProcessRoles> processRoles = null;

    /**
     * Warning conditions generated from the Custom Resource
     */
    protected List<Condition> warningConditions = new ArrayList<>(0);

    // Templates
    protected ResourceTemplate templatePersistentVolumeClaims;
    protected ResourceTemplate templatePodSet;
    protected PodTemplate templatePod;
    protected ResourceTemplate templatePerBrokerService;
    protected ResourceTemplate templatePerBrokerRoute;
    protected ResourceTemplate templatePerBrokerIngress;
    protected ContainerTemplate templateInitContainer;

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param kafka             Kafka custom resource
     * @param pool              KafkaNodePool custom resource
     * @param componentName     Name of the component
     * @param ownerReference    Owner reference which should be used for this component.
     * @param idAssignment      Assignment of node IDs
     * @param sharedEnvironmentProvider Shared environment provider
     */
    private KafkaPool(
            Reconciliation reconciliation,
            Kafka kafka,
            KafkaNodePool pool,
            String componentName,
            OwnerReference ownerReference,
            NodeIdAssignment idAssignment,
            SharedEnvironmentProvider sharedEnvironmentProvider
    ) {
        super(
                reconciliation,
                kafka.getMetadata().getName(),
                kafka.getMetadata().getNamespace(),
                componentName,
                Labels.fromResource(pool)
                        // Strimzi labels
                        .withStrimziKind(kafka.getKind())
                        // This needs ot be selectable through KafkaCluster selector. So we intentionally use the <clusterName>-kafka
                        // as the strimzi.io/name. strimzi.io/pool-name can be used to select through node pool.
                        .withStrimziName(KafkaResources.kafkaComponentName(kafka.getMetadata().getName()))
                        .withStrimziCluster(kafka.getMetadata().getName())
                        .withStrimziComponentType(COMPONENT_TYPE)
                        .withStrimziPoolName(pool.getMetadata().getName())
                        // Kubernetes labels
                        .withKubernetesName(COMPONENT_TYPE)
                        .withKubernetesInstance(kafka.getMetadata().getName())
                        .withKubernetesPartOf(kafka.getMetadata().getName())
                        .withKubernetesManagedBy(STRIMZI_CLUSTER_OPERATOR_NAME),
                ownerReference,
                sharedEnvironmentProvider
        );

        this.poolName = pool.getMetadata().getName();
        this.idAssignment = idAssignment;
    }

    /**
     * Creates the Kafka pool model instance from a Kafka and KafkaNodePool CRs
     *
     * @param reconciliation    Reconciliation marker
     * @param kafka             Kafka custom resource
     * @param pool              Node pool configuration
     * @param idAssignment      Assignment of node IDs
     * @param oldStorage        The current storage configuration (based on the info from Kubernetes cluster, not from
     *                          the Kafka CR)
     * @param ownerReference    Owner reference which should be used for this component. This should be the KafkaNodePool
     *                          CR for regular pool or the Kafka CR for virtual node pool.
     * @param sharedEnvironmentProvider Shared environment provider
     *
     * @return Kafka pool instance
     */
    @SuppressWarnings("deprecation") // Resource configuration in Kafka CR (.spec.kafka.resources) is deprecated
    public static KafkaPool fromCrd(
            Reconciliation reconciliation,
            Kafka kafka,
            KafkaNodePool pool,
            NodeIdAssignment idAssignment,
            Storage oldStorage,
            OwnerReference ownerReference,
            SharedEnvironmentProvider sharedEnvironmentProvider
    ) {
        ModelUtils.validateComputeResources(pool.getSpec().getResources(), "KafkaNodePool.spec.resources");
        StorageUtils.validatePersistentStorage(pool.getSpec().getStorage(), "KafkaNodePool.spec.storage");

        KafkaPool result = new KafkaPool(reconciliation, kafka, pool, componentName(kafka, pool), ownerReference, idAssignment, sharedEnvironmentProvider);

        result.gcLoggingEnabled = isGcLoggingEnabled(kafka, pool);
        result.jvmOptions = pool.getSpec().getJvmOptions() != null ? pool.getSpec().getJvmOptions() : kafka.getSpec().getKafka().getJvmOptions();
        result.resources = pool.getSpec().getResources() != null ? pool.getSpec().getResources() : kafka.getSpec().getKafka().getResources();
        result.processRoles = new HashSet<>(pool.getSpec().getRoles());

        if (oldStorage != null) {
            Storage newStorage = pool.getSpec().getStorage();

            StorageDiff diff = new StorageDiff(reconciliation, oldStorage, newStorage, idAssignment.current(), idAssignment.desired());

            if (diff.issuesDetected()) {
                LOGGER.warnCr(reconciliation, "Only the following changes to Kafka storage are allowed: " +
                        "changing the deleteClaim flag, " +
                        "changing the kraftMetadata flag (but only one volume can be marked to store the KRaft metadata log at a time), " +
                        "adding volumes to Jbod storage or removing volumes from Jbod storage, " +
                        "each volume in Jbod storage should have an unique ID, " +
                        "changing overrides to nodes which do not exist yet, " +
                        "and increasing size of persistent claim volumes (depending on the volume type and used storage class).");
                LOGGER.warnCr(reconciliation, "The desired Kafka storage configuration in the KafkaNodePool resource {}/{} contains changes which are not allowed. As a " +
                        "result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                        "about the detected changes.", pool.getMetadata().getNamespace(), pool.getMetadata().getName());

                Condition warning = StatusUtils.buildWarningCondition("KafkaStorage",
                        "The desired Kafka storage configuration in the KafkaNodePool resource " + pool.getMetadata().getNamespace() + "/" + pool.getMetadata().getName() + " contains changes which are not allowed. As a " +
                                "result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                                "about the detected changes.");
                result.warningConditions.add(warning);

                result.setStorage(oldStorage);
            } else {
                if (!VolumeUtils.kraftMetadataPath(oldStorage).equals(VolumeUtils.kraftMetadataPath(newStorage)))    {
                    // The volume for the KRaft metadata log is changing. We should log it.
                    LOGGER.warnCr(reconciliation, "The KRaft metadata log for KafkaNodePool {}/{} will be moved from volume {} to volume {}.", pool.getMetadata().getNamespace(), pool.getMetadata().getName(), VolumeUtils.kraftMetadataPath(oldStorage), VolumeUtils.kraftMetadataPath(newStorage));
                }

                result.setStorage(newStorage);
            }
        } else {
            result.setStorage(pool.getSpec().getStorage());
        }

        processTemplate(result, kafka.getSpec().getKafka().getTemplate(), pool.getSpec().getTemplate());

        return result;
    }

    /**
     * Processes the template and combines it from the Kafka and KafkaNodePool CRs
     *
     * @param result                    KafkaPool instance
     * @param kafkaClusterTemplate      Template from the Kafka CR
     * @param kafkaNodePoolTemplate     Template from the KafkaNodePool CR
     */
    private static void processTemplate(KafkaPool result, KafkaClusterTemplate kafkaClusterTemplate, KafkaNodePoolTemplate kafkaNodePoolTemplate)   {
        // The Kafka CR sets the initial template
        if (kafkaClusterTemplate != null) {
            result.templatePersistentVolumeClaims = kafkaClusterTemplate.getPersistentVolumeClaim();
            result.templatePodSet = kafkaClusterTemplate.getPodSet();
            result.templatePod = kafkaClusterTemplate.getPod();
            result.templatePerBrokerService = kafkaClusterTemplate.getPerPodService();
            result.templatePerBrokerRoute = kafkaClusterTemplate.getPerPodRoute();
            result.templatePerBrokerIngress = kafkaClusterTemplate.getPerPodIngress();
            result.templateContainer = kafkaClusterTemplate.getKafkaContainer();
            result.templateInitContainer = kafkaClusterTemplate.getInitContainer();
        }

        // If the KafkaNodePool template exists, anything set in it would overwrite the Kafka CR template
        if (kafkaNodePoolTemplate != null) {
            if (kafkaNodePoolTemplate.getPersistentVolumeClaim() != null) result.templatePersistentVolumeClaims = kafkaNodePoolTemplate.getPersistentVolumeClaim();
            if (kafkaNodePoolTemplate.getPodSet() != null) result.templatePodSet = kafkaNodePoolTemplate.getPodSet();
            if (kafkaNodePoolTemplate.getPod() != null) result.templatePod = kafkaNodePoolTemplate.getPod();
            if (kafkaNodePoolTemplate.getPerPodService() != null) result.templatePerBrokerService = kafkaNodePoolTemplate.getPerPodService();
            if (kafkaNodePoolTemplate.getPerPodRoute() != null) result.templatePerBrokerRoute = kafkaNodePoolTemplate.getPerPodRoute();
            if (kafkaNodePoolTemplate.getPerPodIngress() != null) result.templatePerBrokerIngress = kafkaNodePoolTemplate.getPerPodIngress();
            if (kafkaNodePoolTemplate.getKafkaContainer() != null) result.templateContainer = kafkaNodePoolTemplate.getKafkaContainer();
            if (kafkaNodePoolTemplate.getInitContainer() != null) result.templateInitContainer = kafkaNodePoolTemplate.getInitContainer();
        }
    }

    /**
     * Creates a component name from the Kafka CR and KafkaNodePool CR
     *
     * @param kafka     Kafka custom resource
     * @param pool      KafkaNodePool custom resource
     *
     * @return  Component name combining the Kafka and pool name
     */
    public static String componentName(Kafka kafka, KafkaNodePool pool)   {
        return kafka.getMetadata().getName() + "-" + pool.getMetadata().getName();
    }

    private static boolean isGcLoggingEnabled(Kafka kafka, KafkaNodePool pool) {
        if (pool.getSpec().getJvmOptions() != null) {
            return pool.getSpec().getJvmOptions().isGcLoggingEnabled();
        } else if (kafka.getSpec().getKafka().getJvmOptions() != null)  {
            return kafka.getSpec().getKafka().getJvmOptions().isGcLoggingEnabled();
        } else {
            return JvmOptions.DEFAULT_GC_LOGGING_ENABLED;
        }
    }

    /**
     * Set the Storage
     *
     * @param storage Persistent Storage configuration
     */
    protected void setStorage(Storage storage) {
        StorageUtils.validatePersistentStorage(storage, "KafkaNodePool.spec.storage");
        this.storage = storage;
    }

    /**
     * @return  Set with node references belonging to this pool
     */
    public Set<NodeRef> nodes()   {
        return idAssignment.desired()
                .stream()
                .map(this::nodeRef)
                .collect(Collectors.toCollection(LinkedHashSet::new)); // we want this in deterministic order
    }

    /**
     * @return  Set of Kafka nodes that are going to be removed from the Kafka cluster
     */
    public Set<NodeRef> scaledDownNodes() {
        return idAssignment.toBeRemoved()
                .stream()
                .map(this::nodeRef)
                .collect(Collectors.toCollection(LinkedHashSet::new)); // we want this in deterministic order
    }

    /**
     * @return  Set of Kafka nodes that are going to be added to the Kafka cluster
     */
    public Set<NodeRef> scaleUpNodes() {
        return idAssignment.toBeAdded()
                .stream()
                .map(this::nodeRef)
                .collect(Collectors.toCollection(LinkedHashSet::new)); // we want this in deterministic order
    }

    /**
     * Builds node reference from node ID
     *
     * @param nodeId    Node ID
     *
     * @return  Node reference created based on the node ID
     */
    public NodeRef nodeRef(int nodeId)  {
        return new NodeRef(componentName + "-" + nodeId, nodeId, poolName, isController(), isBroker());
    }

    /**
     * Indicates whether a given node ID belongs to this pool
     *
     * @param nodeId    Node ID
     *
     * @return  True if this node ID belongs to this pool. False otherwise.
     */
    public boolean containsNodeId(int nodeId) {
        return idAssignment.desired().contains(nodeId);
    }

    /**
     * @return  True if this node pool has the broker role assigned. False otherwise.
     */
    public boolean isBroker()   {
        return processRoles.contains(ProcessRoles.BROKER);
    }

    /**
     * @return  True if this node pool has the controller role assigned. False otherwise.
     */
    public boolean isController()   {
        return processRoles.contains(ProcessRoles.CONTROLLER);
    }

    /**
     * Generates the status for this node pool
     *
     * @param clusterId     The clusterID or null if it is not known yet.
     *
     * @return  The generated KafkaNodePool status
     */
    public KafkaNodePoolStatus generateNodePoolStatus(String clusterId) {
        return new KafkaNodePoolStatusBuilder()
                .withClusterId(clusterId)
                .withNodeIds(new ArrayList<>(idAssignment.desired()))
                .withRoles(processRoles.stream().sorted().toList())
                .withReplicas(idAssignment.desired().size())
                .withLabelSelector(getSelectorLabels().toSelectorString())
                .withConditions(warningConditions)
                .build();
    }

    /**
     * @return  the set of Kafka node IDs that used to have the broker role but do not have it anymore.
     */
    public Set<Integer> usedToBeBrokerNodes() {
        return idAssignment.usedToBeBroker();
    }
}
