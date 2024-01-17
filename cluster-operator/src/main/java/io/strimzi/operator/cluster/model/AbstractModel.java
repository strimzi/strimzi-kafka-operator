/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderFactory;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;

/**
 * AbstractModel an abstract base model for all components of the {@code Kafka} custom resource
 */
public abstract class AbstractModel {
    /**
     * Name of the Strimzi Cluster operator a used in various labels
     */
    public static final String STRIMZI_CLUSTER_OPERATOR_NAME = "strimzi-cluster-operator";

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractModel.class.getName());

    /**
     * Init container related configuration
     */
    protected static final String INIT_NAME = "kafka-init";
    protected static final String INIT_VOLUME_NAME = "rack-volume";
    protected static final String INIT_VOLUME_MOUNT = "/opt/kafka/init";
    protected static final String ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    protected static final String ENV_VAR_KAFKA_INIT_NODE_NAME = "NODE_NAME";

    protected static final String ENV_VAR_DYNAMIC_HEAP_PERCENTAGE = "STRIMZI_DYNAMIC_HEAP_PERCENTAGE";
    protected static final String ENV_VAR_KAFKA_HEAP_OPTS = "KAFKA_HEAP_OPTS";
    protected static final String ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS = "KAFKA_JVM_PERFORMANCE_OPTS";
    protected static final String ENV_VAR_DYNAMIC_HEAP_MAX = "STRIMZI_DYNAMIC_HEAP_MAX";
    protected static final String ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED = "STRIMZI_KAFKA_GC_LOG_ENABLED";
    protected static final String ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES = "STRIMZI_JAVA_SYSTEM_PROPERTIES";
    protected static final String ENV_VAR_STRIMZI_JAVA_OPTS = "STRIMZI_JAVA_OPTS";
    protected static final String ENV_VAR_STRIMZI_GC_LOG_ENABLED = "STRIMZI_GC_LOG_ENABLED";

    protected final Reconciliation reconciliation;
    protected final String cluster;
    protected final String namespace;
    protected final String componentName;
    protected final OwnerReference ownerReference;
    protected final Labels labels;
    protected final SharedEnvironmentProvider sharedEnvironmentProvider;

    // Docker image configuration
    protected String image;

    protected boolean gcLoggingEnabled = true;
    protected JvmOptions jvmOptions;

    /**
     * Container configuration
     */
    protected ResourceRequirements resources;
    protected Probe readinessProbeOptions;
    protected Probe livenessProbeOptions;

    /**
     * PodSecurityProvider
     */
    protected PodSecurityProvider securityProvider = PodSecurityProviderFactory.getProvider();

    /**
     * Template configurations shared between all AbstractModel subclasses.
     */
    protected ResourceTemplate templateServiceAccount;
    protected ContainerTemplate templateContainer;

    /**
     * Constructor
     *
     * @param reconciliation    The reconciliation marker
     * @param cluster           Name of the cluster to which this component belongs
     * @param namespace         Namespace of this component
     * @param componentName     Name of the Strimzi component usually consisting from the cluster name and component type
     * @param labels            Labels used for this component
     * @param ownerReference    Owner reference used for this component
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected AbstractModel(Reconciliation reconciliation, String cluster, String namespace, String componentName, Labels labels, OwnerReference ownerReference, SharedEnvironmentProvider sharedEnvironmentProvider) {
        this.reconciliation = reconciliation;
        this.cluster = cluster;
        this.namespace = namespace;
        this.componentName = componentName;
        this.labels = labels;
        this.ownerReference = ownerReference;
        this.sharedEnvironmentProvider = sharedEnvironmentProvider;
    }

    /**
     * Constructor
     *
     * @param reconciliation    The reconciliation marker
     * @param resource          Custom resource with metadata containing the namespace and cluster name
     * @param componentName     Name of the Strimzi component usually consisting from the cluster name and component type
     * @param componentType     Type of the component that the extending class is deploying (e.g. Kafka, ZooKeeper etc. )
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected AbstractModel(Reconciliation reconciliation, HasMetadata resource, String componentName, String componentType, SharedEnvironmentProvider sharedEnvironmentProvider) {
        this(
                reconciliation,
                resource.getMetadata().getName(),
                resource.getMetadata().getNamespace(),
                componentName,
                Labels.generateDefaultLabels(resource, componentName, componentType, STRIMZI_CLUSTER_OPERATOR_NAME),
                ModelUtils.createOwnerReference(resource, false),
                sharedEnvironmentProvider
        );
    }

    /**
     * @return the default Kubernetes resource name.
     */
    public String getComponentName() {
        return componentName;
    }

    /**
     * @return The selector labels as an instance of the Labels object.
     */
    public Labels getSelectorLabels() {
        return labels.strimziSelectorLabels();
    }

    /**
     * @return The image name.
     */
    public String getImage() {
        return image;
    }

    /**
     * @return the cluster name.
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * Gets the name of a given pod in a StrimziPodSet.
     *
     * @param podId The ID (ordinal) of the pod.
     * @return The name of the pod with the given name.
     */
    public String getPodName(Integer podId) {
        return componentName + "-" + podId;
    }

    /**
     * @param cluster The cluster name
     * @return The name of the Cluster CA certificate secret.
     */
    public static String clusterCaCertSecretName(String cluster)  {
        return KafkaResources.clusterCaCertificateSecretName(cluster);
    }

    /**
     * @param cluster The cluster name
     * @return The name of the Cluster CA key secret.
     */
    public static String clusterCaKeySecretName(String cluster)  {
        return KafkaResources.clusterCaKeySecretName(cluster);
    }

    /**
     * @return The service account.
     */
    public ServiceAccount generateServiceAccount() {
        return ServiceAccountUtils.createServiceAccount(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateServiceAccount
        );
    }
}
