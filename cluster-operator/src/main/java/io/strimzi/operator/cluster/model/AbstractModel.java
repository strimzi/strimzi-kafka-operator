/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSource;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetUpdateStrategyBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudgetBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverride;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.api.kafka.model.template.PodManagementPolicy;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractModel {

    public static final String STRIMZI_CLUSTER_OPERATOR_NAME = "strimzi-cluster-operator";

    protected static final Logger log = LogManager.getLogger(AbstractModel.class.getName());

    protected static final String DEFAULT_JVM_XMS = "128M";
    protected static final boolean DEFAULT_JVM_GC_LOGGING_ENABLED = false;

    private static final Long DEFAULT_FS_GROUPID = 0L;

    public static final String ANCILLARY_CM_KEY_METRICS = "metrics-config.yml";
    public static final String ANCILLARY_CM_KEY_LOG_CONFIG = "log4j.properties";
    public static final String ENV_VAR_DYNAMIC_HEAP_FRACTION = "DYNAMIC_HEAP_FRACTION";
    public static final String ENV_VAR_KAFKA_HEAP_OPTS = "KAFKA_HEAP_OPTS";
    public static final String ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS = "KAFKA_JVM_PERFORMANCE_OPTS";
    public static final String ENV_VAR_DYNAMIC_HEAP_MAX = "DYNAMIC_HEAP_MAX";
    public static final String NETWORK_POLICY_KEY_SUFFIX = "-network-policy";
    public static final String ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED = "STRIMZI_KAFKA_GC_LOG_ENABLED";
    public static final String ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES = "STRIMZI_JAVA_SYSTEM_PROPERTIES";
    public static final String ENV_VAR_STRIMZI_JAVA_OPTS = "STRIMZI_JAVA_OPTS";
    public static final String ENV_VAR_STRIMZI_GC_LOG_ENABLED = "STRIMZI_GC_LOG_ENABLED";

    public static final String ANNO_STRIMZI_IO_DELETE_CLAIM = Annotations.STRIMZI_DOMAIN + "delete-claim";
    /** Annotation on PVCs storing the original configuration (so we can revert changes). */
    public static final String ANNO_STRIMZI_IO_STORAGE = Annotations.STRIMZI_DOMAIN + "storage";
    @Deprecated
    public static final String ANNO_CO_STRIMZI_IO_DELETE_CLAIM = ClusterOperator.STRIMZI_CLUSTER_OPERATOR_DOMAIN + "/delete-claim";

    public static final String ANNO_STRIMZI_CM_GENERATION = Annotations.STRIMZI_DOMAIN + "cm-generation";
    public static final String ANNO_STRIMZI_LOGGING_HASH = Annotations.STRIMZI_DOMAIN + "logging-hash";

    protected final String cluster;
    protected final String namespace;

    // Docker image configuration
    protected String image;
    // Number of replicas
    protected int replicas;

    protected String readinessPath;
    protected String livenessPath;

    protected String serviceName;
    protected String headlessServiceName;
    protected String name;

    protected static final int METRICS_PORT = 9404;
    protected static final String METRICS_PORT_NAME = "tcp-prometheus";
    protected boolean isMetricsEnabled;

    protected static final int JMX_PORT = 9999;
    protected static final String JMX_PORT_NAME = "jmx";

    protected Iterable<Map.Entry<String, Object>> metricsConfig;
    protected String ancillaryConfigName;

    protected Storage storage;

    protected AbstractConfiguration configuration;

    protected String mountPath;
    public static final String VOLUME_NAME = "data";
    protected String logAndMetricsConfigMountPath;

    protected String logAndMetricsConfigVolumeName;

    private JvmOptions jvmOptions;
    private ResourceRequirements resources;
    private Affinity userAffinity;
    private List<Toleration> tolerations;

    private Logging logging;
    protected boolean gcLoggingEnabled = true;
    protected List<SystemProperty> javaSystemProperties = null;

    protected Labels labels;
    // Templates
    protected Map<String, String> templateStatefulSetLabels;
    protected Map<String, String> templateStatefulSetAnnotations;
    protected Map<String, String> templateDeploymentLabels;
    protected Map<String, String> templateDeploymentAnnotations;
    protected Map<String, String> templatePodLabels;
    protected Map<String, String> templatePodAnnotations;
    protected Map<String, String> templateServiceLabels;
    protected Map<String, String> templateServiceAnnotations;
    protected Map<String, String> templateHeadlessServiceLabels;
    protected Map<String, String> templateHeadlessServiceAnnotations;
    protected List<LocalObjectReference> templateImagePullSecrets;
    protected PodSecurityContext templateSecurityContext;
    protected int templateTerminationGracePeriodSeconds = 30;
    protected Map<String, String> templatePersistentVolumeClaimLabels;
    protected Map<String, String> templatePersistentVolumeClaimAnnotations;
    protected Map<String, String> templatePodDisruptionBudgetLabels;
    protected Map<String, String> templatePodDisruptionBudgetAnnotations;
    protected int templatePodDisruptionBudgetMaxUnavailable = 1;
    protected String templatePodPriorityClassName;
    protected String templatePodSchedulerName;
    protected PodManagementPolicy templatePodManagementPolicy = PodManagementPolicy.PARALLEL;

    // Owner Reference information
    private String ownerApiVersion;
    private String ownerKind;
    private String ownerUid;

    protected io.strimzi.api.kafka.model.Probe readinessProbeOptions;
    protected io.strimzi.api.kafka.model.Probe livenessProbeOptions;

    /**
     * Constructor
     *
     * @param resource         Kubernetes/OpenShift resource with metadata containing the namespace and cluster name
     * @param applicationName  Name of the application that the extending class is deploying
     */
    protected AbstractModel(HasMetadata resource, String applicationName) {
        this.cluster = resource.getMetadata().getName();
        this.namespace = resource.getMetadata().getNamespace();
        this.labels = Labels.generateDefaultLabels(resource, applicationName, STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    protected void setImage(String image) {
        this.image = image;
    }

    protected void setReadinessProbe(io.strimzi.api.kafka.model.Probe probe) {
        this.readinessProbeOptions = probe;
    }

    protected void setLivenessProbe(io.strimzi.api.kafka.model.Probe probe) {
        this.livenessProbeOptions = probe;
    }

    /**
     * @return the Docker image which should be used by this cluster
     */
    public String getName() {
        return name;
    }

    /**
     * @return The service name.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * @return The name of the headless service.
     */
    public String getHeadlessServiceName() {
        return headlessServiceName;
    }

    /**
     * @return The selector labels as an instance of the Labels object.
     */
    public Labels getSelectorLabels() {
        return getLabelsWithStrimziName(name, Collections.emptyMap()).strimziSelectorLabels();
    }

    /**
     * @param name the value for the {@code strimzi.io/name} key
     * @param additionalLabels a nullable map of additional labels to be added to this instance of Labels
     *
     * @return Labels object with the default labels merged with the provided additional labels and the new {@code strimzi.io/name} label
     */
    protected Labels getLabelsWithStrimziName(String name, Map<String, String> additionalLabels) {
        return labels.withStrimziName(name).withAdditionalLabels(additionalLabels);
    }

    /**
     * @param name the value for the {@code strimzi.io/name} key
     * @param additionalLabels a nullable map of additional labels to be added to this instance of Labels
     *
     * @return Labels object with the default labels merged with the provided additional labels, the new {@code strimzi.io/name} label
     * and {@code strimzi.io/discovery} set to true to make the service discoverable
     */
    protected Labels getLabelsWithStrimziNameAndDiscovery(String name, Map<String, String> additionalLabels) {
        return getLabelsWithStrimziName(name, additionalLabels).withStrimziDiscovery();
    }


    /**
     * @return Whether metrics are enabled.
     */
    public boolean isMetricsEnabled() {
        return isMetricsEnabled;
    }

    protected void setMetricsEnabled(boolean isMetricsEnabled) {
        this.isMetricsEnabled = isMetricsEnabled;
    }

    protected void setGcLoggingEnabled(boolean gcLoggingEnabled) {
        this.gcLoggingEnabled = gcLoggingEnabled;
    }

    protected void setJavaSystemProperties(List<SystemProperty> javaSystemProperties) {
        this.javaSystemProperties = javaSystemProperties;
    }

    protected abstract String getDefaultLogConfigFileName();

    /**
     * Returns map with all available loggers for current pod and default values.
     * @return
     */
    protected OrderedProperties getDefaultLogConfig() {
        return getOrderedProperties(getDefaultLogConfigFileName());
    }

    /**
     * @param configFileName The filename
     * @return The OrderedProperties
     */
    public static OrderedProperties getOrderedProperties(String configFileName) {
        OrderedProperties properties = new OrderedProperties();
        if (configFileName != null && !configFileName.isEmpty()) {
            InputStream is = AbstractModel.class.getResourceAsStream("/" + configFileName);
            if (is == null) {
                log.warn("Cannot find resource '{}'", configFileName);
            } else {
                try {
                    properties.addStringPairs(is);
                } catch (IOException e) {
                    log.warn("Unable to read default log config from '{}'", configFileName);
                } finally {
                    try {
                        is.close();
                    } catch (IOException e) {
                        log.error("Failed to close stream. Reason: " + e.getMessage());
                    }
                }
            }
        }
        return properties;
    }

    /**
     * Transforms map to log4j properties file format
     * @param properties map with properties
     * @return
     */
    protected static String createPropertiesString(OrderedProperties properties) {
        return properties.asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding kubernetes/openshift resource.");
    }

    /**
     * @return The logging.
     */
    public Logging getLogging() {
        return logging;
    }

    protected void setLogging(Logging logging) {
        this.logging = logging;
    }

    /**
     * @param logging The logging to parse.
     * @param externalCm The external ConfigMap.
     * @return The logging properties as a String in log4j/2 properties file format.
     */
    public String parseLogging(Logging logging, ConfigMap externalCm) {
        if (logging instanceof InlineLogging) {
            OrderedProperties newSettings = getDefaultLogConfig();
            newSettings.addMapPairs(((InlineLogging) logging).getLoggers());
            return createPropertiesString(newSettings);
        } else if (logging instanceof ExternalLogging) {
            if (externalCm != null && externalCm.getData() != null && externalCm.getData().containsKey(getAncillaryConfigMapKeyLogConfig())) {
                return externalCm.getData().get(getAncillaryConfigMapKeyLogConfig());
            } else {
                log.warn("ConfigMap {} with external logging configuration does not exist or doesn't contain the configuration under the {} key. Default logging settings are used.", ((ExternalLogging) getLogging()).getName(), getAncillaryConfigMapKeyLogConfig());
                return createPropertiesString(getDefaultLogConfig());
            }

        } else {
            log.debug("logging is not set, using default loggers");
            return createPropertiesString(getDefaultLogConfig());
        }
    }

    /**
     * Generates a metrics and logging ConfigMap according to configured defaults.
     * @param cm The ConfigMap.
     * @return The generated ConfigMap.
     */
    public ConfigMap generateMetricsAndLogConfigMap(ConfigMap cm) {
        Map<String, String> data = new HashMap<>();
        data.put(getAncillaryConfigMapKeyLogConfig(), parseLogging(getLogging(), cm));
        if (isMetricsEnabled()) {
            HashMap<String, Object> m = new HashMap<>();
            for (Map.Entry<String, Object> entry : getMetricsConfig()) {
                m.put(entry.getKey(), entry.getValue());
            }
            data.put(ANCILLARY_CM_KEY_METRICS, new JsonObject(m).toString());
        }

        return createConfigMap(getAncillaryConfigName(), data);
    }

    protected Iterable<Map.Entry<String, Object>>  getMetricsConfig() {
        return metricsConfig;
    }

    protected void setMetricsConfig(Iterable<Map.Entry<String, Object>> metricsConfig) {
        this.metricsConfig = metricsConfig;
    }

    /**
     * Returns name of config map used for storing metrics and logging configuration.
     * @return The name of config map used for storing metrics and logging configuration.
     */
    public String getAncillaryConfigName() {
        return ancillaryConfigName;
    }

    protected List<EnvVar> getEnvVars() {
        return null;
    }

    /**
     * @return The storage.
     */
    public Storage getStorage() {
        return storage;
    }

    /**
     * Set Storage
     *
     * @param storage   Persistent Storage configuration
     */
    protected void setStorage(Storage storage) {
        validatePersistentStorage(storage);
        this.storage = storage;
    }

    /**
     * Validates persistent storage
     *
     * @param storage   Persistent Storage configuration
     */
    protected static void validatePersistentStorage(Storage storage)   {
        if (storage instanceof PersistentClaimStorage) {
            PersistentClaimStorage persistentClaimStorage = (PersistentClaimStorage) storage;
            checkPersistentStorageSize(persistentClaimStorage);
        } else if (storage instanceof JbodStorage)  {
            JbodStorage jbodStorage = (JbodStorage) storage;

            if (jbodStorage.getVolumes().size() == 0)   {
                throw new InvalidResourceException("JbodStorage needs to contain at least one volume!");
            }

            for (Storage jbodVolume : jbodStorage.getVolumes()) {
                if (jbodVolume instanceof PersistentClaimStorage) {
                    PersistentClaimStorage persistentClaimStorage = (PersistentClaimStorage) jbodVolume;
                    checkPersistentStorageSize(persistentClaimStorage);
                }
            }
        }
    }

    private static void checkPersistentStorageSize(PersistentClaimStorage storage)   {
        if (storage.getSize() == null || storage.getSize().isEmpty()) {
            throw new InvalidResourceException("The size is mandatory for a persistent-claim storage");
        }
    }

    /**
     * Returns the Configuration object which is passed to the cluster as EnvVar
     *
     * @return  Configuration object with cluster configuration
     */
    public AbstractConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Set the configuration object which might be passed to the cluster as EnvVar
     *
     * @param configuration Configuration object with cluster configuration
     */
    protected void setConfiguration(AbstractConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * @return The image name.
     */
    public String getImage() {
        return this.image;
    }

    /**
     * @return the service account used by the deployed cluster for Kubernetes/OpenShift API operations
     */
    protected String getServiceAccountName() {
        return null;
    }

    /**
     * @return the cluster name
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * Gets the name of a given pod in a StatefulSet.
     * @param podId The Id of the pod.
     * @return The name of the pod with the given name.
     */
    public String getPodName(int podId) {
        return name + "-" + podId;
    }

    /**
     * Sets the affinity as configured by the user in the cluster CR
     * @param affinity
     */
    protected void setUserAffinity(Affinity affinity) {
        this.userAffinity = affinity;
    }

    /**
     * Gets the affinity as configured by the user in the cluster CR
     */
    protected Affinity getUserAffinity() {
        return this.userAffinity;
    }

    /**
     * Gets the tolerations as configured by the user in the cluster CR
     * @return The tolerations.
     */
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    /**
     * Sets the tolerations as configured by the user in the cluster CR
     *
     * @param tolerations The tolerations.
     */
    public void setTolerations(List<Toleration> tolerations) {
        this.tolerations = tolerations;
    }

    /**
     * Gets the affinity to use in a template Pod (in a StatefulSet, or Deployment).
     * In general this may include extra rules than just the {@link #userAffinity}.
     * By default it is just the {@link #userAffinity}.
     */
    protected Affinity getMergedAffinity() {
        return getUserAffinity();
    }

    /**
     * @return a list of init containers to add to the StatefulSet/Deployment
     */
    protected List<Container> getInitContainers(ImagePullPolicy imagePullPolicy) {
        return null;
    }

    /**
     * @return a list of containers to add to the StatefulSet/Deployment
     */
    protected abstract List<Container> getContainers(ImagePullPolicy imagePullPolicy);

    protected ContainerPort createContainerPort(String name, int port, String protocol) {
        ContainerPort containerPort = new ContainerPortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withContainerPort(port)
                .build();
        log.trace("Created container port {}", containerPort);
        return containerPort;
    }

    protected ServicePort createServicePort(String name, int port, int targetPort, String protocol) {
        ServicePort servicePort = createServicePort(name, port, targetPort, null, protocol);
        log.trace("Created service port {}", servicePort);
        return servicePort;
    }

    protected ServicePort createServicePort(String name, int port, int targetPort, Integer nodePort, String protocol) {
        ServicePortBuilder builder = new ServicePortBuilder()
            .withName(name)
            .withProtocol(protocol)
            .withPort(port)
            .withNewTargetPort(targetPort);
        if (nodePort != null) {
            builder.withNodePort(nodePort);
        }
        ServicePort servicePort = builder.build();
        log.trace("Created service port {}", servicePort);
        return servicePort;
    }

    protected PersistentVolumeClaim createPersistentVolumeClaim(int podNumber, String name, PersistentClaimStorage storage) {
        Map<String, Quantity> requests = new HashMap<>();
        requests.put("storage", new Quantity(storage.getSize(), null));

        LabelSelector selector = null;
        if (storage.getSelector() != null && !storage.getSelector().isEmpty()) {
            selector = new LabelSelector(null, storage.getSelector());
        }

        String storageClass = storage.getStorageClass();
        if (storage.getOverrides() != null) {
            storageClass = storage.getOverrides().stream().filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == podNumber && broker.getStorageClass() != null)
                    .map(PersistentClaimStorageOverride::getStorageClass)
                    .findAny()
                    .orElse(storageClass);
        }

        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    // labels with the Strimzi name set to that of the component
                    .withLabels(getLabelsWithStrimziName(this.name, templatePersistentVolumeClaimLabels).toMap())
                    .withAnnotations(mergeLabelsOrAnnotations(Collections.singletonMap(ANNO_STRIMZI_IO_DELETE_CLAIM, Boolean.toString(storage.isDeleteClaim())), templatePersistentVolumeClaimAnnotations))
                .endMetadata()
                .withNewSpec()
                    .withAccessModes("ReadWriteOnce")
                    .withNewResources()
                        .withRequests(requests)
                    .endResources()
                    .withStorageClassName(storageClass)
                    .withSelector(selector)
                .endSpec()
                .build();

        if (storage.isDeleteClaim())    {
            pvc.getMetadata().setOwnerReferences(Collections.singletonList(createOwnerReference()));
        }

        return pvc;
    }

    protected ConfigMap createConfigMap(String name, Map<String, String> data) {

        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withData(data)
                .build();
    }

    protected Secret createSecret(String name, Map<String, String> data) {
        return ModelUtils.createSecret(name, namespace, labels, createOwnerReference(), data);
    }

    protected Service createService(String type, List<ServicePort> ports, Map<String, String> annotations) {
        return createService(serviceName, type, ports, getLabelsWithStrimziName(serviceName, templateServiceLabels),
                getSelectorLabels(), annotations);
    }

    protected Service createDiscoverableService(String type, List<ServicePort> ports, Map<String, String> annotations) {
        return createService(serviceName, type, ports, getLabelsWithStrimziNameAndDiscovery(serviceName, templateServiceLabels),
                getSelectorLabels(), annotations);
    }

    protected Service createService(String name, String type, List<ServicePort> ports, Labels labels, Labels selector, Map<String, String> annotations) {
        return createService(name, type, ports, labels, selector, annotations, null);
    }

    protected Service createService(String name, String type, List<ServicePort> ports, Labels labels, Labels selector, Map<String, String> annotations, String loadBalancerIP) {
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.toMap())
                    .withNamespace(namespace)
                    .withAnnotations(annotations)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withType(type)
                    .withSelector(selector.toMap())
                    .withPorts(ports)
                    .withLoadBalancerIP(loadBalancerIP)
                .endSpec()
                .build();
        log.trace("Created service {}", service);
        return service;
    }

    protected Service createHeadlessService(List<ServicePort> ports) {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(headlessServiceName)
                    .withLabels(getLabelsWithStrimziName(headlessServiceName, templateHeadlessServiceLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(mergeLabelsOrAnnotations(annotations, templateHeadlessServiceAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withClusterIP("None")
                    .withSelector(getSelectorLabels().toMap())
                    .withPorts(ports)
                    .withPublishNotReadyAddresses(true)
                .endSpec()
                .build();
        log.trace("Created headless service {}", service);
        return service;
    }

    protected StatefulSet createStatefulSet(
            Map<String, String> stsAnnotations,
            Map<String, String> podAnnotations,
            List<Volume> volumes,
            List<PersistentVolumeClaim> volumeClaims,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<LocalObjectReference> imagePullSecrets,
            boolean isOpenShift) {

        PodSecurityContext securityContext = templateSecurityContext;

        // if a persistent volume claim is requested and the running cluster is a Kubernetes one and we have no user configured PodSecurityContext
        // we set the security context
        if (ModelUtils.containsPersistentStorage(storage) && !isOpenShift && securityContext == null) {
            securityContext = new PodSecurityContextBuilder()
                    .withFsGroup(AbstractModel.DEFAULT_FS_GROUPID)
                    .build();
        }

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templateStatefulSetLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(mergeLabelsOrAnnotations(stsAnnotations, templateStatefulSetAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withPodManagementPolicy(templatePodManagementPolicy.toValue())
                    .withUpdateStrategy(new StatefulSetUpdateStrategyBuilder().withType("OnDelete").build())
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                    .withServiceName(headlessServiceName)
                    .withReplicas(replicas)
                    .withNewTemplate()
                        .withNewMetadata()
                            .withName(name)
                            .withLabels(getLabelsWithStrimziName(name, templatePodLabels).toMap())
                            .withAnnotations(mergeLabelsOrAnnotations(podAnnotations, templatePodAnnotations))
                        .endMetadata()
                        .withNewSpec()
                            .withServiceAccountName(getServiceAccountName())
                            .withAffinity(affinity)
                            .withInitContainers(initContainers)
                            .withContainers(containers)
                            .withVolumes(volumes)
                            .withTolerations(getTolerations())
                            .withTerminationGracePeriodSeconds(Long.valueOf(templateTerminationGracePeriodSeconds))
                            .withImagePullSecrets(templateImagePullSecrets != null ? templateImagePullSecrets : imagePullSecrets)
                            .withSecurityContext(securityContext)
                            .withPriorityClassName(templatePodPriorityClassName)
                            .withSchedulerName(templatePodSchedulerName != null ? templatePodSchedulerName : "default-scheduler")
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(volumeClaims)
                .endSpec()
                .build();

        return statefulSet;
    }

    protected Deployment createDeployment(
            DeploymentStrategy updateStrategy,
            Map<String, String> deploymentAnnotations,
            Map<String, String> podAnnotations,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<Volume> volumes,
            List<LocalObjectReference> imagePullSecrets) {

        Deployment dep = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templateDeploymentLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(mergeLabelsOrAnnotations(deploymentAnnotations, templateDeploymentAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withStrategy(updateStrategy)
                    .withReplicas(replicas)
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(getLabelsWithStrimziName(name, templatePodLabels).toMap())
                            .withAnnotations(mergeLabelsOrAnnotations(podAnnotations, templatePodAnnotations))
                        .endMetadata()
                        .withNewSpec()
                            .withAffinity(affinity)
                            .withServiceAccountName(getServiceAccountName())
                            .withInitContainers(initContainers)
                            .withContainers(containers)
                            .withVolumes(volumes)
                            .withTolerations(getTolerations())
                            .withTerminationGracePeriodSeconds(Long.valueOf(templateTerminationGracePeriodSeconds))
                            .withImagePullSecrets(templateImagePullSecrets != null ? templateImagePullSecrets : imagePullSecrets)
                            .withSecurityContext(templateSecurityContext)
                            .withPriorityClassName(templatePodPriorityClassName)
                            .withSchedulerName(templatePodSchedulerName)
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        return dep;
    }

    /**
     * Build an environment variable instance with the provided name and value
     *
     * @param name The name of the environment variable
     * @param value The value of the environment variable
     * @return The environment variable instance
     */
    protected static EnvVar buildEnvVar(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    /**
     * Build an environment variable instance which will use a value from a secret
     *
     * @param name The name of the environment variable
     * @param secret The name of the secret which should be used
     * @param key The key under which the value is stored in the secret
     * @return The environment variable instance
     */
    protected static EnvVar buildEnvVarFromSecret(String name, String secret, String key) {
        return new EnvVarBuilder()
                .withName(name)
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(secret)
                        .withKey(key)
                    .endSecretKeyRef()
                .endValueFrom()
                .build();
    }

    /**
     * Build an environment variable instance with the provided name from a field reference
     * using Downward API
     *
     * @param name The name of the environment variable
     * @param field The field path from which getting the value
     * @return The environment variable instance
     */
    protected static EnvVar buildEnvVarFromFieldRef(String name, String field) {

        EnvVarSource envVarSource = new EnvVarSourceBuilder()
                .withNewFieldRef()
                    .withFieldPath(field)
                .endFieldRef()
                .build();

        return new EnvVarBuilder().withName(name).withValueFrom(envVarSource).build();
    }

    /**
     * Gets the given container's environment.
     * @param container The container
     * @return The environment of the given container.
     */
    public static Map<String, String> containerEnvVars(Container container) {
        return container.getEnv().stream().collect(
            Collectors.toMap(EnvVar::getName, EnvVar::getValue,
                // On duplicates, last in wins
                (u, v) -> v));
    }

    public Labels getLabels() {
        return labels;
    }

    public void setLabels(Labels labels) {
        this.labels = labels;
    }

    /**
     * @param resources The resource requirements.
     */
    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    /**
     * @return The resource requirements.
     */
    public ResourceRequirements getResources() {
        return resources;
    }

    /**
     * @param jvmOptions The JVM options.
     */
    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }

    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    /**
     * Adds KAFKA_HEAP_OPTS variable to the EnvVar list if any heap related options were specified.
     *
     * @param envVars List of Environment Variables
     */
    protected void heapOptions(List<EnvVar> envVars, double dynamicHeapFraction, long dynamicHeapMaxBytes) {
        StringBuilder kafkaHeapOpts = new StringBuilder();
        String xms = jvmOptions != null ? jvmOptions.getXms() : null;

        if (xms != null) {
            kafkaHeapOpts.append("-Xms").append(xms);
        }

        String xmx = jvmOptions != null ? jvmOptions.getXmx() : null;
        if (xmx != null) {
            // Honour explicit max heap
            kafkaHeapOpts.append(' ').append("-Xmx").append(xmx);
        } else {
            ResourceRequirements resources = getResources();
            Map<String, Quantity> cpuMemory = resources == null ? null : resources.getRequests();
            // Delegate to the container to figure out only when CGroup memory limits are defined to prevent allocating
            // too much memory on the kubelet.
            if (cpuMemory != null && cpuMemory.get("memory") != null) {
                envVars.add(buildEnvVar(ENV_VAR_DYNAMIC_HEAP_FRACTION, Double.toString(dynamicHeapFraction)));
                if (dynamicHeapMaxBytes > 0) {
                    envVars.add(buildEnvVar(ENV_VAR_DYNAMIC_HEAP_MAX, Long.toString(dynamicHeapMaxBytes)));
                }
            // When no memory limit, `Xms`, and `Xmx` are defined then set a default `Xms` and
            // leave `Xmx` undefined.
            } else if (xms == null) {
                kafkaHeapOpts.append("-Xms").append(DEFAULT_JVM_XMS);
            }
        }

        String trim = kafkaHeapOpts.toString().trim();
        if (!trim.isEmpty()) {
            envVars.add(buildEnvVar(ENV_VAR_KAFKA_HEAP_OPTS, trim));
        }
    }

    /**
     * Adds KAFKA_JVM_PERFORMANCE_OPTS variable to the EnvVar list if any performance related options were specified.
     *
     * @param envVars List of Environment Variables
     */
    protected void jvmPerformanceOptions(List<EnvVar> envVars) {
        StringBuilder jvmPerformanceOpts = new StringBuilder();
        Boolean server = jvmOptions != null ? jvmOptions.isServer() : null;

        if (server != null && server) {
            jvmPerformanceOpts.append("-server");
        }

        Map<String, String> xx = jvmOptions != null ? jvmOptions.getXx() : null;
        if (xx != null) {
            xx.forEach((k, v) -> {
                jvmPerformanceOpts.append(' ').append("-XX:");

                if ("true".equalsIgnoreCase(v))   {
                    jvmPerformanceOpts.append("+").append(k);
                } else if ("false".equalsIgnoreCase(v)) {
                    jvmPerformanceOpts.append("-").append(k);
                } else  {
                    jvmPerformanceOpts.append(k).append("=").append(v);
                }
            });
        }

        String trim = jvmPerformanceOpts.toString().trim();
        if (!trim.isEmpty()) {
            envVars.add(buildEnvVar(ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS, trim));
        }
    }

    /**
     * Generate the OwnerReference object to link newly created objects to their parent (the custom resource)
     *
     * @return
     */
    protected OwnerReference createOwnerReference() {
        return new OwnerReferenceBuilder()
                .withApiVersion(ownerApiVersion)
                .withKind(ownerKind)
                .withName(cluster)
                .withUid(ownerUid)
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();
    }

    /**
     * Set fields needed to generate the OwnerReference object
     *
     * @param parent The resource which should be used as parent. It will be used to gather the date needed for generating OwnerReferences.
     */
    protected void setOwnerReference(HasMetadata parent)  {
        this.ownerApiVersion = parent.getApiVersion();
        this.ownerKind = parent.getKind();
        this.ownerUid = parent.getMetadata().getUid();
    }

    /**
     * Generated a Map with Prometheus annotations
     *
     * @return Map with Prometheus annotations using the default port (9404) and path (/metrics)
     */
    protected Map<String, String> getPrometheusAnnotations()    {
        if (isMetricsEnabled) {
            Map<String, String> annotations = new HashMap<String, String>(3);

            annotations.put("prometheus.io/port", String.valueOf(METRICS_PORT));
            annotations.put("prometheus.io/scrape", "true");
            annotations.put("prometheus.io/path", "/metrics");

            return annotations;
        } else {
            return null;
        }
    }

    /**
     * Creates the PodDisruptionBudget
     *
     * @return
     */
    protected PodDisruptionBudget createPodDisruptionBudget()   {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templatePodDisruptionBudgetLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(templatePodDisruptionBudgetAnnotations)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(templatePodDisruptionBudgetMaxUnavailable)
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }

    /**
     * When ImagePullPolicy is not specified by the user, Kubernetes will automatically set it based on the image
     *    :latest results in Always
     *    anything else results in IfNotPresent
     * This causes issues in diffing. So we emulate here the Kubernetes defaults and set the policy accoridngly already on our side.
     *
     * @param requestedImagePullPolicy  The imagePullPolicy requested by the user (is always preferred when set, ignored when null)
     * @param image The image used for the container. From its tag we determine the default policy
     * @return  The Image Pull Policy: Always, Never or IfNotPresent
     */
    protected String determineImagePullPolicy(ImagePullPolicy requestedImagePullPolicy, String image)  {
        if (requestedImagePullPolicy != null)   {
            return requestedImagePullPolicy.toString();
        }

        if (image.toLowerCase(Locale.ENGLISH).endsWith(":latest"))  {
            return ImagePullPolicy.ALWAYS.toString();
        } else {
            return ImagePullPolicy.IFNOTPRESENT.toString();
        }
    }

    String getAncillaryConfigMapKeyLogConfig() {
        return ANCILLARY_CM_KEY_LOG_CONFIG;
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

    @SafeVarargs
    protected static Map<String, String> mergeLabelsOrAnnotations(Map<String, String> internal, Map<String, String>... templates) {
        
        Map<String, String> merged = new HashMap<>();

        if (internal != null) {
            merged.putAll(internal);
        }

        if (templates != null) {
            for (Map<String, String> template : templates) {

                if (template == null) {
                    continue;
                }
                List<String> invalidAnnotations = template
                    .keySet()
                    .stream()
                    .filter(key -> key.startsWith(Labels.STRIMZI_DOMAIN))
                    .collect(Collectors.toList());
                if (invalidAnnotations.size() > 0) {
                    throw new InvalidResourceException("User labels or annotations includes a Strimzi annotation: " + invalidAnnotations.toString());
                }

                // Remove Kubernetes Domain specific labels
                Map<String, String> filteredTemplate = template
                    .entrySet()
                    .stream()
                    .filter(entryset -> !entryset.getKey().startsWith(Labels.KUBERNETES_DOMAIN))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                merged.putAll(filteredTemplate);
            }
        }

        return merged;
    }

    /**
     * @return The service account.
     */
    public ServiceAccount generateServiceAccount() {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(getServiceAccountName())
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                    .withLabels(labels.toMap())
                .endMetadata()
            .build();
    }

    /**
     * Adds the supplied list of container environment variables {@see io.strimzi.api.kafka.model.ContainerEnvVar} to the
     * supplied list of fabric8 environment variables {@see io.fabric8.kubernetes.api.model.EnvVar}, checking first if the
     * environment variable key has already been set in the existing list and then converts them. If a key is already in
     * use then the container environment variable will not be added to the environment variable list and a warning will
     * be logged.
     *
     * @param existingEnvs The list of fabric8 environment variable object that will be modified.
     * @param containerEnvs The list of container environment variable objects to be converted and added to the existing
     *                      environment variable list
     **/
    protected void addContainerEnvsToExistingEnvs(List<EnvVar> existingEnvs, List<ContainerEnvVar> containerEnvs) {

        if (containerEnvs != null) {
            // Create set of env var names to test if any user defined template env vars will conflict with those set above
            Set<String> predefinedEnvs = new HashSet<String>();
            for (EnvVar envVar : existingEnvs) {
                predefinedEnvs.add(envVar.getName());
            }

            // Set custom env vars from the user defined template
            for (ContainerEnvVar containerEnvVar : containerEnvs) {
                if (predefinedEnvs.contains(containerEnvVar.getName())) {
                    log.warn("User defined container template environment variable " + containerEnvVar.getName() +
                            " is already in use and will be ignored");
                } else {
                    existingEnvs.add(buildEnvVar(containerEnvVar.getName(), containerEnvVar.getValue()));
                }
            }
        }
    }
}
