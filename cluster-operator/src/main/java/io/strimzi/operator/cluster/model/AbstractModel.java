/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderFactory;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * AbstractModel an abstract base model for all components of the {@code Kafka} custom resource
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
public abstract class AbstractModel {
    /**
     * Name of the Strimzi Cluster operator a used in various labels
     */
    public static final String STRIMZI_CLUSTER_OPERATOR_NAME = "strimzi-cluster-operator";

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractModel.class.getName());
    protected static final String LOG4J2_MONITOR_INTERVAL = "30";

    protected static final String DEFAULT_JVM_XMS = "128M";
    protected static final boolean DEFAULT_JVM_GC_LOGGING_ENABLED = false;

    /**
     * Init container related configuration
     */
    protected static final String INIT_NAME = "kafka-init";
    protected static final String INIT_VOLUME_NAME = "rack-volume";
    protected static final String INIT_VOLUME_MOUNT = "/opt/kafka/init";
    protected static final String ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    protected static final String ENV_VAR_KAFKA_INIT_NODE_NAME = "NODE_NAME";

    /**
     * Key under which the metrics configuration is stored in the ConfigMap
     */
    public static final String ANCILLARY_CM_KEY_METRICS = "metrics-config.json";
    /**
     * Key under which the Log4j properties are stored in the ConfigMap
     */
    public static final String ANCILLARY_CM_KEY_LOG_CONFIG = "log4j.properties";

    protected static final String ENV_VAR_DYNAMIC_HEAP_PERCENTAGE = "STRIMZI_DYNAMIC_HEAP_PERCENTAGE";
    protected static final String ENV_VAR_KAFKA_HEAP_OPTS = "KAFKA_HEAP_OPTS";
    protected static final String ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS = "KAFKA_JVM_PERFORMANCE_OPTS";
    protected static final String ENV_VAR_DYNAMIC_HEAP_MAX = "STRIMZI_DYNAMIC_HEAP_MAX";
    protected static final String ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED = "STRIMZI_KAFKA_GC_LOG_ENABLED";
    protected static final String ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES = "STRIMZI_JAVA_SYSTEM_PROPERTIES";
    protected static final String ENV_VAR_STRIMZI_JAVA_OPTS = "STRIMZI_JAVA_OPTS";
    protected static final String ENV_VAR_STRIMZI_GC_LOG_ENABLED = "STRIMZI_GC_LOG_ENABLED";

    /**
     * Configure statically defined environment variables which are passed to all operands.
     * This includes HTTP/HTTPS Proxy env vars or the FIPS_MODE.
     */
    protected static final List<EnvVar> STATIC_ENV_VARS;
    static {
        List<EnvVar> envVars = new ArrayList<>(3);

        if (System.getenv(ClusterOperatorConfig.HTTP_PROXY) != null)    {
            envVars.add(buildEnvVar(ClusterOperatorConfig.HTTP_PROXY, System.getenv(ClusterOperatorConfig.HTTP_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.HTTPS_PROXY) != null)    {
            envVars.add(buildEnvVar(ClusterOperatorConfig.HTTPS_PROXY, System.getenv(ClusterOperatorConfig.HTTPS_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.NO_PROXY) != null)    {
            envVars.add(buildEnvVar(ClusterOperatorConfig.NO_PROXY, System.getenv(ClusterOperatorConfig.NO_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.FIPS_MODE) != null)    {
            envVars.add(buildEnvVar(ClusterOperatorConfig.FIPS_MODE, System.getenv(ClusterOperatorConfig.FIPS_MODE)));
        }

        if (envVars.size() > 0) {
            STATIC_ENV_VARS = Collections.unmodifiableList(envVars);
        } else {
            STATIC_ENV_VARS = Collections.emptyList();
        }
    }

    protected final Reconciliation reconciliation;
    protected final String cluster;
    protected final String namespace;
    protected final String componentName;
    protected final OwnerReference ownerReference;

    protected String serviceName;
    protected String headlessServiceName;

    // Docker image configuration
    protected String image;
    // Number of replicas
    protected int replicas;

    protected Labels labels;

    /**
     * Application configuration
     */
    protected AbstractConfiguration configuration;
    private Logging logging;
    protected boolean gcLoggingEnabled = true;
    private JvmOptions jvmOptions;

    /**
     * Volume and Storage configuration
     */
    protected Storage storage;

    /**
     * Base name used to name data volumes
     */
    public static final String VOLUME_NAME = "data";
    protected String mountPath;

    /**
     * Metrics configuration
     */
    protected boolean isMetricsEnabled;
    protected static final String METRICS_PORT_NAME = "tcp-prometheus";
    protected static final int METRICS_PORT = 9404;
    protected MetricsConfig metricsConfigInCm;
    protected String ancillaryConfigMapName;
    protected String logAndMetricsConfigMountPath;
    protected String logAndMetricsConfigVolumeName;

    /**
     * JMX configuration used for components such as Kafka and JMX Trans
     */
    protected static final String JMX_PORT_NAME = "jmx";
    protected static final int JMX_PORT = 9999;

    /**
     * Container configuration
     */
    private ResourceRequirements resources;
    protected io.strimzi.api.kafka.model.Probe startupProbeOptions;
    protected String readinessPath;
    protected io.strimzi.api.kafka.model.Probe readinessProbeOptions;
    protected String livenessPath;
    protected io.strimzi.api.kafka.model.Probe livenessProbeOptions;

    /**
     * PodSecurityProvider
     */
    protected PodSecurityProvider securityProvider = PodSecurityProviderFactory.getProvider();

    /**
     * Template configuration
     * Used to allow all components to have configurable labels, annotations, security context etc
     */
    protected Map<String, String> templateServiceLabels;
    protected Map<String, String> templateServiceAnnotations;
    protected IpFamilyPolicy templateServiceIpFamilyPolicy;
    protected List<IpFamily> templateServiceIpFamilies;
    protected Map<String, String> templateHeadlessServiceLabels;
    protected Map<String, String> templateHeadlessServiceAnnotations;
    protected IpFamilyPolicy templateHeadlessServiceIpFamilyPolicy;
    protected List<IpFamily> templateHeadlessServiceIpFamilies;
    protected Map<String, String> templateServiceAccountLabels;
    protected Map<String, String> templateServiceAccountAnnotations;
    protected Map<String, String> templateJmxSecretLabels;
    protected Map<String, String> templateJmxSecretAnnotations;

    protected List<Condition> warningConditions = new ArrayList<>(0);

    /**
     * Constructor
     *
     * @param reconciliation    The reconciliation marker
     * @param resource          Custom resource with metadata containing the namespace and cluster name
     * @param componentName     Name of the Strimzi component usually consisting from the cluster name and component type
     * @param componentType     Type of the component that the extending class is deploying (e.g. Kafka, ZooKeeper etc. )
     */
    protected AbstractModel(Reconciliation reconciliation, HasMetadata resource, String componentName, String componentType) {
        this.reconciliation = reconciliation;
        this.cluster = resource.getMetadata().getName();
        this.namespace = resource.getMetadata().getNamespace();
        this.componentName = componentName;
        this.labels = Labels.generateDefaultLabels(resource, componentName, componentType, STRIMZI_CLUSTER_OPERATOR_NAME);
        this.ownerReference = ModelUtils.createOwnerReference(resource, false);
    }

    /**
     * @return The number of replicas
     */
    public int getReplicas() {
        return replicas;
    }

    protected void setReplicas(int replicas) {
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

    protected void setStartupProbe(io.strimzi.api.kafka.model.Probe probe) {
        this.startupProbeOptions = probe;
    }

    /**
     * @return the default Kubernetes resource name.
     */
    public String getComponentName() {
        return componentName;
    }

    /**
     * @return The Kubernetes service name.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * @return The Kubernetes headless service name.
     */
    public String getHeadlessServiceName() {
        return headlessServiceName;
    }

    /**
     * @return The selector labels as an instance of the Labels object.
     */
    public Labels getSelectorLabels() {
        return labels.strimziSelectorLabels();
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

    protected abstract String getDefaultLogConfigFileName();

    /**
     * @return OrderedProperties map with all available loggers for current pod and default values.
     */
    public OrderedProperties getDefaultLogConfig() {
        String logConfigFileName = getDefaultLogConfigFileName();
        if (logConfigFileName == null || logConfigFileName.isEmpty()) {
            return new OrderedProperties();
        }
        return getOrderedProperties(reconciliation, getDefaultLogConfigFileName());
    }

    /**
     * Read a config file and returns the properties in a deterministic order.
     *
     * @param reconciliation The reconciliation
     * @param configFileName The filename.
     * @return The OrderedProperties of the inputted file.
     */
    public static OrderedProperties getOrderedProperties(Reconciliation reconciliation, String configFileName) {
        if (configFileName == null || configFileName.isEmpty()) {
            throw new IllegalArgumentException("configFileName must be non-empty string");
        }
        OrderedProperties properties = new OrderedProperties();
        InputStream is = AbstractModel.class.getResourceAsStream("/" + configFileName);
        if (is == null) {
            LOGGER.warnCr(reconciliation, "Cannot find resource '{}'", configFileName);
        } else {
            try {
                properties.addStringPairs(is);
            } catch (IOException e) {
                LOGGER.warnCr(reconciliation, "Unable to read default log config from '{}'", configFileName);
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    LOGGER.errorCr(reconciliation, "Failed to close stream. Reason: " + e.getMessage());
                }
            }
        }
        return properties;
    }

    /**
     * Transforms map to log4j properties file format.
     *
     * @param properties map of log4j properties.
     * @return log4j properties as a String.
     */
    public String createLog4jProperties(OrderedProperties properties) {
        return properties.asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.");
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
     * Regarding to used implementation we may need to patch an appender.
     * If the user does not provide the appender in tuple logger: level, it should be added and warn message printed.
     * @return true if patching needs to be done due to dynamic configuration, otherwise false
     */
    protected boolean shouldPatchLoggerAppender() {
        return false;
    }

    /**
     * Generates the logging configuration as a String. The configuration is generated based on the default logging
     * configuration files from resources, the (optional) inline logging configuration from the custom resource
     * and the (optional) external logging configuration in a user-provided ConfigMap.
     *
     * @param logging       The logging configuration from the custom resource
     * @param externalCm    The user-provided ConfigMap with custom Log4j / Log4j2 file
     *
     * @return              String with the Log4j / Log4j2 properties used for configuration
     */
    public String loggingConfiguration(Logging logging, ConfigMap externalCm) {
        if (logging instanceof InlineLogging) {
            InlineLogging inlineLogging = (InlineLogging) logging;
            OrderedProperties newSettings = getDefaultLogConfig();

            if (inlineLogging.getLoggers() != null) {
                // Inline logging as specified and some loggers are configured
                if (shouldPatchLoggerAppender()) {
                    String rootAppenderName = getRootAppenderNamesFromDefaultLoggingConfig(newSettings);
                    String newRootLogger = inlineLogging.getLoggers().get("log4j.rootLogger");
                    newSettings.addMapPairs(inlineLogging.getLoggers());

                    if (newRootLogger != null && !rootAppenderName.isEmpty() && !newRootLogger.contains(",")) {
                        // this should never happen as appender name is added in default configuration
                        LOGGER.debugCr(reconciliation, "Newly set rootLogger does not contain appender. Setting appender to {}.", rootAppenderName);
                        String level = newSettings.asMap().get("log4j.rootLogger");
                        newSettings.addPair("log4j.rootLogger", level + ", " + rootAppenderName);
                    }
                } else {
                    newSettings.addMapPairs(inlineLogging.getLoggers());
                }
            }

            return createLog4jProperties(newSettings);
        } else if (logging instanceof ExternalLogging) {
            ExternalLogging externalLogging = (ExternalLogging) logging;
            if (externalLogging.getValueFrom() != null && externalLogging.getValueFrom().getConfigMapKeyRef() != null && externalLogging.getValueFrom().getConfigMapKeyRef().getKey() != null) {
                if (externalCm != null && externalCm.getData() != null && externalCm.getData().containsKey(externalLogging.getValueFrom().getConfigMapKeyRef().getKey())) {
                    return maybeAddMonitorIntervalToExternalLogging(externalCm.getData().get(externalLogging.getValueFrom().getConfigMapKeyRef().getKey()));
                } else {
                    throw new InvalidResourceException(
                        String.format("ConfigMap %s with external logging configuration does not exist or doesn't contain the configuration under the %s key.",
                            externalLogging.getValueFrom().getConfigMapKeyRef().getName(),
                            externalLogging.getValueFrom().getConfigMapKeyRef().getKey())
                    );
                }
            } else {
                throw new InvalidResourceException("Property logging.valueFrom has to be specified when using external logging.");
            }
        } else {
            LOGGER.debugCr(reconciliation, "logging is not set, using default loggers");
            return createLog4jProperties(getDefaultLogConfig());
        }
    }

    private String getRootAppenderNamesFromDefaultLoggingConfig(OrderedProperties newSettings) {
        String logger = newSettings.asMap().get("log4j.rootLogger");
        String appenderName = "";
        if (logger != null) {
            String[] tmp = logger.trim().split(",", 2);
            if (tmp.length == 2) {
                appenderName = tmp[1].trim();
            } else {
                LOGGER.warnCr(reconciliation, "Logging configuration for root logger does not contain appender.");
            }
        } else {
            LOGGER.warnCr(reconciliation, "Logger log4j.rootLogger not set.");
        }
        return appenderName;
    }

    /**
     * Adds 'monitorInterval=30' to external logging ConfigMap. If ConfigMap already has this value, it is persisted.
     *
     * @param data String with log4j2 properties in format key=value separated by new lines
     * @return log4j2 configuration with monitorInterval property
     */
    protected String maybeAddMonitorIntervalToExternalLogging(String data) {
        OrderedProperties orderedProperties = new OrderedProperties();
        orderedProperties.addStringPairs(data);

        Optional<String> mi = orderedProperties.asMap().keySet().stream()
                .filter(key -> key.matches("^monitorInterval$")).findFirst();
        if (mi.isPresent()) {
            return data;
        } else {
            // do not override custom value
            return data + "\nmonitorInterval=" + LOG4J2_MONITOR_INTERVAL + "\n";
        }
    }

    /**
     * Generates a metrics and logging ConfigMap according to configured defaults. This is used with most operands, but
     * not all of them. Kafka brokers have own methods in the KafkaCluster class. So does the Bridge. And Kafka Exporter
     * has no metrics or logging ConfigMap at all.
     *
     * @param metricsAndLogging The external CMs
     * @return The generated ConfigMap.
     */
    public ConfigMap generateMetricsAndLogConfigMap(MetricsAndLogging metricsAndLogging) {
        Map<String, String> data = new HashMap<>(2);
        data.put(getAncillaryConfigMapKeyLogConfig(), loggingConfiguration(getLogging(), metricsAndLogging.getLoggingCm()));
        if (getMetricsConfigInCm() != null) {
            String parseResult = metricsConfiguration(metricsAndLogging.getMetricsCm());
            if (parseResult != null) {
                this.setMetricsEnabled(true);
                data.put(ANCILLARY_CM_KEY_METRICS, parseResult);
            }
        }
        return createConfigMap(ancillaryConfigMapName, data);
    }

    /**
     * Generates Prometheus metrics configuration based on the JMXExporter configuration from the user-provided ConfigMap.
     *
     * @param externalCm    ConfigMap with the JMX Prometheus configuration YAML
     *
     * @return              String with JSON formatted metrics configuration
     */
    public String metricsConfiguration(ConfigMap externalCm) {
        if (getMetricsConfigInCm() != null) {
            if (getMetricsConfigInCm() instanceof JmxPrometheusExporterMetrics) {
                if (externalCm == null) {
                    LOGGER.warnCr(reconciliation, "ConfigMap {} does not exist. Metrics disabled.",
                            ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName());
                    throw new InvalidResourceException("ConfigMap " + ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName() + " does not exist.");
                } else {
                    String data = externalCm.getData().get(((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getKey());
                    if (data == null) {
                        LOGGER.warnCr(reconciliation, "ConfigMap {} does not contain specified key {}.", ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName(),
                                ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getKey());
                        throw new InvalidResourceException("ConfigMap " + ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName()
                                + " does not contain specified key " + ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getKey() + ".");
                    } else {
                        if (data.isEmpty()) {
                            return "{}";
                        }
                        try {
                            ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
                            Object yaml = yamlReader.readValue(data, Object.class);
                            ObjectMapper jsonWriter = new ObjectMapper();
                            return jsonWriter.writeValueAsString(yaml);
                        } catch (JsonProcessingException e) {
                            throw new InvalidResourceException("Parsing metrics configuration failed. ", e);
                        }
                    }
                }
            } else {
                LOGGER.warnCr(reconciliation, "Unknown type of metrics {}.", getMetricsConfigInCm().getClass());
                throw new InvalidResourceException("Unknown type of metrics " + getMetricsConfigInCm().getClass() + ".");
            }
        }
        return null;
    }

    protected void setMetricsConfigInCm(MetricsConfig metricsConfigInCm) {
        this.metricsConfigInCm = metricsConfigInCm;
    }

    /**
     * @return  Returns the ConfigMap with metrics configuration
     */
    public MetricsConfig getMetricsConfigInCm() {
        return metricsConfigInCm;
    }

    /**
     * Returns name of config map used for storing metrics and logging configuration.
     * @return The name of config map used for storing metrics and logging configuration.
     */
    public String getAncillaryConfigMapName() {
        return ancillaryConfigMapName;
    }

    /**
     * Returns a lit of environment variables which are required by all containers.
     *
     * Contains:
     * The mirrored HTTP Proxy environment variables
     *
     * @return  List of required environment variables for all containers
     */
    protected List<EnvVar> getRequiredEnvVars() {
        // HTTP Proxy configuration should be passed to all images
        return STATIC_ENV_VARS;
    }

    /**
     * To be overridden by implementing classes
     *
     * @return null
     */
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
     * Set the Storage
     *
     * @param storage Persistent Storage configuration
     */
    protected void setStorage(Storage storage) {
        validatePersistentStorage(storage);
        this.storage = storage;
    }

    /**
     * Validates persistent storage
     * If storage is of a persistent type, validations are made
     * If storage is not of a persistent type, validation passes
     *
     * @param storage   Persistent Storage configuration
     * @throws InvalidResourceException if validations fails for any reason
     */
    protected static void validatePersistentStorage(Storage storage)   {
        if (storage instanceof PersistentClaimStorage persistentClaimStorage) {
            checkPersistentStorageSizeIsValid(persistentClaimStorage);

        } else if (storage instanceof JbodStorage jbodStorage)  {

            if (jbodStorage.getVolumes().size() == 0)   {
                throw new InvalidResourceException("JbodStorage needs to contain at least one volume!");
            }

            for (Storage jbodVolume : jbodStorage.getVolumes()) {
                if (jbodVolume instanceof PersistentClaimStorage persistentClaimStorage) {
                    checkPersistentStorageSizeIsValid(persistentClaimStorage);
                }
            }
        }
    }

    /**
     * Checks if the supplied PersistentClaimStorage has a valid size
     *
     * @param storage   PersistentClaimStorage configuration
     *
     * @throws InvalidResourceException if the persistent storage size is not valid
     */
    private static void checkPersistentStorageSizeIsValid(PersistentClaimStorage storage)   {
        if (storage.getSize() == null || storage.getSize().isEmpty()) {
            throw new InvalidResourceException("The size is mandatory for a persistent-claim storage");
        }
    }

    /**
     * @return an implementation of AbstractConfiguration configured by a user for a component.
     */
    public AbstractConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Set the configuration object which may be configured by the user for some components.
     *
     * @param configuration Configuration settings for a component.
     */
    protected void setConfiguration(AbstractConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * @return The image name.
     */
    public String getImage() {
        return image;
    }

    /**
     * @return the name of the service account used by the deployed cluster for Kubernetes API operations.
     */
    protected String getServiceAccountName() {
        return null;
    }

    /**
     * @return the cluster name.
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * Gets the name of a given pod in a StatefulSet.
     *
     * @param podId The Id (ordinal) of the pod.
     * @return The name of the pod with the given name.
     */
    public String getPodName(Integer podId) {
        return componentName + "-" + podId;
    }

    /**
     * Default null, to be overridden by implementing classes
     *
     * @return a list of init containers to add to the StatefulSet/Deployment
     */
    protected List<Container> getInitContainers(ImagePullPolicy imagePullPolicy) {
        return null;
    }

    /**
     * To be overridden by implementing classes
     *
     * @return a list of containers to add to the StatefulSet/Deployment
     */
    protected abstract List<Container> getContainers(ImagePullPolicy imagePullPolicy);

    protected ContainerPort createContainerPort(String name, int port, String protocol) {
        ContainerPort containerPort = new ContainerPortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withContainerPort(port)
                .build();
        LOGGER.traceCr(reconciliation, "Created container port {}", containerPort);
        return containerPort;
    }

    protected ServicePort createServicePort(String name, int port, int targetPort, String protocol) {
        ServicePort servicePort = createServicePort(name, port, targetPort, null, protocol);
        LOGGER.traceCr(reconciliation, "Created service port {}", servicePort);
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
        LOGGER.traceCr(reconciliation, "Created service port {}", servicePort);
        return servicePort;
    }

    protected ConfigMap createConfigMap(String name, Map<String, String> data) {
        return createConfigMap(name, labels.toMap(), data);
    }

    protected ConfigMap createConfigMap(String name, Map<String, String> labels, Map<String, String> data) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withData(data)
                .build();
    }

    protected Secret createSecret(String name, Map<String, String> data, Map<String, String> customAnnotations) {
        return ModelUtils.createSecret(name, namespace, labels, ownerReference, data, customAnnotations, emptyMap());
    }

    protected Secret createJmxSecret(String name, Map<String, String> data) {
        return ModelUtils.createSecret(name, namespace, labels, ownerReference, data, templateJmxSecretAnnotations, templateJmxSecretLabels);
    }

    protected Service createService(String type, List<ServicePort> ports, Map<String, String> annotations) {
        return createService(serviceName, type, ports, labels.withAdditionalLabels(templateServiceLabels),
                getSelectorLabels(), annotations, templateServiceIpFamilyPolicy, templateServiceIpFamilies);
    }

    protected Service createDiscoverableService(String type, List<ServicePort> ports, Map<String, String> additionalLabels, Map<String, String> annotations) {
        return createService(serviceName, type, ports, labels.withAdditionalLabels(additionalLabels).withStrimziDiscovery(), getSelectorLabels(), annotations, templateServiceIpFamilyPolicy, templateServiceIpFamilies);
    }

    protected Service createService(String name, String type, List<ServicePort> ports, Labels labels, Labels selector, Map<String, String> annotations, IpFamilyPolicy ipFamilyPolicy, List<IpFamily> ipFamilies) {
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.toMap())
                    .withNamespace(namespace)
                    .withAnnotations(annotations)
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withType(type)
                    .withSelector(selector.toMap())
                    .withPorts(ports)
                .endSpec()
                .build();

        if (ipFamilyPolicy != null) {
            service.getSpec().setIpFamilyPolicy(ipFamilyPolicy.toValue());
        }

        if (ipFamilies != null && !ipFamilies.isEmpty()) {
            service.getSpec().setIpFamilies(ipFamilies.stream().map(IpFamily::toValue).collect(Collectors.toList()));
        }

        LOGGER.traceCr(reconciliation, "Created service {}", service);
        return service;
    }

    /**
     * Creates a headless service
     *
     * Uses Alpha annotation service.alpha.kubernetes.io/tolerate-unready-endpoints for older versions of Kubernetes still supported by Strimzi,
     * replaced by the publishNotReadyAddresses field in the spec,  annotation is ignored in later versions of Kubernetes
     */
    protected Service createHeadlessService(List<ServicePort> ports) {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(headlessServiceName)
                    .withLabels(labels.withAdditionalLabels(templateHeadlessServiceLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(annotations, templateHeadlessServiceAnnotations))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withClusterIP("None")
                    .withSelector(getSelectorLabels().toMap())
                    .withPorts(ports)
                    .withPublishNotReadyAddresses(true)
                .endSpec()
                .build();

        if (templateHeadlessServiceIpFamilyPolicy != null) {
            service.getSpec().setIpFamilyPolicy(templateHeadlessServiceIpFamilyPolicy.toValue());
        }

        if (templateHeadlessServiceIpFamilies != null && !templateHeadlessServiceIpFamilies.isEmpty()) {
            service.getSpec().setIpFamilies(templateHeadlessServiceIpFamilies.stream().map(IpFamily::toValue).collect(Collectors.toList()));
        }

        LOGGER.traceCr(reconciliation, "Created headless service {}", service);
        return service;
    }

    /**
     * Build an environment variable with the provided name and value
     *
     * @param name The name of the environment variable
     * @param value The value of the environment variable
     * @return The environment variable object
     */
    protected static EnvVar buildEnvVar(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    /**
     * Build an environment variable which will use a value from a secret
     *
     * @param name The name of the environment variable
     * @param secret The name of the secret where the value is stored
     * @param key The key under which the value is stored in the secret
     *
     * @return The environment variable object
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
     * using the Downward API
     *
     * @param name The name of the environment variable
     * @param field The field path from which the value is set
     *
     * @return The environment variable object
     */
    protected static EnvVar buildEnvVarFromFieldRef(String name, String field) {

        EnvVarSource envVarSource = new EnvVarSourceBuilder()
                .withNewFieldRef()
                    .withFieldPath(field)
                .endFieldRef()
                .build();

        return new EnvVarBuilder()
                .withName(name)
                .withValueFrom(envVarSource)
                .build();
    }

    /**
     * Gets the given container's environment as a Map
     *
     * @param container The container to retrieve the EnvVars from
     *
     * @return A map of the environment variables of the given container
     *         The Environmental variable values indexed by their names
     */
    public static Map<String, String> containerEnvVars(Container container) {
        return container.getEnv().stream().collect(
            Collectors.toMap(EnvVar::getName, EnvVar::getValue,
                // On duplicates, last-in wins
                (u, v) -> v));
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

    /**
     * @return The JVM options.
     */
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    /**
     * When ImagePullPolicy is not specified by the user, Kubernetes will automatically set it based on the image
     *    :latest results in        Always
     *    anything else results in  IfNotPresent
     * This causes issues in diffing. To work around this we emulate here the Kubernetes defaults and set the policy accordingly on our side.
     *
     * This is applied to the Strimzi Kafka images which use the tag format :latest-kafka-x.y.z but have the same function
     * as if they were :latest
     * Therefore they should behave the same with an ImagePullPolicy of Always.
     *
     * @param requestedImagePullPolicy  The imagePullPolicy requested by the user (is always preferred when set, ignored when null)
     * @param image The image used for the container, from its tag we determine the default policy if requestedImagePullPolicy is null
     *
     * @return  The Image Pull Policy: Always, Never or IfNotPresent
     */
    protected String determineImagePullPolicy(ImagePullPolicy requestedImagePullPolicy, String image)  {
        if (requestedImagePullPolicy != null)   {
            return requestedImagePullPolicy.toString();
        }

        if (image.toLowerCase(Locale.ENGLISH).contains(":latest"))  {
            return ImagePullPolicy.ALWAYS.toString();
        } else {
            return ImagePullPolicy.IFNOTPRESENT.toString();
        }
    }

    /**
     * @return  The key under which tge logging configuration is stored in the Logging Config Map
     */
    public String getAncillaryConfigMapKeyLogConfig() {
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

    /**
     * @return The service account.
     */
    public ServiceAccount generateServiceAccount() {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(getServiceAccountName())
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerReference)
                    .withLabels(labels.withAdditionalLabels(templateServiceAccountLabels).toMap())
                    .withAnnotations(templateServiceAccountAnnotations)
                .endMetadata()
            .build();
    }

    /**
     * Adds the supplied list of user configured container environment variables {@see io.strimzi.api.kafka.model.ContainerEnvVar} to the
     * supplied list of fabric8 environment variables {@see io.fabric8.kubernetes.api.model.EnvVar},
     * checking first if the environment variable key has already been set in the existing list and then converts them.
     *
     * If a key is already in use then the container environment variable will not be added to the environment variable
     * list and a warning will be logged.
     *
     * @param existingEnvs  The list of fabric8 environment variable object that will be added to.
     * @param containerEnvs The list of container environment variable objects to be converted and added to the existing
     *                      environment variable list
     **/
    protected void addContainerEnvsToExistingEnvs(List<EnvVar> existingEnvs, List<ContainerEnvVar> containerEnvs) {
        if (containerEnvs != null) {
            // Create set of env var names to test if any user defined template env vars will conflict with those set above
            Set<String> predefinedEnvs = new HashSet<>();
            for (EnvVar envVar : existingEnvs) {
                predefinedEnvs.add(envVar.getName());
            }

            // Set custom env vars from the user defined template
            for (ContainerEnvVar containerEnvVar : containerEnvs) {
                if (predefinedEnvs.contains(containerEnvVar.getName())) {
                    LOGGER.warnCr(reconciliation, "User defined container template environment variable {} is already in use and will be ignored",  containerEnvVar.getName());
                } else {
                    existingEnvs.add(buildEnvVar(containerEnvVar.getName(), containerEnvVar.getValue()));
                }
            }
        }
    }

    /**
     * Adds warning condition to the list of warning conditions
     *
     * @param warning  Condition which will be added to the warning conditions
     */
    public void addWarningCondition(Condition warning) {
        warningConditions.add(warning);
    }

    /**
     * Returns a list of warning conditions set by the model. Returns an empty list if no warning conditions were set.
     *
     * @return  List of warning conditions.
     */
    public List<Condition> getWarningConditions() {
        return warningConditions;
    }
}
