/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderFactory;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * AbstractModel an abstract base model for all components of the {@code Kafka} custom resource
 */
public abstract class AbstractModel {
    /**
     * Name of the Strimzi Cluster operator a used in various labels
     */
    public static final String STRIMZI_CLUSTER_OPERATOR_NAME = "strimzi-cluster-operator";

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractModel.class.getName());

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

    protected final Reconciliation reconciliation;
    protected final String cluster;
    protected final String namespace;
    protected final String componentName;
    protected final OwnerReference ownerReference;
    protected final Labels labels;

    // Docker image configuration
    protected String image;
    // Number of replicas
    protected int replicas;

    /**
     * Application configuration
     */
    protected AbstractConfiguration configuration;
    protected Logging logging;
    protected boolean gcLoggingEnabled = true;
    protected JvmOptions jvmOptions;

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
    protected ResourceRequirements resources;
    protected String readinessPath;
    protected io.strimzi.api.kafka.model.Probe readinessProbeOptions;
    protected String livenessPath;
    protected io.strimzi.api.kafka.model.Probe livenessProbeOptions;

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
     * @return Whether metrics are enabled.
     */
    public boolean isMetricsEnabled() {
        return isMetricsEnabled;
    }

    /**
     * @return The logging configuration
     */
    public Logging getLogging() {
        return logging;
    }

    /**
     * Generates the logging configuration as a String. The configuration is generated based on the default logging
     * configuration files from resources, the (optional) inline logging configuration from the custom resource
     * and the (optional) external logging configuration in a user-provided ConfigMap.
     *
     * @param externalCm The user-provided ConfigMap with custom Log4j / Log4j2 file
     *
     * @return String with the Log4j / Log4j2 properties used for configuration
     */
    public String loggingConfiguration(ConfigMap externalCm) {
        return loggingConfiguration(externalCm, false);
    }

    /**
     * Generates the logging configuration as a String. The configuration is generated based on the default logging
     * configuration files from resources, the (optional) inline logging configuration from the custom resource
     * and the (optional) external logging configuration in a user-provided ConfigMap.
     *
     * @param externalCm                    The user-provided ConfigMap with custom Log4j / Log4j2 file
     * @param shouldPatchLoggerAppender     Indicator if logger appender should be patched
     *
     * @return String with the Log4j / Log4j2 properties used for configuration
     */
    String loggingConfiguration(ConfigMap externalCm, boolean shouldPatchLoggerAppender) {
        return LoggingUtils
                .loggingConfiguration(
                        reconciliation,
                        this.getClass().getSimpleName(),
                        shouldPatchLoggerAppender,
                        !ANCILLARY_CM_KEY_LOG_CONFIG.equals(getAncillaryConfigMapKeyLogConfig()), // If the file name for the Config Map is not log4j.properties, we assume it is Log4j2
                        logging,
                        externalCm
                );
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
        data.put(getAncillaryConfigMapKeyLogConfig(), loggingConfiguration(metricsAndLogging.getLoggingCm()));
        if (getMetricsConfigInCm() != null) {
            String parseResult = metricsConfiguration(metricsAndLogging.getMetricsCm());
            if (parseResult != null) {
                this.isMetricsEnabled = true;
                data.put(ANCILLARY_CM_KEY_METRICS, parseResult);
            }
        }

        return ConfigMapUtils.createConfigMap(ancillaryConfigMapName, namespace, labels, ownerReference, data);
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
        return ServiceAccountUtils.createServiceAccount(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateServiceAccount
        );
    }
}
