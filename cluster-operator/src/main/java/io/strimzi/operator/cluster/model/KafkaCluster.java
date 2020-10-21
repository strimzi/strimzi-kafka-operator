/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.HTTPIngressPath;
import io.fabric8.kubernetes.api.model.extensions.HTTPIngressPathBuilder;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.extensions.IngressBuilder;
import io.fabric8.kubernetes.api.model.extensions.IngressRule;
import io.fabric8.kubernetes.api.model.extensions.IngressRuleBuilder;
import io.fabric8.kubernetes.api.model.extensions.IngressTLS;
import io.fabric8.kubernetes.api.model.extensions.IngressTLSBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPort;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaAuthorization;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloak;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.listener.ExternalListenerBootstrapOverride;
import io.strimzi.api.kafka.model.listener.ExternalListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.IngressListenerBrokerConfiguration;
import io.strimzi.api.kafka.model.listener.IngressListenerConfiguration;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalIngress;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalLoadBalancer;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePort;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalRoute;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerOverride;
import io.strimzi.api.kafka.model.listener.NodePortListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.NodePortListenerOverride;
import io.strimzi.api.kafka.model.listener.RouteListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.RouteListenerOverride;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.ExternalTrafficPolicy;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.addAll;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static io.strimzi.operator.cluster.model.CruiseControl.CRUISE_CONTROL_METRIC_REPORTER;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaCluster extends AbstractModel {
    protected static final String APPLICATION_NAME = "kafka";

    protected static final String INIT_NAME = "kafka-init";
    protected static final String INIT_VOLUME_NAME = "rack-volume";
    protected static final String INIT_VOLUME_MOUNT = "/opt/kafka/init";
    protected static final String ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    protected static final String ENV_VAR_KAFKA_INIT_NODE_NAME = "NODE_NAME";
    protected static final String ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS = "EXTERNAL_ADDRESS";
    protected static final String ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS_TYPE = "EXTERNAL_ADDRESS_TYPE";

    private static final String ENV_VAR_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";

    // OAUTH ENV VARS
    protected static final String ENV_VAR_STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET = "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET";
    protected static final String ENV_VAR_STRIMZI_TLS_9093_OAUTH_CLIENT_SECRET = "STRIMZI_TLS_9093_OAUTH_CLIENT_SECRET";
    protected static final String ENV_VAR_STRIMZI_EXTERNAL_9094_OAUTH_CLIENT_SECRET = "STRIMZI_EXTERNAL_9094_OAUTH_CLIENT_SECRET";

    // For port names in services, a 'tcp-' prefix is added to support Istio protocol selection
    // This helps Istio to avoid using a wildcard listener and instead present IP:PORT pairs which effects
    // proper listener, routing and metrics configuration sent to Envoy
    protected static final int CLIENT_PORT = 9092;
    protected static final String CLIENT_PORT_NAME = "tcp-clients";

    public static final int REPLICATION_PORT = 9091;
    protected static final String REPLICATION_PORT_NAME = "tcp-replication";

    protected static final int CLIENT_TLS_PORT = 9093;
    protected static final String CLIENT_TLS_PORT_NAME = "tcp-clientstls";

    protected static final int EXTERNAL_PORT = 9094;
    protected static final String EXTERNAL_PORT_NAME = "tcp-external";

    protected static final int ROUTE_PORT = 443;

    protected static final String KAFKA_NAME = "kafka";
    protected static final String CLUSTER_CA_CERTS_VOLUME = "cluster-ca";
    protected static final String BROKER_CERTS_VOLUME = "broker-certs";
    protected static final String CLIENT_CA_CERTS_VOLUME = "client-ca-cert";
    protected static final String CLUSTER_CA_CERTS_VOLUME_MOUNT = "/opt/kafka/cluster-ca-certs";
    protected static final String BROKER_CERTS_VOLUME_MOUNT = "/opt/kafka/broker-certs";
    protected static final String CLIENT_CA_CERTS_VOLUME_MOUNT = "/opt/kafka/client-ca-certs";
    protected static final String OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/certificates";

    private static final String NAME_SUFFIX = "-kafka";

    private static final String KAFKA_METRIC_REPORTERS_CONFIG_FIELD = "metric.reporters";
    private static final String KAFKA_NUM_PARTITIONS_CONFIG_FIELD = "num.partitions";
    private static final String KAFKA_REPLICATION_FACTOR_CONFIG_FIELD = "default.replication.factor";
    private static final String CC_NUM_PARTITIONS_CONFIG_FIELD = "cruise.control.metrics.topic.num.partitions";
    private static final String CC_REPLICATION_FACTOR_CONFIG_FIELD = "cruise.control.metrics.topic.replication.factor";

    protected static final String KAFKA_JMX_SECRET_SUFFIX = NAME_SUFFIX + "-jmx";
    protected static final String SECRET_JMX_USERNAME_KEY = "jmx-username";
    protected static final String SECRET_JMX_PASSWORD_KEY = "jmx-password";
    protected static final String ENV_VAR_KAFKA_JMX_USERNAME = "KAFKA_JMX_USERNAME";
    protected static final String ENV_VAR_KAFKA_JMX_PASSWORD = "KAFKA_JMX_PASSWORD";

    // Suffixes for secrets with certificates
    private static final String SECRET_BROKERS_SUFFIX = NAME_SUFFIX + "-brokers";

    /**
     * Records the Kafka version currently running inside Kafka StatefulSet
     */
    public static final String ANNO_STRIMZI_IO_KAFKA_VERSION = Annotations.STRIMZI_DOMAIN + "kafka-version";
    /**
     * Records the state of the Kafka upgrade process. Unset outside of upgrades.
     */
    public static final String ANNO_STRIMZI_IO_FROM_VERSION = Annotations.STRIMZI_DOMAIN + "from-version";
    /**
     * Records the state of the Kafka upgrade process. Unset outside of upgrades.
     */
    public static final String ANNO_STRIMZI_IO_TO_VERSION = Annotations.STRIMZI_DOMAIN + "to-version";

    /**
     * Records the state of the Kafka upgrade process. Unset outside of upgrades.
     */
    public static final String ANNO_STRIMZI_BROKER_CONFIGURATION_HASH = Annotations.STRIMZI_DOMAIN + "broker-configuration-hash";

    public static final String ANNO_STRIMZI_CUSTOM_CERT_THUMBPRINT_TLS_LISTENER = Annotations.STRIMZI_DOMAIN + "custom-cert-tls-listener-thumbprint";
    public static final String ANNO_STRIMZI_CUSTOM_CERT_THUMBPRINT_EXTERNAL_LISTENER = Annotations.STRIMZI_DOMAIN + "custom-cert-external-listener-thumbprint";

    // Env vars for JMX service
    protected static final String ENV_VAR_KAFKA_JMX_ENABLED = "KAFKA_JMX_ENABLED";

    // Name of the broker configuration file in the config map
    public static final String BROKER_CONFIGURATION_FILENAME = "server.config";
    public static final String BROKER_ADVERTISED_HOSTNAMES_FILENAME = "advertised-hostnames.config";
    public static final String BROKER_ADVERTISED_PORTS_FILENAME = "advertised-ports.config";

    // Kafka configuration
    private Rack rack;
    private String initImage;
    private KafkaListeners listeners;
    private KafkaAuthorization authorization;
    private KafkaVersion kafkaVersion;
    private CruiseControlSpec cruiseControlSpec;
    private String kafkaDefaultNumPartitions = "1";
    private String kafkaDefaultReplicationFactor = "1";
    private String ccNumPartitions = null;
    private String ccReplicationFactor = null;
    private boolean isJmxEnabled;
    private boolean isJmxAuthenticated;
    private CertAndKeySecretSource secretSourceExternal = null;
    private CertAndKeySecretSource secretSourceTls = null;
    private String brokersConfiguration;

    // Templates
    protected Map<String, String> templateExternalBootstrapServiceLabels;
    protected Map<String, String> templateExternalBootstrapServiceAnnotations;
    protected Map<String, String> templatePerPodServiceLabels;
    protected Map<String, String> templatePerPodServiceAnnotations;
    protected Map<String, String> templateExternalBootstrapRouteLabels;
    protected Map<String, String> templateExternalBootstrapRouteAnnotations;
    protected Map<String, String> templatePerPodRouteLabels;
    protected Map<String, String> templatePerPodRouteAnnotations;
    protected Map<String, String> templateExternalBootstrapIngressLabels;
    protected Map<String, String> templateExternalBootstrapIngressAnnotations;
    protected Map<String, String> templatePerPodIngressLabels;
    protected Map<String, String> templatePerPodIngressAnnotations;
    protected List<ContainerEnvVar> templateKafkaContainerEnvVars;
    protected List<ContainerEnvVar> templateInitContainerEnvVars;

    protected SecurityContext templateKafkaContainerSecurityContext;
    protected SecurityContext templateInitContainerSecurityContext;

    protected ExternalTrafficPolicy templateExternalBootstrapServiceTrafficPolicy;
    protected List<String> templateExternalBootstrapServiceLoadBalancerSourceRanges;
    protected ExternalTrafficPolicy templatePerPodServiceTrafficPolicy;
    protected List<String> templatePerPodServiceLoadBalancerSourceRanges;

    // Configuration defaults
    private static final int DEFAULT_REPLICAS = 3;
    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder().withTimeoutSeconds(5)
            .withInitialDelaySeconds(15).build();
    private static final boolean DEFAULT_KAFKA_METRICS_ENABLED = false;

    /**
     * Private key and certificate for each Kafka Pod name
     * used as server certificates for Kafka brokers
     */
    private Map<String, CertAndKey> brokerCerts;

    /**
     * Lists with volumes, persistent volume claims and related volume mount paths for the storage
     */
    List<Volume> dataVolumes;
    List<PersistentVolumeClaim> dataPvcs;
    List<VolumeMount> dataVolumeMountPaths;

    /**
     * Constructor
     *
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    private KafkaCluster(HasMetadata resource) {
        super(resource, APPLICATION_NAME);
        this.name = kafkaClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.headlessServiceName = headlessServiceName(cluster);
        this.ancillaryConfigMapName = metricAndLogConfigsName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.isMetricsEnabled = DEFAULT_KAFKA_METRICS_ENABLED;

        this.mountPath = "/var/lib/kafka";

        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/kafka/custom-config/";

        this.initImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_INIT_IMAGE, "strimzi/operator:latest");
    }

    public static String kafkaClusterName(String cluster) {
        return KafkaResources.kafkaStatefulSetName(cluster);
    }

    public static String metricAndLogConfigsName(String cluster) {
        return KafkaResources.kafkaMetricsAndLogConfigMapName(cluster);
    }

    public static String serviceName(String cluster) {
        return KafkaResources.bootstrapServiceName(cluster);
    }

    public static String podDnsName(String namespace, String cluster, int podId) {
        return podDnsName(namespace, cluster, KafkaCluster.kafkaPodName(cluster, podId));
    }

    public static String podDnsName(String namespace, String cluster, String podName) {
        return ModelUtils.podDnsName(
                namespace,
                KafkaCluster.headlessServiceName(cluster),
                podName);
    }

    public static String podDnsNameWithoutClusterDomain(String namespace, String cluster, int podId) {
        return podDnsNameWithoutClusterDomain(namespace, cluster, KafkaCluster.kafkaPodName(cluster, podId));
    }

    public static String podDnsNameWithoutClusterDomain(String namespace, String cluster, String podName) {
        return ModelUtils.podDnsNameWithoutClusterDomain(
                namespace,
                KafkaCluster.headlessServiceName(cluster),
                podName);
    }

    /**
     * Generates the name of the service used as bootstrap service for external clients.
     *
     * @param cluster The name of the cluster.
     * @return The name of the external bootstrap service.
     */
    public static String externalBootstrapServiceName(String cluster) {
        return KafkaResources.externalBootstrapServiceName(cluster);
    }

    /**
     * Generates the name of the service for exposing individual pods.
     *
     * @param cluster The name of the cluster.
     * @param pod     Pod sequence number assign by StatefulSet.
     * @return The name of the external service.
     */
    public static String externalServiceName(String cluster, int pod) {
        return kafkaClusterName(cluster) + "-" + pod;
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the headless service.
     */
    public static String headlessServiceName(String cluster) {
        return KafkaResources.brokersServiceName(cluster);
    }

    /**
     * Gets the name of the given Kafka pod.
     *
     * @param cluster The name of the cluster.
     * @param pod     The id of the pod
     * @return The name of the pod.
     */
    public static String kafkaPodName(String cluster, int pod) {
        return kafkaClusterName(cluster) + "-" + pod;
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the clients CA key Secret.
     */
    public static String clientsCaKeySecretName(String cluster) {
        return KafkaResources.clientsCaKeySecretName(cluster);
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the brokers Secret.
     */
    public static String brokersSecretName(String cluster) {
        return cluster + KafkaCluster.SECRET_BROKERS_SUFFIX;
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the jmx Secret.
     */
    public static String jmxSecretName(String cluster) {
        return cluster + KafkaCluster.KAFKA_JMX_SECRET_SUFFIX;
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the clients CA certificate Secret.
     */
    public static String clientsCaCertSecretName(String cluster) {
        return KafkaResources.clientsCaCertificateSecretName(cluster);
    }

    public static KafkaCluster fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        return fromCrd(kafkaAssembly, versions, null, 0);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:JavaNCSS"})
    public static KafkaCluster fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions, Storage oldStorage, int oldReplicas) {
        KafkaCluster result = new KafkaCluster(kafkaAssembly);

        result.setOwnerReference(kafkaAssembly);

        KafkaSpec kafkaSpec = kafkaAssembly.getSpec();
        KafkaClusterSpec kafkaClusterSpec = kafkaSpec.getKafka();

        result.setReplicas(kafkaClusterSpec.getReplicas());

        validateIntConfigProperty("offsets.topic.replication.factor", kafkaClusterSpec);
        validateIntConfigProperty("transaction.state.log.replication.factor", kafkaClusterSpec);
        validateIntConfigProperty("transaction.state.log.min.isr", kafkaClusterSpec);

        result.setImage(versions.kafkaImage(kafkaClusterSpec.getImage(), kafkaClusterSpec.getVersion()));

        if (kafkaClusterSpec.getReadinessProbe() != null) {
            result.setReadinessProbe(kafkaClusterSpec.getReadinessProbe());
        }

        if (kafkaClusterSpec.getLivenessProbe() != null) {
            result.setLivenessProbe(kafkaClusterSpec.getLivenessProbe());
        }

        result.setRack(kafkaClusterSpec.getRack());

        String initImage = kafkaClusterSpec.getBrokerRackInitImage();
        if (initImage == null) {
            initImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_INIT_IMAGE, "strimzi/operator:latest");
        }
        result.setInitImage(initImage);

        Logging logging = kafkaClusterSpec.getLogging();
        result.setLogging(logging == null ? new InlineLogging() : logging);

        result.setGcLoggingEnabled(kafkaClusterSpec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : kafkaClusterSpec.getJvmOptions().isGcLoggingEnabled());
        if (kafkaClusterSpec.getJvmOptions() != null) {
            result.setJavaSystemProperties(kafkaClusterSpec.getJvmOptions().getJavaSystemProperties());
        }

        result.setJvmOptions(kafkaClusterSpec.getJvmOptions());

        if (kafkaClusterSpec.getJmxOptions() != null) {
            result.setJmxEnabled(Boolean.TRUE);
            AuthenticationUtils.configureKafkaJmxOptions(kafkaClusterSpec.getJmxOptions().getAuthentication(), result);
        }

        KafkaConfiguration configuration = new KafkaConfiguration(kafkaClusterSpec.getConfig().entrySet());
        // If  required Cruise Control metric reporter configurations are missing set them using Kafka defaults
        if (configuration.getConfigOption(CC_NUM_PARTITIONS_CONFIG_FIELD) == null) {
            result.ccNumPartitions = configuration.getConfigOption(KAFKA_NUM_PARTITIONS_CONFIG_FIELD, result.kafkaDefaultNumPartitions);
        }
        if (configuration.getConfigOption(CC_REPLICATION_FACTOR_CONFIG_FIELD) == null) {
            result.ccReplicationFactor = configuration.getConfigOption(KAFKA_REPLICATION_FACTOR_CONFIG_FIELD, result.kafkaDefaultReplicationFactor);
        }
        String metricReporters =  configuration.getConfigOption(KAFKA_METRIC_REPORTERS_CONFIG_FIELD);
        Set<String> metricReporterList = new HashSet<>();
        if (metricReporters != null) {
            addAll(metricReporterList, configuration.getConfigOption(KAFKA_METRIC_REPORTERS_CONFIG_FIELD).split(","));
        }

        if (kafkaSpec.getCruiseControl() != null && kafkaClusterSpec.getReplicas() < 2) {
            throw new InvalidResourceException("Kafka " +
                    kafkaAssembly.getMetadata().getNamespace() + "/" + kafkaAssembly.getMetadata().getName() +
                    " has invalid configuration. Cruise Control cannot be deployed with a single-node Kafka cluster. It requires at least two Kafka nodes.");
        }
        result.cruiseControlSpec  = kafkaSpec.getCruiseControl();
        if (result.cruiseControlSpec != null) {
            metricReporterList.add(CRUISE_CONTROL_METRIC_REPORTER);
        } else {
            metricReporterList.remove(CRUISE_CONTROL_METRIC_REPORTER);
        }
        if (!metricReporterList.isEmpty()) {
            configuration.setConfigOption(KAFKA_METRIC_REPORTERS_CONFIG_FIELD, String.join(",", metricReporterList));
        } else {
            configuration.removeConfigOption(KAFKA_METRIC_REPORTERS_CONFIG_FIELD);
        }

        List<String> errorsInConfig = configuration.validate(versions.version(kafkaClusterSpec.getVersion()));
        if (!errorsInConfig.isEmpty()) {
            for (String error : errorsInConfig) {
                log.warn("Kafka {}/{} has invalid spec.kafka.config: {}",
                        kafkaAssembly.getMetadata().getNamespace(),
                        kafkaAssembly.getMetadata().getName(),
                        error);
            }
            throw new InvalidResourceException("Kafka " +
                    kafkaAssembly.getMetadata().getNamespace() + "/" + kafkaAssembly.getMetadata().getName() +
                    " has invalid spec.kafka.config: " +
                    String.join(", ", errorsInConfig));
        }
        result.setConfiguration(configuration);

        Map<String, Object> metrics = kafkaClusterSpec.getMetrics();
        if (metrics != null) {
            result.setMetricsEnabled(true);
            result.setMetricsConfig(metrics.entrySet());
        }

        if (oldStorage != null) {
            Storage newStorage = kafkaClusterSpec.getStorage();
            AbstractModel.validatePersistentStorage(newStorage);

            StorageDiff diff = new StorageDiff(oldStorage, newStorage, oldReplicas, kafkaClusterSpec.getReplicas());

            if (!diff.isEmpty()) {
                log.warn("Only the following changes to Kafka storage are allowed: " +
                        "changing the deleteClaim flag, " +
                        "adding volumes to Jbod storage or removing volumes from Jbod storage, " +
                        "changing overrides to nodes which do not exist yet" +
                        "and increasing size of persistent claim volumes (depending on the volume type and used storage class).");
                log.warn("Your desired Kafka storage configuration contains changes which are not allowed. As a " +
                        "result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                        "about the detected changes.");

                Condition warning = StatusUtils.buildWarningCondition("KafkaStorage",
                        "The desired Kafka storage configuration contains changes which are not allowed. As a " +
                                "result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                                "about the detected changes.");
                result.addWarningCondition(warning);

                result.setStorage(oldStorage);
            } else {
                result.setStorage(newStorage);
            }
        } else {
            result.setStorage(kafkaClusterSpec.getStorage());
        }

        result.setDataVolumesClaimsAndMountPaths(result.getStorage());

        result.setResources(kafkaClusterSpec.getResources());

        KafkaListeners listeners = kafkaClusterSpec.getListeners();
        result.setListeners(listeners);

        boolean isListenerOAuth = false;
        if (listeners != null) {
            if (listeners.getPlain() != null) {
                if (listeners.getPlain().getAuth() instanceof KafkaListenerAuthenticationTls) {
                    throw new InvalidResourceException("You cannot configure TLS authentication on a plain listener.");
                } else if (listeners.getPlain().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                    validateOauth((KafkaListenerAuthenticationOAuth) listeners.getPlain().getAuth(), "Plain listener");
                    isListenerOAuth = true;
                }
            }

            if (listeners.getExternal() != null) {
                if (!result.isExposedWithTls() && listeners.getExternal().getAuth() instanceof KafkaListenerAuthenticationTls) {
                    throw new InvalidResourceException("TLS Client Authentication can be used only with enabled TLS encryption!");
                } else if (listeners.getExternal().getAuth() != null && listeners.getExternal().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                    validateOauth((KafkaListenerAuthenticationOAuth) listeners.getExternal().getAuth(), "External listener");
                    isListenerOAuth = true;
                }
            }

            if (listeners.getTls() != null && listeners.getTls().getAuth() != null && listeners.getTls().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                validateOauth((KafkaListenerAuthenticationOAuth) listeners.getTls().getAuth(), "TLS listener");
                isListenerOAuth = true;
            }

            if (listeners.getExternal() != null) {
                if (result.isExposedWithIngress()) {
                    if (((KafkaListenerExternalIngress) listeners.getExternal()).getConfiguration() != null) {
                        result.setSecretSourceExternal(((KafkaListenerExternalIngress) listeners.getExternal()).getConfiguration().getBrokerCertChainAndKey());
                    }
                }

                if (result.isExposedWithNodePort()) {
                    if (((KafkaListenerExternalNodePort) listeners.getExternal()).getConfiguration() != null) {
                        result.setSecretSourceExternal(((KafkaListenerExternalNodePort) listeners.getExternal()).getConfiguration().getBrokerCertChainAndKey());
                    }
                }

                if (result.isExposedWithLoadBalancer()) {
                    if (((KafkaListenerExternalLoadBalancer) listeners.getExternal()).getConfiguration() != null) {
                        result.setSecretSourceExternal(((KafkaListenerExternalLoadBalancer) listeners.getExternal()).getConfiguration().getBrokerCertChainAndKey());
                    }
                }

                if (result.isExposedWithRoute()) {
                    if (((KafkaListenerExternalRoute) listeners.getExternal()).getConfiguration() != null) {
                        result.setSecretSourceExternal(((KafkaListenerExternalRoute) listeners.getExternal()).getConfiguration().getBrokerCertChainAndKey());
                    }
                }
            }

            if (listeners.getTls() != null && listeners.getTls().getConfiguration() != null) {
                result.setSecretSourceTls(listeners.getTls().getConfiguration().getBrokerCertChainAndKey());
            }
        }

        if (kafkaClusterSpec.getAuthorization() instanceof KafkaAuthorizationKeycloak) {
            if (!isListenerOAuth) {
                throw new InvalidResourceException("You cannot configure Keycloak Authorization without any listener with OAuth based authentication");
            } else {
                KafkaAuthorizationKeycloak authorizationKeycloak = (KafkaAuthorizationKeycloak) kafkaClusterSpec.getAuthorization();
                if (authorizationKeycloak.getClientId() == null || authorizationKeycloak.getTokenEndpointUri() == null) {
                    log.error("Keycloak Authorization: Token Endpoint URI and clientId are both required");
                    throw new InvalidResourceException("Keycloak Authorization: Token Endpoint URI and clientId are both required");
                }
            }
        }

        result.setAuthorization(kafkaClusterSpec.getAuthorization());

        if (kafkaClusterSpec.getTemplate() != null) {
            KafkaClusterTemplate template = kafkaClusterSpec.getTemplate();

            if (template.getStatefulset() != null) {
                if (template.getStatefulset().getPodManagementPolicy() != null) {
                    result.templatePodManagementPolicy = template.getStatefulset().getPodManagementPolicy();
                }

                if (template.getStatefulset().getMetadata() != null) {
                    result.templateStatefulSetLabels = template.getStatefulset().getMetadata().getLabels();
                    result.templateStatefulSetAnnotations = template.getStatefulset().getMetadata().getAnnotations();
                }
            }

            ModelUtils.parsePodTemplate(result, template.getPod());

            if (template.getBootstrapService() != null && template.getBootstrapService().getMetadata() != null) {
                result.templateServiceLabels = template.getBootstrapService().getMetadata().getLabels();
                result.templateServiceAnnotations = template.getBootstrapService().getMetadata().getAnnotations();
            }

            if (template.getBrokersService() != null && template.getBrokersService().getMetadata() != null) {
                result.templateHeadlessServiceLabels = template.getBrokersService().getMetadata().getLabels();
                result.templateHeadlessServiceAnnotations = template.getBrokersService().getMetadata().getAnnotations();
            }

            if (template.getExternalBootstrapService() != null) {
                if (template.getExternalBootstrapService().getMetadata() != null) {
                    result.templateExternalBootstrapServiceLabels = template.getExternalBootstrapService().getMetadata().getLabels();
                    result.templateExternalBootstrapServiceAnnotations = template.getExternalBootstrapService().getMetadata().getAnnotations();
                }

                result.templateExternalBootstrapServiceTrafficPolicy = template.getExternalBootstrapService().getExternalTrafficPolicy();

                if (result.isExposedWithLoadBalancer()) {
                    result.templateExternalBootstrapServiceLoadBalancerSourceRanges = template.getExternalBootstrapService().getLoadBalancerSourceRanges();
                } else if (template.getExternalBootstrapService().getLoadBalancerSourceRanges() != null
                        && template.getExternalBootstrapService().getLoadBalancerSourceRanges().size() > 0) {
                    // LoadBalancerSourceRanges have been set, but LaodBalancers are not used
                    log.warn("The Kafka.spec.kafka.template.externalBootstrapService.loadBalancerSourceRanges option can be used only with load balancer type listeners");
                }
            }

            if (template.getPerPodService() != null) {
                if (template.getPerPodService().getMetadata() != null) {
                    result.templatePerPodServiceLabels = template.getPerPodService().getMetadata().getLabels();
                    result.templatePerPodServiceAnnotations = template.getPerPodService().getMetadata().getAnnotations();
                }

                result.templatePerPodServiceTrafficPolicy = template.getPerPodService().getExternalTrafficPolicy();

                if (result.isExposedWithLoadBalancer()) {
                    result.templatePerPodServiceLoadBalancerSourceRanges = template.getPerPodService().getLoadBalancerSourceRanges();
                } else if (template.getPerPodService().getLoadBalancerSourceRanges() != null
                        && template.getPerPodService().getLoadBalancerSourceRanges().size() > 0) {
                    // LoadBalancerSourceRanges have been set, but LoadBalancers are not used
                    log.warn("The Kafka.spec.kafka.template.perPodService.loadBalancerSourceRanges option can be used only with load balancer type listeners");
                }
            }

            if (template.getExternalBootstrapRoute() != null && template.getExternalBootstrapRoute().getMetadata() != null) {
                result.templateExternalBootstrapRouteLabels = template.getExternalBootstrapRoute().getMetadata().getLabels();
                result.templateExternalBootstrapRouteAnnotations = template.getExternalBootstrapRoute().getMetadata().getAnnotations();
            }

            if (template.getPerPodRoute() != null && template.getPerPodRoute().getMetadata() != null) {
                result.templatePerPodRouteLabels = template.getPerPodRoute().getMetadata().getLabels();
                result.templatePerPodRouteAnnotations = template.getPerPodRoute().getMetadata().getAnnotations();
            }

            if (template.getExternalBootstrapIngress() != null && template.getExternalBootstrapIngress().getMetadata() != null) {
                result.templateExternalBootstrapIngressLabels = template.getExternalBootstrapIngress().getMetadata().getLabels();
                result.templateExternalBootstrapIngressAnnotations = template.getExternalBootstrapIngress().getMetadata().getAnnotations();
            }

            if (template.getPerPodIngress() != null && template.getPerPodIngress().getMetadata() != null) {
                result.templatePerPodIngressLabels = template.getPerPodIngress().getMetadata().getLabels();
                result.templatePerPodIngressAnnotations = template.getPerPodIngress().getMetadata().getAnnotations();
            }

            if (template.getPersistentVolumeClaim() != null && template.getPersistentVolumeClaim().getMetadata() != null) {
                result.templatePersistentVolumeClaimLabels = Util.mergeLabelsOrAnnotations(template.getPersistentVolumeClaim().getMetadata().getLabels(),
                        result.templateStatefulSetLabels);
                result.templatePersistentVolumeClaimAnnotations = template.getPersistentVolumeClaim().getMetadata().getAnnotations();
            }

            if (template.getKafkaContainer() != null && template.getKafkaContainer().getEnv() != null) {
                result.templateKafkaContainerEnvVars = template.getKafkaContainer().getEnv();
            }

            if (template.getInitContainer() != null && template.getInitContainer().getEnv() != null) {
                result.templateInitContainerEnvVars = template.getInitContainer().getEnv();
            }

            if (template.getKafkaContainer() != null && template.getKafkaContainer().getSecurityContext() != null) {
                result.templateKafkaContainerSecurityContext = template.getKafkaContainer().getSecurityContext();
            }

            if (template.getInitContainer() != null && template.getInitContainer().getSecurityContext() != null) {
                result.templateInitContainerSecurityContext = template.getInitContainer().getSecurityContext();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(result, template.getPodDisruptionBudget());
        }

        // Kafka Cluster needs special treatment for Affinity and Tolerations because of deprecated fields in spec
        result.setUserAffinity(affinity(kafkaClusterSpec));
        result.setTolerations(tolerations(kafkaClusterSpec));

        result.kafkaVersion = versions.version(kafkaClusterSpec.getVersion());
        return result;
    }

    protected static void validateIntConfigProperty(String propertyName, KafkaClusterSpec kafkaClusterSpec) {
        String orLess = kafkaClusterSpec.getReplicas() > 1 ? " or less" : "";
        if (kafkaClusterSpec.getConfig() != null && kafkaClusterSpec.getConfig().get(propertyName) != null) {
            try {
                int propertyVal = Integer.parseInt(kafkaClusterSpec.getConfig().get(propertyName).toString());
                if (propertyVal > kafkaClusterSpec.getReplicas()) {
                    throw new InvalidResourceException("Kafka configuration option '" + propertyName + "' should be set to " + kafkaClusterSpec.getReplicas() + orLess + " because 'spec.kafka.replicas' is " + kafkaClusterSpec.getReplicas());
                }
            } catch (NumberFormatException e) {
                throw new InvalidResourceException("Property " + propertyName + " should be an integer");
            }
        }
    }

    @SuppressWarnings("deprecation")
    static List<Toleration> tolerations(KafkaClusterSpec kafkaClusterSpec) {
        return ModelUtils.tolerations("spec.kafka.tolerations", kafkaClusterSpec.getTolerations(), "spec.kafka.template.pod.tolerations", kafkaClusterSpec.getTemplate() == null ? null : kafkaClusterSpec.getTemplate().getPod());
    }

    @SuppressWarnings("deprecation")
    static Affinity affinity(KafkaClusterSpec kafkaClusterSpec) {
        if (kafkaClusterSpec.getTemplate() != null
                && kafkaClusterSpec.getTemplate().getPod() != null
                && kafkaClusterSpec.getTemplate().getPod().getAffinity() != null) {
            if (kafkaClusterSpec.getAffinity() != null) {
                log.warn("Affinity given on both spec.kafka.affinity and spec.kafka.template.pod.affinity; latter takes precedence");
            }
            return kafkaClusterSpec.getTemplate().getPod().getAffinity();
        } else {
            return kafkaClusterSpec.getAffinity();
        }
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param kafka                    The Kafka custom resource
     * @param clusterCa                The CA for cluster certificates
     * @param externalBootstrapDnsName The set of DNS names for bootstrap service (should be appended to every broker certificate)
     * @param externalDnsNames         The list of DNS names for broker pods (should be appended only to specific certificates for given broker)
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     */
    public void generateCertificates(Kafka kafka, ClusterCa clusterCa, Set<String> externalBootstrapDnsName,
            Map<Integer, Set<String>> externalDnsNames, boolean isMaintenanceTimeWindowsSatisfied) {
        log.debug("Generating certificates");

        try {
            brokerCerts = clusterCa.generateBrokerCerts(kafka, externalBootstrapDnsName, externalDnsNames, isMaintenanceTimeWindowsSatisfied);
        } catch (IOException e) {
            log.warn("Error while generating certificates", e);
        }

        log.debug("End generating certificates");
    }

    /**
     * Generates ports for bootstrap service.
     * The bootstrap service contains only the client interfaces.
     * Not the replication interface which doesn't need bootstrap service.
     *
     * @return List with generated ports
     */
    private List<ServicePort> getServicePorts() {
        List<ServicePort> ports = new ArrayList<>(3);
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));

        if (listeners != null && listeners.getPlain() != null) {
            ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));
        }

        if (listeners != null && listeners.getTls() != null) {
            ports.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));
        }
        return ports;
    }

    /**
     * Generates ports for headless service.
     * The headless service contains both the client interfaces as well as replication interface.
     *
     * @return List with generated ports
     */
    private List<ServicePort> getHeadlessServicePorts() {
        List<ServicePort> ports = new ArrayList<>(4);
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));

        if (listeners != null && listeners.getPlain() != null) {
            ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));
        }

        if (listeners != null && listeners.getTls() != null) {
            ports.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));
        }

        if (isJmxEnabled()) {
            ports.add(createServicePort(JMX_PORT_NAME, JMX_PORT, JMX_PORT, "TCP"));
        }

        return ports;
    }

    /**
     * Generates a Service according to configured defaults
     *
     * @return The generated Service
     */
    public Service generateService() {
        return createDiscoverableService("ClusterIP", getServicePorts(),
                Util.mergeLabelsOrAnnotations(getInternalDiscoveryAnnotation(), templateServiceAnnotations));
    }

    /**
     * Generates a JSON String with the discovery annotation for the internal bootstrap service
     *
     * @return  JSON with discovery annotation
     */
    /*test*/ Map<String, String> getInternalDiscoveryAnnotation() {
        JsonArray anno = new JsonArray();

        if (listeners != null) {
            if (listeners.getPlain() != null) {
                JsonObject discovery = new JsonObject();
                discovery.put("port", 9092);
                discovery.put("tls", false);
                discovery.put("protocol", "kafka");

                if (listeners.getPlain().getAuth() != null) {
                    discovery.put("auth", listeners.getPlain().getAuth().getType());
                } else {
                    discovery.put("auth", "none");
                }

                anno.add(discovery);
            }

            if (listeners.getTls() != null) {
                JsonObject discovery = new JsonObject();
                discovery.put("port", 9093);
                discovery.put("tls", true);
                discovery.put("protocol", "kafka");

                if (listeners.getTls().getAuth() != null) {
                    discovery.put("auth", listeners.getTls().getAuth().getType());
                } else {
                    discovery.put("auth", "none");
                }

                anno.add(discovery);
            }
        }

        return singletonMap(Labels.STRIMZI_DISCOVERY_LABEL, anno.encodePrettily());
    }

    /**
     * Utility function to help to determine the type of service based on external listener configuration
     *
     * @return Service type
     */
    private String getExternalServiceType() {
        if (isExposedWithNodePort()) {
            return "NodePort";
        } else if (isExposedWithLoadBalancer()) {
            return "LoadBalancer";
        } else {
            return "ClusterIP";
        }
    }

    /**
     * Generates external bootstrap service. This service is used for exposing it externally.
     * It exposes only the external port 9094.
     * Separate service is used to make sure that we do not expose the internal ports to the outside of the cluster
     *
     * @return The generated Service
     */
    public Service generateExternalBootstrapService() {
        if (isExposed()) {
            String externalBootstrapServiceName = externalBootstrapServiceName(cluster);

            List<ServicePort> ports;
            Integer nodePort = null;
            if (isExposedWithNodePort()) {
                KafkaListenerExternalNodePort externalNodePort = (KafkaListenerExternalNodePort) listeners.getExternal();
                if (externalNodePort.getOverrides() != null && externalNodePort.getOverrides().getBootstrap() != null) {
                    nodePort = externalNodePort.getOverrides().getBootstrap().getNodePort();
                }
            }
            ports = Collections.singletonList(createServicePort(EXTERNAL_PORT_NAME, EXTERNAL_PORT, EXTERNAL_PORT,
                    nodePort, "TCP"));

            Map<String, String> dnsAnnotations = Collections.emptyMap();
            String loadBalancerIP = null;

            if (isExposedWithLoadBalancer())    {
                KafkaListenerExternalLoadBalancer externalLb = (KafkaListenerExternalLoadBalancer) listeners.getExternal();

                if (externalLb.getOverrides() != null && externalLb.getOverrides().getBootstrap() != null) {
                    dnsAnnotations = externalLb.getOverrides().getBootstrap().getDnsAnnotations();
                    loadBalancerIP = externalLb.getOverrides().getBootstrap().getLoadBalancerIP();
                }
            } else if (isExposedWithNodePort()) {
                KafkaListenerExternalNodePort externalNp = (KafkaListenerExternalNodePort) listeners.getExternal();

                if (externalNp.getOverrides() != null && externalNp.getOverrides().getBootstrap() != null) {
                    dnsAnnotations = externalNp.getOverrides().getBootstrap().getDnsAnnotations();
                }
            }

            Service service = createService(externalBootstrapServiceName, getExternalServiceType(), ports,
                    getLabelsWithStrimziName(externalBootstrapServiceName, templateExternalBootstrapServiceLabels), getSelectorLabels(),
                    Util.mergeLabelsOrAnnotations(dnsAnnotations, templateExternalBootstrapServiceAnnotations), loadBalancerIP);

            if (isExposedWithLoadBalancer()) {
                if (templateExternalBootstrapServiceLoadBalancerSourceRanges != null) {
                    service.getSpec().setLoadBalancerSourceRanges(templateExternalBootstrapServiceLoadBalancerSourceRanges);
                }
            }

            if (isExposedWithLoadBalancer() || isExposedWithNodePort()) {
                if (templateExternalBootstrapServiceTrafficPolicy != null)  {
                    service.getSpec().setExternalTrafficPolicy(templateExternalBootstrapServiceTrafficPolicy.toValue());
                }
            }

            return service;
        }

        return null;
    }

    /**
     * Generates service for pod. This service is used for exposing it externally.
     *
     * @param pod Number of the pod for which this service should be generated
     * @return The generated Service
     */
    public Service generateExternalService(int pod) {
        if (isExposed()) {
            String perPodServiceName = externalServiceName(cluster, pod);

            List<ServicePort> ports = new ArrayList<>(1);
            Integer nodePort = null;
            if (isExposedWithNodePort()) {
                KafkaListenerExternalNodePort externalNodePort = (KafkaListenerExternalNodePort) listeners.getExternal();
                if (externalNodePort.getOverrides() != null && externalNodePort.getOverrides().getBrokers() != null) {
                    nodePort = externalNodePort.getOverrides().getBrokers().stream()
                            .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getNodePort() != null)
                            .map(NodePortListenerBrokerOverride::getNodePort)
                            .findAny().orElse(null);
                }
            }
            ports.add(createServicePort(EXTERNAL_PORT_NAME, EXTERNAL_PORT, EXTERNAL_PORT, nodePort, "TCP"));

            Map<String, String> dnsAnnotations = Collections.emptyMap();
            String loadBalancerIP = null;

            if (isExposedWithLoadBalancer())    {
                KafkaListenerExternalLoadBalancer externalLb = (KafkaListenerExternalLoadBalancer) listeners.getExternal();

                if (externalLb.getOverrides() != null && externalLb.getOverrides().getBrokers() != null) {
                    dnsAnnotations = externalLb.getOverrides().getBrokers().stream()
                            .filter(broker -> broker != null && broker.getBroker() == pod)
                            .map(LoadBalancerListenerBrokerOverride::getDnsAnnotations)
                            .findAny()
                            .orElse(Collections.emptyMap());

                    loadBalancerIP = externalLb.getOverrides().getBrokers().stream()
                            .filter(brokerService -> brokerService != null && brokerService.getBroker() == pod
                                    && brokerService.getLoadBalancerIP() != null)
                            .map(LoadBalancerListenerBrokerOverride::getLoadBalancerIP)
                            .findAny()
                            .orElse(null);

                    if (loadBalancerIP != null && loadBalancerIP.isEmpty()) {
                        loadBalancerIP = null;
                    }
                }
            } else if (isExposedWithNodePort()) {
                KafkaListenerExternalNodePort externalNp = (KafkaListenerExternalNodePort) listeners.getExternal();

                if (externalNp.getOverrides() != null && externalNp.getOverrides().getBrokers() != null) {
                    dnsAnnotations = externalNp.getOverrides().getBrokers().stream()
                            .filter(broker -> broker != null && broker.getBroker() == pod)
                            .map(NodePortListenerBrokerOverride::getDnsAnnotations)
                            .findAny()
                            .orElse(Collections.emptyMap());
                }
            }

            Labels selector = getSelectorLabels().withStatefulSetPod(kafkaPodName(cluster, pod));

            Service service = createService(perPodServiceName, getExternalServiceType(), ports,
                    getLabelsWithStrimziName(perPodServiceName, templatePerPodServiceLabels), selector,
                    Util.mergeLabelsOrAnnotations(dnsAnnotations, templatePerPodServiceAnnotations), loadBalancerIP);

            if (isExposedWithLoadBalancer()) {
                if (templatePerPodServiceLoadBalancerSourceRanges != null) {
                    service.getSpec().setLoadBalancerSourceRanges(templatePerPodServiceLoadBalancerSourceRanges);
                }
            }

            if (isExposedWithLoadBalancer() || isExposedWithNodePort()) {
                if (templatePerPodServiceTrafficPolicy != null)  {
                    service.getSpec().setExternalTrafficPolicy(templatePerPodServiceTrafficPolicy.toValue());
                }
            }

            return service;
        }

        return null;
    }

    /**
     * Generates route for pod. This route is used for exposing it externally using OpenShift Routes.
     *
     * @param pod Number of the pod for which this route should be generated
     * @return The generated Route
     */
    public Route generateExternalRoute(int pod) {
        if (isExposedWithRoute()) {
            String perPodServiceName = externalServiceName(cluster, pod);

            Route route = new RouteBuilder()
                    .withNewMetadata()
                        .withName(perPodServiceName)
                        .withLabels(getLabelsWithStrimziName(perPodServiceName, templatePerPodRouteLabels).toMap())
                        .withAnnotations(templatePerPodRouteAnnotations)
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withNewTo()
                            .withKind("Service")
                            .withName(perPodServiceName)
                        .endTo()
                        .withNewPort()
                            .withNewTargetPort(EXTERNAL_PORT)
                        .endPort()
                        .withNewTls()
                            .withTermination("passthrough")
                        .endTls()
                    .endSpec()
                    .build();

            KafkaListenerExternalRoute listener = (KafkaListenerExternalRoute) listeners.getExternal();
            if (listener.getOverrides() != null && listener.getOverrides().getBrokers() != null) {
                String specHost = listener.getOverrides().getBrokers().stream()
                        .filter(broker -> broker != null && broker.getBroker() == pod
                                && broker.getHost() != null)
                        .map(RouteListenerBrokerOverride::getHost)
                        .findAny()
                        .orElse(null);

                if (specHost != null && !specHost.isEmpty()) {
                    route.getSpec().setHost(specHost);
                }
            }

            return route;
        }

        return null;
    }

    /**
     * Generates a bootstrap route which can be used to bootstrap clients outside of OpenShift.
     *
     * @return The generated Routes
     */
    public Route generateExternalBootstrapRoute() {
        if (isExposedWithRoute()) {
            Route route = new RouteBuilder()
                    .withNewMetadata()
                        .withName(serviceName)
                        .withLabels(getLabelsWithStrimziName(serviceName, templateExternalBootstrapRouteLabels).toMap())
                        .withAnnotations(Util.mergeLabelsOrAnnotations(null, templateExternalBootstrapRouteAnnotations))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withNewTo()
                            .withKind("Service")
                            .withName(externalBootstrapServiceName(cluster))
                        .endTo()
                        .withNewPort()
                            .withNewTargetPort(EXTERNAL_PORT)
                        .endPort()
                        .withNewTls()
                            .withTermination("passthrough")
                        .endTls()
                    .endSpec()
                    .build();

            KafkaListenerExternalRoute listener = (KafkaListenerExternalRoute) listeners.getExternal();
            if (listener.getOverrides() != null && listener.getOverrides().getBootstrap() != null && listener.getOverrides().getBootstrap().getHost() != null && !listener.getOverrides().getBootstrap().getHost().isEmpty()) {
                route.getSpec().setHost(listener.getOverrides().getBootstrap().getHost());
            }

            return route;
        }

        return null;
    }

    /**
     * Generates ingress for pod. This ingress is used for exposing it externally using Nginx Ingress.
     *
     * @param pod Number of the pod for which this ingress should be generated
     * @return The generated Ingress
     */
    public Ingress generateExternalIngress(int pod) {
        if (isExposedWithIngress()) {
            KafkaListenerExternalIngress listener = (KafkaListenerExternalIngress) listeners.getExternal();
            Map<String, String> dnsAnnotations = null;
            String host = null;

            if (listener.getConfiguration() != null && listener.getConfiguration().getBrokers() != null) {
                host = listener.getConfiguration().getBrokers().stream()
                        .filter(broker -> broker != null && broker.getBroker() == pod
                                && broker.getHost() != null)
                        .map(IngressListenerBrokerConfiguration::getHost)
                        .findAny()
                        .orElseThrow(() -> new InvalidResourceException("Hostname for broker with id " + pod + " is required for exposing Kafka cluster using Ingress"));

                dnsAnnotations = listener.getConfiguration().getBrokers().stream()
                        .filter(broker -> broker != null && broker.getBroker() == pod)
                        .map(IngressListenerBrokerConfiguration::getDnsAnnotations)
                        .findAny()
                        .orElse(null);
            }

            String perPodServiceName = externalServiceName(cluster, pod);

            HTTPIngressPath path = new HTTPIngressPathBuilder()
                    .withPath("/")
                    .withNewBackend()
                        .withNewServicePort(EXTERNAL_PORT)
                        .withServiceName(perPodServiceName)
                    .endBackend()
                    .build();

            IngressRule rule = new IngressRuleBuilder()
                    .withHost(host)
                    .withNewHttp()
                        .withPaths(path)
                    .endHttp()
                    .build();

            IngressTLS tls = new IngressTLSBuilder()
                    .withHosts(host)
                    .build();

            Ingress ingress = new IngressBuilder()
                    .withNewMetadata()
                        .withName(perPodServiceName)
                        .withLabels(getLabelsWithStrimziName(perPodServiceName, templatePerPodIngressLabels).toMap())
                        .withAnnotations(Util.mergeLabelsOrAnnotations(generateInternalIngressAnnotations(listener), templatePerPodIngressAnnotations, dnsAnnotations))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withRules(rule)
                        .withTls(tls)
                    .endSpec()
                    .build();

            return ingress;
        }

        return null;
    }

    /**
     * Generates a bootstrap ingress which can be used to bootstrap clients outside of Kubernetes.
     *
     * @return The generated Ingress
     */
    public Ingress generateExternalBootstrapIngress() {
        if (isExposedWithIngress()) {
            KafkaListenerExternalIngress listener = (KafkaListenerExternalIngress) listeners.getExternal();
            Map<String, String> dnsAnnotations;
            String host;

            if (listener.getConfiguration() != null && listener.getConfiguration().getBootstrap() != null && listener.getConfiguration().getBootstrap().getHost() != null) {
                host = listener.getConfiguration().getBootstrap().getHost();
                dnsAnnotations = listener.getConfiguration().getBootstrap().getDnsAnnotations();
            } else {
                throw new InvalidResourceException("Bootstrap hostname is required for exposing Kafka cluster using Ingress");
            }

            HTTPIngressPath path = new HTTPIngressPathBuilder()
                    .withPath("/")
                    .withNewBackend()
                        .withNewServicePort(EXTERNAL_PORT)
                        .withServiceName(externalBootstrapServiceName(cluster))
                    .endBackend()
                    .build();

            IngressRule rule = new IngressRuleBuilder()
                    .withHost(host)
                    .withNewHttp()
                        .withPaths(path)
                    .endHttp()
                    .build();

            IngressTLS tls = new IngressTLSBuilder()
                    .withHosts(host)
                    .build();

            Ingress ingress = new IngressBuilder()
                    .withNewMetadata()
                        .withName(serviceName)
                        .withLabels(getLabelsWithStrimziName(serviceName, templateExternalBootstrapIngressLabels).toMap())
                        .withAnnotations(Util.mergeLabelsOrAnnotations(generateInternalIngressAnnotations(listener), templateExternalBootstrapIngressAnnotations, dnsAnnotations))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withRules(rule)
                        .withTls(tls)
                    .endSpec()
                    .build();

            return ingress;
        }

        return null;
    }

    /**
     * Generates the annotations needed to configure the Ingress as TLS passthrough
     *
     * @param ingressListener The Ingress listener object with additional parameters and options
     * @return Map with the annotations
     */
    private Map<String, String> generateInternalIngressAnnotations(KafkaListenerExternalIngress ingressListener) {
        Map<String, String> internalAnnotations = new HashMap<>(4);

        if (ingressListener.getIngressClass() != null) {
            internalAnnotations.put("kubernetes.io/ingress.class", ingressListener.getIngressClass());
        } else {
            internalAnnotations.put("kubernetes.io/ingress.class", "nginx");
        }

        internalAnnotations.put("ingress.kubernetes.io/ssl-passthrough", "true");
        internalAnnotations.put("nginx.ingress.kubernetes.io/ssl-passthrough", "true");
        internalAnnotations.put("nginx.ingress.kubernetes.io/backend-protocol", "HTTPS");

        return internalAnnotations;
    }

    /**
     * Generates a headless Service according to configured defaults
     *
     * @return The generated Service
     */
    public Service generateHeadlessService() {
        return createHeadlessService(getHeadlessServicePorts());
    }

    /**
     * Generates a StatefulSet according to configured defaults
     *
     * @param isOpenShift      True iff this operator is operating within OpenShift.
     * @param imagePullPolicy  The image pull policy.
     * @param imagePullSecrets The image pull secrets.
     * @return The generated StatefulSet.
     */
    public StatefulSet generateStatefulSet(boolean isOpenShift,
                                           ImagePullPolicy imagePullPolicy,
                                           List<LocalObjectReference> imagePullSecrets) {
        Map<String, String> stsAnnotations = new HashMap<>(2);
        stsAnnotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion.version());
        stsAnnotations.put(ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(storage));

        return createStatefulSet(
                stsAnnotations,
                emptyMap(),
                getVolumes(isOpenShift),
                getVolumeClaims(),
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                imagePullSecrets,
                isOpenShift);
    }

    /**
     * Generate the Secret containing the Kafka brokers certificates signed by the cluster CA certificate used for TLS based
     * internal communication with Zookeeper.
     * It also contains the related Kafka brokers private keys.
     *
     * @return The generated Secret
     */
    public Secret generateBrokersSecret() {

        Map<String, String> data = new HashMap<>(replicas * 4);
        for (int i = 0; i < replicas; i++) {
            CertAndKey cert = brokerCerts.get(KafkaCluster.kafkaPodName(cluster, i));
            data.put(KafkaCluster.kafkaPodName(cluster, i) + ".key", cert.keyAsBase64String());
            data.put(KafkaCluster.kafkaPodName(cluster, i) + ".crt", cert.certAsBase64String());
            data.put(KafkaCluster.kafkaPodName(cluster, i) + ".p12", cert.keyStoreAsBase64String());
            data.put(KafkaCluster.kafkaPodName(cluster, i) + ".password", cert.storePasswordAsBase64String());
        }
        return createSecret(KafkaCluster.brokersSecretName(cluster), data);
    }

    /**
     * Generate the Secret containing the username and password to secure the jmx port on the kafka brokers
     *
     * @return The generated Secret
     */
    public Secret generateJmxSecret() {
        Map<String, String> data = new HashMap<>(2);
        String[] keys = {SECRET_JMX_USERNAME_KEY, SECRET_JMX_PASSWORD_KEY};
        PasswordGenerator passwordGenerator = new PasswordGenerator(16);
        for (String key : keys) {
            data.put(key, Base64.getEncoder().encodeToString(passwordGenerator.generate().getBytes(StandardCharsets.US_ASCII)));
        }

        return createSecret(KafkaCluster.jmxSecretName(cluster), data);
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(5);
        portList.add(createContainerPort(REPLICATION_PORT_NAME, REPLICATION_PORT, "TCP"));

        if (listeners != null && listeners.getPlain() != null) {
            portList.add(createContainerPort(CLIENT_PORT_NAME, CLIENT_PORT, "TCP"));
        }

        if (listeners != null && listeners.getTls() != null) {
            portList.add(createContainerPort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, "TCP"));
        }

        if (isExposed()) {
            portList.add(createContainerPort(EXTERNAL_PORT_NAME, EXTERNAL_PORT, "TCP"));
        }

        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    /**
     * @return The port of the plain listener.
     */
    public int getClientPort() {
        return this.CLIENT_PORT;
    }

    /**
     * @return The port of the tls listener
     */
    public int getClientTlsPort() {
        return this.CLIENT_TLS_PORT;
    }

    /**
     * @return The port of loadbalancer for the external listener.
     */
    public int getLoadbalancerPort() {
        return this.EXTERNAL_PORT;
    }

    /**
     * @return The port of route for the external listener.
     */
    public int getRoutePort() {
        return this.ROUTE_PORT;
    }

    /**
     * Fill the StatefulSet with volumes, persistent volume claims and related volume mount paths for the storage
     * It's called recursively on the related inner volumes if the storage is of {@link Storage#TYPE_JBOD} type
     *
     * @param storage the Storage instance from which building volumes, persistent volume claims and
     *                related volume mount paths
     */
    private void setDataVolumesClaimsAndMountPaths(Storage storage) {
        dataVolumeMountPaths = VolumeUtils.getDataVolumeMountPaths(storage, mountPath);
        dataPvcs = VolumeUtils.getDataPersistentVolumeClaims(storage);
        dataVolumes = VolumeUtils.getDataVolumes(storage);
    }

    /**
     * Generate the persistent volume claims for the storage It's called recursively on the related inner volumes if the
     * storage is of {@link Storage#TYPE_JBOD} type.
     *
     * @param storage the Storage instance from which building volumes, persistent volume claims and
     *                related volume mount paths.
     * @return The PersistentVolumeClaims.
     */
    public List<PersistentVolumeClaim> generatePersistentVolumeClaims(Storage storage) {
        List<PersistentVolumeClaim> pvcs = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof PersistentClaimStorage) {
                Integer id = ((PersistentClaimStorage) storage).getId();
                String pvcBaseName = VolumeUtils.getVolumePrefix(id) + "-" + name;

                for (int i = 0; i < replicas; i++) {
                    pvcs.add(createPersistentVolumeClaim(i, pvcBaseName + "-" + i, (PersistentClaimStorage) storage));
                }
            } else if (storage instanceof JbodStorage) {
                for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
                    if (volume.getId() == null)
                        throw new InvalidResourceException("Volumes under JBOD storage type have to have 'id' property");
                    // it's called recursively for setting the information from the current volume
                    pvcs.addAll(generatePersistentVolumeClaims(volume));
                }
            }
        }

        return pvcs;
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>();
        volumeList.addAll(dataVolumes);

        if (rack != null || isExposedWithNodePort()) {
            volumeList.add(VolumeUtils.createEmptyDirVolume(INIT_VOLUME_NAME, null));
        }

        volumeList.add(VolumeUtils.createSecretVolume(CLUSTER_CA_CERTS_VOLUME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(BROKER_CERTS_VOLUME, KafkaCluster.brokersSecretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(CLIENT_CA_CERTS_VOLUME, KafkaCluster.clientsCaCertSecretName(cluster), isOpenShift));

        if (secretSourceExternal != null) {
            Map<String, String> items = new HashMap<>(2);
            items.put(secretSourceExternal.getKey(), "tls.key");
            items.put(secretSourceExternal.getCertificate(), "tls.crt");

            volumeList.add(VolumeUtils.createSecretVolume("custom-external-9094-certs", this.secretSourceExternal.getSecretName(), items, isOpenShift));
        }

        if (secretSourceTls != null) {
            Map<String, String> items = new HashMap<>(2);
            items.put(secretSourceTls.getKey(), "tls.key");
            items.put(secretSourceTls.getCertificate(), "tls.crt");

            volumeList.add(VolumeUtils.createSecretVolume("custom-tls-9093-certs", this.secretSourceTls.getSecretName(), items, isOpenShift));
        }
        volumeList.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
        volumeList.add(new VolumeBuilder().withName("ready-files").withNewEmptyDir().withMedium("Memory").endEmptyDir().build());

        if (listeners != null) {
            if (listeners.getPlain() != null) {
                if (listeners.getPlain().getAuth() != null) {
                    if (listeners.getPlain().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getPlain().getAuth();
                        volumeList.addAll(AuthenticationUtils.configureOauthCertificateVolumes("oauth-plain-9092", oauth.getTlsTrustedCertificates(), isOpenShift));
                    }
                }
            }

            if (listeners.getTls() != null) {
                if (listeners.getTls().getAuth() != null) {
                    if (listeners.getTls().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getTls().getAuth();
                        volumeList.addAll(AuthenticationUtils.configureOauthCertificateVolumes("oauth-tls-9093", oauth.getTlsTrustedCertificates(), isOpenShift));
                    }
                }
            }

            if (listeners.getExternal() != null) {
                if (listeners.getExternal().getAuth() != null) {
                    if (listeners.getExternal().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getExternal().getAuth();
                        volumeList.addAll(AuthenticationUtils.configureOauthCertificateVolumes("oauth-external-9094", oauth.getTlsTrustedCertificates(), isOpenShift));
                    }
                }
            }
        }

        if (authorization instanceof KafkaAuthorizationKeycloak) {
            KafkaAuthorizationKeycloak keycloakAuthz = (KafkaAuthorizationKeycloak) authorization;
            volumeList.addAll(AuthenticationUtils.configureOauthCertificateVolumes("authz-keycloak", keycloakAuthz.getTlsTrustedCertificates(), isOpenShift));
        }

        return volumeList;
    }

    /* test */ List<PersistentVolumeClaim> getVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>();
        pvcList.addAll(dataPvcs);
        return pvcList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.addAll(dataVolumeMountPaths);

        volumeMountList.add(VolumeUtils.createVolumeMount(CLUSTER_CA_CERTS_VOLUME, CLUSTER_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(VolumeUtils.createVolumeMount(BROKER_CERTS_VOLUME, BROKER_CERTS_VOLUME_MOUNT));
        volumeMountList.add(VolumeUtils.createVolumeMount(CLIENT_CA_CERTS_VOLUME, CLIENT_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
        volumeMountList.add(VolumeUtils.createVolumeMount("ready-files", "/var/opt/kafka"));

        if (secretSourceExternal != null) {
            volumeMountList.add(VolumeUtils.createVolumeMount("custom-external-9094-certs", "/opt/kafka/certificates/custom-external-9094-certs"));
        }

        if (secretSourceTls != null) {
            volumeMountList.add(VolumeUtils.createVolumeMount("custom-tls-9093-certs", "/opt/kafka/certificates/custom-tls-9093-certs"));
        }

        if (rack != null || isExposedWithNodePort()) {
            volumeMountList.add(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT));
        }

        if (listeners != null) {
            if (listeners.getPlain() != null) {
                if (listeners.getPlain().getAuth() != null) {
                    if (listeners.getPlain().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getPlain().getAuth();
                        volumeMountList.addAll(AuthenticationUtils.configureOauthCertificateVolumeMounts("oauth-plain-9092", oauth.getTlsTrustedCertificates(), OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs"));
                    }
                }
            }

            if (listeners.getTls() != null) {
                if (listeners.getTls().getAuth() != null) {
                    if (listeners.getTls().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getTls().getAuth();
                        volumeMountList.addAll(AuthenticationUtils.configureOauthCertificateVolumeMounts("oauth-tls-9093", oauth.getTlsTrustedCertificates(), OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-tls-9093-certs"));
                    }
                }
            }

            if (listeners.getExternal() != null) {
                if (listeners.getExternal().getAuth() != null) {
                    if (listeners.getExternal().getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getExternal().getAuth();
                        volumeMountList.addAll(AuthenticationUtils.configureOauthCertificateVolumeMounts("oauth-external-9094", oauth.getTlsTrustedCertificates(), OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-external-9094-certs"));
                    }
                }
            }
        }

        if (authorization instanceof KafkaAuthorizationKeycloak) {
            KafkaAuthorizationKeycloak keycloakAuthz = (KafkaAuthorizationKeycloak) authorization;
            volumeMountList.addAll(AuthenticationUtils.configureOauthCertificateVolumeMounts("authz-keycloak", keycloakAuthz.getTlsTrustedCertificates(), OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-keycloak-certs"));
        }

        return volumeMountList;
    }

    /**
     * Returns a combined affinity: Adding the affinity needed for the "kafka-rack" to the {@link #getUserAffinity()}.
     */
    @Override
    protected Affinity getMergedAffinity() {
        Affinity userAffinity = getUserAffinity();
        AffinityBuilder builder = new AffinityBuilder(userAffinity == null ? new Affinity() : userAffinity);
        if (rack != null) {
            // If there's a rack config, we need to add a podAntiAffinity to spread the brokers among the racks
            builder = builder
                    .editOrNewPodAntiAffinity()
                        .addNewPreferredDuringSchedulingIgnoredDuringExecution()
                            .withWeight(100)
                            .withNewPodAffinityTerm()
                                .withTopologyKey(rack.getTopologyKey())
                                .withNewLabelSelector()
                                    .addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, cluster)
                                    .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, name)
                                .endLabelSelector()
                            .endPodAffinityTerm()
                        .endPreferredDuringSchedulingIgnoredDuringExecution()
                    .endPodAntiAffinity();

            // We also need to add node affinity to make sure the pods are scheduled only on nodes with the rack label
            NodeSelectorRequirement selector = new NodeSelectorRequirementBuilder()
                    .withNewOperator("Exists")
                    .withNewKey(rack.getTopologyKey())
                    .build();

            if (userAffinity != null
                    && userAffinity.getNodeAffinity() != null
                    && userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution() != null
                    && userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms() != null) {
                // User has specified some Node Selector Terms => we should enhance them
                List<NodeSelectorTerm> oldTerms = userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms();
                List<NodeSelectorTerm> enhancedTerms = new ArrayList<>(oldTerms.size());

                for (NodeSelectorTerm term : oldTerms) {
                    NodeSelectorTerm enhancedTerm = new NodeSelectorTermBuilder(term)
                            .addToMatchExpressions(selector)
                            .build();
                    enhancedTerms.add(enhancedTerm);
                }

                builder = builder
                        .editOrNewNodeAffinity()
                            .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                                .withNodeSelectorTerms(enhancedTerms)
                            .endRequiredDuringSchedulingIgnoredDuringExecution()
                        .endNodeAffinity();
            } else {
                // User has not specified any selector terms => we add our own
                builder = builder
                        .editOrNewNodeAffinity()
                            .editOrNewRequiredDuringSchedulingIgnoredDuringExecution()
                                .addNewNodeSelectorTerm()
                                    .withMatchExpressions(selector)
                                .endNodeSelectorTerm()
                            .endRequiredDuringSchedulingIgnoredDuringExecution()
                        .endNodeAffinity();
            }
        }

        return builder.build();
    }

    protected List<EnvVar> getInitContainerEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVarFromFieldRef(ENV_VAR_KAFKA_INIT_NODE_NAME, "spec.nodeName"));

        if (rack != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY, rack.getTopologyKey()));
        }

        if (isExposedWithNodePort()) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS, "TRUE"));

            KafkaListenerExternalNodePort listener = (KafkaListenerExternalNodePort) listeners.getExternal();

            if (listener.getConfiguration() != null && listener.getConfiguration().getPreferredAddressType() != null)    {
                varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS_TYPE, listener.getConfiguration().getPreferredAddressType().toValue()));
            }
        }

        // Add shared environment variables used for all containers
        varList.addAll(getSharedEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateInitContainerEnvVars);

        return varList;
    }

    @Override
    protected List<Container> getInitContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> initContainers = new ArrayList<>(1);

        if (rack != null || isExposedWithNodePort()) {
            ResourceRequirements resources = new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100m"))
                    .addToRequests("memory", new Quantity("128Mi"))
                    .addToLimits("cpu", new Quantity("1"))
                    .addToLimits("memory", new Quantity("256Mi"))
                    .build();

            Container initContainer = new ContainerBuilder()
                    .withName(INIT_NAME)
                    .withImage(initImage)
                    .withArgs("/opt/strimzi/bin/kafka_init_run.sh")
                    .withResources(resources)
                    .withEnv(getInitContainerEnvVars())
                    .withVolumeMounts(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT))
                    .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, initImage))
                    .withSecurityContext(templateInitContainerSecurityContext)
                    .build();

            initContainers.add(initContainer);
        }

        return initContainers;
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        Container container = new ContainerBuilder()
                .withName(KAFKA_NAME)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withVolumeMounts(getVolumeMounts())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ModelUtils.newProbeBuilder(livenessProbeOptions)
                        .withNewExec()
                            .withCommand("/opt/kafka/kafka_liveness.sh")
                        .endExec().build())
                .withReadinessProbe(ModelUtils.newProbeBuilder(readinessProbeOptions)
                        .withNewExec()
                            // The kafka-agent will create /var/opt/kafka/kafka-ready in the container
                            .withCommand("test", "-f", "/var/opt/kafka/kafka-ready")
                        .endExec().build())
                .withResources(getResources())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withCommand("/opt/kafka/kafka_run.sh")
                .withSecurityContext(templateKafkaContainerSecurityContext)
                .build();

        return singletonList(container);
    }

    @Override
    protected String getServiceAccountName() {
        return initContainerServiceAccountName(cluster);
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_KAFKA_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        if (javaSystemProperties != null) {
            varList.add(buildEnvVar(ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, ModelUtils.getJavaSystemPropertiesToString(javaSystemProperties)));
        }

        heapOptions(varList, 0.5, 5L * 1024L * 1024L * 1024L);
        jvmPerformanceOptions(varList);

        if (listeners != null) {
            if (listeners.getPlain() != null) {
                if (listeners.getPlain().getAuth() != null) {
                    if (KafkaListenerAuthenticationOAuth.TYPE_OAUTH.equals(listeners.getPlain().getAuth().getType())) {
                        // set OAUTH configuration
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getPlain().getAuth();

                        if (oauth.getClientSecret() != null)    {
                            varList.add(buildEnvVarFromSecret(ENV_VAR_STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET, oauth.getClientSecret().getSecretName(), oauth.getClientSecret().getKey()));
                        }
                    }
                }
            }

            if (listeners.getTls() != null) {
                if (listeners.getTls().getAuth() != null) {
                    if (KafkaListenerAuthenticationOAuth.TYPE_OAUTH.equals(listeners.getTls().getAuth().getType())) {
                        // set OAUTH configuration
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getTls().getAuth();

                        if (oauth.getClientSecret() != null)    {
                            varList.add(buildEnvVarFromSecret(ENV_VAR_STRIMZI_TLS_9093_OAUTH_CLIENT_SECRET, oauth.getClientSecret().getSecretName(), oauth.getClientSecret().getKey()));
                        }
                    }
                }
            }

            if (listeners.getExternal() != null) {
                if (listeners.getExternal().getAuth() != null) {

                    if (KafkaListenerAuthenticationOAuth.TYPE_OAUTH.equals(listeners.getExternal().getAuth().getType())) {
                        // set OAUTH configuration
                        KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listeners.getExternal().getAuth();

                        if (oauth.getClientSecret() != null)    {
                            varList.add(buildEnvVarFromSecret(ENV_VAR_STRIMZI_EXTERNAL_9094_OAUTH_CLIENT_SECRET, oauth.getClientSecret().getSecretName(), oauth.getClientSecret().getKey()));
                        }
                    }
                }
            }
        }

        if (isJmxEnabled()) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_JMX_ENABLED, "true"));
            if (isJmxAuthenticated) {
                varList.add(buildEnvVarFromSecret(ENV_VAR_KAFKA_JMX_USERNAME, jmxSecretName(cluster), SECRET_JMX_USERNAME_KEY));
                varList.add(buildEnvVarFromSecret(ENV_VAR_KAFKA_JMX_PASSWORD, jmxSecretName(cluster), SECRET_JMX_PASSWORD_KEY));
            }
        }

        // Add shared environment variables used for all containers
        varList.addAll(getSharedEnvVars());

        // Add user defined environment variables to the Kafka broker containers
        addContainerEnvsToExistingEnvs(varList, templateKafkaContainerEnvVars);

        return varList;
    }

    /**
     * Validates provided OAuth configuration. Throws InvalidResourceException when OAuth configuration contains forbidden combinations.
     *
     * @param oAuth     OAuth type authentication object
     */
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private static void validateOauth(KafkaListenerAuthenticationOAuth oAuth, String listener) {
        boolean hasJwksRefreshSecondsValidInput =  oAuth.getJwksRefreshSeconds() != null && oAuth.getJwksRefreshSeconds() > 0;
        boolean hasJwksExpirySecondsValidInput = oAuth.getJwksExpirySeconds() != null && oAuth.getJwksExpirySeconds() > 0;
        boolean hasJwksMinRefreshPauseSecondsValidInput = oAuth.getJwksMinRefreshPauseSeconds() != null && oAuth.getJwksMinRefreshPauseSeconds() >= 0;

        if (oAuth.getIntrospectionEndpointUri() == null && oAuth.getJwksEndpointUri() == null) {
            log.error("{}: Introspection endpoint URI or JWKS endpoint URI has to be specified", listener);
            throw new InvalidResourceException(listener + ": Introspection endpoint URI or JWKS endpoint URI has to be specified");
        }

        if (oAuth.getValidIssuerUri() == null && oAuth.isCheckIssuer()) {
            log.error("{}: Valid Issuer URI has to be specified or 'checkIssuer' set to false", listener);
            throw new InvalidResourceException(listener + ": Valid Issuer URI has to be specified or 'checkIssuer' set to false");
        }

        if (oAuth.getIntrospectionEndpointUri() != null && (oAuth.getClientId() == null || oAuth.getClientSecret() == null)) {
            log.error("{}: Introspection Endpoint URI needs to be configured together with clientId and clientSecret", listener);
            throw new InvalidResourceException(listener + ": Introspection Endpoint URI needs to be configured together with clientId and clientSecret");
        }

        if (oAuth.getUserInfoEndpointUri() != null && oAuth.getIntrospectionEndpointUri() == null) {
            log.error("{}: User Info Endpoint URI can only be used if Introspection Endpoint URI is also configured", listener);
            throw new InvalidResourceException(listener + ": User Info Endpoint URI can only be used if the Introspection Endpoint URI is also configured");
        }

        if (oAuth.getJwksEndpointUri() == null && (hasJwksRefreshSecondsValidInput || hasJwksExpirySecondsValidInput || hasJwksMinRefreshPauseSecondsValidInput)) {
            log.error("{}: jwksRefreshSeconds, jwksExpirySeconds and jwksMinRefreshPauseSeconds can only be used together with jwksEndpointUri", listener);
            throw new InvalidResourceException(listener + ": jwksRefreshSeconds, jwksExpirySeconds and jwksMinRefreshPauseSeconds can only be used together with jwksEndpointUri");
        }

        if (oAuth.getJwksRefreshSeconds() != null && !hasJwksRefreshSecondsValidInput) {
            log.error("{}: jwksRefreshSeconds needs to be a positive integer (set to: {})", listener, oAuth.getJwksRefreshSeconds());
            throw new InvalidResourceException(listener + ": jwksRefreshSeconds needs to be a positive integer (set to: " + oAuth.getJwksRefreshSeconds() + ")");
        }

        if (oAuth.getJwksExpirySeconds() != null && !hasJwksExpirySecondsValidInput) {
            log.error("{}: jwksExpirySeconds needs to be a positive integer (set to: {})", listener, oAuth.getJwksExpirySeconds());
            throw new InvalidResourceException(listener + ": jwksExpirySeconds needs to be a positive integer (set to: " + oAuth.getJwksExpirySeconds() + ")");
        }

        if (oAuth.getJwksMinRefreshPauseSeconds() != null && !hasJwksMinRefreshPauseSecondsValidInput) {
            log.error("{}: jwksMinRefreshPauseSeconds needs to be a positive integer or zero (set to: {})", listener, oAuth.getJwksMinRefreshPauseSeconds());
            throw new InvalidResourceException(listener + ": jwksMinRefreshPauseSeconds needs to be a positive integer or zero (set to: " + oAuth.getJwksMinRefreshPauseSeconds() + ")");
        }

        if ((hasJwksExpirySecondsValidInput && hasJwksRefreshSecondsValidInput && oAuth.getJwksExpirySeconds() < oAuth.getJwksRefreshSeconds() + 60) ||
                (!hasJwksExpirySecondsValidInput && hasJwksRefreshSecondsValidInput && KafkaListenerAuthenticationOAuth.DEFAULT_JWKS_EXPIRY_SECONDS < oAuth.getJwksRefreshSeconds() + 60) ||
                (hasJwksExpirySecondsValidInput && !hasJwksRefreshSecondsValidInput && oAuth.getJwksExpirySeconds() < KafkaListenerAuthenticationOAuth.DEFAULT_JWKS_REFRESH_SECONDS + 60)) {
            log.error("{}: The refresh interval has to be at least 60 seconds shorter then the expiry interval specified in `jwksExpirySeconds`", listener);
            throw new InvalidResourceException(listener + ": The refresh interval has to be at least 60 seconds shorter then the expiry interval specified in `jwksExpirySeconds`");
        }

        if (!oAuth.isAccessTokenIsJwt()) {
            if (oAuth.getJwksEndpointUri() != null) {
                log.error("{}: accessTokenIsJwt=false can not be used together with jwksEndpointUri", listener);
                throw new InvalidResourceException(listener + ": accessTokenIsJwt=false can not be used together with jwksEndpointUri");
            }
            if (!oAuth.isCheckAccessTokenType()) {
                log.error("{}: checkAccessTokenType can not be set to false when accessTokenIsJwt is false", listener);
                throw new InvalidResourceException(listener + ": checkAccessTokenType can not be set to false when accessTokenIsJwt is false");
            }
        }

        if (!oAuth.isCheckAccessTokenType() && oAuth.getIntrospectionEndpointUri() != null) {
            log.error("{}: checkAccessTokenType=false can not be used together with introspectionEndpointUri", listener);
            throw new InvalidResourceException(listener + ": checkAccessTokenType=false can not be used together with introspectionEndpointUri");
        }

        if (oAuth.getValidTokenType() != null && oAuth.getIntrospectionEndpointUri() == null) {
            log.error("{}: validTokenType can only be used with introspectionEndpointUri", listener);
            throw new InvalidResourceException(listener + ": validTokenType can only be used with introspectionEndpointUri");

        }
    }

    protected void setRack(Rack rack) {
        this.rack = rack;
    }

    protected void setInitImage(String initImage) {
        this.initImage = initImage;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "kafkaDefaultLoggingProperties";
    }

    /**
     * Get the name of the kafka service account given the name of the {@code kafkaResourceName}.
     *
     * @param kafkaResourceName The name of the Kafka resource.
     * @return The name of the ServiceAccount.
     */
    public static String initContainerServiceAccountName(String kafkaResourceName) {
        return kafkaClusterName(kafkaResourceName);
    }

    /**
     * Get the name of the kafka init container role binding given the name of the {@code namespace} and {@code cluster}.
     *
     * @param namespace The namespace.
     * @param cluster   The cluster name.
     * @return The name of the init container's cluster role binding.
     */
    public static String initContainerClusterRoleBindingName(String namespace, String cluster) {
        return "strimzi-" + namespace + "-" + cluster + "-kafka-init";
    }

    /**
     * Creates the ClusterRoleBinding which is used to bind the Kafka SA to the ClusterRole
     * which permissions the Kafka init container to access K8S nodes (necessary for rack-awareness).
     *
     * @param assemblyNamespace The namespace.
     * @return The cluster role binding.
     */
    public ClusterRoleBinding generateClusterRoleBinding(String assemblyNamespace) {
        if (rack != null || isExposedWithNodePort()) {
            Subject ks = new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName(initContainerServiceAccountName(cluster))
                    .withNamespace(assemblyNamespace)
                    .build();

            RoleRef roleRef = new RoleRefBuilder()
                    .withName("strimzi-kafka-broker")
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .build();

            return new ClusterRoleBindingBuilder()
                    .withNewMetadata()
                        .withName(initContainerClusterRoleBindingName(namespace, cluster))
                        .withOwnerReferences(createOwnerReference())
                        .withLabels(labels.toMap())
                    .endMetadata()
                    .withSubjects(ks)
                    .withRoleRef(roleRef)
                    .build();
        } else {
            return null;
        }
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the network policy.
     */
    public static String policyName(String cluster) {
        return cluster + NETWORK_POLICY_KEY_SUFFIX + NAME_SUFFIX;
    }

    /**
     * @param namespaceAndPodSelectorNetworkPolicySupported whether the kube cluster supports namespace selectors
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy(boolean namespaceAndPodSelectorNetworkPolicySupported) {
        List<NetworkPolicyIngressRule> rules = new ArrayList<>(5);

        NetworkPolicyIngressRule replicationRule = new NetworkPolicyIngressRuleBuilder()
                .addNewPort()
                    .withNewPort(REPLICATION_PORT)
                .endPort()
                .build();

        // Restrict access to 9091 / replication port
        if (namespaceAndPodSelectorNetworkPolicySupported) {
            NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                    .withNewPodSelector() // cluster operator
                        .addToMatchLabels(Labels.STRIMZI_KIND_LABEL, "cluster-operator")
                    .endPodSelector()
                    .withNewNamespaceSelector()
                    .endNamespaceSelector()
                    .build();

            NetworkPolicyPeer kafkaClusterPeer = new NetworkPolicyPeerBuilder()
                    .withNewPodSelector() // kafka cluster
                        .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, kafkaClusterName(cluster))
                    .endPodSelector()
                    .build();

            NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeerBuilder()
                    .withNewPodSelector() // entity operator
                        .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(cluster))
                    .endPodSelector()
                    .build();

            NetworkPolicyPeer kafkaExporterPeer = new NetworkPolicyPeerBuilder()
                    .withNewPodSelector() // kafka exporter
                        .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, KafkaExporter.kafkaExporterName(cluster))
                    .endPodSelector()
                    .build();

            NetworkPolicyPeer cruiseControlPeer = new NetworkPolicyPeerBuilder()
                    .withNewPodSelector() // cruise control
                    .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, CruiseControl.cruiseControlName(cluster))
                    .endPodSelector()
                    .build();

            List<NetworkPolicyPeer> clientsPortPeers = new ArrayList<>(4);
            clientsPortPeers.add(clusterOperatorPeer);
            clientsPortPeers.add(kafkaClusterPeer);
            clientsPortPeers.add(entityOperatorPeer);
            clientsPortPeers.add(kafkaExporterPeer);
            clientsPortPeers.add(cruiseControlPeer);

            replicationRule.setFrom(clientsPortPeers);
        }

        rules.add(replicationRule);

        // Free access to 9092, 9093 and 9094 ports
        if (listeners != null) {
            if (listeners.getPlain() != null) {
                NetworkPolicyPort plainPort = new NetworkPolicyPort();
                plainPort.setPort(new IntOrString(CLIENT_PORT));

                NetworkPolicyIngressRule plainRule = new NetworkPolicyIngressRuleBuilder()
                        .withPorts(plainPort)
                        .withFrom(listeners.getPlain().getNetworkPolicyPeers())
                        .build();

                rules.add(plainRule);
            }

            if (listeners.getTls() != null) {
                NetworkPolicyPort tlsPort = new NetworkPolicyPort();
                tlsPort.setPort(new IntOrString(CLIENT_TLS_PORT));

                NetworkPolicyIngressRule tlsRule = new NetworkPolicyIngressRuleBuilder()
                        .withPorts(tlsPort)
                        .withFrom(listeners.getTls().getNetworkPolicyPeers())
                        .build();

                rules.add(tlsRule);
            }

            if (isExposed()) {
                NetworkPolicyPort externalPort = new NetworkPolicyPort();
                externalPort.setPort(new IntOrString(EXTERNAL_PORT));

                NetworkPolicyIngressRule externalRule = new NetworkPolicyIngressRuleBuilder()
                        .withPorts(externalPort)
                        .withFrom(listeners.getExternal().getNetworkPolicyPeers())
                        .build();

                rules.add(externalRule);
            }
        }

        if (isMetricsEnabled) {
            NetworkPolicyPort metricsPort = new NetworkPolicyPort();
            metricsPort.setPort(new IntOrString(METRICS_PORT));

            NetworkPolicyIngressRule metricsRule = new NetworkPolicyIngressRuleBuilder()
                    .withPorts(metricsPort)
                    .withFrom()
                    .build();

            rules.add(metricsRule);
        }

        if (isJmxEnabled()) {
            NetworkPolicyPort jmxPort = new NetworkPolicyPort();
            jmxPort.setPort(new IntOrString(JMX_PORT));

            NetworkPolicyIngressRule jmxRule = new NetworkPolicyIngressRuleBuilder()
                    .withPorts(jmxPort)
                    .withFrom()
                    .build();

            rules.add(jmxRule);
        }

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(policyName(cluster))
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withNewPodSelector()
                        .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, kafkaClusterName(cluster))
                    .endPodSelector()
                    .withIngress(rules)
                .endSpec()
                .build();

        log.trace("Created network policy {}", networkPolicy);
        return networkPolicy;
    }

    /**
     * Generates the PodDisruptionBudget.
     *
     * @return The PodDisruptionBudget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    /**
     * Sets the object with Kafka listeners configuration.
     *
     * @param listeners The listeners.
     */
    public void setListeners(KafkaListeners listeners) {
        this.listeners = listeners;
    }

    /**
     * @return The listener object from the CRD.
     */
    public KafkaListeners getListeners() {
        return listeners;
    }

    /**
     * @return Return if the jmx has been enabled
     */
    public boolean isJmxEnabled() {
        return isJmxEnabled;
    }

    /**
     * Sets the object with jmx options enabled
     *
     * @param jmxEnabled if jmx is enabled
     */
    public void setJmxEnabled(boolean jmxEnabled) {
        isJmxEnabled = jmxEnabled;
    }

    /**
     * Sets the object with Kafka authorization configuration.
     *
     * @param authorization The authorization.
     */
    public void setAuthorization(KafkaAuthorization authorization) {
        this.authorization = authorization;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside of OpenShift / Kubernetes.
     *
     * @return true when the Kafka cluster is exposed.
     */
    public boolean isExposed() {
        return listeners != null && listeners.getExternal() != null;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside of OpenShift using OpenShift routes
     *
     * @return true when the Kafka cluster is exposed using OpenShift routes.
     */
    public boolean isExposedWithRoute() {
        return isExposed() && listeners.getExternal() instanceof KafkaListenerExternalRoute;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside using LoadBalancers
     *
     * @return true when the Kafka cluster is exposed using load balancer.
     */
    public boolean isExposedWithLoadBalancer() {
        return isExposed() && listeners.getExternal() instanceof KafkaListenerExternalLoadBalancer;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside using NodePort type services
     *
     * @return true when the Kafka cluster is exposed to the outside using NodePort.
     */
    public boolean isExposedWithNodePort() {
        return isExposed() && listeners.getExternal() instanceof KafkaListenerExternalNodePort;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside of Kubernetes using Ingress
     *
     * @return true when the Kafka cluster is exposed using Kubernetes Ingress.
     */
    public boolean isExposedWithIngress() {
        return isExposed() && listeners.getExternal() instanceof KafkaListenerExternalIngress;
    }

    /**
     * Returns the list broker overrides for external listeners.
     */
    private List<ExternalListenerBrokerOverride> getExternalListenerBrokerOverride() {
        List<ExternalListenerBrokerOverride> brokerOverride = new ArrayList<>();

        if (isExposedWithNodePort()) {
            NodePortListenerOverride overrides = ((KafkaListenerExternalNodePort) listeners.getExternal()).getOverrides();

            if (overrides != null && overrides.getBrokers() != null) {
                brokerOverride.addAll(overrides.getBrokers());
            }
        } else if (isExposedWithLoadBalancer()) {
            LoadBalancerListenerOverride overrides = ((KafkaListenerExternalLoadBalancer) listeners.getExternal()).getOverrides();

            if (overrides != null && overrides.getBrokers() != null) {
                brokerOverride.addAll(overrides.getBrokers());
            }
        } else if (isExposedWithRoute()) {
            RouteListenerOverride overrides = ((KafkaListenerExternalRoute) listeners.getExternal()).getOverrides();

            if (overrides != null && overrides.getBrokers() != null) {
                brokerOverride.addAll(overrides.getBrokers());
            }
        } else if (isExposedWithIngress()) {
            IngressListenerConfiguration configuration = ((KafkaListenerExternalIngress) listeners.getExternal()).getConfiguration();

            if (configuration != null && configuration.getBrokers() != null) {
                brokerOverride.addAll(configuration.getBrokers());
            }
        }

        return brokerOverride;
    }

    /**
     * Returns the bootstrap override for external listeners
     *
     * @return The ExternalListenerBootstrapOverride.
     */
    public ExternalListenerBootstrapOverride getExternalListenerBootstrapOverride() {
        ExternalListenerBootstrapOverride bootstrapOverride = null;

        if (isExposedWithNodePort()) {
            NodePortListenerOverride overrides = ((KafkaListenerExternalNodePort) listeners.getExternal()).getOverrides();

            if (overrides != null) {
                bootstrapOverride = overrides.getBootstrap();
            }
        } else if (isExposedWithLoadBalancer()) {
            LoadBalancerListenerOverride overrides = ((KafkaListenerExternalLoadBalancer) listeners.getExternal()).getOverrides();

            if (overrides != null) {
                bootstrapOverride = overrides.getBootstrap();
            }
        } else if (isExposedWithRoute()) {
            RouteListenerOverride overrides = ((KafkaListenerExternalRoute) listeners.getExternal()).getOverrides();

            if (overrides != null) {
                bootstrapOverride = overrides.getBootstrap();
            }
        } else if (isExposedWithIngress()) {
            IngressListenerConfiguration configuration = ((KafkaListenerExternalIngress) listeners.getExternal()).getConfiguration();

            if (configuration != null) {
                bootstrapOverride = configuration.getBootstrap();
            }
        }

        return bootstrapOverride;
    }

    /**
     * Returns the advertised address of external service.
     *
     * @param podNumber The pod number.
     * @return The advertised address of the external service.
     */
    public String getExternalServiceAdvertisedHostOverride(int podNumber) {
        String advertisedHost = null;
        List<ExternalListenerBrokerOverride> brokerOverride = getExternalListenerBrokerOverride();

        advertisedHost = brokerOverride.stream()
                .filter(brokerService -> brokerService != null && brokerService.getBroker() == podNumber
                        && brokerService.getAdvertisedHost() != null)
                .map(ExternalListenerBrokerOverride::getAdvertisedHost)
                .findAny()
                .orElse(null);

        if (advertisedHost != null && advertisedHost.isEmpty()) {
            advertisedHost = null;
        }

        return advertisedHost;
    }

    /**
     * Returns advertised address of external nodeport service.
     *
     * @param podNumber The pod number.
     * @return The advertised address of external nodeport service.
     */
    public Integer getExternalServiceAdvertisedPortOverride(int podNumber) {
        Integer advertisedPort = null;
        List<ExternalListenerBrokerOverride> brokerOverride = getExternalListenerBrokerOverride();

        advertisedPort = brokerOverride.stream()
                .filter(brokerService -> brokerService != null && brokerService.getBroker() == podNumber
                        && brokerService.getAdvertisedPort() != null)
                .map(ExternalListenerBrokerOverride::getAdvertisedPort)
                .findAny()
                .orElse(null);

        if (advertisedPort != null && advertisedPort == 0) {
            advertisedPort = null;
        }

        return advertisedPort;
    }

    /**
     * Returns the advertised URL for given pod.
     * It will take into account the overrides specified by the user.
     * If some segment is not know - e.g. the hostname for the NodePort access, it should be left empty
     *
     * @param podNumber Pod index
     * @param address   The advertised hostname
     * @return The advertised hostname in format podNumber://address (e.g. 1://my-broker-1)
     */
    public String getExternalAdvertisedHostname(int podNumber, String address) {
        String advertisedHost = getExternalServiceAdvertisedHostOverride(podNumber);

        if (advertisedHost == null && address == null)  {
            return null;
        }

        String url = podNumber
                + "://"
                + (advertisedHost != null ? advertisedHost : address);

        return url;
    }

    /**
     * Returns the advertised port for given pod.
     * It will take into account the overrides specified by the user.
     *
     * @param podNumber Pod index
     * @param port      The advertised port
     * @return The advertised port in format podNumber://port (e.g. 1://9094)
     */
    public String getExternalAdvertisedPort(int podNumber, String port) {
        Integer advertisedPort = getExternalServiceAdvertisedPortOverride(podNumber);

        String url = podNumber
                + "://"
                + (advertisedPort != null ? advertisedPort : port);

        return url;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside of OpenShift with TLS enabled
     *
     * @return True when the Kafka cluster is exposed to the outside of OpenShift with TLS enabled
     */
    public boolean isExposedWithTls() {
        if (isExposed()) {
            if (listeners.getExternal() instanceof KafkaListenerExternalRoute
                    || listeners.getExternal() instanceof KafkaListenerExternalIngress) {
                return true;
            } else {
                if (listeners.getExternal() instanceof KafkaListenerExternalLoadBalancer) {
                    return ((KafkaListenerExternalLoadBalancer) listeners.getExternal()).isTls();
                } else if (listeners.getExternal() instanceof KafkaListenerExternalNodePort) {
                    return ((KafkaListenerExternalNodePort) listeners.getExternal()).isTls();
                }
            }
        }

        return false;
    }

    public CertAndKeySecretSource getSecretSourceExternal() {
        return this.secretSourceExternal;
    }

    public CertAndKeySecretSource getSecretSourceTls() {
        return this.secretSourceTls;
    }

    public void setSecretSourceExternal(CertAndKeySecretSource secretSourceExternal) {
        this.secretSourceExternal = secretSourceExternal;
    }

    public void setSecretSourceTls(CertAndKeySecretSource secretSourceTls) {
        this.secretSourceTls = secretSourceTls;
    }

    @Override
    public KafkaConfiguration getConfiguration() {
        return (KafkaConfiguration) configuration;
    }

    public boolean isJmxAuthenticated() {
        return isJmxAuthenticated;
    }

    public void setJmxAuthenticated(boolean jmxAuthenticated) {
        isJmxAuthenticated = jmxAuthenticated;
    }

    /**
     * Returns the preferred node address type if configured by the user. Returns null otherwise.
     *
     * @return Preferred node address type as selected by the user
     */
    public String getPreferredNodeAddressType() {
        if (isExposedWithNodePort()) {
            KafkaListenerExternalNodePort listener = (KafkaListenerExternalNodePort) listeners.getExternal();

            if (listener.getConfiguration() != null
                    && listener.getConfiguration().getPreferredAddressType() != null) {
                return listener.getConfiguration().getPreferredAddressType().toValue();
            }
        }

        return null;
    }

    private String generateBrokerConfiguration()   {
        String result = new KafkaBrokerConfigurationBuilder()
                .withBrokerId()
                .withRackId(rack)
                .withZookeeper(cluster)
                .withLogDirs(VolumeUtils.getDataVolumeMountPaths(storage, mountPath))
                .withListeners(cluster, namespace, listeners)
                .withAuthorization(cluster, authorization)
                .withCruiseControl(cluster, cruiseControlSpec, ccNumPartitions, ccReplicationFactor)
                .withUserConfiguration(configuration)
                .build().trim();
        return result;
    }

    public String getBrokersConfiguration() {
        return this.brokersConfiguration;
    }

    public ConfigMap generateAncillaryConfigMap(ConfigMap externalLoggingCm, Set<String> advertisedHostnames, Set<String> advertisedPorts)   {
        ConfigMap cm = generateMetricsAndLogConfigMap(externalLoggingCm);
        this.brokersConfiguration = generateBrokerConfiguration();
        cm.getData().put(BROKER_CONFIGURATION_FILENAME, this.brokersConfiguration);

        if (!advertisedHostnames.isEmpty()) {
            cm.getData().put(BROKER_ADVERTISED_HOSTNAMES_FILENAME, String.join(" ", advertisedHostnames));
        }

        if (!advertisedPorts.isEmpty()) {
            cm.getData().put(BROKER_ADVERTISED_PORTS_FILENAME, String.join(" ", advertisedPorts));
        }

        return cm;
    }

    public KafkaVersion getKafkaVersion() {
        return this.kafkaVersion;
    }
}
