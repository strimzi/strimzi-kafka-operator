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
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressPath;
import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressPathBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressTLS;
import io.fabric8.kubernetes.api.model.networking.v1.IngressTLSBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
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
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.ExternalTrafficPolicy;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
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
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.ListenersUtils.isListenerWithOAuth;
import static java.util.Collections.addAll;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static io.strimzi.operator.cluster.model.CruiseControl.CRUISE_CONTROL_METRIC_REPORTER;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaCluster extends AbstractModel {
    protected static final String APPLICATION_NAME = "kafka";

    protected static final String ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS = "EXTERNAL_ADDRESS";

    private static final String ENV_VAR_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";

    // For port names in services, a 'tcp-' prefix is added to support Istio protocol selection
    // This helps Istio to avoid using a wildcard listener and instead present IP:PORT pairs which effects
    // proper listener, routing and metrics configuration sent to Envoy
    public static final int REPLICATION_PORT = 9091;
    protected static final String REPLICATION_PORT_NAME = "tcp-replication";
    public static final int CONTROLPLANE_PORT = 9090;
    protected static final String CONTROLPLANE_PORT_NAME = "tcp-ctrlplane"; // port name is up to 15 characters

    // Ingress and Route listeners advertise port 443 regardless what port is used in Kafka, so we store them here
    protected static final int ROUTE_PORT = 443;
    protected static final int INGRESS_PORT = 443;

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

    protected static final String KAFKA_JMX_SECRET_SUFFIX = NAME_SUFFIX + "-jmx";
    protected static final String SECRET_JMX_USERNAME_KEY = "jmx-username";
    protected static final String SECRET_JMX_PASSWORD_KEY = "jmx-password";
    protected static final String ENV_VAR_KAFKA_JMX_USERNAME = "KAFKA_JMX_USERNAME";
    protected static final String ENV_VAR_KAFKA_JMX_PASSWORD = "KAFKA_JMX_PASSWORD";

    protected static final String CO_ENV_VAR_CUSTOM_KAFKA_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_LABELS";

    // Suffixes for secrets with certificates
    private static final String SECRET_BROKERS_SUFFIX = NAME_SUFFIX + "-brokers";

    /**
     * Records the Kafka version currently running inside Kafka StatefulSet
     */
    public static final String ANNO_STRIMZI_IO_KAFKA_VERSION = Annotations.STRIMZI_DOMAIN + "kafka-version";

    /**
     * Records the used log.message.format.version
     */
    public static final String ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION = Annotations.STRIMZI_DOMAIN + "log-message-format-version";

    /**
     * Records the used inter.broker.protocol.version
     */
    public static final String ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION = Annotations.STRIMZI_DOMAIN + "inter-broker-protocol-version";

    /**
     * Records the state of the Kafka upgrade process. Unset outside of upgrades.
     */
    public static final String ANNO_STRIMZI_BROKER_CONFIGURATION_HASH = Annotations.STRIMZI_DOMAIN + "broker-configuration-hash";

    public static final String ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS = Annotations.STRIMZI_DOMAIN + "custom-listener-cert-thumbprints";

    // Env vars for JMX service
    protected static final String ENV_VAR_KAFKA_JMX_ENABLED = "KAFKA_JMX_ENABLED";

    // Name of the broker configuration file in the config map
    public static final String BROKER_CONFIGURATION_FILENAME = "server.config";
    public static final String BROKER_LISTENERS_FILENAME = "listeners.config";
    public static final String BROKER_ADVERTISED_HOSTNAMES_FILENAME = "advertised-hostnames.config";
    public static final String BROKER_ADVERTISED_PORTS_FILENAME = "advertised-ports.config";

    // Cruise Control defaults
    private static final String CRUISE_CONTROL_DEFAULT_NUM_PARTITIONS = "1";
    private static final String CRUISE_CONTROL_DEFAULT_REPLICATION_FACTOR = "1";

    // Kafka configuration
    private Rack rack;
    private String initImage;
    private List<GenericKafkaListener> listeners;
    private KafkaAuthorization authorization;
    private KafkaVersion kafkaVersion;
    private CruiseControlSpec cruiseControlSpec;
    private String ccNumPartitions = null;
    private String ccReplicationFactor = null;
    private String ccMinInSyncReplicas = null;
    private boolean isJmxEnabled;
    private boolean isJmxAuthenticated;
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

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_KAFKA_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    private KafkaCluster(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, APPLICATION_NAME);
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

        this.initImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_INIT_IMAGE, "quay.io/strimzi/operator:latest");
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
        return DnsNameGenerator.podDnsName(namespace, KafkaCluster.headlessServiceName(cluster), podName);
    }

    public static String podDnsNameWithoutClusterDomain(String namespace, String cluster, int podId) {
        return podDnsNameWithoutClusterDomain(namespace, cluster, KafkaCluster.kafkaPodName(cluster, podId));
    }

    public static String podDnsNameWithoutClusterDomain(String namespace, String cluster, String podName) {
        return DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace, KafkaCluster.headlessServiceName(cluster), podName);
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

    public static KafkaCluster fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        return fromCrd(reconciliation, kafkaAssembly, versions, null, 0);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:JavaNCSS"})
    public static KafkaCluster fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions, Storage oldStorage, int oldReplicas) {
        KafkaCluster result = new KafkaCluster(reconciliation, kafkaAssembly);

        result.setOwnerReference(kafkaAssembly);

        KafkaSpec kafkaSpec = kafkaAssembly.getSpec();
        KafkaClusterSpec kafkaClusterSpec = kafkaSpec.getKafka();

        result.setReplicas(kafkaClusterSpec.getReplicas());

        validateIntConfigProperty("default.replication.factor", kafkaClusterSpec);
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
            initImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_INIT_IMAGE, "quay.io/strimzi/operator:latest");
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

        // Handle Kafka broker configuration
        KafkaVersion desiredVersion = versions.version(kafkaClusterSpec.getVersion());
        KafkaConfiguration configuration = new KafkaConfiguration(reconciliation, kafkaClusterSpec.getConfig().entrySet());
        configureCruiseControlMetrics(kafkaAssembly, result, configuration);
        validateConfiguration(reconciliation, kafkaAssembly, desiredVersion, configuration);
        result.setConfiguration(configuration);

        // Parse different types of metrics configurations
        ModelUtils.parseMetrics(result, kafkaClusterSpec);

        if (oldStorage != null) {
            Storage newStorage = kafkaClusterSpec.getStorage();
            AbstractModel.validatePersistentStorage(newStorage);

            StorageDiff diff = new StorageDiff(reconciliation, oldStorage, newStorage, oldReplicas, kafkaClusterSpec.getReplicas());

            if (!diff.isEmpty()) {
                LOGGER.warnCr(reconciliation, "Only the following changes to Kafka storage are allowed: " +
                        "changing the deleteClaim flag, " +
                        "adding volumes to Jbod storage or removing volumes from Jbod storage, " +
                        "changing overrides to nodes which do not exist yet" +
                        "and increasing size of persistent claim volumes (depending on the volume type and used storage class).");
                LOGGER.warnCr(reconciliation, "The desired Kafka storage configuration in the custom resource {}/{} contains changes which are not allowed. As a " +
                        "result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                        "about the detected changes.", kafkaAssembly.getMetadata().getNamespace(), kafkaAssembly.getMetadata().getName());

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

        // Configure listeners
        if (kafkaClusterSpec.getListeners() == null || kafkaClusterSpec.getListeners().isEmpty()) {
            LOGGER.errorCr(reconciliation, "The required field .spec.kafka.listeners is missing");
            throw new InvalidResourceException("The required field .spec.kafka.listeners is missing");
        }
        List<GenericKafkaListener> listeners = kafkaClusterSpec.getListeners();
        ListenersValidator.validate(reconciliation, kafkaClusterSpec.getReplicas(), listeners);
        result.setListeners(listeners);

        // Set authorization
        if (kafkaClusterSpec.getAuthorization() instanceof KafkaAuthorizationKeycloak) {
            if (!ListenersUtils.hasListenerWithOAuth(listeners)) {
                throw new InvalidResourceException("You cannot configure Keycloak Authorization without any listener with OAuth based authentication");
            } else {
                KafkaAuthorizationKeycloak authorizationKeycloak = (KafkaAuthorizationKeycloak) kafkaClusterSpec.getAuthorization();
                if (authorizationKeycloak.getClientId() == null || authorizationKeycloak.getTokenEndpointUri() == null) {
                    LOGGER.errorCr(reconciliation, "Keycloak Authorization: Token Endpoint URI and clientId are both required");
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
            ModelUtils.parseInternalServiceTemplate(result, template.getBootstrapService());
            ModelUtils.parseInternalHeadlessServiceTemplate(result, template.getBrokersService());

            if (template.getExternalBootstrapService() != null) {
                if (template.getExternalBootstrapService().getMetadata() != null) {
                    result.templateExternalBootstrapServiceLabels = template.getExternalBootstrapService().getMetadata().getLabels();
                    result.templateExternalBootstrapServiceAnnotations = template.getExternalBootstrapService().getMetadata().getAnnotations();
                }
            }

            if (template.getPerPodService() != null) {
                if (template.getPerPodService().getMetadata() != null) {
                    result.templatePerPodServiceLabels = template.getPerPodService().getMetadata().getLabels();
                    result.templatePerPodServiceAnnotations = template.getPerPodService().getMetadata().getAnnotations();
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

            if (template.getClusterRoleBinding() != null && template.getClusterRoleBinding().getMetadata() != null) {
                result.templateClusterRoleBindingLabels = template.getClusterRoleBinding().getMetadata().getLabels();
                result.templateClusterRoleBindingAnnotations = template.getClusterRoleBinding().getMetadata().getAnnotations();
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

            if (template.getServiceAccount() != null && template.getServiceAccount().getMetadata() != null) {
                result.templateServiceAccountLabels = template.getServiceAccount().getMetadata().getLabels();
                result.templateServiceAccountAnnotations = template.getServiceAccount().getMetadata().getAnnotations();
            }

            if (template.getJmxSecret() != null && template.getJmxSecret().getMetadata() != null) {
                result.templateJmxSecretLabels = template.getJmxSecret().getMetadata().getLabels();
                result.templateJmxSecretAnnotations = template.getJmxSecret().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(result, template.getPodDisruptionBudget());
        }

        result.templatePodLabels = Util.mergeLabelsOrAnnotations(result.templatePodLabels, DEFAULT_POD_LABELS);

        result.kafkaVersion = versions.version(kafkaClusterSpec.getVersion());
        return result;
    }

    /**
     * Depending on the Cruise Control configuration, it enhances the Kafka configuration to enable the Cruise Control
     * metric reporter and the configuration of its topics.
     *
     * @param kafkaAssembly     Kafka custom resource
     * @param kafkaCluster      KafkaCluster instance
     * @param configuration     Kafka broker configuration
     */
    private static void configureCruiseControlMetrics(Kafka kafkaAssembly, KafkaCluster kafkaCluster, KafkaConfiguration configuration) {
        // If  required Cruise Control metric reporter configurations are missing set them using Kafka defaults
        if (configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS.getValue()) == null) {
            kafkaCluster.ccNumPartitions = configuration.getConfigOption(KAFKA_NUM_PARTITIONS_CONFIG_FIELD, CRUISE_CONTROL_DEFAULT_NUM_PARTITIONS);
        }
        if (configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue()) == null) {
            kafkaCluster.ccReplicationFactor = configuration.getConfigOption(KAFKA_REPLICATION_FACTOR_CONFIG_FIELD, CRUISE_CONTROL_DEFAULT_REPLICATION_FACTOR);
        }
        if (configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue()) == null) {
            kafkaCluster.ccMinInSyncReplicas = "1";
        } else {
            // If the user has set the CC minISR but it is higher than the set number of replicas for the metrics topics then we need to abort and make
            // sure that the user sets minISR <= replicationFactor
            if (Integer.parseInt(kafkaCluster.ccMinInSyncReplicas) > Integer.parseInt(kafkaCluster.ccReplicationFactor)) {
                throw new IllegalArgumentException(
                        "The Cruise Control metric topic minISR was set to a value (" + kafkaCluster.ccMinInSyncReplicas + ") " +
                                "which is higher than the number of replicas for that topic (" + kafkaCluster.ccReplicationFactor + "). " +
                                "Please ensure that the CC metrics topic minISR is <= to the topic's replication factor."
                );
            }
        }
        String metricReporters = configuration.getConfigOption(KAFKA_METRIC_REPORTERS_CONFIG_FIELD);
        Set<String> metricReporterList = new HashSet<>();
        if (metricReporters != null) {
            addAll(metricReporterList, configuration.getConfigOption(KAFKA_METRIC_REPORTERS_CONFIG_FIELD).split(","));
        }

        if (kafkaAssembly.getSpec().getCruiseControl() != null && kafkaAssembly.getSpec().getKafka().getReplicas() < 2) {
            throw new InvalidResourceException("Kafka " +
                    kafkaAssembly.getMetadata().getNamespace() + "/" + kafkaAssembly.getMetadata().getName() +
                    " has invalid configuration. Cruise Control cannot be deployed with a single-node Kafka cluster. It requires at least two Kafka nodes.");
        }
        kafkaCluster.cruiseControlSpec = kafkaAssembly.getSpec().getCruiseControl();
        if (kafkaCluster.cruiseControlSpec != null) {
            metricReporterList.add(CRUISE_CONTROL_METRIC_REPORTER);
        } else {
            metricReporterList.remove(CRUISE_CONTROL_METRIC_REPORTER);
        }
        if (!metricReporterList.isEmpty()) {
            configuration.setConfigOption(KAFKA_METRIC_REPORTERS_CONFIG_FIELD, String.join(",", metricReporterList));
        } else {
            configuration.removeConfigOption(KAFKA_METRIC_REPORTERS_CONFIG_FIELD);
        }
    }

    /**
     * Validates the Kafka broker configuration against the configuration options of the desired Kafka version.
     *
     * @param reconciliation    The reconciliation
     * @param kafkaAssembly     Kafka custom resource
     * @param desiredVersion    Desired Kafka version
     * @param configuration     Kafka broker configuration
     */
    private static void validateConfiguration(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion desiredVersion, KafkaConfiguration configuration) {
        List<String> errorsInConfig = configuration.validate(desiredVersion);

        if (!errorsInConfig.isEmpty()) {
            for (String error : errorsInConfig) {
                LOGGER.warnCr(reconciliation, "Kafka {}/{} has invalid spec.kafka.config: {}",
                        kafkaAssembly.getMetadata().getNamespace(),
                        kafkaAssembly.getMetadata().getName(),
                        error);
            }

            throw new InvalidResourceException("Kafka " +
                    kafkaAssembly.getMetadata().getNamespace() + "/" + kafkaAssembly.getMetadata().getName() +
                    " has invalid spec.kafka.config: " +
                    String.join(", ", errorsInConfig));
        }
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

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param kafka                    The Kafka custom resource
     * @param clusterCa                The CA for cluster certificates
     * @param externalBootstrapDnsName The set of DNS names for bootstrap service (should be appended to every broker certificate)
     * @param externalDnsNames         The list of DNS names for broker pods (should be appended only to specific certificates for given broker)
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     */
    public void generateCertificates(Kafka kafka, ClusterCa clusterCa, Set<String> externalBootstrapDnsName,
                                     Map<Integer, Set<String>> externalDnsNames, boolean isMaintenanceTimeWindowsSatisfied) {
        LOGGER.debugCr(reconciliation, "Generating certificates");

        try {
            brokerCerts = clusterCa.generateBrokerCerts(kafka, externalBootstrapDnsName, externalDnsNames, isMaintenanceTimeWindowsSatisfied);
        } catch (IOException e) {
            LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
        }

        LOGGER.debugCr(reconciliation, "End generating certificates");
    }

    /**
     * Generates ports for bootstrap service.
     * The bootstrap service contains only the client interfaces.
     * Not the replication interface which doesn't need bootstrap service.
     *
     * @return List with generated ports
     */
    private List<ServicePort> getServicePorts() {
        List<GenericKafkaListener> internalListeners = ListenersUtils.internalListeners(listeners);

        List<ServicePort> ports = new ArrayList<>(internalListeners.size() + 1);
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));

        for (GenericKafkaListener listener : internalListeners) {
            ports.add(createServicePort(ListenersUtils.backwardsCompatiblePortName(listener), listener.getPort(), listener.getPort(), "TCP"));
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
        List<GenericKafkaListener> internalListeners = ListenersUtils.internalListeners(listeners);

        List<ServicePort> ports = new ArrayList<>(internalListeners.size() + 3);
        ports.add(createServicePort(CONTROLPLANE_PORT_NAME, CONTROLPLANE_PORT, CONTROLPLANE_PORT, "TCP"));
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));

        for (GenericKafkaListener listener : internalListeners) {
            ports.add(createServicePort(ListenersUtils.backwardsCompatiblePortName(listener), listener.getPort(), listener.getPort(), "TCP"));
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
        return createDiscoverableService("ClusterIP", getServicePorts(), templateServiceLabels,
                Util.mergeLabelsOrAnnotations(getInternalDiscoveryAnnotation(), templateServiceAnnotations));
    }

    /**
     * Generates a JSON String with the discovery annotation for the internal bootstrap service
     *
     * @return  JSON with discovery annotation
     */
    /*test*/ Map<String, String> getInternalDiscoveryAnnotation() {
        JsonArray anno = new JsonArray();

        for (GenericKafkaListener listener : listeners) {
            JsonObject discovery = new JsonObject();
            discovery.put("port", listener.getPort());
            discovery.put("tls", listener.isTls());
            discovery.put("protocol", "kafka");

            if (listener.getAuth() != null) {
                discovery.put("auth", listener.getAuth().getType());
            } else {
                discovery.put("auth", "none");
            }

            anno.add(discovery);
        }

        return singletonMap(Labels.STRIMZI_DISCOVERY_LABEL, anno.encodePrettily());
    }

    /**
     * Generates list of external bootstrap services. These services are used for exposing it externally.
     * Separate services are used to make sure that we do expose the right port in the right way.
     *
     * @return The list with generated Services
     */
    public List<Service> generateExternalBootstrapServices() {
        List<GenericKafkaListener> externalListeners = ListenersUtils.externalListeners(listeners);
        List<Service> services = new ArrayList<>(externalListeners.size());

        for (GenericKafkaListener listener : externalListeners)   {
            String serviceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(cluster, listener);

            List<ServicePort> ports = Collections.singletonList(
                    createServicePort(ListenersUtils.backwardsCompatiblePortName(listener),
                            listener.getPort(),
                            listener.getPort(),
                            ListenersUtils.bootstrapNodePort(listener),
                            "TCP")
            );

            Service service = createService(
                    serviceName,
                    ListenersUtils.serviceType(listener),
                    ports,
                    getLabelsWithStrimziName(name, Util.mergeLabelsOrAnnotations(templateExternalBootstrapServiceLabels, ListenersUtils.bootstrapLabels(listener))),
                    getSelectorLabels(),
                    Util.mergeLabelsOrAnnotations(ListenersUtils.bootstrapAnnotations(listener), templateExternalBootstrapServiceAnnotations),
                    ListenersUtils.ipFamilyPolicy(listener),
                    ListenersUtils.ipFamilies(listener)
            );

            if (KafkaListenerType.LOADBALANCER == listener.getType()) {
                String loadBalancerIP = ListenersUtils.bootstrapLoadBalancerIP(listener);
                if (loadBalancerIP != null) {
                    service.getSpec().setLoadBalancerIP(loadBalancerIP);
                }

                List<String> loadBalancerSourceRanges = ListenersUtils.loadBalancerSourceRanges(listener);
                if (loadBalancerSourceRanges != null
                        && !loadBalancerSourceRanges.isEmpty()) {
                    service.getSpec().setLoadBalancerSourceRanges(loadBalancerSourceRanges);
                }

                List<String> finalizers = ListenersUtils.finalizers(listener);
                if (finalizers != null
                        && !finalizers.isEmpty()) {
                    service.getMetadata().setFinalizers(finalizers);
                }
            }

            if (KafkaListenerType.LOADBALANCER == listener.getType() || KafkaListenerType.NODEPORT == listener.getType()) {
                ExternalTrafficPolicy etp = ListenersUtils.externalTrafficPolicy(listener);
                if (etp != null) {
                    service.getSpec().setExternalTrafficPolicy(etp.toValue());
                } else {
                    service.getSpec().setExternalTrafficPolicy(ExternalTrafficPolicy.CLUSTER.toValue());
                }
            }

            services.add(service);
        }

        return services;
    }

    /**
     * Generates list of service for pod. These services are used for exposing it externally.
     *
     * @param pod Number of the pod for which this service should be generated
     * @return The list with generated Services
     */
    public List<Service> generateExternalServices(int pod) {
        List<GenericKafkaListener> externalListeners = ListenersUtils.externalListeners(listeners);
        List<Service> services = new ArrayList<>(externalListeners.size());

        for (GenericKafkaListener listener : externalListeners)   {
            String serviceName = ListenersUtils.backwardsCompatibleBrokerServiceName(cluster, pod, listener);

            List<ServicePort> ports = Collections.singletonList(
                    createServicePort(
                            ListenersUtils.backwardsCompatiblePortName(listener),
                            listener.getPort(),
                            listener.getPort(),
                            ListenersUtils.brokerNodePort(listener, pod),
                            "TCP")
            );

            Labels selector = getSelectorLabels().withStatefulSetPod(kafkaPodName(cluster, pod));

            Service service = createService(
                    serviceName,
                    ListenersUtils.serviceType(listener),
                    ports,
                    getLabelsWithStrimziName(name, Util.mergeLabelsOrAnnotations(templatePerPodServiceLabels, ListenersUtils.brokerLabels(listener, pod))),
                    selector,
                    Util.mergeLabelsOrAnnotations(ListenersUtils.brokerAnnotations(listener, pod), templatePerPodServiceAnnotations),
                    ListenersUtils.ipFamilyPolicy(listener),
                    ListenersUtils.ipFamilies(listener)
            );

            if (KafkaListenerType.LOADBALANCER == listener.getType()) {
                String loadBalancerIP = ListenersUtils.brokerLoadBalancerIP(listener, pod);
                if (loadBalancerIP != null) {
                    service.getSpec().setLoadBalancerIP(loadBalancerIP);
                }

                List<String> loadBalancerSourceRanges = ListenersUtils.loadBalancerSourceRanges(listener);
                if (loadBalancerSourceRanges != null
                        && !loadBalancerSourceRanges.isEmpty()) {
                    service.getSpec().setLoadBalancerSourceRanges(loadBalancerSourceRanges);
                }

                List<String> finalizers = ListenersUtils.finalizers(listener);
                if (finalizers != null
                        && !finalizers.isEmpty()) {
                    service.getMetadata().setFinalizers(finalizers);
                }
            }

            if (KafkaListenerType.LOADBALANCER == listener.getType() || KafkaListenerType.NODEPORT == listener.getType()) {
                ExternalTrafficPolicy etp = ListenersUtils.externalTrafficPolicy(listener);
                if (etp != null) {
                    service.getSpec().setExternalTrafficPolicy(etp.toValue());
                } else {
                    service.getSpec().setExternalTrafficPolicy(ExternalTrafficPolicy.CLUSTER.toValue());
                }
            }

            services.add(service);
        }

        return services;
    }

        /**
     * Generates a list of bootstrap route which can be used to bootstrap clients outside of OpenShift.
     *
     * @return The list of generated Routes
     */
    public List<Route> generateExternalBootstrapRoutes() {
        List<GenericKafkaListener> routeListeners = ListenersUtils.routeListeners(listeners);
        List<Route> routes = new ArrayList<>(routeListeners.size());

        for (GenericKafkaListener listener : routeListeners)   {
            String routeName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(cluster, listener);
            String serviceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(cluster, listener);

            Route route = new RouteBuilder()
                    .withNewMetadata()
                        .withName(routeName)
                        .withLabels(Util.mergeLabelsOrAnnotations(getLabelsWithStrimziName(name, templateExternalBootstrapRouteLabels).toMap(), ListenersUtils.bootstrapLabels(listener)))
                        .withAnnotations(Util.mergeLabelsOrAnnotations(templateExternalBootstrapRouteAnnotations, ListenersUtils.bootstrapAnnotations(listener)))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withNewTo()
                            .withKind("Service")
                            .withName(serviceName)
                        .endTo()
                        .withNewPort()
                            .withNewTargetPort(listener.getPort())
                        .endPort()
                        .withNewTls()
                            .withTermination("passthrough")
                        .endTls()
                    .endSpec()
                    .build();

            String host = ListenersUtils.bootstrapHost(listener);
            if (host != null)   {
                route.getSpec().setHost(host);
            }

            routes.add(route);
        }

        return routes;
    }

    /**
     * Generates list of routes for pod. These routes are used for exposing it externally using OpenShift Routes.
     *
     * @param pod Number of the pod for which this route should be generated
     * @return The list with generated Routes
     */
    public List<Route> generateExternalRoutes(int pod) {
        List<GenericKafkaListener> routeListeners = ListenersUtils.routeListeners(listeners);
        List<Route> routes = new ArrayList<>(routeListeners.size());

        for (GenericKafkaListener listener : routeListeners)   {
            String routeName = ListenersUtils.backwardsCompatibleBrokerServiceName(cluster, pod, listener);
            Route route = new RouteBuilder()
                    .withNewMetadata()
                        .withName(routeName)
                        .withLabels(getLabelsWithStrimziName(name, Util.mergeLabelsOrAnnotations(templatePerPodRouteLabels, ListenersUtils.brokerLabels(listener, pod))).toMap())
                        .withAnnotations(Util.mergeLabelsOrAnnotations(templatePerPodRouteAnnotations, ListenersUtils.brokerAnnotations(listener, pod)))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withNewTo()
                            .withKind("Service")
                            .withName(routeName)
                        .endTo()
                        .withNewPort()
                            .withNewTargetPort(listener.getPort())
                        .endPort()
                        .withNewTls()
                            .withTermination("passthrough")
                        .endTls()
                    .endSpec()
                    .build();

            String host = ListenersUtils.brokerHost(listener, pod);
            if (host != null)   {
                route.getSpec().setHost(host);
            }

            routes.add(route);
        }

        return routes;
    }

    /**
     * Generates a list of bootstrap ingress which can be used to bootstrap clients outside of Kubernetes.
     *
     * @return The list of generated Ingresses
     */
    public List<Ingress> generateExternalBootstrapIngresses() {
        List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(listeners);
        List<Ingress> ingresses = new ArrayList<>(ingressListeners.size());

        for (GenericKafkaListener listener : ingressListeners)   {
            String ingressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(cluster, listener);
            String serviceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(cluster, listener);

            String host = ListenersUtils.bootstrapHost(listener);
            String ingressClass = ListenersUtils.ingressClass(listener);

            HTTPIngressPath path = new HTTPIngressPathBuilder()
                    .withPath("/")
                    .withPathType("Prefix")
                    .withNewBackend()
                        .withNewService()
                            .withName(serviceName)
                            .withNewPort()
                                .withNumber(listener.getPort())
                            .endPort()
                        .endService()
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
                        .withName(ingressName)
                        .withLabels(getLabelsWithStrimziName(name, Util.mergeLabelsOrAnnotations(templateExternalBootstrapIngressLabels, ListenersUtils.bootstrapLabels(listener))).toMap())
                        .withAnnotations(Util.mergeLabelsOrAnnotations(generateInternalIngressAnnotations(), templateExternalBootstrapIngressAnnotations, ListenersUtils.bootstrapAnnotations(listener)))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withIngressClassName(ingressClass)
                        .withRules(rule)
                        .withTls(tls)
                    .endSpec()
                    .build();

            ingresses.add(ingress);
        }

        return ingresses;
    }

    /**
     * Generates a list of bootstrap ingress which can be used to bootstrap clients outside of Kubernetes.
     *
     * @return The list of generated Ingresses
     */
    public List<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> generateExternalBootstrapIngressesV1Beta1() {
        List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(listeners);
        List<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingresses = new ArrayList<>(ingressListeners.size());

        for (GenericKafkaListener listener : ingressListeners)   {
            String ingressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(cluster, listener);
            String serviceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(cluster, listener);

            String host = ListenersUtils.bootstrapHost(listener);
            String ingressClass = ListenersUtils.ingressClass(listener);

            io.fabric8.kubernetes.api.model.networking.v1beta1.HTTPIngressPath path = new io.fabric8.kubernetes.api.model.networking.v1beta1.HTTPIngressPathBuilder()
                    .withPath("/")
                    .withNewBackend()
                        .withNewServicePort(listener.getPort())
                        .withServiceName(serviceName)
                    .endBackend()
                    .build();

            io.fabric8.kubernetes.api.model.networking.v1beta1.IngressRule rule = new io.fabric8.kubernetes.api.model.networking.v1beta1.IngressRuleBuilder()
                    .withHost(host)
                    .withNewHttp()
                        .withPaths(path)
                    .endHttp()
                    .build();

            io.fabric8.kubernetes.api.model.networking.v1beta1.IngressTLS tls = new io.fabric8.kubernetes.api.model.networking.v1beta1.IngressTLSBuilder()
                    .withHosts(host)
                    .build();

            io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress ingress = new io.fabric8.kubernetes.api.model.networking.v1beta1.IngressBuilder()
                    .withNewMetadata()
                        .withName(ingressName)
                        .withLabels(getLabelsWithStrimziName(name, Util.mergeLabelsOrAnnotations(templateExternalBootstrapIngressLabels, ListenersUtils.bootstrapLabels(listener))).toMap())
                        .withAnnotations(Util.mergeLabelsOrAnnotations(generateInternalIngressAnnotations(), templateExternalBootstrapIngressAnnotations, ListenersUtils.bootstrapAnnotations(listener)))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withIngressClassName(ingressClass)
                        .withRules(rule)
                        .withTls(tls)
                    .endSpec()
                    .build();

            ingresses.add(ingress);
        }

        return ingresses;
    }

    /**
     * Generates list of ingress for pod. This ingress is used for exposing it externally using Nginx Ingress.
     *
     * @param pod Number of the pod for which this ingress should be generated
     * @return The list of generated Ingresses
     */
    public List<Ingress> generateExternalIngresses(int pod) {
        List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(listeners);
        List<Ingress> ingresses = new ArrayList<>(ingressListeners.size());

        for (GenericKafkaListener listener : ingressListeners)   {
            String ingressName = ListenersUtils.backwardsCompatibleBrokerServiceName(cluster, pod, listener);
            String host = ListenersUtils.brokerHost(listener, pod);
            String ingressClass = ListenersUtils.ingressClass(listener);

            HTTPIngressPath path = new HTTPIngressPathBuilder()
                    .withPath("/")
                    .withPathType("Prefix")
                    .withNewBackend()
                        .withNewService()
                            .withName(ingressName)
                            .withNewPort()
                                .withNumber(listener.getPort())
                            .endPort()
                        .endService()
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
                        .withName(ingressName)
                        .withLabels(getLabelsWithStrimziName(name, Util.mergeLabelsOrAnnotations(templatePerPodIngressLabels, ListenersUtils.brokerLabels(listener, pod))).toMap())
                        .withAnnotations(Util.mergeLabelsOrAnnotations(generateInternalIngressAnnotations(), templatePerPodIngressAnnotations, ListenersUtils.brokerAnnotations(listener, pod)))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withIngressClassName(ingressClass)
                        .withRules(rule)
                        .withTls(tls)
                    .endSpec()
                    .build();

            ingresses.add(ingress);
        }

        return ingresses;
    }

    /**
     * Generates list of ingress for pod. This ingress is used for exposing it externally using Nginx Ingress.
     *
     * @param pod Number of the pod for which this ingress should be generated
     * @return The list of generated Ingresses
     */
    public List<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> generateExternalIngressesV1Beta1(int pod) {
        List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(listeners);
        List<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingresses = new ArrayList<>(ingressListeners.size());

        for (GenericKafkaListener listener : ingressListeners)   {
            String ingressName = ListenersUtils.backwardsCompatibleBrokerServiceName(cluster, pod, listener);
            String host = ListenersUtils.brokerHost(listener, pod);
            String ingressClass = ListenersUtils.ingressClass(listener);

            io.fabric8.kubernetes.api.model.networking.v1beta1.HTTPIngressPath path = new io.fabric8.kubernetes.api.model.networking.v1beta1.HTTPIngressPathBuilder()
                    .withPath("/")
                    .withNewBackend()
                        .withNewServicePort(listener.getPort())
                        .withServiceName(ingressName)
                    .endBackend()
                    .build();

            io.fabric8.kubernetes.api.model.networking.v1beta1.IngressRule rule = new io.fabric8.kubernetes.api.model.networking.v1beta1.IngressRuleBuilder()
                    .withHost(host)
                    .withNewHttp()
                        .withPaths(path)
                    .endHttp()
                    .build();

            io.fabric8.kubernetes.api.model.networking.v1beta1.IngressTLS tls = new io.fabric8.kubernetes.api.model.networking.v1beta1.IngressTLSBuilder()
                    .withHosts(host)
                    .build();

            io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress ingress = new io.fabric8.kubernetes.api.model.networking.v1beta1.IngressBuilder()
                    .withNewMetadata()
                        .withName(ingressName)
                        .withLabels(getLabelsWithStrimziName(name, Util.mergeLabelsOrAnnotations(templatePerPodIngressLabels, ListenersUtils.brokerLabels(listener, pod))).toMap())
                        .withAnnotations(Util.mergeLabelsOrAnnotations(generateInternalIngressAnnotations(), templatePerPodIngressAnnotations, ListenersUtils.brokerAnnotations(listener, pod)))
                        .withNamespace(namespace)
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withIngressClassName(ingressClass)
                        .withRules(rule)
                        .withTls(tls)
                    .endSpec()
                    .build();

            ingresses.add(ingress);
        }

        return ingresses;
    }

    /**
     * Generates the annotations needed to configure the Ingress as TLS passthrough
     *
     * @return Map with the annotations
     */
    private Map<String, String> generateInternalIngressAnnotations() {
        Map<String, String> internalAnnotations = new HashMap<>(3);

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

        Map<String, String> podAnnotations = new HashMap<>(4);
        podAnnotations.put(ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(storage));
        podAnnotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion.version());
        podAnnotations.put(ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, getLogMessageFormatVersion());
        podAnnotations.put(ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, getInterBrokerProtocolVersion());

        return createStatefulSet(
                stsAnnotations,
                podAnnotations,
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

        return createJmxSecret(KafkaCluster.jmxSecretName(cluster), data);
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> ports = new ArrayList<>(listeners.size() + 3);
        ports.add(createContainerPort(CONTROLPLANE_PORT_NAME, CONTROLPLANE_PORT, "TCP"));
        ports.add(createContainerPort(REPLICATION_PORT_NAME, REPLICATION_PORT, "TCP"));

        for (GenericKafkaListener listener : listeners) {
            ports.add(createContainerPort(ListenersUtils.backwardsCompatiblePortName(listener), listener.getPort(), "TCP"));
        }

        if (isMetricsEnabled) {
            ports.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return ports;
    }

    /**
     * @return The port of route for the external listener.
     */
    public int getRoutePort() {
        return ROUTE_PORT;
    }

    /**
     * @return The port of ingress for the external listener.
     */
    public int getIngressPort() {
        return INGRESS_PORT;
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
        List<Volume> volumeList = new ArrayList<>(dataVolumes);

        if (rack != null || isExposedWithNodePort()) {
            volumeList.add(VolumeUtils.createEmptyDirVolume(INIT_VOLUME_NAME, "1Mi", "Memory"));
        }

        volumeList.add(createTempDirVolume());
        volumeList.add(VolumeUtils.createSecretVolume(CLUSTER_CA_CERTS_VOLUME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(BROKER_CERTS_VOLUME, KafkaCluster.brokersSecretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(CLIENT_CA_CERTS_VOLUME, KafkaCluster.clientsCaCertSecretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
        volumeList.add(VolumeUtils.createEmptyDirVolume("ready-files", "1Ki", "Memory"));

        for (GenericKafkaListener listener : listeners) {
            if (listener.isTls()
                    && listener.getConfiguration() != null
                    && listener.getConfiguration().getBrokerCertChainAndKey() != null)  {
                CertAndKeySecretSource secretSource = listener.getConfiguration().getBrokerCertChainAndKey();

                Map<String, String> items = new HashMap<>(2);
                items.put(secretSource.getKey(), "tls.key");
                items.put(secretSource.getCertificate(), "tls.crt");

                volumeList.add(
                        VolumeUtils.createSecretVolume(
                                "custom-" + ListenersUtils.identifier(listener) + "-certs",
                                secretSource.getSecretName(),
                                items,
                                isOpenShift
                        )
                );
            }

            if (isListenerWithOAuth(listener))   {
                KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listener.getAuth();
                volumeList.addAll(AuthenticationUtils.configureOauthCertificateVolumes("oauth-" + ListenersUtils.identifier(listener), oauth.getTlsTrustedCertificates(), isOpenShift));
            }
        }

        if (authorization instanceof KafkaAuthorizationKeycloak) {
            KafkaAuthorizationKeycloak keycloakAuthz = (KafkaAuthorizationKeycloak) authorization;
            volumeList.addAll(AuthenticationUtils.configureOauthCertificateVolumes("authz-keycloak", keycloakAuthz.getTlsTrustedCertificates(), isOpenShift));
        }

        return volumeList;
    }

    /* test */ List<PersistentVolumeClaim> getVolumeClaims() {
        return new ArrayList<>(dataPvcs);
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(dataVolumeMountPaths);

        volumeMountList.add(createTempDirVolumeMount());
        volumeMountList.add(VolumeUtils.createVolumeMount(CLUSTER_CA_CERTS_VOLUME, CLUSTER_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(VolumeUtils.createVolumeMount(BROKER_CERTS_VOLUME, BROKER_CERTS_VOLUME_MOUNT));
        volumeMountList.add(VolumeUtils.createVolumeMount(CLIENT_CA_CERTS_VOLUME, CLIENT_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
        volumeMountList.add(VolumeUtils.createVolumeMount("ready-files", "/var/opt/kafka"));

        if (rack != null || isExposedWithNodePort()) {
            volumeMountList.add(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT));
        }

        for (GenericKafkaListener listener : listeners) {
            String identifier = ListenersUtils.identifier(listener);

            if (listener.isTls()
                    && listener.getConfiguration() != null
                    && listener.getConfiguration().getBrokerCertChainAndKey() != null)  {
                volumeMountList.add(VolumeUtils.createVolumeMount("custom-" + identifier + "-certs", "/opt/kafka/certificates/custom-" + identifier + "-certs"));
            }

            if (isListenerWithOAuth(listener))   {
                KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listener.getAuth();
                volumeMountList.addAll(AuthenticationUtils.configureOauthCertificateVolumeMounts("oauth-" + identifier, oauth.getTlsTrustedCertificates(), OAUTH_TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-" + identifier + "-certs"));
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

            builder = ModelUtils.populateAffinityBuilderWithRackLabelSelector(builder, userAffinity, rack.getTopologyKey());
        }

        return builder.build();
    }

    protected List<EnvVar> getInitContainerEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVarFromFieldRef(ENV_VAR_KAFKA_INIT_NODE_NAME, "spec.nodeName"));

        if (rack != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY, rack.getTopologyKey()));
        }

        if (!ListenersUtils.nodePortListeners(listeners).isEmpty()) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS, "TRUE"));
        }

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateInitContainerEnvVars);

        return varList;
    }

    @Override
    protected List<Container> getInitContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> initContainers = new ArrayList<>(1);

        if (rack != null || !ListenersUtils.nodePortListeners(listeners).isEmpty()) {
            Container initContainer = new ContainerBuilder()
                    .withName(INIT_NAME)
                    .withImage(initImage)
                    .withArgs("/opt/strimzi/bin/kafka_init_run.sh")
                    .withEnv(getInitContainerEnvVars())
                    .withVolumeMounts(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT))
                    .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, initImage))
                    .withSecurityContext(templateInitContainerSecurityContext)
                    .build();

            if (getResources() != null) {
                initContainer.setResources(getResources());
            }
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
                .withLivenessProbe(ProbeGenerator.defaultBuilder(livenessProbeOptions)
                        .withNewExec()
                            .withCommand("/opt/kafka/kafka_liveness.sh")
                        .endExec().build())
                .withReadinessProbe(ProbeGenerator.defaultBuilder(readinessProbeOptions)
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
    public String getServiceAccountName() {
        return kafkaClusterName(cluster);
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

        for (GenericKafkaListener listener : listeners) {
            if (isListenerWithOAuth(listener))   {
                KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) listener.getAuth();

                if (oauth.getClientSecret() != null)    {
                    varList.add(buildEnvVarFromSecret("STRIMZI_" + ListenersUtils.envVarIdentifier(listener) + "_OAUTH_CLIENT_SECRET", oauth.getClientSecret().getSecretName(), oauth.getClientSecret().getKey()));
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
        varList.addAll(getRequiredEnvVars());

        // Add user defined environment variables to the Kafka broker containers
        addContainerEnvsToExistingEnvs(varList, templateKafkaContainerEnvVars);

        return varList;
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
                    .withName(getServiceAccountName())
                    .withNamespace(assemblyNamespace)
                    .build();

            RoleRef roleRef = new RoleRefBuilder()
                    .withName("strimzi-kafka-broker")
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .build();

            return getClusterRoleBinding(KafkaResources.initContainerClusterRoleBindingName(cluster, namespace), ks, roleRef);
        } else {
            return null;
        }
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the network policy.
     */
    public static String networkPolicyName(String cluster) {
        return cluster + NETWORK_POLICY_KEY_SUFFIX + NAME_SUFFIX;
    }

    /**
     * Generates the NetworkPolicies relevant for Kafka brokers
     *
     * @param operatorNamespace                             Namespace where the Strimzi Cluster Operator runs. Null if not configured.
     * @param operatorNamespaceLabels                       Labels of the namespace where the Strimzi Cluster Operator runs. Null if not configured.
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy(String operatorNamespace, Labels operatorNamespaceLabels) {
        // Internal peers => Strimzi components which need access
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector() // Cluster Operator
                     .addToMatchLabels(Labels.STRIMZI_KIND_LABEL, "cluster-operator")
                .endPodSelector()
                    .build();
        ModelUtils.setClusterOperatorNetworkPolicyNamespaceSelector(clusterOperatorPeer, namespace, operatorNamespace, operatorNamespaceLabels);

        NetworkPolicyPeer kafkaClusterPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector() // Kafka cluster
                     .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, kafkaClusterName(cluster))
                .endPodSelector()
                .build();

        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector() // Entity Operator
                     .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(cluster))
                .endPodSelector()
                .build();

        NetworkPolicyPeer kafkaExporterPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector() // Kafka Exporter
                     .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, KafkaExporter.kafkaExporterName(cluster))
                .endPodSelector()
                .build();

        NetworkPolicyPeer cruiseControlPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector() // Cruise Control
                     .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, CruiseControl.cruiseControlName(cluster))
                .endPodSelector()
                .build();

        // List of network policy rules for all ports
        // Default size is number of listeners configured by the user + 4 (Control Plane listener, replication listener, metrics and JMX)
        List<NetworkPolicyIngressRule> rules = new ArrayList<>(listeners.size() + 4);

        // Control Plane rule covers the control plane listener.
        // Control plane listener is used by Kafka for internal coordination only
        NetworkPolicyIngressRule controlPlaneRule = new NetworkPolicyIngressRuleBuilder()
                .addNewPort()
                .withNewPort(CONTROLPLANE_PORT)
                .withProtocol("TCP")
                .endPort()
                .build();

        controlPlaneRule.setFrom(List.of(kafkaClusterPeer));
        rules.add(controlPlaneRule);

        // Replication rule covers the replication listener.
        // Replication listener is used by Kafka but also by our own tools => Operators, Cruise Control, and Kafka Exporter
        NetworkPolicyIngressRule replicationRule = new NetworkPolicyIngressRuleBuilder()
                .addNewPort()
                .withNewPort(REPLICATION_PORT)
                .withProtocol("TCP")
                .endPort()
                .build();

        replicationRule.setFrom(List.of(clusterOperatorPeer, kafkaClusterPeer, entityOperatorPeer, kafkaExporterPeer, cruiseControlPeer));
        rules.add(replicationRule);

        // User-configured listeners are by default open for all.
        // But users can pass peers in the Kafka CR
        for (GenericKafkaListener listener : listeners) {
            NetworkPolicyIngressRule plainRule = new NetworkPolicyIngressRuleBuilder()
                    .addNewPort()
                        .withNewPort(listener.getPort())
                        .withProtocol("TCP")
                    .endPort()
                    .withFrom(listener.getNetworkPolicyPeers())
                    .build();

            rules.add(plainRule);
        }

        // The Metrics port (if enabled) is opened to all by default
        if (isMetricsEnabled) {
            NetworkPolicyIngressRule metricsRule = new NetworkPolicyIngressRuleBuilder()
                    .addNewPort()
                        .withNewPort(METRICS_PORT)
                        .withProtocol("TCP")
                    .endPort()
                    .withFrom()
                    .build();

            rules.add(metricsRule);
        }

        // The JMX port (if enabled) is opened to all by default
        if (isJmxEnabled()) {
            NetworkPolicyIngressRule jmxRule = new NetworkPolicyIngressRuleBuilder()
                    .addNewPort()
                        .withNewPort(JMX_PORT)
                        .withProtocol("TCP")
                    .endPort()
                    .withFrom()
                    .build();

            rules.add(jmxRule);
        }

        // Build the final network policy with all rules covering all the ports
        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(networkPolicyName(cluster))
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

        LOGGER.traceCr(reconciliation, "Created network policy {}", networkPolicy);
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
    public void setListeners(List<GenericKafkaListener> listeners) {
        this.listeners = listeners;
    }

    /**
     * @return The listeners
     */
    public List<GenericKafkaListener> getListeners() {
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
        return ListenersUtils.hasExternalListener(listeners);
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside using NodePort type services
     *
     * @return true when the Kafka cluster is exposed to the outside using NodePort.
     */
    public boolean isExposedWithNodePort() {
        return ListenersUtils.hasNodePortListener(listeners);
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside of Kubernetes using Ingress
     *
     * @return true when the Kafka cluster is exposed using Kubernetes Ingress.
     */
    public boolean isExposedWithIngress() {
        return ListenersUtils.hasIngressListener(listeners);
    }

    /**
     * Returns the advertised URL for given pod.
     * It will take into account the overrides specified by the user.
     * If some segment is not know - e.g. the hostname for the NodePort access, it should be left empty
     *
     * @param listener Listener where the configuration should be found
     * @param podNumber Pod index
     * @param address   The advertised hostname
     * @return The advertised hostname in format listenerIdentifier_podNumber://address (e.g. LB_9094_1://my-broker-1)
     */
    public String getAdvertisedHostname(GenericKafkaListener listener, int podNumber, String address) {
        String advertisedHost = ListenersUtils.brokerAdvertisedHost(listener, podNumber);

        if (advertisedHost == null && address == null)  {
            return null;
        }

        return ListenersUtils.envVarIdentifier(listener)
                + "_" + podNumber
                + "://"
                + (advertisedHost != null ? advertisedHost : address);
    }

    /**
     * Returns the advertised port for given pod.
     * It will take into account the overrides specified by the user.
     *
     * @param listener Listener where the configuration should be found
     * @param podNumber Pod index
     * @param port      The advertised port
     * @return The advertised port in format listenerIdentifier_podNumber://port (e.g. LB_9094_1://9094)
     */
    public String getAdvertisedPort(GenericKafkaListener listener, int podNumber, Integer port) {
        Integer advertisedPort = ListenersUtils.brokerAdvertisedPort(listener, podNumber);

        return ListenersUtils.envVarIdentifier(listener)
                + "_" + podNumber
                + "://"
                + (advertisedPort != null ? advertisedPort : port);
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

    private String generateBrokerConfiguration(boolean controlPlaneListener)   {
        return new KafkaBrokerConfigurationBuilder()
                .withBrokerId()
                .withRackId(rack)
                .withZookeeper(cluster)
                .withLogDirs(VolumeUtils.getDataVolumeMountPaths(storage, mountPath))
                .withListeners(cluster, namespace, listeners, controlPlaneListener)
                .withAuthorization(cluster, authorization)
                .withCruiseControl(cluster, cruiseControlSpec, ccNumPartitions, ccReplicationFactor, ccMinInSyncReplicas)
                .withUserConfiguration(configuration)
                .build().trim();
    }

    public String getBrokersConfiguration() {
        return this.brokersConfiguration;
    }

    public ConfigMap generateAncillaryConfigMap(MetricsAndLogging metricsAndLogging, Set<String> advertisedHostnames, Set<String> advertisedPorts, boolean controlPlaneListener)   {
        ConfigMap cm = generateMetricsAndLogConfigMap(metricsAndLogging);

        this.brokersConfiguration = generateBrokerConfiguration(controlPlaneListener);

        cm.getData().put(BROKER_CONFIGURATION_FILENAME, this.brokersConfiguration);
        cm.getData().put(BROKER_ADVERTISED_HOSTNAMES_FILENAME, String.join(" ", advertisedHostnames));
        cm.getData().put(BROKER_ADVERTISED_PORTS_FILENAME, String.join(" ", advertisedPorts));
        cm.getData().put(BROKER_LISTENERS_FILENAME,
                listeners.stream().map(ListenersUtils::envVarIdentifier).collect(Collectors.joining(" ")));

        return cm;
    }

    public KafkaVersion getKafkaVersion() {
        return this.kafkaVersion;
    }

    @Override
    protected boolean shouldPatchLoggerAppender() {
        return true;
    }

    public String getLogMessageFormatVersion() {
        return configuration.getConfigOption(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION);
    }

    public void setLogMessageFormatVersion(String logMessageFormatVersion) {
        configuration.setConfigOption(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, logMessageFormatVersion);
    }

    public String getInterBrokerProtocolVersion() {
        return configuration.getConfigOption(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION);
    }

    public void setInterBrokerProtocolVersion(String interBrokerProtocolVersion) {
        configuration.setConfigOption(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, interBrokerProtocolVersion);
    }
}
