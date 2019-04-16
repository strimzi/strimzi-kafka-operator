/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPort;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleRef;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubject;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubjectBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JbodStorage;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaAuthorization;
import io.strimzi.api.kafka.model.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.listener.ExternalListenerBootstrapOverride;
import io.strimzi.api.kafka.model.listener.ExternalListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerOverride;
import io.strimzi.api.kafka.model.listener.NodePortListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.NodePortListenerOverride;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalLoadBalancer;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePort;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalRoute;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.SingleVolumeStorage;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.listener.RouteListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.RouteListenerOverride;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaCluster extends AbstractModel {
    protected static final String INIT_NAME = "kafka-init";
    protected static final String INIT_VOLUME_NAME = "rack-volume";
    protected static final String INIT_VOLUME_MOUNT = "/opt/kafka/init";
    private static final String ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    private static final String ENV_VAR_KAFKA_INIT_NODE_NAME = "NODE_NAME";
    private static final String ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS = "EXTERNAL_ADDRESS";
    private static final String ENV_VAR_KAFKA_INIT_EXTERNAL_ADVERTISED_ADDRESSES = "EXTERNAL_ADVERTISED_ADDRESSES";
    /** {@code TRUE} when the CLIENT listener (PLAIN transport) should be enabled*/
    private static final String ENV_VAR_KAFKA_CLIENT_ENABLED = "KAFKA_CLIENT_ENABLED";
    /** The authentication to configure for the CLIENT listener (PLAIN transport). */
    private static final String ENV_VAR_KAFKA_CLIENT_AUTHENTICATION = "KAFKA_CLIENT_AUTHENTICATION";
    /** {@code TRUE} when the CLIENTTLS listener (TLS transport) should be enabled*/
    private static final String ENV_VAR_KAFKA_CLIENTTLS_ENABLED = "KAFKA_CLIENTTLS_ENABLED";
    /** The authentication to configure for the CLIENTTLS listener (TLS transport) . */
    private static final String ENV_VAR_KAFKA_CLIENTTLS_AUTHENTICATION = "KAFKA_CLIENTTLS_AUTHENTICATION";
    public static final String ENV_VAR_KAFKA_EXTERNAL_ENABLED = "KAFKA_EXTERNAL_ENABLED";
    protected static final String ENV_VAR_KAFKA_EXTERNAL_ADDRESSES = "KAFKA_EXTERNAL_ADDRESSES";
    protected static final String ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION = "KAFKA_EXTERNAL_AUTHENTICATION";
    protected static final String ENV_VAR_KAFKA_EXTERNAL_TLS = "KAFKA_EXTERNAL_TLS";
    private static final String ENV_VAR_KAFKA_AUTHORIZATION_TYPE = "KAFKA_AUTHORIZATION_TYPE";
    private static final String ENV_VAR_KAFKA_AUTHORIZATION_SUPER_USERS = "KAFKA_AUTHORIZATION_SUPER_USERS";
    public static final String ENV_VAR_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static final String ENV_VAR_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";
    public static final String ENV_VAR_KAFKA_LOG_DIRS = "KAFKA_LOG_DIRS";

    public static final String ENV_VAR_KAFKA_CONFIGURATION = "KAFKA_CONFIGURATION";

    protected static final int CLIENT_PORT = 9092;
    protected static final String CLIENT_PORT_NAME = "clients";

    protected static final int REPLICATION_PORT = 9091;
    protected static final String REPLICATION_PORT_NAME = "replication";

    protected static final int CLIENT_TLS_PORT = 9093;
    protected static final String CLIENT_TLS_PORT_NAME = "clientstls";

    protected static final int EXTERNAL_PORT = 9094;
    protected static final String EXTERNAL_PORT_NAME = "external";

    protected static final String KAFKA_NAME = "kafka";
    protected static final String CLUSTER_CA_CERTS_VOLUME = "cluster-ca";
    protected static final String BROKER_CERTS_VOLUME = "broker-certs";
    protected static final String CLIENT_CA_CERTS_VOLUME = "client-ca-cert";
    protected static final String CLUSTER_CA_CERTS_VOLUME_MOUNT = "/opt/kafka/cluster-ca-certs";
    protected static final String BROKER_CERTS_VOLUME_MOUNT = "/opt/kafka/broker-certs";
    protected static final String CLIENT_CA_CERTS_VOLUME_MOUNT = "/opt/kafka/client-ca-certs";
    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_KAFKA_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/kafka-brokers/";
    protected static final String TLS_SIDECAR_CLUSTER_CA_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/cluster-ca-certs/";

    private static final String NAME_SUFFIX = "-kafka";

    // Suffixes for secrets with certificates
    private static final String SECRET_BROKERS_SUFFIX = NAME_SUFFIX + "-brokers";

    /** Records the Kafka version currently running inside Kafka StatefulSet */
    public static final String ANNO_STRIMZI_IO_KAFKA_VERSION = Annotations.STRIMZI_DOMAIN + "/kafka-version";
    /** Records the state of the Kafka upgrade process. Unset outside of upgrades. */
    public static final String ANNO_STRIMZI_IO_FROM_VERSION = Annotations.STRIMZI_DOMAIN + "/from-version";
    /** Records the state of the Kafka upgrade process. Unset outside of upgrades. */
    public static final String ANNO_STRIMZI_IO_TO_VERSION = Annotations.STRIMZI_DOMAIN + "/to-version";

    // Kafka configuration
    private String zookeeperConnect;
    private Rack rack;
    private String initImage;
    private TlsSidecar tlsSidecar;
    private KafkaListeners listeners;
    private KafkaAuthorization authorization;
    private Set<String> externalAddresses = new HashSet<>();
    private KafkaVersion kafkaVersion;

    // Templates
    protected Map<String, String> templateExternalBootstrapServiceLabels;
    protected Map<String, String> templateExternalBootstrapServiceAnnotations;
    protected Map<String, String> templatePerPodServiceLabels;
    protected Map<String, String> templatePerPodServiceAnnotations;
    protected Map<String, String> templateExternalBootstrapRouteLabels;
    protected Map<String, String> templateExternalBootstrapRouteAnnotations;
    protected Map<String, String> templatePerPodRouteLabels;
    protected Map<String, String> templatePerPodRouteAnnotations;

    // Configuration defaults
    private static final int DEFAULT_REPLICAS = 3;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_KAFKA_METRICS_ENABLED = false;

    /**
     * Private key and certificate for each Kafka Pod name
     * used as server certificates for Kafka brokers
     */
    private Map<String, CertAndKey> brokerCerts;

    /**
     * Lists with volumes, persistent volume claims and related volume mount paths for the storage
     */
    List<Volume> dataVolumes = new ArrayList<>();
    List<PersistentVolumeClaim> dataPvcs = new ArrayList<>();
    List<VolumeMount> dataVolumeMountPaths = new ArrayList<>();

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka cluster resources are going to be created
     * @param cluster  overall cluster name
     * @param labels    labels to add to the cluster
     */
    private KafkaCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = kafkaClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.headlessServiceName = headlessServiceName(cluster);
        this.ancillaryConfigName = metricAndLogConfigsName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.readinessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.readinessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.livenessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.livenessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_KAFKA_METRICS_ENABLED;

        setZookeeperConnect(ZookeeperCluster.serviceName(cluster) + ":2181");

        this.mountPath = "/var/lib/kafka";

        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/kafka/custom-config/";

        this.initImage = System.getenv().getOrDefault("STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE", "strimzi/operator:latest");
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
        return String.format("%s.%s.%s.svc.%s",
                KafkaCluster.kafkaPodName(cluster, podId),
                KafkaCluster.headlessServiceName(cluster),
                namespace,
                ModelUtils.KUBERNETES_SERVICE_DNS_DOMAIN);
    }

    /**
     * Generates the name of the service used as bootstrap service for external clients
     *
     * @param cluster Name of the cluster
     * @return
     */
    public static String externalBootstrapServiceName(String cluster) {
        return KafkaResources.externalBootstrapServiceName(cluster);
    }

    /**
     * Generates the name of the service for exposing individual pods
     *
     * @param cluster Name of the cluster
     * @param pod   Pod sequence number assign by StatefulSet
     * @return
     */
    public static String externalServiceName(String cluster, int pod) {
        return kafkaClusterName(cluster) + "-" + pod;
    }

    public static String headlessServiceName(String cluster) {
        return KafkaResources.brokersServiceName(cluster);
    }

    public static String kafkaPodName(String cluster, int pod) {
        return kafkaClusterName(cluster) + "-" + pod;
    }

    public static String clientsCaKeySecretName(String cluster) {
        return KafkaResources.clientsCaKeySecretName(cluster);
    }

    public static String brokersSecretName(String cluster) {
        return cluster + KafkaCluster.SECRET_BROKERS_SUFFIX;
    }

    public static String clientsCaCertSecretName(String cluster) {
        return KafkaResources.clientsCaCertificateSecretName(cluster);
    }

    public static KafkaCluster fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        return fromCrd(kafkaAssembly, versions, null);
    }

    public static KafkaCluster fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions, Storage oldStorage) {
        KafkaCluster result = new KafkaCluster(kafkaAssembly.getMetadata().getNamespace(),
                kafkaAssembly.getMetadata().getName(),
                Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));

        result.setOwnerReference(kafkaAssembly);

        KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();

        result.setReplicas(kafkaClusterSpec.getReplicas());

        String image = versions.kafkaImage(kafkaClusterSpec.getImage(), kafkaClusterSpec.getVersion());
        if (image == null) {
            throw new InvalidResourceException("Version " + kafkaClusterSpec.getVersion() + " is not supported. Supported versions are: " + String.join(", ", versions.supportedVersions()) + ".");
        }
        result.setImage(image);

        if (kafkaClusterSpec.getReadinessProbe() != null) {
            result.setReadinessInitialDelay(kafkaClusterSpec.getReadinessProbe().getInitialDelaySeconds());
            result.setReadinessTimeout(kafkaClusterSpec.getReadinessProbe().getTimeoutSeconds());
        }

        if (kafkaClusterSpec.getLivenessProbe() != null) {
            result.setLivenessInitialDelay(kafkaClusterSpec.getLivenessProbe().getInitialDelaySeconds());
            result.setLivenessTimeout(kafkaClusterSpec.getLivenessProbe().getTimeoutSeconds());
        }

        result.setRack(kafkaClusterSpec.getRack());

        String initImage = kafkaClusterSpec.getBrokerRackInitImage();
        if (initImage == null) {
            initImage = System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_INIT_IMAGE", "strimzi/operator:latest");
        }
        result.setInitImage(initImage);

        Logging logging = kafkaClusterSpec.getLogging();
        result.setLogging(logging == null ? new InlineLogging() : logging);

        result.setGcLoggingEnabled(kafkaClusterSpec.getJvmOptions() == null ? true : kafkaClusterSpec.getJvmOptions().isGcLoggingEnabled());

        result.setJvmOptions(kafkaClusterSpec.getJvmOptions());

        result.setConfiguration(new KafkaConfiguration(kafkaClusterSpec.getConfig().entrySet()));

        Map<String, Object> metrics = kafkaClusterSpec.getMetrics();
        if (metrics != null) {
            result.setMetricsEnabled(true);
            result.setMetricsConfig(metrics.entrySet());
        }

        if (oldStorage != null) {
            Storage newStorage = kafkaClusterSpec.getStorage();
            StorageDiff diff = new StorageDiff(oldStorage, newStorage);

            if (!diff.isEmpty()) {
                log.warn("Changing Kafka storage is not possible. The changes will be ignored.");
                result.setStorage(oldStorage);
            } else {
                result.setStorage(newStorage);
            }
        } else {
            result.setStorage(kafkaClusterSpec.getStorage());
        }

        result.setDataVolumesClaimsAndMountPaths(result.getStorage());

        result.setUserAffinity(kafkaClusterSpec.getAffinity());

        result.setResources(kafkaClusterSpec.getResources());

        result.setTolerations(kafkaClusterSpec.getTolerations());

        TlsSidecar tlsSidecar = kafkaClusterSpec.getTlsSidecar();
        if (tlsSidecar == null) {
            tlsSidecar = new TlsSidecar();
        }
        if (tlsSidecar.getImage() == null) {
            String tlsSidecarImage = versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version());
            if (tlsSidecarImage == null) {
                throw new InvalidResourceException("Version " + kafkaClusterSpec.getVersion() + " is not supported. Supported versions are: " + String.join(", ", versions.supportedVersions()) + ".");
            }
            tlsSidecar.setImage(tlsSidecarImage);
        }
        result.setTlsSidecar(tlsSidecar);

        KafkaListeners listeners = kafkaClusterSpec.getListeners();
        result.setListeners(listeners);

        if (listeners != null) {
            if (listeners.getPlain() != null
                && listeners.getPlain().getAuthentication() instanceof KafkaListenerAuthenticationTls) {
                throw new InvalidResourceException("You cannot configure TLS authentication on a plain listener.");
            }

            if (listeners.getExternal() != null && !result.isExposedWithTls() && listeners.getExternal().getAuth() instanceof KafkaListenerAuthenticationTls)  {
                throw new InvalidResourceException("TLS Client Authentication can be used only with enabled TLS encryption!");
            }
        }

        result.setAuthorization(kafkaClusterSpec.getAuthorization());

        if (kafkaClusterSpec.getTemplate() != null) {
            KafkaClusterTemplate template = kafkaClusterSpec.getTemplate();

            if (template.getStatefulset() != null && template.getStatefulset().getMetadata() != null)  {
                result.templateStatefulSetLabels = template.getStatefulset().getMetadata().getLabels();
                result.templateStatefulSetAnnotations = template.getStatefulset().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodTemplate(result, template.getPod());

            if (template.getBootstrapService() != null && template.getBootstrapService().getMetadata() != null)  {
                result.templateServiceLabels = template.getBootstrapService().getMetadata().getLabels();
                result.templateServiceAnnotations = template.getBootstrapService().getMetadata().getAnnotations();
            }

            if (template.getBrokersService() != null && template.getBrokersService().getMetadata() != null)  {
                result.templateHeadlessServiceLabels = template.getBrokersService().getMetadata().getLabels();
                result.templateHeadlessServiceAnnotations = template.getBrokersService().getMetadata().getAnnotations();
            }

            if (template.getExternalBootstrapService() != null && template.getExternalBootstrapService().getMetadata() != null)  {
                result.templateExternalBootstrapServiceLabels = template.getExternalBootstrapService().getMetadata().getLabels();
                result.templateExternalBootstrapServiceAnnotations = template.getExternalBootstrapService().getMetadata().getAnnotations();
            }

            if (template.getPerPodService() != null && template.getPerPodService().getMetadata() != null)  {
                result.templatePerPodServiceLabels = template.getPerPodService().getMetadata().getLabels();
                result.templatePerPodServiceAnnotations = template.getPerPodService().getMetadata().getAnnotations();
            }

            if (template.getExternalBootstrapRoute() != null && template.getExternalBootstrapRoute().getMetadata() != null)  {
                result.templateExternalBootstrapRouteLabels = template.getExternalBootstrapRoute().getMetadata().getLabels();
                result.templateExternalBootstrapRouteAnnotations = template.getExternalBootstrapRoute().getMetadata().getAnnotations();
            }

            if (template.getPerPodRoute() != null && template.getPerPodRoute().getMetadata() != null)  {
                result.templatePerPodRouteLabels = template.getPerPodRoute().getMetadata().getLabels();
                result.templatePerPodRouteAnnotations = template.getPerPodRoute().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(result, template.getPodDisruptionBudget());
        }

        result.kafkaVersion = versions.version(kafkaClusterSpec.getVersion());
        return result;
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param kafka     The Kafka custom resource
     * @param clusterCa The CA for cluster certificates
     * @param externalBootstrapDnsName  The set of DNS names for bootstrap service (should be appended to every broker certificate)
     * @param externalDnsNames The list of DNS names for broker pods (should be appended only to specific certificates for given broker)
     */
    public void generateCertificates(Kafka kafka, ClusterCa clusterCa, Set<String> externalBootstrapDnsName, Map<Integer, Set<String>> externalDnsNames) {
        log.debug("Generating certificates");

        try {
            brokerCerts = clusterCa.generateBrokerCerts(kafka, externalBootstrapDnsName, externalDnsNames);
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
        List<ServicePort> ports = new ArrayList<>(4);
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));

        if (listeners != null && listeners.getPlain() != null) {
            ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));
        }

        if (listeners != null && listeners.getTls() != null) {
            ports.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));
        }

        if (isMetricsEnabled()) {
            ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
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
     * Generates a Service according to configured defaults
     * @return The generated Service
     */
    public Service generateService() {
        return createService("ClusterIP", getServicePorts(), mergeAnnotations(getPrometheusAnnotations(), templateServiceAnnotations));
    }

    /**
     * Utility function to help to determine the type of service based on external listener configuration
     *
     * @return  Service type
     */
    private String getExternalServiceType() {
        if (isExposedWithNodePort()) {
            return "NodePort";
        } else if (isExposedWithLoadBalancer()) {
            return "LoadBalancer";
        } else  {
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

            return createService(externalBootstrapServiceName, getExternalServiceType(), ports,
                getLabelsWithName(externalBootstrapServiceName, templateExternalBootstrapServiceLabels),
                getSelectorLabels(),
                mergeAnnotations(Collections.EMPTY_MAP, templateExternalBootstrapServiceAnnotations));
        }

        return null;
    }

    /**
     * Generates service for pod. This service is used for exposing it externally.
     *
     * @param pod   Number of the pod for which this service should be generated
     * @return The generated Service
     */
    public Service generateExternalService(int pod) {
        if (isExposed()) {
            String perPodServiceName = externalServiceName(cluster, pod);

            List<ServicePort> ports = new ArrayList<>(1);
            Integer nodePort = null;
            if (isExposedWithNodePort()) {
                KafkaListenerExternalNodePort externalNodePort = (KafkaListenerExternalNodePort) listeners.getExternal();
                if (externalNodePort.getOverrides() != null &&  externalNodePort.getOverrides().getBrokers() != null) {
                    nodePort = externalNodePort.getOverrides().getBrokers().stream()
                        .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getNodePort() != null)
                        .map(NodePortListenerBrokerOverride::getNodePort)
                        .findAny().orElse(null);
                }
            }
            ports.add(createServicePort(EXTERNAL_PORT_NAME, EXTERNAL_PORT, EXTERNAL_PORT, nodePort, "TCP"));

            Labels selector = Labels.fromMap(getSelectorLabels()).withStatefulSetPod(kafkaPodName(cluster, pod));

            return createService(perPodServiceName, getExternalServiceType(), ports,
                getLabelsWithName(perPodServiceName, templatePerPodServiceLabels), selector.toMap(),
                mergeAnnotations(Collections.EMPTY_MAP, templatePerPodServiceAnnotations));
        }

        return null;
    }

    /**
     * Generates route for pod. This route is used for exposing it externally using OpenShift Routes.
     *
     * @param pod   Number of the pod for which this route should be generated
     * @return The generated Route
     */
    public Route generateExternalRoute(int pod) {
        if (isExposedWithRoute()) {
            String perPodServiceName = externalServiceName(cluster, pod);

            Route route = new RouteBuilder()
                    .withNewMetadata()
                        .withName(perPodServiceName)
                        .withLabels(getLabelsWithName(perPodServiceName, templatePerPodRouteLabels))
                        .withAnnotations(mergeAnnotations(null, templatePerPodRouteAnnotations))
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
            if (listener.getOverrides() != null && listener.getOverrides().getBrokers() != null)  {
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
     * @return The generated Routes
     */
    public Route generateExternalBootstrapRoute() {
        if (isExposedWithRoute()) {
            Route route = new RouteBuilder()
                    .withNewMetadata()
                        .withName(serviceName)
                        .withLabels(getLabelsWithName(serviceName, templateExternalBootstrapRouteLabels))
                        .withAnnotations(mergeAnnotations(null, templateExternalBootstrapRouteAnnotations))
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
            if (listener.getOverrides() != null && listener.getOverrides().getBootstrap() != null && listener.getOverrides().getBootstrap().getHost() != null && !listener.getOverrides().getBootstrap().getHost().isEmpty())  {
                route.getSpec().setHost(listener.getOverrides().getBootstrap().getHost());
            }

            return route;
        }

        return null;
    }

    /**
     * Generates a headless Service according to configured defaults
     * @return The generated Service
     */
    public Service generateHeadlessService() {
        return createHeadlessService(getHeadlessServicePorts());
    }

    /**
     * Generates a StatefulSet according to configured defaults
     * @param isOpenShift True iff this operator is operating within OpenShift.
     * @return The generate StatefulSet
     */
    public StatefulSet generateStatefulSet(boolean isOpenShift, ImagePullPolicy imagePullPolicy) {
        HashMap<String, String> annotations = new HashMap<>(2);
        annotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion.version());
        annotations.put(ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(storage));

        return createStatefulSet(
                annotations,
                getVolumes(isOpenShift),
                getVolumeClaims(),
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
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

        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < replicas; i++) {
            CertAndKey cert = brokerCerts.get(KafkaCluster.kafkaPodName(cluster, i));
            data.put(KafkaCluster.kafkaPodName(cluster, i) + ".key", cert.keyAsBase64String());
            data.put(KafkaCluster.kafkaPodName(cluster, i) + ".crt", cert.certAsBase64String());
        }
        return createSecret(KafkaCluster.brokersSecretName(cluster), data);
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
     * Fill the StatefulSet with volumes, persistent volume claims and related volume mount paths for the storage
     * It's called recursively on the related inner volumes if the storage is of {@link Storage#TYPE_JBOD} type
     *
     * @param storage the Storage instance from which building volumes, persistent volume claims and
     *                related volume mount paths
     */
    private void setDataVolumesClaimsAndMountPaths(Storage storage) {
        if (storage != null) {
            Integer id;
            if (storage instanceof EphemeralStorage) {
                id = ((EphemeralStorage) storage).getId();
            } else if (storage instanceof PersistentClaimStorage) {
                id = ((PersistentClaimStorage) storage).getId();
            } else if (storage instanceof JbodStorage) {
                for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
                    if (volume.getId() == null)
                        throw new InvalidResourceException("Volumes under JBOD storage type have to have 'id' property");
                    // it's called recursively for setting the information from the current volume
                    setDataVolumesClaimsAndMountPaths(volume);
                }
                return;
            } else {
                throw new IllegalStateException("The declared storage '" + storage.getType() + "' is not supported");
            }

            String name = ModelUtils.getVolumePrefix(id);
            String mountPath = this.mountPath + "/" + name;

            if (storage instanceof EphemeralStorage) {
                dataVolumes.add(createEmptyDirVolume(name));
            } else if (storage instanceof PersistentClaimStorage) {
                dataPvcs.add(createPersistentVolumeClaimTemplate(name, (PersistentClaimStorage) storage));
            }
            dataVolumeMountPaths.add(createVolumeMount(name, mountPath));
        }
    }

    /**
     * Generate the persistent volume claims for the storage It's called recursively on the related inner volumes if the
     * storage is of {@link Storage#TYPE_JBOD} type
     *
     * @param storage the Storage instance from which building volumes, persistent volume claims and
     *                related volume mount paths
     */
    public List<PersistentVolumeClaim> generatePersistentVolumeClaims(Storage storage) {
        List<PersistentVolumeClaim> pvcs = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof PersistentClaimStorage) {
                Integer id = ((PersistentClaimStorage) storage).getId();
                String pvcBaseName = ModelUtils.getVolumePrefix(id) + "-" + name;

                for (int i = 0; i < replicas; i++)  {
                    pvcs.add(createPersistentVolumeClaim(pvcBaseName + "-" + i, (PersistentClaimStorage) storage));
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
            volumeList.add(createEmptyDirVolume(INIT_VOLUME_NAME));
        }
        volumeList.add(createSecretVolume(CLUSTER_CA_CERTS_VOLUME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));
        volumeList.add(createSecretVolume(BROKER_CERTS_VOLUME, KafkaCluster.brokersSecretName(cluster), isOpenShift));
        volumeList.add(createSecretVolume(CLIENT_CA_CERTS_VOLUME, KafkaCluster.clientsCaCertSecretName(cluster), isOpenShift));
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
        volumeList.add(new VolumeBuilder().withName("ready-files").withNewEmptyDir().withMedium("Memory").endEmptyDir().build());

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

        volumeMountList.add(createVolumeMount(CLUSTER_CA_CERTS_VOLUME, CLUSTER_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(BROKER_CERTS_VOLUME, BROKER_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(CLIENT_CA_CERTS_VOLUME, CLIENT_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
        volumeMountList.add(createVolumeMount("ready-files", "/var/opt/kafka"));

        if (rack != null || isExposedWithNodePort()) {
            volumeMountList.add(createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT));
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
        }
        return builder.build();
    }

    @Override
    protected List<Container> getInitContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> initContainers = new ArrayList<>();

        if (rack != null || isExposedWithNodePort()) {
            ResourceRequirements resources = new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100m"))
                    .addToRequests("memory", new Quantity("128Mi"))
                    .addToLimits("cpu", new Quantity("1"))
                    .addToLimits("memory", new Quantity("256Mi"))
                    .build();

            List<EnvVar> varList = new ArrayList<>();
            varList.add(buildEnvVarFromFieldRef(ENV_VAR_KAFKA_INIT_NODE_NAME, "spec.nodeName"));

            if (rack != null) {
                varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY, rack.getTopologyKey()));
            }

            if (isExposedWithNodePort()) {
                varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS, "TRUE"));
                varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_EXTERNAL_ADVERTISED_ADDRESSES, String.join(" ", externalAddresses)));
            }

            Container initContainer = new ContainerBuilder()
                    .withName(INIT_NAME)
                    .withImage(initImage)
                    .withArgs("/opt/strimzi/bin/kafka_init_run.sh")
                    .withResources(resources)
                    .withEnv(varList)
                    .withVolumeMounts(createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT))
                    .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, initImage))
                    .build();

            initContainers.add(initContainer);
        }

        return initContainers;
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        List<Container> containers = new ArrayList<>();

        Container container = new ContainerBuilder()
                .withName(KAFKA_NAME)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withVolumeMounts(getVolumeMounts())
                .withPorts(getContainerPortList())
                .withNewLivenessProbe()
                    .withInitialDelaySeconds(livenessInitialDelay)
                    .withTimeoutSeconds(livenessTimeout)
                    .withNewExec()
                        .withCommand("/opt/kafka/kafka_liveness.sh")
                    .endExec()
                .endLivenessProbe()
                .withNewReadinessProbe()
                    .withInitialDelaySeconds(readinessInitialDelay)
                    .withTimeoutSeconds(readinessTimeout)
                    .withNewExec()
                        // The kafka-agent will create /var/opt/kafka/kafka-ready in the container
                        .withCommand("test", "-f", "/var/opt/kafka/kafka-ready")
                    .endExec()
                .endReadinessProbe()
                .withResources(getResources())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withCommand("/opt/kafka/kafka_run.sh")
                .build();

        String tlsSidecarImage = getImage();
        if (tlsSidecar != null && tlsSidecar.getImage() != null) {
            tlsSidecarImage = tlsSidecar.getImage();
        }

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withCommand("/opt/stunnel/kafka_stunnel_run.sh")
                .withLivenessProbe(ModelUtils.tlsSidecarLivenessProbe(tlsSidecar))
                .withReadinessProbe(ModelUtils.tlsSidecarReadinessProbe(tlsSidecar))
                .withResources(tlsSidecar != null ? tlsSidecar.getResources() : null)
                .withEnv(asList(buildEnvVar(ENV_VAR_KAFKA_ZOOKEEPER_CONNECT, zookeeperConnect),
                        ModelUtils.tlsSidecarLogEnvVar(tlsSidecar)))
                .withVolumeMounts(createVolumeMount(BROKER_CERTS_VOLUME, TLS_SIDECAR_KAFKA_CERTS_VOLUME_MOUNT),
                        createVolumeMount(CLUSTER_CA_CERTS_VOLUME, TLS_SIDECAR_CLUSTER_CA_CERTS_VOLUME_MOUNT))
                .withLifecycle(new LifecycleBuilder().withNewPreStop()
                            .withNewExec().withCommand("/opt/stunnel/kafka_stunnel_pre_stop.sh",
                                String.valueOf(templateTerminationGracePeriodSeconds))
                        .endExec().endPreStop().build())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, tlsSidecarImage))
                .build();

        containers.add(container);
        containers.add(tlsSidecarContainer);

        return containers;
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

        heapOptions(varList, 0.5, 5L * 1024L * 1024L * 1024L);
        jvmPerformanceOptions(varList);

        if (configuration != null && !configuration.getConfiguration().isEmpty()) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONFIGURATION, configuration.getConfiguration()));
        }

        if (listeners != null)  {
            if (listeners.getPlain() != null)   {
                varList.add(buildEnvVar(ENV_VAR_KAFKA_CLIENT_ENABLED, "TRUE"));

                if (listeners.getPlain().getAuthentication() != null) {
                    varList.add(buildEnvVar(ENV_VAR_KAFKA_CLIENT_AUTHENTICATION, listeners.getPlain().getAuthentication().getType()));
                }
            }

            if (listeners.getTls() != null) {
                varList.add(buildEnvVar(ENV_VAR_KAFKA_CLIENTTLS_ENABLED, "TRUE"));

                if (listeners.getTls().getAuth() != null) {
                    varList.add(buildEnvVar(ENV_VAR_KAFKA_CLIENTTLS_AUTHENTICATION, listeners.getTls().getAuth().getType()));
                }
            }

            if (listeners.getExternal() != null) {
                varList.add(buildEnvVar(ENV_VAR_KAFKA_EXTERNAL_ENABLED, listeners.getExternal().getType()));
                varList.add(buildEnvVar(ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", externalAddresses)));
                varList.add(buildEnvVar(ENV_VAR_KAFKA_EXTERNAL_TLS, Boolean.toString(isExposedWithTls())));

                if (listeners.getExternal().getAuth() != null) {
                    varList.add(buildEnvVar(ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION, listeners.getExternal().getAuth().getType()));
                }
            }
        }

        if (authorization != null && KafkaAuthorizationSimple.TYPE_SIMPLE.equals(authorization.getType()))  {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_AUTHORIZATION_TYPE, KafkaAuthorizationSimple.TYPE_SIMPLE));

            KafkaAuthorizationSimple simpleAuthz = (KafkaAuthorizationSimple) authorization;
            if (simpleAuthz.getSuperUsers() != null && simpleAuthz.getSuperUsers().size() > 0)  {
                String superUsers = simpleAuthz.getSuperUsers().stream().map(e -> String.format("User:%s", e)).collect(Collectors.joining(";"));
                varList.add(buildEnvVar(ENV_VAR_KAFKA_AUTHORIZATION_SUPER_USERS, superUsers));
            }
        }

        String logDirs = dataVolumeMountPaths.stream()
                .map(volumeMount -> volumeMount.getMountPath()).collect(Collectors.joining(","));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_LOG_DIRS, logDirs));

        return varList;
    }

    protected void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    protected void setRack(Rack rack) {
        this.rack = rack;
    }

    protected void setInitImage(String initImage) {
        this.initImage = initImage;
    }

    protected void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "kafkaDefaultLoggingProperties";
    }

    /**
     * Get the name of the kafka service account given the name of the {@code kafkaResourceName}.
     */
    public static String initContainerServiceAccountName(String kafkaResourceName) {
        return kafkaClusterName(kafkaResourceName);
    }

    /**
     * Get the name of the kafka init container role binding given the name of the {@code namespace} and {@code cluster}.
     */
    public static String initContainerClusterRoleBindingName(String namespace, String cluster) {
        return "strimzi-" + namespace + "-" + cluster + "-kafka-init";
    }

    /**
     * Creates the ClusterRoleBinding which is used to bind the Kafka SA to the ClusterRole
     * which permissions the Kafka init container to access K8S nodes (necessary for rack-awareness).
     */
    public KubernetesClusterRoleBinding generateClusterRoleBinding(String assemblyNamespace) {
        if (rack != null || isExposedWithNodePort()) {
            KubernetesSubject ks = new KubernetesSubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName(initContainerServiceAccountName(cluster))
                    .withNamespace(assemblyNamespace)
                    .build();

            KubernetesRoleRef roleRef = new KubernetesRoleRefBuilder()
                    .withName("strimzi-kafka-broker")
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .build();

            return new KubernetesClusterRoleBindingBuilder()
                    .withNewMetadata()
                        .withName(initContainerClusterRoleBindingName(namespace, cluster))
                        .withNamespace(assemblyNamespace)
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

    public static String policyName(String cluster) {
        return cluster + NETWORK_POLICY_KEY_SUFFIX + NAME_SUFFIX;
    }

    public NetworkPolicy generateNetworkPolicy() {
        List<NetworkPolicyIngressRule> rules = new ArrayList<>(5);

        // Restrict access to 9091 / replication port
        NetworkPolicyPort replicationPort = new NetworkPolicyPort();
        replicationPort.setPort(new IntOrString(REPLICATION_PORT));

        NetworkPolicyPeer kafkaClusterPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector = new LabelSelector();
        Map<String, String> expressions = new HashMap<>();
        expressions.put(Labels.STRIMZI_NAME_LABEL, kafkaClusterName(cluster));
        labelSelector.setMatchLabels(expressions);
        kafkaClusterPeer.setPodSelector(labelSelector);

        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector2 = new LabelSelector();
        Map<String, String> expressions2 = new HashMap<>();
        expressions2.put(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(cluster));
        labelSelector2.setMatchLabels(expressions2);
        entityOperatorPeer.setPodSelector(labelSelector2);

        NetworkPolicyIngressRule replicationRule = new NetworkPolicyIngressRuleBuilder()
                .withPorts(replicationPort)
                .withFrom(kafkaClusterPeer, entityOperatorPeer)
                .build();

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

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(policyName(cluster))
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withPodSelector(labelSelector)
                    .withIngress(rules)
                .endSpec()
                .build();

        log.trace("Created network policy {}", networkPolicy);
        return networkPolicy;
    }

    /**
     * Generates the PodDisruptionBudget
     *
     * @return
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    /**
     * Sets the object with Kafka listeners configuration
     *
     * @param listeners
     */
    public void setListeners(KafkaListeners listeners) {
        this.listeners = listeners;
    }

    /**
     * Sets the object with Kafka authorization configuration
     *
     * @param authorization
     */
    public void setAuthorization(KafkaAuthorization authorization) {
        this.authorization = authorization;
    }

    /**
     * Sets the Map with Kafka pod's external addresses
     *
     * @param externalAddresses Set with external addresses
     */
    public void setExternalAddresses(Set<String> externalAddresses) {
        this.externalAddresses = externalAddresses;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside of OpenShift / Kubernetes
     *
     * @return
     */
    public boolean isExposed()  {
        return listeners != null && listeners.getExternal() != null;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside of OpenShift using OpenShift routes
     *
     * @return
     */
    public boolean isExposedWithRoute()  {
        return isExposed() && listeners.getExternal() instanceof KafkaListenerExternalRoute;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside using LoadBalancers
     *
     * @return
     */
    public boolean isExposedWithLoadBalancer()  {
        return isExposed() && listeners.getExternal() instanceof KafkaListenerExternalLoadBalancer;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside using NodePort type services
     *
     * @return
     */
    public boolean isExposedWithNodePort()  {
        return isExposed() && listeners.getExternal() instanceof KafkaListenerExternalNodePort;
    }

    /**
     * Returns the list broker overrides for external listeners
     *
     * @return
     */
    private List<ExternalListenerBrokerOverride> getExternalListenerBrokerOverride()  {
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
        }

        return brokerOverride;
    }

    /**
     * Returns the bootstrap override for external listeners
     *
     * @return
     */
    public ExternalListenerBootstrapOverride getExternalListenerBootstrapOverride()  {
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
        }

        return bootstrapOverride;
    }

    /**
     * Returns advertised address of external nodeport service
     *
     * @return
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
     * Returns advertised address of external nodeport service
     *
     * @return
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
     * @param port      The advertised port
     * @return          The advertised URL in format podNumber://address:port (e.g. 1://my-broker-1:9094)
     */
    public String getExternalAdvertisedUrl(int podNumber, String address, String port)  {
        String advertisedHost = getExternalServiceAdvertisedHostOverride(podNumber);
        Integer advertisedPort = getExternalServiceAdvertisedPortOverride(podNumber);

        String url = String.valueOf(podNumber)
                + "://" + (advertisedHost != null ? advertisedHost : address)
                + ":" + (advertisedPort != null ? advertisedPort : port);

        return url;
    }

    /**
     * Returns true when the Kafka cluster is exposed to the outside of OpenShift with TLS enabled
     *
     * @return
     */
    public boolean isExposedWithTls() {
        if (isExposed() && listeners.getExternal() instanceof KafkaListenerExternalRoute) {
            return true;
        } else if (isExposed()) {
            if (listeners.getExternal() instanceof KafkaListenerExternalLoadBalancer) {
                return ((KafkaListenerExternalLoadBalancer) listeners.getExternal()).isTls();
            } else if (listeners.getExternal() instanceof KafkaListenerExternalNodePort) {
                return ((KafkaListenerExternalNodePort) listeners.getExternal()).isTls();
            }
        }

        return false;
    }

    @Override
    public KafkaConfiguration getConfiguration() {
        return (KafkaConfiguration) configuration;
    }
}
