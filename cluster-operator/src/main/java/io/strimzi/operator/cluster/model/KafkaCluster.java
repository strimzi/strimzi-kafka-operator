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
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicy;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyPort;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaAuthorization;
import io.strimzi.api.kafka.model.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.KafkaListenerExternalLoadBalancer;
import io.strimzi.api.kafka.model.KafkaListenerExternalNodePort;
import io.strimzi.api.kafka.model.KafkaListenerExternalRoute;
import io.strimzi.api.kafka.model.KafkaListeners;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class KafkaCluster extends AbstractModel {

    protected static final String INIT_NAME = "kafka-init";
    protected static final String INIT_VOLUME_NAME = "rack-volume";
    protected static final String INIT_VOLUME_MOUNT = "/opt/kafka/init";
    private static final String ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    private static final String ENV_VAR_KAFKA_INIT_NODE_NAME = "NODE_NAME";
    private static final String ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS = "EXTERNAL_ADDRESS";
    /** {@code TRUE} when the CLIENT listener (PLAIN transport) should be enabled*/
    private static final String ENV_VAR_KAFKA_CLIENT_ENABLED = "KAFKA_CLIENT_ENABLED";
    /** The authentication to configure for the CLIENT listener (PLAIN transport). */
    private static final String ENV_VAR_KAFKA_CLIENT_AUTHENTICATION = "KAFKA_CLIENT_AUTHENTICATION";
    /** {@code TRUE} when the CLIENTTLS listener (TLS transport) should be enabled*/
    private static final String ENV_VAR_KAFKA_CLIENTTLS_ENABLED = "KAFKA_CLIENTTLS_ENABLED";
    /** The authentication to configure for the CLIENTTLS listener (TLS transport) . */
    private static final String ENV_VAR_KAFKA_CLIENTTLS_AUTHENTICATION = "KAFKA_CLIENTTLS_AUTHENTICATION";
    protected static final String ENV_VAR_KAFKA_EXTERNAL_ENABLED = "KAFKA_EXTERNAL_ENABLED";
    protected static final String ENV_VAR_KAFKA_EXTERNAL_ADDRESSES = "KAFKA_EXTERNAL_ADDRESSES";
    protected static final String ENV_VAR_KAFKA_EXTERNAL_AUTHENTICATION = "KAFKA_EXTERNAL_AUTHENTICATION";
    private static final String ENV_VAR_KAFKA_AUTHORIZATION_TYPE = "KAFKA_AUTHORIZATION_TYPE";
    private static final String ENV_VAR_KAFKA_AUTHORIZATION_SUPER_USERS = "KAFKA_AUTHORIZATION_SUPER_USERS";
    public static final String ENV_VAR_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static final String ENV_VAR_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_CONFIGURATION = "KAFKA_CONFIGURATION";
    public static final String ENV_VAR_TLS_SIDECAR_LOG_LEVEL = "TLS_SIDECAR_LOG_LEVEL";

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
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-bootstrap";
    private static final String HEADLESS_SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-brokers";

    // Suffixes for secrets with certificates
    private static final String SECRET_BROKERS_SUFFIX = NAME_SUFFIX + "-brokers";
    private static final String SECRET_CLUSTER_PUBLIC_KEY_SUFFIX = "-cert";
    private static final String SECRET_CLIENTS_CA_SUFFIX = "-clients-ca";
    private static final String SECRET_CLIENTS_PUBLIC_KEY_SUFFIX = "-clients-ca-cert";

    protected static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";

    // Kafka configuration
    private String zookeeperConnect;
    private Rack rack;
    private String initImage;
    private TlsSidecar tlsSidecar;
    private KafkaListeners listeners;
    private KafkaAuthorization authorization;
    private SortedMap<Integer, String> externalAddresses = new TreeMap<>();

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
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka cluster resources are going to be created
     * @param cluster  overall cluster name
     */
    private KafkaCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = kafkaClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.headlessServiceName = headlessServiceName(cluster);
        this.ancillaryConfigName = metricAndLogConfigsName(cluster);
        this.image = KafkaClusterSpec.DEFAULT_IMAGE;
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

        this.initImage = KafkaClusterSpec.DEFAULT_INIT_IMAGE;
        this.validLoggerFields = getDefaultLogConfig();
    }

    public static String kafkaClusterName(String cluster) {
        return cluster + KafkaCluster.NAME_SUFFIX;
    }

    public static String metricAndLogConfigsName(String cluster) {
        return cluster + KafkaCluster.METRICS_AND_LOG_CONFIG_SUFFIX;
    }

    public static String serviceName(String cluster) {
        return cluster + KafkaCluster.SERVICE_NAME_SUFFIX;
    }

    /**
     * Generates the name of the service used as bootstrap service for external clients
     *
     * @param cluster Name of the cluster
     * @return
     */
    public static String externalBootstrapServiceName(String cluster) {
        return kafkaClusterName(cluster) + "-external-bootstrap";
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
        return cluster + KafkaCluster.HEADLESS_SERVICE_NAME_SUFFIX;
    }

    public static String kafkaPodName(String cluster, int pod) {
        return kafkaClusterName(cluster) + "-" + pod;
    }

    public static String clientsCASecretName(String cluster) {
        return cluster + KafkaCluster.SECRET_CLIENTS_CA_SUFFIX;
    }

    public static String brokersSecretName(String cluster) {
        return cluster + KafkaCluster.SECRET_BROKERS_SUFFIX;
    }

    public static String clientsPublicKeyName(String cluster) {
        return cluster + KafkaCluster.SECRET_CLIENTS_PUBLIC_KEY_SUFFIX;
    }

    public static String clusterPublicKeyName(String cluster) {
        return getClusterCaName(cluster);
    }

    public static KafkaCluster fromCrd(Kafka kafkaAssembly) {
        KafkaCluster result = new KafkaCluster(kafkaAssembly.getMetadata().getNamespace(),
                kafkaAssembly.getMetadata().getName(),
                Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));
        result.setOwnerReference(kafkaAssembly);
        KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
        result.setReplicas(kafkaClusterSpec.getReplicas());
        String image = kafkaClusterSpec.getImage();
        if (image == null) {
            image = KafkaClusterSpec.DEFAULT_IMAGE;
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
            initImage = KafkaClusterSpec.DEFAULT_INIT_IMAGE;
        }
        result.setInitImage(initImage);
        Logging logging = kafkaClusterSpec.getLogging();
        result.setLogging(logging == null ? new InlineLogging() : logging);
        result.setJvmOptions(kafkaClusterSpec.getJvmOptions());
        result.setConfiguration(new KafkaConfiguration(kafkaClusterSpec.getConfig().entrySet()));
        Map<String, Object> metrics = kafkaClusterSpec.getMetrics();
        if (metrics != null && !metrics.isEmpty()) {
            result.setMetricsEnabled(true);
            result.setMetricsConfig(metrics.entrySet());
        }
        result.setStorage(kafkaClusterSpec.getStorage());
        result.setUserAffinity(kafkaClusterSpec.getAffinity());
        result.setResources(kafkaClusterSpec.getResources());
        result.setTolerations(kafkaClusterSpec.getTolerations());

        result.setTlsSidecar(kafkaClusterSpec.getTlsSidecar());

        KafkaListeners listeners = kafkaClusterSpec.getListeners();
        if (listeners != null) {
            if (listeners.getPlain() != null
                && listeners.getPlain().getAuthentication() instanceof KafkaListenerAuthenticationTls) {
                throw new InvalidResourceException("You cannot configure TLS authentication on a plain listener.");
            }
        }
        result.setListeners(listeners);
        result.setAuthorization(kafkaClusterSpec.getAuthorization());

        return result;
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     * @param clusterCa The certificates
     */
    public void generateCertificates(Kafka kafka, ClusterCa clusterCa, ClientsCa clientsCa, String externalBootstrapAddress, Map<Integer, String> externalAddresses) {
        log.debug("Generating certificates");

        try {
            clientsCa.createOrRenew(kafka.getMetadata().getNamespace(), kafka.getMetadata().getName(),
                    labels.toMap(), createOwnerReference());
            brokerCerts = clusterCa.generateBrokerCerts(kafka, externalBootstrapAddress, externalAddresses);
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
        return createService("ClusterIP", getServicePorts(), getPrometheusAnnotations());
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

            List<ServicePort> ports = Collections.singletonList(createServicePort(EXTERNAL_PORT_NAME, EXTERNAL_PORT, EXTERNAL_PORT, "TCP"));

            return createService(externalBootstrapServiceName, getExternalServiceType(), ports, getLabelsWithName(externalBootstrapServiceName), getSelectorLabels(), Collections.emptyMap());
        }

        return null;
    }

    /**
     * Generates service for pod {@pod}. This service is used for exposing it externally.
     *
     * @param pod   Number of the pod for which this service should be generated
     * @return The generated Service
     */
    public Service generateExternalService(int pod) {
        if (isExposed()) {
            String perPodServiceName = externalServiceName(cluster, pod);

            List<ServicePort> ports = new ArrayList<>(1);
            ports.add(createServicePort(EXTERNAL_PORT_NAME, EXTERNAL_PORT, EXTERNAL_PORT, "TCP"));

            Labels selector = Labels.fromMap(getSelectorLabels()).withStatefulSetPod(kafkaPodName(cluster, pod));

            return createService(perPodServiceName, getExternalServiceType(), ports, getLabelsWithName(perPodServiceName), selector.toMap(), Collections.emptyMap());
        }

        return null;
    }

    /**
     * Generates route for pod {@pod}. This route is used for exposing it externally using OpenShift Routes.
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
                        .withLabels(getLabelsWithName(perPodServiceName))
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
                        .withLabels(getLabelsWithName(serviceName))
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

            return route;

        }

        return null;
    }

    /**
     * Generates a headless Service according to configured defaults
     * @return The generated Service
     */
    public Service generateHeadlessService() {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        return createHeadlessService(getHeadlessServicePorts(), annotations);
    }

    /**
     * Generates a StatefulSet according to configured defaults
     * @param isOpenShift True iff this operator is operating within OpenShift.
     * @return The generate StatefulSet
     */
    public StatefulSet generateStatefulSet(boolean isOpenShift) {
        return createStatefulSet(
                getVolumes(),
                getVolumeClaims(),
                getVolumeMounts(),
                getMergedAffinity(),
                getInitContainers(),
                getContainers(),
                isOpenShift);
    }

    /**
     * Generate the Secret containing CA self-signed certificate for TLS communication.
     * It also contains the private key-certificate (signed by cluster CA) for each brokers as well as for communicating
     * with Zookeeper as well
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

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();
        if (storage instanceof EphemeralStorage) {
            volumeList.add(createEmptyDirVolume(VOLUME_NAME));
        }

        if (rack != null || isExposedWithNodePort()) {
            volumeList.add(createEmptyDirVolume(INIT_VOLUME_NAME));
        }
        volumeList.add(createSecretVolume(CLUSTER_CA_CERTS_VOLUME, AbstractModel.getClusterCaName(cluster)));
        volumeList.add(createSecretVolume(BROKER_CERTS_VOLUME, KafkaCluster.brokersSecretName(cluster)));
        volumeList.add(createSecretVolume(CLIENT_CA_CERTS_VOLUME, KafkaCluster.clientsPublicKeyName(cluster)));
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));

        return volumeList;
    }

    private List<PersistentVolumeClaim> getVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>();
        if (storage instanceof PersistentClaimStorage) {
            pvcList.add(createPersistentVolumeClaim(VOLUME_NAME));
        }
        return pvcList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(createVolumeMount(VOLUME_NAME, mountPath));

        volumeMountList.add(createVolumeMount(CLUSTER_CA_CERTS_VOLUME, CLUSTER_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(BROKER_CERTS_VOLUME, BROKER_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(CLIENT_CA_CERTS_VOLUME, CLIENT_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));

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
    protected List<Container> getInitContainers() {
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
            }

            Container initContainer = new ContainerBuilder()
                    .withName(INIT_NAME)
                    .withImage(initImage)
                    .withResources(resources)
                    .withEnv(varList)
                    .withVolumeMounts(createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT))
                    .build();

            initContainers.add(initContainer);
        }

        return initContainers;
    }

    @Override
    protected List<Container> getContainers() {

        List<Container> containers = new ArrayList<>();

        Container container = new ContainerBuilder()
                .withName(KAFKA_NAME)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withVolumeMounts(getVolumeMounts())
                .withPorts(getContainerPortList())
                .withLivenessProbe(createTcpSocketProbe(REPLICATION_PORT, livenessInitialDelay, livenessTimeout))
                .withReadinessProbe(createTcpSocketProbe(REPLICATION_PORT, readinessInitialDelay, readinessTimeout))
                .withResources(resources(getResources()))
                .build();

        String tlsSidecarImage = (tlsSidecar != null && tlsSidecar.getImage() != null) ?
                tlsSidecar.getImage() : KafkaClusterSpec.DEFAULT_TLS_SIDECAR_IMAGE;

        Resources tlsSidecarResources = (tlsSidecar != null) ? tlsSidecar.getResources() : null;

        TlsSidecarLogLevel tlsSidecarLogLevel = (tlsSidecar != null) ? tlsSidecar.getLogLevel() : TlsSidecarLogLevel.NOTICE;

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withResources(resources(tlsSidecarResources))
                .withEnv(asList(buildEnvVar(ENV_VAR_KAFKA_ZOOKEEPER_CONNECT, zookeeperConnect),
                        buildEnvVar(ENV_VAR_TLS_SIDECAR_LOG_LEVEL, tlsSidecarLogLevel.toValue())))
                .withVolumeMounts(createVolumeMount(BROKER_CERTS_VOLUME, TLS_SIDECAR_KAFKA_CERTS_VOLUME_MOUNT),
                        createVolumeMount(CLUSTER_CA_CERTS_VOLUME, TLS_SIDECAR_CLUSTER_CA_CERTS_VOLUME_MOUNT))
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
        varList.add(buildEnvVar(ENV_VAR_KAFKA_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
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
                varList.add(buildEnvVar(ENV_VAR_KAFKA_EXTERNAL_ADDRESSES, String.join(" ", externalAddresses.values())));

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

    public ServiceAccount generateInitContainerServiceAccount() {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(initContainerServiceAccountName(cluster))
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
            .build();
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
    public ClusterRoleBindingOperator.ClusterRoleBinding generateClusterRoleBinding(String assemblyNamespace) {
        if (rack != null || isExposedWithNodePort()) {
            return new ClusterRoleBindingOperator.ClusterRoleBinding(
                    initContainerClusterRoleBindingName(namespace, cluster),
                    "strimzi-kafka-broker",
                    assemblyNamespace, initContainerServiceAccountName(cluster),
                    createOwnerReference());
        } else {
            return null;
        }
    }

    public static String policyName(String cluster) {
        return cluster + NETWORK_POLICY_KEY_SUFFIX + NAME_SUFFIX;
    }

    public NetworkPolicy generateNetworkPolicy() {
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

        // Free access to 9092, 9093 and 9094 ports
        NetworkPolicyPort plainPort = new NetworkPolicyPort();
        plainPort.setPort(new IntOrString(CLIENT_PORT));

        NetworkPolicyIngressRule plainRule = new NetworkPolicyIngressRuleBuilder()
                .withPorts(plainPort)
                .withFrom()
                .build();

        NetworkPolicyPort tlsPort = new NetworkPolicyPort();
        tlsPort.setPort(new IntOrString(CLIENT_TLS_PORT));

        NetworkPolicyIngressRule tlsRule = new NetworkPolicyIngressRuleBuilder()
                .withPorts(tlsPort)
                .withFrom()
                .build();

        NetworkPolicyPort externalPort = new NetworkPolicyPort();
        externalPort.setPort(new IntOrString(EXTERNAL_PORT));

        NetworkPolicyIngressRule externalRule = new NetworkPolicyIngressRuleBuilder()
                .withPorts(externalPort)
                .withFrom()
                .build();

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(policyName(cluster))
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withPodSelector(labelSelector)
                    .withIngress(replicationRule, plainRule, tlsRule, externalRule)
                .endSpec()
                .build();

        log.trace("Created network policy {}", networkPolicy);
        return networkPolicy;
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
     * @param externalAddresses Map with external addresses
     */
    public void setExternalAddresses(SortedMap<Integer, String> externalAddresses) {
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
}