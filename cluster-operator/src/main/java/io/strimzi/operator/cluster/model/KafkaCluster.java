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
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.Sidecar;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.operator.resource.ClusterRoleBindingOperator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;

public class KafkaCluster extends AbstractModel {

    protected static final String INIT_NAME = "kafka-init";
    protected static final String RACK_VOLUME_NAME = "rack-volume";
    protected static final String RACK_VOLUME_MOUNT = "/opt/kafka/rack";
    private static final String ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    private static final String ENV_VAR_KAFKA_INIT_NODE_NAME = "NODE_NAME";

    protected static final int CLIENT_PORT = 9092;
    protected static final String CLIENT_PORT_NAME = "clients";

    protected static final int REPLICATION_PORT = 9091;
    protected static final String REPLICATION_PORT_NAME = "replication";

    protected static final int CLIENT_TLS_PORT = 9093;
    protected static final String CLIENT_TLS_PORT_NAME = "clientstls";

    protected static final String KAFKA_NAME = "kafka";
    protected static final String BROKER_CERTS_VOLUME = "broker-certs";
    protected static final String CLIENT_CA_CERTS_VOLUME = "client-ca-cert";
    protected static final String BROKER_CERTS_VOLUME_MOUNT = "/opt/kafka/broker-certs";
    protected static final String CLIENT_CA_CERTS_VOLUME_MOUNT = "/opt/kafka/client-ca-cert";
    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_VOLUME_MOUNT = "/etc/tls-sidecar/certs/";

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
    private Sidecar tlsSidecar;

    // Configuration defaults
    private static final int DEFAULT_REPLICAS = 3;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_KAFKA_METRICS_ENABLED = false;

    // Kafka configuration keys (EnvVariables)
    public static final String ENV_VAR_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static final String ENV_VAR_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_CONFIGURATION = "KAFKA_CONFIGURATION";
    protected static final String ENV_VAR_KAFKA_LOG_CONFIGURATION = "KAFKA_LOG_CONFIGURATION";

    private CertAndKey clientsCA;
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
        this.image = Kafka.DEFAULT_IMAGE;
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

        this.initImage = Kafka.DEFAULT_INIT_IMAGE;
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
        return getClusterCaName(cluster) + KafkaCluster.SECRET_CLUSTER_PUBLIC_KEY_SUFFIX;
    }

    public static KafkaCluster fromCrd(CertManager certManager, KafkaAssembly kafkaAssembly, List<Secret> secrets) {
        KafkaCluster result = new KafkaCluster(kafkaAssembly.getMetadata().getNamespace(),
                kafkaAssembly.getMetadata().getName(),
                Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));
        Kafka kafka = kafkaAssembly.getSpec().getKafka();
        result.setReplicas(kafka.getReplicas());
        String image = kafka.getImage();
        if (image == null) {
            image = Kafka.DEFAULT_IMAGE;
        }
        result.setImage(image);
        if (kafka.getReadinessProbe() != null) {
            result.setReadinessInitialDelay(kafka.getReadinessProbe().getInitialDelaySeconds());
            result.setReadinessTimeout(kafka.getReadinessProbe().getTimeoutSeconds());
        }
        if (kafka.getLivenessProbe() != null) {
            result.setLivenessInitialDelay(kafka.getLivenessProbe().getInitialDelaySeconds());
            result.setLivenessTimeout(kafka.getLivenessProbe().getTimeoutSeconds());
        }
        result.setRack(kafka.getRack());

        String initImage = kafka.getBrokerRackInitImage();
        if (initImage == null) {
            initImage = Kafka.DEFAULT_INIT_IMAGE;
        }
        result.setInitImage(initImage);
        result.setLogging(kafka.getLogging());
        result.setJvmOptions(kafka.getJvmOptions());
        result.setConfiguration(new KafkaConfiguration(kafka.getConfig().entrySet()));
        Map<String, Object> metrics = kafka.getMetrics();
        if (metrics != null && !metrics.isEmpty()) {
            result.setMetricsEnabled(true);
            result.setMetricsConfig(metrics.entrySet());
        }
        result.setStorage(kafka.getStorage());
        result.setUserAffinity(kafka.getAffinity());
        result.setResources(kafka.getResources());
        result.setTolerations(kafka.getTolerations());

        result.generateCertificates(certManager, secrets);
        result.setTlsSidecar(kafka.getTlsSidecar());

        return result;
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param certManager CertManager instance for handling certificates creation
     * @param secrets The Secrets storing certificates
     */
    public void generateCertificates(CertManager certManager, List<Secret> secrets) {
        log.debug("Generating certificates");

        try {
            Optional<Secret> clusterCAsecret = secrets.stream().filter(s -> s.getMetadata().getName().equals(getClusterCaName(cluster)))
                    .findFirst();
            if (clusterCAsecret.isPresent()) {
                // get the generated CA private key + self-signed certificate for each broker
                clusterCA = new CertAndKey(
                        decodeFromSecret(clusterCAsecret.get(), "cluster-ca.key"),
                        decodeFromSecret(clusterCAsecret.get(), "cluster-ca.crt"));

                // CA private key + self-signed certificate for clients communications
                Optional<Secret> clientsCAsecret = secrets.stream().filter(s -> s.getMetadata().getName().equals(KafkaCluster.clientsCASecretName(cluster))).findFirst();
                if (!clientsCAsecret.isPresent()) {
                    log.debug("Clients CA to generate");
                    File clientsCAkeyFile = File.createTempFile("tls", "clients-ca-key");
                    File clientsCAcertFile = File.createTempFile("tls", "clients-ca-cert");

                    Subject sbj = new Subject();
                    sbj.setOrganizationName("io.strimzi");
                    sbj.setCommonName("kafka-clients-ca");

                    certManager.generateSelfSignedCert(clientsCAkeyFile, clientsCAcertFile, sbj, CERTS_EXPIRATION_DAYS);
                    clientsCA =
                            new CertAndKey(Files.readAllBytes(clientsCAkeyFile.toPath()), Files.readAllBytes(clientsCAcertFile.toPath()));
                    if (!clientsCAkeyFile.delete()) {
                        log.warn("{} cannot be deleted", clientsCAkeyFile.getName());
                    }
                    if (!clientsCAcertFile.delete()) {
                        log.warn("{} cannot be deleted", clientsCAcertFile.getName());
                    }
                } else {
                    log.debug("Clients CA already exists");
                    clientsCA = new CertAndKey(
                            decodeFromSecret(clientsCAsecret.get(), "clients-ca.key"),
                            decodeFromSecret(clientsCAsecret.get(), "clients-ca.crt"));
                }

                // recover or generates the private key + certificate for each broker for internal and clients communication
                Optional<Secret> clusterSecret = secrets.stream().filter(s -> s.getMetadata().getName().equals(KafkaCluster.brokersSecretName(cluster)))
                        .findFirst();

                int replicasInternalSecret = !clusterSecret.isPresent() ? 0 : (clusterSecret.get().getData().size() - 1) / 2;

                log.debug("Internal communication certificates");
                brokerCerts = maybeCopyOrGenerateCerts(certManager, clusterSecret, replicasInternalSecret, clusterCA, KafkaCluster::kafkaPodName);
            } else {
                throw new NoCertificateSecretException("The cluster CA certificate Secret is missing");
            }

        } catch (IOException e) {
            e.printStackTrace();
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
        List<ServicePort> ports = new ArrayList<>(2);
        ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));
        ports.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));
        if (isMetricsEnabled()) {
            ports.add(createServicePort(metricsPortName, metricsPort, metricsPort, "TCP"));
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
        List<ServicePort> ports = new ArrayList<>(2);
        ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));
        ports.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));
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
     * Generate the Secret containing CA private key and self-signed certificate used
     * for signing brokers certificates used for communication with clients
     * @return The generated Secret
     */
    public Secret generateClientsCASecret() {
        Map<String, String> data = new HashMap<>();
        data.put("clients-ca.key", Base64.getEncoder().encodeToString(clientsCA.key()));
        data.put("clients-ca.crt", Base64.getEncoder().encodeToString(clientsCA.cert()));
        return createSecret(KafkaCluster.clientsCASecretName(cluster), data);
    }

    /**
     * Generate the Secret containing just the self-signed CA certificate used
     * for signing client certificates. It is used for the broker truststore.
     * @return The generated Secret
     */
    public Secret generateClientsPublicKeySecret() {
        Map<String, String> data = new HashMap<>();
        data.put("ca.crt", Base64.getEncoder().encodeToString(clientsCA.cert()));
        return createSecret(KafkaCluster.clientsPublicKeyName(cluster), data);
    }

    /**
     * Generate the Secret containing just the self-signed CA certificate used
     * for signing brokers certificates used for communication with clients
     * It's useful for users to extract the certificate itself to put as trusted on the clients
     * @return The generated Secret
     */
    public Secret generateClusterPublicKeySecret() {
        Map<String, String> data = new HashMap<>();
        data.put("ca.crt", Base64.getEncoder().encodeToString(clusterCA.cert()));
        return createSecret(KafkaCluster.clusterPublicKeyName(cluster), data);
    }

    /**
     * Generate the Secret containing CA self-signed certificate for TLS communication.
     * It also contains the private key-certificate (signed by cluster CA) for each brokers as well as for communicating
     * with Zookeeper as well
     * @return The generated Secret
     */
    public Secret generateBrokersSecret() {
        Base64.Encoder encoder = Base64.getEncoder();

        Map<String, String> data = new HashMap<>();
        data.put("cluster-ca.crt", encoder.encodeToString(clusterCA.cert()));

        for (int i = 0; i < replicas; i++) {
            CertAndKey cert = brokerCerts.get(KafkaCluster.kafkaPodName(cluster, i));
            data.put(KafkaCluster.kafkaPodName(cluster, i) + ".key", encoder.encodeToString(cert.key()));
            data.put(KafkaCluster.kafkaPodName(cluster, i) + ".crt", encoder.encodeToString(cert.cert()));
        }
        return createSecret(KafkaCluster.brokersSecretName(cluster), data);
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(3);
        portList.add(createContainerPort(CLIENT_PORT_NAME, CLIENT_PORT, "TCP"));
        portList.add(createContainerPort(REPLICATION_PORT_NAME, REPLICATION_PORT, "TCP"));
        portList.add(createContainerPort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, "TCP"));
        if (isMetricsEnabled) {
            portList.add(createContainerPort(metricsPortName, metricsPort, "TCP"));
        }

        return portList;
    }

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();
        if (storage instanceof EphemeralStorage) {
            volumeList.add(createEmptyDirVolume(VOLUME_NAME));
        }

        if (rack != null) {
            volumeList.add(createEmptyDirVolume(RACK_VOLUME_NAME));
        }
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

        volumeMountList.add(createVolumeMount(BROKER_CERTS_VOLUME, BROKER_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(CLIENT_CA_CERTS_VOLUME, CLIENT_CA_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));

        if (rack != null) {
            volumeMountList.add(createVolumeMount(RACK_VOLUME_NAME, RACK_VOLUME_MOUNT));
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

        if (rack != null) {

            ResourceRequirements resources = new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100m"))
                    .addToRequests("memory", new Quantity("128Mi"))
                    .addToLimits("cpu", new Quantity("1"))
                    .addToLimits("memory", new Quantity("256Mi"))
                    .build();

            List<EnvVar> varList =
                    Arrays.asList(buildEnvVarFromFieldRef(ENV_VAR_KAFKA_INIT_NODE_NAME, "spec.nodeName"),
                            buildEnvVar(ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY, rack.getTopologyKey()));

            Container initContainer = new ContainerBuilder()
                    .withName(INIT_NAME)
                    .withImage(initImage)
                    .withResources(resources)
                    .withEnv(varList)
                    .withVolumeMounts(createVolumeMount(RACK_VOLUME_NAME, RACK_VOLUME_MOUNT))
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
                tlsSidecar.getImage() : Kafka.DEFAULT_TLS_SIDECAR_IMAGE;

        Resources tlsSidecarResources = (tlsSidecar != null) ? tlsSidecar.getResources() : null;

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withResources(resources(tlsSidecarResources))
                .withEnv(singletonList(buildEnvVar(ENV_VAR_KAFKA_ZOOKEEPER_CONNECT, zookeeperConnect)))
                .withVolumeMounts(createVolumeMount(BROKER_CERTS_VOLUME, TLS_SIDECAR_VOLUME_MOUNT))
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
        // A hack to force rolling when the logging config changes
        if (getLogging() != null && getLogging().getCm() != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_LOG_CONFIGURATION, getLogging().getCm().toString()));
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

    protected void setTlsSidecar(Sidecar tlsSidecar) {
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
                    .addToLabels("app", "strimzi")
                    .withNamespace(namespace)
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
     * Get the name of the kafka kafkaResourceName role binding given the name of the {@code kafkaResourceName}.
     */
    public static String initContainerClusterRoleBindingName(String kafkaResourceName) {
        return "strimzi-" + kafkaResourceName + "-kafka-init";
    }

    /**
     * Creates the ClusterRoleBinding which is used to bind the Kafka SA to the ClusterRole
     * which permissions the Kafka init container to access K8S nodes (necessary for rack-awareness).
     */
    public ClusterRoleBindingOperator.ClusterRoleBinding generateClusterRoleBinding(String assemblyNamespace) {
        if (rack != null) {
            return new ClusterRoleBindingOperator.ClusterRoleBinding(
                    initContainerClusterRoleBindingName(cluster),
                    "strimzi-kafka-broker",
                    assemblyNamespace, initContainerServiceAccountName(cluster));
        } else {
            return null;
        }
    }


}