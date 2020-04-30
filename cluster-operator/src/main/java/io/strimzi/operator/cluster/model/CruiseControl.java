/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.template.CruiseControlTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.cruisecontrol.Capacity;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.VolumeUtils.createConfigMapVolume;
import static io.strimzi.operator.cluster.model.VolumeUtils.createSecretVolume;
import static io.strimzi.operator.cluster.model.VolumeUtils.createVolumeMount;

public class CruiseControl extends AbstractModel {
    protected static final String APPLICATION_NAME = "cruise-control";

    public static final String CRUISE_CONTROL_METRIC_REPORTER = "com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter";

    protected static final String CRUISE_CONTROL_CONTAINER_NAME = "cruise-control";
    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_CC_CERTS_VOLUME_NAME = "cc-certs";
    protected static final String TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/cc-certs/";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/cluster-ca-certs/";
    protected static final String LOG_AND_METRICS_CONFIG_VOLUME_NAME = "cruise-control-logging";
    protected static final String LOG_AND_METRICS_CONFIG_VOLUME_MOUNT = "/opt/cruise-control/custom-config/";
    private static final String NAME_SUFFIX = "-cruise-control";

    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "logging";

    private String zookeeperConnect;

    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 1;

    // Default probe settings (liveness and readiness) for health checks
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder()
            .withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT)
            .build();

    private TlsSidecar tlsSidecar;
    private String tlsSidecarImage;
    private String minInsyncReplicas = "1";
    private Double brokerDiskMiBCapacity;
    private int brokerCpuUtilizationCapacity;
    private Double brokerInboundNetworkKiBPerSecondCapacity;
    private Double brokerOuboundNetworkKiBPerSecondCapacity;

    public static final String REST_API_PORT_NAME = "rest-api";
    public static final int REST_API_PORT = 9090;
    protected static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9091;
    public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";

    // Cruise Control configuration keys (EnvVariables)
    protected static final String ENV_VAR_CRUISE_CONTROL_CONFIGURATION = "CRUISE_CONTROL_CONFIGURATION";
    protected static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    protected static final String ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    protected static final String ENV_VAR_MIN_INSYNC_REPLICAS = "MIN_INSYNC_REPLICAS";
    protected static final String ENV_VAR_BROKER_DISK_MIB_CAPACITY = "BROKER_DISK_MIB_CAPACITY";
    protected static final String ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY = "BROKER_CPU_UTILIZATION_CAPACITY";
    protected static final String ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY = "BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY";
    protected static final String ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY = "BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY";

    // Templates
    protected List<ContainerEnvVar> templateCruiseControlContainerEnvVars;
    protected List<ContainerEnvVar> templateTlsSidecarContainerEnvVars;

    protected SecurityContext templateCruiseControlContainerSecurityContext;
    protected SecurityContext templateTlsSidecarContainerSecurityContext;

    private boolean isDeployed;

    /**
     * Constructor
     *
     * @param resource  Kubernetes/OpenShift resource with metadata containing the namespace and cluster name
     */
    protected CruiseControl(HasMetadata resource) {
        super(resource, APPLICATION_NAME);
        this.name = CruiseControlResources.deploymentName(cluster);
        this.serviceName = CruiseControlResources.serviceName(cluster);
        this.ancillaryConfigName = metricAndLogConfigsName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/kafkacruisecontrol/state";
        this.livenessPath = "/kafkacruisecontrol/state";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.mountPath = "/var/lib/kafka";
        this.logAndMetricsConfigVolumeName = LOG_AND_METRICS_CONFIG_VOLUME_NAME;
        this.logAndMetricsConfigMountPath = LOG_AND_METRICS_CONFIG_VOLUME_MOUNT;

        this.zookeeperConnect = defaultZookeeperConnect(cluster);
    }

    public static String metricAndLogConfigsName(String cluster) {
        return CruiseControlResources.logAndMetricsConfigMapName(cluster);
    }

    protected void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return ZookeeperCluster.serviceName(cluster) + ":" + EntityOperatorSpec.DEFAULT_ZOOKEEPER_PORT;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.serviceName(cluster) + ":" + DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    public static CruiseControl fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        CruiseControl cruiseControl = null;
        CruiseControlSpec spec  = kafkaAssembly.getSpec().getCruiseControl();
        KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();

        if (spec != null) {
            cruiseControl = new CruiseControl(kafkaAssembly);
            cruiseControl.isDeployed = true;

            cruiseControl.setReplicas(DEFAULT_REPLICAS);
            String image = spec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            cruiseControl.setImage(image);

            TlsSidecar tlsSidecar = spec.getTlsSidecar();
            if (tlsSidecar == null) {
                tlsSidecar = new TlsSidecar();
            }

            String tlsSideCarImage = tlsSidecar.getImage();
            if (tlsSideCarImage == null) {
                tlsSideCarImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }

            tlsSidecar.setImage(tlsSideCarImage);
            cruiseControl.tlsSidecarImage = tlsSideCarImage;
            cruiseControl.setTlsSidecar(tlsSidecar);

            cruiseControl = updateConfiguration(spec, cruiseControl);

            KafkaConfiguration configuration = new KafkaConfiguration(kafkaClusterSpec.getConfig().entrySet());
            if (configuration.getConfigOption(MIN_INSYNC_REPLICAS) != null) {
                cruiseControl.minInsyncReplicas = configuration.getConfigOption(MIN_INSYNC_REPLICAS);
            }

            Capacity capacity = new Capacity(kafkaAssembly.getSpec());
            cruiseControl.brokerDiskMiBCapacity = capacity.getDiskMiB();
            cruiseControl.brokerCpuUtilizationCapacity = capacity.getCpuUtilization();
            cruiseControl.brokerInboundNetworkKiBPerSecondCapacity = capacity.getInboundNetworkKiBPerSecond();
            cruiseControl.brokerOuboundNetworkKiBPerSecondCapacity = capacity.getOutboundNetworkKiBPerSecond();

            if (spec.getReadinessProbe() != null) {
                cruiseControl.setReadinessProbe(spec.getReadinessProbe());
            }

            if (spec.getLivenessProbe() != null) {
                cruiseControl.setLivenessProbe(spec.getLivenessProbe());
            }

            Logging logging = spec.getLogging();
            cruiseControl.setLogging(logging == null ? new InlineLogging() : logging);

            cruiseControl.setGcLoggingEnabled(spec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : spec.getJvmOptions().isGcLoggingEnabled());
            cruiseControl.setJvmOptions(spec.getJvmOptions());

            cruiseControl.setUserAffinity(affinity(spec));
            cruiseControl.setResources(spec.getResources());
            cruiseControl.setTolerations(tolerations(spec));
            cruiseControl.setOwnerReference(kafkaAssembly);
            cruiseControl = updateTemplate(spec, cruiseControl);
        }

        return cruiseControl;
    }

    public static CruiseControl updateConfiguration(CruiseControlSpec spec, CruiseControl cruiseControl) {
        CruiseControlConfiguration configuration = new CruiseControlConfiguration(spec.getConfig().entrySet());
        for (Map.Entry<String, String> entry : configuration.getCruiseControlDefaultPropertiesMap().entrySet()) {
            if (configuration.getConfigOption(entry.getKey()) == null) {
                configuration.setConfigOption(entry.getKey(), entry.getValue());
            }
        }
        cruiseControl.setConfiguration(configuration);
        return cruiseControl;
    }

    public static CruiseControl updateTemplate(CruiseControlSpec spec, CruiseControl cruiseControl) {
        if (spec.getTemplate() != null) {
            CruiseControlTemplate template = spec.getTemplate();

            ModelUtils.parsePodTemplate(cruiseControl, template.getPod());

            if (template.getDeployment() != null && template.getDeployment().getMetadata() != null) {
                cruiseControl.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                cruiseControl.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
            }

            if (template.getApiService() != null && template.getApiService().getMetadata() != null) {
                cruiseControl.templateServiceLabels = template.getApiService().getMetadata().getLabels();
                cruiseControl.templateServiceAnnotations = template.getApiService().getMetadata().getAnnotations();
            }

            if (template.getCruiseControlContainer() != null && template.getCruiseControlContainer().getEnv() != null) {
                cruiseControl.templateCruiseControlContainerEnvVars = template.getCruiseControlContainer().getEnv();
            }

            if (template.getTlsSidecarContainer() != null && template.getTlsSidecarContainer().getEnv() != null) {
                cruiseControl.templateTlsSidecarContainerEnvVars = template.getTlsSidecarContainer().getEnv();
            }

            if (template.getCruiseControlContainer() != null && template.getCruiseControlContainer().getSecurityContext() != null) {
                cruiseControl.templateCruiseControlContainerSecurityContext = template.getCruiseControlContainer().getSecurityContext();
            }

            if (template.getTlsSidecarContainer() != null && template.getTlsSidecarContainer().getSecurityContext() != null) {
                cruiseControl.templateTlsSidecarContainerSecurityContext = template.getTlsSidecarContainer().getSecurityContext();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(cruiseControl, template.getPodDisruptionBudget());
        }
        return cruiseControl;
    }

    static List<Toleration> tolerations(CruiseControlSpec spec) {
        if (spec.getTemplate() != null
                && spec.getTemplate().getPod() != null
                && spec.getTemplate().getPod().getTolerations() != null) {
            return spec.getTemplate().getPod().getTolerations();
        }
        return null;
    }

    static Affinity affinity(CruiseControlSpec spec) {
        if (spec.getTemplate() != null
                && spec.getTemplate().getPod() != null
                && spec.getTemplate().getPod().getAffinity() != null) {
            return spec.getTemplate().getPod().getAffinity();
        }
        return null;
    }

    public static String cruiseControlName(String cluster) {
        return CruiseControlResources.deploymentName(cluster);
    }

    public static String cruiseControlServiceName(String cluster) {
        return CruiseControlResources.serviceName(cluster);
    }

    public Service generateService() {
        if (!isDeployed()) {
            return null;
        }

        List<ServicePort> ports = Collections.singletonList(createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP"));
        return createService("ClusterIP", ports, templateServiceAnnotations);
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);

        portList.add(createContainerPort(REST_API_PORT_NAME, REST_API_PORT, "TCP"));

        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        return Arrays.asList(createSecretVolume(TLS_SIDECAR_CC_CERTS_VOLUME_NAME, CruiseControl.secretName(cluster), isOpenShift),
                createSecretVolume(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift),
                createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
    }

    protected List<VolumeMount> getVolumeMounts() {
        return Arrays.asList(createVolumeMount(CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_NAME, CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT),
                createVolumeMount(CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT),
                createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
    }

    public Deployment generateDeployment(boolean isOpenShift, Map<String, String> annotations, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (!isDeployed()) {
            return null;
        }

        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("RollingUpdate")
                .withRollingUpdate(new RollingUpdateDeploymentBuilder()
                        .withMaxSurge(new IntOrString(1))
                        .withMaxUnavailable(new IntOrString(0))
                        .build())
                .build();

        return createDeployment(
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                getVolumes(isOpenShift),
                imagePullSecrets);
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>();
        Container container = new ContainerBuilder()
                .withName(CRUISE_CONTROL_CONTAINER_NAME)
                .withImage(getImage())
                .withCommand("/opt/cruise-control/cruise_control_run.sh")
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ModelUtils.createHttpProbe(livenessPath, REST_API_PORT_NAME, livenessProbeOptions))
                .withReadinessProbe(ModelUtils.createHttpProbe(readinessPath, REST_API_PORT_NAME, readinessProbeOptions))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateCruiseControlContainerSecurityContext)
                .build();

        String tlsSidecarImage = this.tlsSidecarImage;
        if (tlsSidecar != null && tlsSidecar.getImage() != null) {
            tlsSidecarImage = tlsSidecar.getImage();
        }

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withCommand("/opt/stunnel/cruise_control_stunnel_run.sh")
                .withLivenessProbe(ModelUtils.tlsSidecarLivenessProbe(tlsSidecar))
                .withReadinessProbe(ModelUtils.tlsSidecarReadinessProbe(tlsSidecar))
                .withResources(tlsSidecar != null ? tlsSidecar.getResources() : null)
                .withEnv(getTlsSidecarEnvVars())
                .withVolumeMounts(createVolumeMount(TLS_SIDECAR_CC_CERTS_VOLUME_NAME, TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT),
                        createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT))
                .withLifecycle(new LifecycleBuilder().withNewPreStop().withNewExec()
                        .withCommand("/opt/stunnel/cruise_control_stunnel_pre_stop.sh",
                                String.valueOf(templateTerminationGracePeriodSeconds))
                        .endExec().endPreStop().build())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, tlsSidecarImage))
                .withSecurityContext(templateTlsSidecarContainerSecurityContext)
                .build();

        containers.add(container);
        containers.add(tlsSidecarContainer);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS, String.valueOf(defaultBootstrapServers(cluster))));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        varList.add(buildEnvVar(ENV_VAR_MIN_INSYNC_REPLICAS, String.valueOf(minInsyncReplicas)));

        varList.add(buildEnvVar(ENV_VAR_BROKER_DISK_MIB_CAPACITY, String.valueOf(brokerDiskMiBCapacity)));
        varList.add(buildEnvVar(ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY, String.valueOf(brokerCpuUtilizationCapacity)));
        varList.add(buildEnvVar(ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY, String.valueOf(brokerInboundNetworkKiBPerSecondCapacity)));
        varList.add(buildEnvVar(ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY,  String.valueOf(brokerOuboundNetworkKiBPerSecondCapacity)));

        heapOptions(varList, 1.0, 0L);
        jvmPerformanceOptions(varList);

        if (configuration != null && !configuration.getConfiguration().isEmpty()) {
            varList.add(buildEnvVar(ENV_VAR_CRUISE_CONTROL_CONFIGURATION, configuration.getConfiguration()));
        }
        addContainerEnvsToExistingEnvs(varList, templateCruiseControlContainerEnvVars);

        return varList;
    }

    /**
     * Generates the PodDisruptionBudget.
     *
     * @return The PodDisruptionBudget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "cruiseControlDefaultLoggingProperties";
    }

    @Override
    protected String getServiceAccountName() {
        return CruiseControlResources.serviceAccountName(cluster);
    }

    /**
     * Get the name of the Cruise Control service account given the name of the {@code cluster}.
     * @param cluster The cluster name
     * @return The name of the Cruise Control service account.
     */
    public static String cruiseControlServiceAccountName(String cluster) {
        return CruiseControlResources.serviceAccountName(cluster);
    }

    protected List<EnvVar> getTlsSidecarEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ModelUtils.tlsSidecarLogEnvVar(tlsSidecar));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));

        addContainerEnvsToExistingEnvs(varList, templateTlsSidecarContainerEnvVars);

        return varList;
    }

    /**
     * Generates the name of the Cruise Control secret with certificates for connecting to Kafka brokers
     *
     * @param kafkaCluster  Name of the Kafka Custom Resource
     * @return  Name of the Cruise Control secret
     */
    public static String secretName(String kafkaCluster) {
        return CruiseControlResources.secretName(kafkaCluster);
    }

    /**
     * Returns whether the Cruise Control is enabled or not
     *
     * @return True if Cruise Control is enabled. False otherwise.
     */
    private boolean isDeployed() {
        return isDeployed;
    }

    /**
     * Generate the Secret containing the Cruise Control certificate signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka and Zookeeper.
     * It also contains the related Cruise Control private key.
     *
     * @param clusterCa The cluster CA.
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     * @return The generated Secret.
     */
    public Secret generateSecret(ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        if (!isDeployed()) {
            return null;
        }
        Secret secret = clusterCa.cruiseControlSecret();
        return ModelUtils.buildSecret(clusterCa, secret, namespace, CruiseControl.secretName(cluster), name, "cruise-control", labels, createOwnerReference(), isMaintenanceTimeWindowsSatisfied);
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
        List<NetworkPolicyIngressRule> rules = new ArrayList<>(1);

        // CO can access the REST API
        NetworkPolicyIngressRule restApiRule = new NetworkPolicyIngressRuleBuilder()
                .addNewPort()
                    .withNewPort(REST_API_PORT)
                .endPort()
                .build();

        if (namespaceAndPodSelectorNetworkPolicySupported) {
            NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                    .withNewPodSelector() // cluster operator
                        .addToMatchLabels(Labels.STRIMZI_KIND_LABEL, "cluster-operator")
                    .endPodSelector()
                    .withNewNamespaceSelector()
                    .endNamespaceSelector()
                    .build();
            restApiRule.setFrom(Collections.singletonList(clusterOperatorPeer));
        }

        rules.add(restApiRule);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(policyName(cluster))
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withNewPodSelector()
                        .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, cruiseControlName(cluster))
                    .endPodSelector()
                .withIngress(rules)
                .endSpec()
                .build();

        log.trace("Created network policy {}", networkPolicy);
        return networkPolicy;
    }
}
