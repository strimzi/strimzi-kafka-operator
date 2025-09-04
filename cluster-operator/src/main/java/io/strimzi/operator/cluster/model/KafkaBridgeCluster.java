/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeAdminClientSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeTemplate;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Rack;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.InternalServiceTemplate;
import io.strimzi.api.kafka.model.common.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka Bridge model class
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class KafkaBridgeCluster extends AbstractModel implements SupportsLogging, SupportsMetrics {
    /**
     * Default Strimzi Metrics Reporter allow list.
     * Check example dashboards compatibility in case of changes to existing regexes.
     */
    private static final List<String> DEFAULT_METRICS_ALLOW_LIST = List.of(
            "kafka_consumer_consumer_metrics.*",
            "kafka_producer_kafka_metrics_count_count",
            "kafka_producer_producer_metrics.*"
    );

    /**
     * HTTP port configuration
     */
    public static final int DEFAULT_REST_API_PORT = 8080;

    /* test */ static final String COMPONENT_TYPE = "kafka-bridge";
    protected static final String REST_API_PORT_NAME = "rest-api";
    protected static final String TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/strimzi/bridge-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT = "/opt/strimzi/bridge-password/";
    protected static final String ENV_VAR_KAFKA_INIT_INIT_FOLDER_KEY = "INIT_FOLDER";
    private static final String KAFKA_BRIDGE_CONFIG_VOLUME_NAME = "kafka-bridge-configurations";

    // Cluster Operator environment variables for custom discovery labels and annotations
    protected static final String CO_ENV_VAR_CUSTOM_SERVICE_LABELS = "STRIMZI_CUSTOM_KAFKA_BRIDGE_SERVICE_LABELS";
    protected static final String CO_ENV_VAR_CUSTOM_SERVICE_ANNOTATIONS = "STRIMZI_CUSTOM_KAFKA_BRIDGE_SERVICE_ANNOTATIONS";

    // Kafka Bridge configuration keys (EnvVariables)
    protected static final String ENV_VAR_PREFIX = "KAFKA_BRIDGE_";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS = "KAFKA_BRIDGE_TRUSTED_CERTS";
    protected static final String OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/strimzi/oauth-certs/";
    protected static final String OAUTH_SECRETS_BASE_VOLUME_MOUNT = "/opt/strimzi/oauth/";
    protected static final String KAFKA_BRIDGE_CONFIG_VOLUME_MOUNT = "/opt/strimzi/custom-config/";
    protected static final String CO_ENV_VAR_CUSTOM_BRIDGE_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_BRIDGE_LABELS";
    protected static final String INIT_VOLUME_MOUNT = "/opt/strimzi/init";

    /**
     * Key under which the bridge configuration is stored in ConfigMap
     */
    public static final String BRIDGE_CONFIGURATION_FILENAME = "application.properties";

    private int replicas;
    private ClientTls tls;
    private KafkaClientAuthentication authentication;
    private KafkaBridgeHttpConfig http;
    private String bootstrapServers;
    private KafkaBridgeAdminClientSpec kafkaBridgeAdminClient;
    private KafkaBridgeConsumerSpec kafkaBridgeConsumer;
    private KafkaBridgeProducerSpec kafkaBridgeProducer;
    private boolean isLegacyMetricsConfigEnabled = false;
    private LoggingModel logging;
    private MetricsModel metrics;

    // Templates
    private PodDisruptionBudgetTemplate templatePodDisruptionBudget;
    private ResourceTemplate templateInitClusterRoleBinding;
    private DeploymentTemplate templateDeployment;
    private PodTemplate templatePod;
    private InternalServiceTemplate templateService;
    private ContainerTemplate templateInitContainer;

    private Tracing tracing;

    @SuppressFBWarnings({"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"}) // This field is initialized in the fromCrd method
    private Rack rack;

    private String initImage;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_BRIDGE_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    private KafkaBridgeCluster(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, KafkaBridgeResources.componentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);
    }

    /**
     * Create the KafkaBridge model instance from the KafkaBridge custom resource
     *
     * @param reconciliation Reconciliation marker
     * @param kafkaBridge    KafkaBridge custom resource
     * @param sharedEnvironmentProvider Shared environment provider
     * @return KafkaBridgeCluster instance
     */
    @SuppressWarnings({"checkstyle:NPathComplexity", "deprecation"})
    public static KafkaBridgeCluster fromCrd(Reconciliation reconciliation,
                                             KafkaBridge kafkaBridge,
                                             SharedEnvironmentProvider sharedEnvironmentProvider) {
        KafkaBridgeCluster result = new KafkaBridgeCluster(reconciliation, kafkaBridge, sharedEnvironmentProvider);

        KafkaBridgeSpec spec = kafkaBridge.getSpec();
        result.tracing = spec.getTracing();
        result.resources = spec.getResources();
        result.logging = new LoggingModel(spec, result.getClass().getSimpleName());
        result.gcLoggingEnabled = spec.getJvmOptions() == null ? JvmOptions.DEFAULT_GC_LOGGING_ENABLED : spec.getJvmOptions().isGcLoggingEnabled();
        result.jvmOptions = spec.getJvmOptions();
        String image = spec.getImage();
        if (image == null) {
            image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE, "quay.io/strimzi/kafka-bridge:latest");
        }
        result.image = image;
        result.replicas = spec.getReplicas();
        result.setBootstrapServers(spec.getBootstrapServers());
        result.setKafkaAdminClientConfiguration(spec.getAdminClient());
        result.setKafkaConsumerConfiguration(spec.getConsumer());
        result.setKafkaProducerConfiguration(spec.getProducer());
        result.rack = spec.getRack();
        String initImage = spec.getClientRackInitImage();
        if (initImage == null) {
            initImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_INIT_IMAGE, "quay.io/strimzi/operator:latest");
        }
        result.initImage = initImage;

        result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(spec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
        result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(spec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);

        if (spec.getMetricsConfig() instanceof JmxPrometheusExporterMetrics) {
            result.metrics = new JmxPrometheusExporterModel(spec);
        } else if (spec.getMetricsConfig() instanceof StrimziMetricsReporter) {
            result.metrics = new StrimziMetricsReporterModel(spec, DEFAULT_METRICS_ALLOW_LIST);
        } else {
            result.isLegacyMetricsConfigEnabled = spec.getEnableMetrics();
        }

        result.setTls(spec.getTls() != null ? spec.getTls() : null);

        String warnMsg = AuthenticationUtils.validateClientAuthentication(spec.getAuthentication(), spec.getTls() != null);
        if (!warnMsg.isEmpty()) {
            LOGGER.warnCr(reconciliation, warnMsg);
        }
        result.setAuthentication(spec.getAuthentication());

        if (spec.getTemplate() != null) {
            fromCrdTemplate(result, spec);
        }

        if (spec.getHttp() != null) {
            result.setKafkaBridgeHttpConfig(spec.getHttp());
        } else {
            LOGGER.warnCr(reconciliation, "The spec.http property is missing.");
            throw new InvalidResourceException("The HTTP configuration for the bridge is not specified.");
        }

        return result;
    }

    private static void fromCrdTemplate(final KafkaBridgeCluster kafkaBridgeCluster, final KafkaBridgeSpec spec) {
        KafkaBridgeTemplate template = spec.getTemplate();

        kafkaBridgeCluster.templatePodDisruptionBudget = template.getPodDisruptionBudget();
        kafkaBridgeCluster.templateInitClusterRoleBinding = template.getClusterRoleBinding();
        kafkaBridgeCluster.templateDeployment = template.getDeployment();
        kafkaBridgeCluster.templatePod = template.getPod();
        kafkaBridgeCluster.templateService = template.getApiService();
        kafkaBridgeCluster.templateServiceAccount = template.getServiceAccount();
        kafkaBridgeCluster.templateContainer = template.getBridgeContainer();
        kafkaBridgeCluster.templateInitContainer = template.getInitContainer();
    }

    /**
     * @return  Generates and returns the Kubernetes service for the Kafka Bridge
     */
    public Service generateService() {
        int port = DEFAULT_REST_API_PORT;
        if (http != null) {
            port = http.getPort();
        }

        return ServiceUtils.createDiscoverableClusterIpService(
                KafkaBridgeResources.serviceName(cluster),
                namespace,
                labels,
                ownerReference,
                templateService,
                List.of(ServiceUtils.createServicePort(REST_API_PORT_NAME, port, REST_API_PORT_NAME, "TCP")),
                labels.strimziSelectorLabels(),
                ModelUtils.getCustomLabelsOrAnnotations(CO_ENV_VAR_CUSTOM_SERVICE_LABELS),
                Util.mergeLabelsOrAnnotations(getDiscoveryAnnotation(port), ModelUtils.getCustomLabelsOrAnnotations(CO_ENV_VAR_CUSTOM_SERVICE_ANNOTATIONS))
        );
    }

    /**
     * Generates a JSON String with the discovery annotation for the bridge service
     *
     * @return  JSON with discovery annotation
     */
    /*test*/ Map<String, String> getDiscoveryAnnotation(int port) {
        JsonObject discovery = new JsonObject();
        discovery.put("port", port);
        discovery.put("tls", false);
        discovery.put("auth", "none");
        discovery.put("protocol", "http");

        JsonArray anno = new JsonArray();
        anno.add(discovery);

        return Collections.singletonMap(Labels.STRIMZI_DISCOVERY_LABEL, anno.encodePrettily());
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(3);

        int port = DEFAULT_REST_API_PORT;
        if (http != null) {
            port = http.getPort();
        }

        portList.add(ContainerUtils.createContainerPort(REST_API_PORT_NAME, port));

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(2);
        volumeList.add(VolumeUtils.createTempDirVolume(templatePod));
        volumeList.add(VolumeUtils.createConfigMapVolume(KAFKA_BRIDGE_CONFIG_VOLUME_NAME, KafkaBridgeResources.configMapName(cluster)));

        if (tls != null) {
            CertUtils.createTrustedCertificatesVolumes(volumeList, tls.getTrustedCertificates(), isOpenShift);
        }

        if (rack != null) {
            volumeList.add(VolumeUtils.createEmptyDirVolume(INIT_VOLUME_NAME, "1Mi", "Memory"));
        }

        AuthenticationUtils.configurePKCS12ClientAuthenticationVolumes(authentication, volumeList, "oauth-certs", isOpenShift, "", true);

        TemplateUtils.addAdditionalVolumes(templatePod, volumeList);

        return volumeList;
    }

    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(2);

        volumeMountList.add(VolumeUtils.createTempDirVolumeMount());
        volumeMountList.add(VolumeUtils.createVolumeMount(KAFKA_BRIDGE_CONFIG_VOLUME_NAME, KAFKA_BRIDGE_CONFIG_VOLUME_MOUNT));

        if (tls != null) {
            CertUtils.createTrustedCertificatesVolumeMounts(volumeMountList, tls.getTrustedCertificates(), TLS_CERTS_BASE_VOLUME_MOUNT);
        }

        if (rack != null) {
            volumeMountList.add(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT));
        }

        AuthenticationUtils.configurePKCS12ClientAuthenticationVolumeMounts(authentication, volumeMountList, TLS_CERTS_BASE_VOLUME_MOUNT, PASSWORD_VOLUME_MOUNT, OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, "oauth-certs", "", true, OAUTH_SECRETS_BASE_VOLUME_MOUNT);

        TemplateUtils.addAdditionalVolumeMounts(volumeMountList, templateContainer);

        return volumeMountList;
    }

    private List<VolumeMount> getInitContainerVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT));

        TemplateUtils.addAdditionalVolumeMounts(volumeMountList, templateInitContainer);

        return volumeMountList;
    }

    /**
     * Generates the Bridge Kubernetes Deployment
     *
     * @param annotations       Map with annotations
     * @param isOpenShift       Flag indicating if we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy configuration
     * @param imagePullSecrets  List of image pull secrets
     *
     * @return  Generated Kubernetes Deployment resource
     */
    public Deployment generateDeployment(Map<String, String> annotations, boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        return WorkloadUtils.createDeployment(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateDeployment,
                replicas,
                null,
                WorkloadUtils.deploymentStrategy(TemplateUtils.deploymentStrategy(templateDeployment, DeploymentStrategy.ROLLING_UPDATE)),
                WorkloadUtils.createPodTemplateSpec(
                        componentName,
                        labels,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        annotations,
                        getMergedAffinity(),
                        ContainerUtils.listOrNull(createInitContainer(imagePullPolicy)),
                        List.of(createContainer(imagePullPolicy)),
                        getVolumes(isOpenShift),
                        imagePullSecrets,
                        securityProvider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(templatePod))
                )
        );
    }

    private Container createInitContainer(ImagePullPolicy imagePullPolicy) {
        if (rack != null) {
            return ContainerUtils.createContainer(
                    INIT_NAME,
                    initImage,
                    List.of("/opt/strimzi/bin/kafka_init_run.sh"),
                    securityProvider.bridgeInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateInitContainer)),
                    resources,
                    getInitContainerEnvVars(),
                    null,
                    getInitContainerVolumeMounts(),
                    null,
                    null,
                    imagePullPolicy
            );
        } else {
            return null;
        }
    }

    private Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                componentName,
                image,
                List.of("/opt/strimzi/bin/docker/kafka_bridge_run.sh"),
                securityProvider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                getContainerPortList(),
                getVolumeMounts(),
                ProbeUtils.httpProbe(livenessProbeOptions, "/healthy", REST_API_PORT_NAME),
                ProbeUtils.httpProbe(readinessProbeOptions, "/ready", REST_API_PORT_NAME),
                imagePullPolicy
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        JvmOptionUtils.javaOptions(varList, jvmOptions);

        if (tls != null && tls.getTrustedCertificates() != null && !tls.getTrustedCertificates().isEmpty()) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS, CertUtils.trustedCertsEnvVar(tls.getTrustedCertificates())));
        }

        // Client authentication env var is needed to generate oauth truststore certificates in PKCS12 format in container script
        AuthenticationUtils.configureClientAuthenticationEnvVars(authentication, varList, name -> ENV_VAR_PREFIX + name);

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    /**
     * Set the HTTP configuration
     * @param kafkaBridgeHttpConfig HTTP configuration
     */
    protected void setKafkaBridgeHttpConfig(KafkaBridgeHttpConfig kafkaBridgeHttpConfig) {
        this.http = kafkaBridgeHttpConfig;
    }

    /**
     * Set the tls configuration with the certificate to trust
     *
     * @param tls trusted certificates list
     */
    protected void setTls(ClientTls tls) {
        this.tls = tls;
    }

    /**
     * Sets the configured authentication
     *
     * @param authentication Authentication configuration
     */
    protected void setAuthentication(KafkaClientAuthentication authentication) {
        this.authentication = authentication;
    }

    /**
     * Generates the PodDisruptionBudget
     *
     * @return The pod disruption budget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return PodDisruptionBudgetUtils.createPodDisruptionBudget(componentName, namespace, labels, ownerReference, templatePodDisruptionBudget);
    }

    /**
     * Set Kafka AdminClient's configuration
     * @param kafkaBridgeAdminClient configuration
     */
    protected void setKafkaAdminClientConfiguration(KafkaBridgeAdminClientSpec kafkaBridgeAdminClient) {
        this.kafkaBridgeAdminClient = kafkaBridgeAdminClient;
    }

    /**
     * Set Kafka consumer's configuration
     * @param kafkaBridgeConsumer configuration
     */
    protected void setKafkaConsumerConfiguration(KafkaBridgeConsumerSpec kafkaBridgeConsumer) {
        this.kafkaBridgeConsumer = kafkaBridgeConsumer;
    }

    /**
     * Set Kafka producer's configuration
     * @param kafkaBridgeProducer configuration
     */
    protected void setKafkaProducerConfiguration(KafkaBridgeProducerSpec kafkaBridgeProducer) {
        this.kafkaBridgeProducer = kafkaBridgeProducer;
    }

    /**
     * Set Bootstrap servers for connection to cluster
     * @param bootstrapServers bootstrap servers
     */
    protected void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * @return  The HTTP configuration of the Bridge
     */
    public KafkaBridgeHttpConfig getHttp() {
        return this.http;
    }

    /**
     * Returns a combined affinity: Adding the affinity needed for the "kafka-rack" to the user-provided affinity.
     */
    protected Affinity getMergedAffinity() {
        Affinity userAffinity = templatePod != null && templatePod.getAffinity() != null ? templatePod.getAffinity() : new Affinity();
        AffinityBuilder builder = new AffinityBuilder(userAffinity);
        if (rack != null) {
            builder = ModelUtils.populateAffinityBuilderWithRackLabelSelector(builder, userAffinity, rack.getTopologyKey());
        }
        return builder.build();
    }

    /**
     * Creates the ClusterRoleBinding which is used to bind the Kafka Bridge SA to the ClusterRole
     * which permissions the Kafka init container to access K8S nodes (necessary for rack-awareness).
     *
     * @return The cluster role binding.
     */
    public ClusterRoleBinding generateClusterRoleBinding() {
        if (rack != null) {
            Subject subject = new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName(componentName)
                    .withNamespace(namespace)
                    .build();

            RoleRef roleRef = new RoleRefBuilder()
                    .withName("strimzi-kafka-client")
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .build();

            return RbacUtils
                    .createClusterRoleBinding(KafkaBridgeResources.initContainerClusterRoleBindingName(cluster, namespace), roleRef, List.of(subject), labels, templateInitClusterRoleBinding);
        } else {
            return null;
        }
    }

    protected List<EnvVar> getInitContainerEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVarFromFieldRef(ENV_VAR_KAFKA_INIT_NODE_NAME, "spec.nodeName"));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY, rack.getTopologyKey()));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_INIT_INIT_FOLDER_KEY, INIT_VOLUME_MOUNT));

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateInitContainer);

        return varList;
    }

    /**
     * Generates a ConfigMap containing the bridge configuration related to HTTP and Kafka clients.
     * It also generates the metrics and logging configuration. If this operand doesn't support logging
     * or metrics, they will not be set.
     *
     * @param metricsAndLogging     The external CMs with logging and metrics configuration
     *
     * @return The generated ConfigMap
     */
    public ConfigMap generateBridgeConfigMap(MetricsAndLogging metricsAndLogging) {
        // generate the ConfigMap data entries for the metrics and logging configuration
        Map<String, String> data = ConfigMapUtils.generateMetricsAndLogConfigMapData(reconciliation, this, metricsAndLogging);
        // add the ConfigMap data entry for the bridge HTTP and Kafka clients related configuration
        KafkaBridgeConfigurationBuilder builder =
                new KafkaBridgeConfigurationBuilder(reconciliation, cluster, bootstrapServers)
                        .withTracing(tracing)
                        .withTls(tls)
                        .withAuthentication(authentication)
                        .withKafkaAdminClient(kafkaBridgeAdminClient)
                        .withKafkaProducer(kafkaBridgeProducer)
                        .withKafkaConsumer(kafkaBridgeConsumer)
                        .withHttp(http, kafkaBridgeProducer, kafkaBridgeConsumer);

        if ((metrics instanceof JmxPrometheusExporterModel) || isLegacyMetricsConfigEnabled) {
            builder.withJmxPrometheusExporter((JmxPrometheusExporterModel) metrics, isLegacyMetricsConfigEnabled);
        } else if (metrics instanceof StrimziMetricsReporterModel) {
            builder.withStrimziMetricsReporter((StrimziMetricsReporterModel) metrics);
        }

        data.put(BRIDGE_CONFIGURATION_FILENAME, builder.build());

        return ConfigMapUtils
                .createConfigMap(
                        KafkaBridgeResources.configMapName(cluster),
                        namespace,
                        labels,
                        ownerReference,
                        data
                );
    }

    /**
     * @return The number of replicas
     */
    public int getReplicas() {
        return replicas;
    }

    /**
     * @return  Logging Model instance for configuring logging
     */
    public LoggingModel logging()   {
        return logging;
    }

    /**
     * @return Metrics Model instance for configuring Prometheus metrics
     */
    public MetricsModel metrics() {
        return metrics;
    }
}
