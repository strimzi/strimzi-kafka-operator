/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.KafkaBridgeTls;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.template.KafkaBridgeTemplate;
import io.strimzi.api.kafka.model.tracing.Tracing;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaBridgeCluster extends AbstractModel {
    public static final String APPLICATION_NAME = "kafka-bridge";


    // Port configuration
    public static final int DEFAULT_REST_API_PORT = 8080;
    protected static final String REST_API_PORT_NAME = "rest-api";

    protected static final String TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/strimzi/bridge-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT = "/opt/strimzi/bridge-password/";

    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 1;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final boolean DEFAULT_KAFKA_BRIDGE_METRICS_ENABLED = false;

    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder()
            .withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT)
            .withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).build();

    // Cluster Operator environment variables for custom discovery labels and annotations
    protected static final String CO_ENV_VAR_CUSTOM_LABELS = "STRIMZI_CUSTOM_KAFKA_BRIDGE_SERVICE_LABELS";
    protected static final String CO_ENV_VAR_CUSTOM_ANNOTATIONS = "STRIMZI_CUSTOM_KAFKA_BRIDGE_SERVICE_ANNOTATIONS";

    // Kafka Bridge configuration keys (EnvVariables)
    protected static final String ENV_VAR_PREFIX = "KAFKA_BRIDGE_";
    protected static final String ENV_VAR_KAFKA_BRIDGE_METRICS_ENABLED = "KAFKA_BRIDGE_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS = "KAFKA_BRIDGE_BOOTSTRAP_SERVERS";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TLS = "KAFKA_BRIDGE_TLS";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS = "KAFKA_BRIDGE_TRUSTED_CERTS";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_CERT = "KAFKA_BRIDGE_TLS_AUTH_CERT";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_KEY = "KAFKA_BRIDGE_TLS_AUTH_KEY";
    protected static final String ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE = "KAFKA_BRIDGE_SASL_PASSWORD_FILE";
    protected static final String ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME = "KAFKA_BRIDGE_SASL_USERNAME";
    protected static final String ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM = "KAFKA_BRIDGE_SASL_MECHANISM";
    protected static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG = "KAFKA_BRIDGE_OAUTH_CONFIG";
    protected static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET = "KAFKA_BRIDGE_OAUTH_CLIENT_SECRET";
    protected static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN = "KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN";
    protected static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN = "KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN";
    protected static final String OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/strimzi/oauth-certs/";
    protected static final String ENV_VAR_STRIMZI_TRACING = "STRIMZI_TRACING";

    protected static final String ENV_VAR_KAFKA_BRIDGE_PRODUCER_CONFIG = "KAFKA_BRIDGE_PRODUCER_CONFIG";
    protected static final String ENV_VAR_KAFKA_BRIDGE_CONSUMER_CONFIG = "KAFKA_BRIDGE_CONSUMER_CONFIG";
    protected static final String ENV_VAR_KAFKA_BRIDGE_ID = "KAFKA_BRIDGE_ID";

    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_ENABLED = "KAFKA_BRIDGE_AMQP_ENABLED";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_FLOW_CREDIT = "KAFKA_BRIDGE_AMQP_FLOW_CREDIT";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_MODE = "KAFKA_BRIDGE_AMQP_MODE";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_HOST = "KAFKA_BRIDGE_AMQP_HOST";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_PORT = "KAFKA_BRIDGE_AMQP_PORT";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_CERT_DIR = "KAFKA_BRIDGE_AMQP_CERT_DIR";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_MESSAGE_CONNVERTER = "KAFKA_BRIDGE_AMQP_MESSAGE_CONNVERTER";

    protected static final String ENV_VAR_KAFKA_BRIDGE_HTTP_ENABLED = "KAFKA_BRIDGE_HTTP_ENABLED";
    protected static final String ENV_VAR_KAFKA_BRIDGE_HTTP_HOST = "KAFKA_BRIDGE_HTTP_HOST";
    protected static final String ENV_VAR_KAFKA_BRIDGE_HTTP_PORT = "KAFKA_BRIDGE_HTTP_PORT";
    protected static final String ENV_VAR_KAFKA_BRIDGE_CORS_ENABLED = "KAFKA_BRIDGE_CORS_ENABLED";
    protected static final String ENV_VAR_KAFKA_BRIDGE_CORS_ALLOWED_ORIGINS = "KAFKA_BRIDGE_CORS_ALLOWED_ORIGINS";
    protected static final String ENV_VAR_KAFKA_BRIDGE_CORS_ALLOWED_METHODS = "KAFKA_BRIDGE_CORS_ALLOWED_METHODS";

    private KafkaBridgeTls tls;
    private KafkaClientAuthentication authentication;
    private KafkaBridgeHttpConfig http;
    private boolean httpEnabled = false;
    private boolean amqpEnabled = false;
    private String bootstrapServers;
    private KafkaBridgeConsumerSpec kafkaBridgeConsumer;
    private KafkaBridgeProducerSpec kafkaBridgeProducer;
    private List<ContainerEnvVar> templateContainerEnvVars;
    private SecurityContext templateContainerSecurityContext;
    private Tracing tracing;

    /**
     * Constructor
     *
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected KafkaBridgeCluster(HasMetadata resource) {
        super(resource, APPLICATION_NAME);
        this.name = KafkaBridgeResources.deploymentName(cluster);
        this.serviceName = KafkaBridgeResources.serviceName(cluster);
        this.ancillaryConfigMapName = KafkaBridgeResources.metricsAndLogConfigMapName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/ready";
        this.livenessPath = "/healthy";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.isMetricsEnabled = DEFAULT_KAFKA_BRIDGE_METRICS_ENABLED;

        this.mountPath = "/var/lib/bridge";
        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/strimzi/custom-config/";
    }

    public static KafkaBridgeCluster fromCrd(KafkaBridge kafkaBridge, KafkaVersion.Lookup versions) {

        KafkaBridgeCluster kafkaBridgeCluster = new KafkaBridgeCluster(kafkaBridge);

        KafkaBridgeSpec spec = kafkaBridge.getSpec();
        kafkaBridgeCluster.tracing = spec.getTracing();
        kafkaBridgeCluster.setResources(spec.getResources());
        kafkaBridgeCluster.setLogging(spec.getLogging());
        kafkaBridgeCluster.setGcLoggingEnabled(spec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : spec.getJvmOptions().isGcLoggingEnabled());
        if (spec.getJvmOptions() != null) {
            kafkaBridgeCluster.setJavaSystemProperties(spec.getJvmOptions().getJavaSystemProperties());
        }
        String image = spec.getImage();
        if (image == null) {
            image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE, "strimzi/kafka-bridge:latest");
        }
        kafkaBridgeCluster.setImage(image);
        kafkaBridgeCluster.setReplicas(spec.getReplicas());
        kafkaBridgeCluster.setBootstrapServers(spec.getBootstrapServers());
        kafkaBridgeCluster.setKafkaConsumerConfiguration(spec.getConsumer());
        kafkaBridgeCluster.setKafkaProducerConfiguration(spec.getProducer());
        if (kafkaBridge.getSpec().getLivenessProbe() != null) {
            kafkaBridgeCluster.setLivenessProbe(kafkaBridge.getSpec().getLivenessProbe());
        }

        if (kafkaBridge.getSpec().getReadinessProbe() != null) {
            kafkaBridgeCluster.setReadinessProbe(kafkaBridge.getSpec().getReadinessProbe());
        }

        kafkaBridgeCluster.setMetricsEnabled(spec.getEnableMetrics());

        kafkaBridgeCluster.setTls(spec.getTls() != null ? spec.getTls() : null);

        AuthenticationUtils.validateClientAuthentication(spec.getAuthentication(), spec.getTls() != null);
        kafkaBridgeCluster.setAuthentication(spec.getAuthentication());

        if (spec.getTemplate() != null) {
            KafkaBridgeTemplate template = spec.getTemplate();

            if (template.getDeployment() != null && template.getDeployment().getMetadata() != null)  {
                kafkaBridgeCluster.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                kafkaBridgeCluster.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodTemplate(kafkaBridgeCluster, template.getPod());

            if (template.getApiService() != null && template.getApiService().getMetadata() != null)  {
                kafkaBridgeCluster.templateServiceLabels = Util.mergeLabelsOrAnnotations(template.getApiService().getMetadata().getLabels(),
                        ModelUtils.getCustomLabelsOrAnnotations(CO_ENV_VAR_CUSTOM_LABELS));
                kafkaBridgeCluster.templateServiceAnnotations = template.getApiService().getMetadata().getAnnotations();
            }

            if (template.getBridgeContainer() != null && template.getBridgeContainer().getEnv() != null) {
                kafkaBridgeCluster.templateContainerEnvVars = template.getBridgeContainer().getEnv();
            }

            if (template.getBridgeContainer() != null && template.getBridgeContainer().getSecurityContext() != null) {
                kafkaBridgeCluster.templateContainerSecurityContext = template.getBridgeContainer().getSecurityContext();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(kafkaBridgeCluster, template.getPodDisruptionBudget());
        }

        if (spec.getHttp() != null) {
            kafkaBridgeCluster.setHttpEnabled(true);
            kafkaBridgeCluster.setKafkaBridgeHttpConfig(spec.getHttp());
        } else {
            log.warn("No protocol specified.");
            throw new InvalidResourceException("No protocol for communication with Bridge specified. Use HTTP.");
        }
        kafkaBridgeCluster.setOwnerReference(kafkaBridge);

        return kafkaBridgeCluster;
    }
    
    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(3);

        int port = DEFAULT_REST_API_PORT;
        if (http != null) {
            port = http.getPort();
        }

        ports.add(createServicePort(REST_API_PORT_NAME, port, port, "TCP"));

        return createDiscoverableService("ClusterIP", ports, Util.mergeLabelsOrAnnotations(getDiscoveryAnnotation(port), templateServiceAnnotations, ModelUtils.getCustomLabelsOrAnnotations(CO_ENV_VAR_CUSTOM_ANNOTATIONS)));
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

        portList.add(createContainerPort(REST_API_PORT_NAME, port, "TCP"));

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(1);
        volumeList.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));

        if (tls != null) {
            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    // skipping if a volume with same Secret name was already added
                    if (!volumeList.stream().anyMatch(v -> v.getName().equals(certSecretSource.getSecretName()))) {
                        volumeList.add(VolumeUtils.createSecretVolume(certSecretSource.getSecretName(), certSecretSource.getSecretName(), isOpenShift));
                    }
                }
            }
        }

        AuthenticationUtils.configureClientAuthenticationVolumes(authentication, volumeList, "oauth-certs", isOpenShift);

        return volumeList;
    }

    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(1);
        volumeMountList.add(VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));

        if (tls != null) {
            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    // skipping if a volume mount with same Secret name was already added
                    if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(certSecretSource.getSecretName()))) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(certSecretSource.getSecretName(),
                                TLS_CERTS_BASE_VOLUME_MOUNT + certSecretSource.getSecretName()));
                    }
                }
            }
        }

        AuthenticationUtils.configureClientAuthenticationVolumeMounts(authentication, volumeMountList, TLS_CERTS_BASE_VOLUME_MOUNT, PASSWORD_VOLUME_MOUNT, OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, "oauth-certs");

        return volumeMountList;
    }

    public Deployment generateDeployment(Map<String, String> annotations, boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
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
                annotations,
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                getVolumes(isOpenShift),
                imagePullSecrets);
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        List<Container> containers = new ArrayList<>(1);

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withCommand("/opt/strimzi/bin/docker/kafka_bridge_run.sh")
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ModelUtils.createHttpProbe(livenessPath, REST_API_PORT_NAME, livenessProbeOptions))
                .withReadinessProbe(ModelUtils.createHttpProbe(readinessPath, REST_API_PORT_NAME, readinessProbeOptions))
                .withVolumeMounts(getVolumeMounts())
                .withResources(getResources())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateContainerSecurityContext)
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        if (javaSystemProperties != null) {
            varList.add(buildEnvVar(ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, ModelUtils.getJavaSystemPropertiesToString(javaSystemProperties)));
        }

        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS, bootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_CONSUMER_CONFIG, kafkaBridgeConsumer == null ? "" : new KafkaBridgeConsumerConfiguration(kafkaBridgeConsumer.getConfig().entrySet()).getConfiguration()));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_PRODUCER_CONFIG, kafkaBridgeProducer == null ? "" : new KafkaBridgeProducerConfiguration(kafkaBridgeProducer.getConfig().entrySet()).getConfiguration()));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_ID, cluster));

        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_HTTP_ENABLED, String.valueOf(httpEnabled)));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_HTTP_HOST, KafkaBridgeHttpConfig.HTTP_DEFAULT_HOST));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_HTTP_PORT, String.valueOf(http != null ? http.getPort() : KafkaBridgeHttpConfig.HTTP_DEFAULT_PORT)));

        if (http != null && http.getCors() != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_CORS_ENABLED, "true"));

            if (http.getCors().getAllowedOrigins() != null) {
                varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_CORS_ALLOWED_ORIGINS, String.join(",", http.getCors().getAllowedOrigins())));
            }

            if (http.getCors().getAllowedMethods() != null) {
                varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_CORS_ALLOWED_METHODS, String.join(",", http.getCors().getAllowedMethods())));
            }
        } else {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_CORS_ENABLED, "false"));
        }

        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_AMQP_ENABLED, String.valueOf(amqpEnabled)));

        if (tls != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_TLS, "true"));

            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                StringBuilder sb = new StringBuilder();
                boolean separator = false;
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    if (separator) {
                        sb.append(";");
                    }
                    sb.append(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
                    separator = true;
                }
                varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS, sb.toString()));
            }
        }

        AuthenticationUtils.configureClientAuthenticationEnvVars(authentication, varList, name -> ENV_VAR_PREFIX + name);

        if (tracing != null) {
            varList.add(buildEnvVar(ENV_VAR_STRIMZI_TRACING, tracing.getType()));
        }

        // Add shared environment variables used for all containers
        varList.addAll(getSharedEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateContainerEnvVars);

        return varList;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "kafkaBridgeDefaultLoggingProperties";
    }

    @Override
    public String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
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
    protected void setTls(KafkaBridgeTls tls) {
        this.tls = tls;
    }

    /**
     * Sets the configured authentication
     *
     * @param authetication Authentication configuration
     */
    protected void setAuthentication(KafkaClientAuthentication authetication) {
        this.authentication = authetication;
    }

    /**
     * Generates the PodDisruptionBudget
     *
     * @return The pod disruption budget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    @Override
    protected String getServiceAccountName() {
        return KafkaBridgeResources.serviceAccountName(cluster);
    }

    /**
     * Set whether the HTTP is enabled
     * @param httpEnabled HTTP enabled
     */
    protected void setHttpEnabled(boolean httpEnabled) {
        this.httpEnabled = httpEnabled;
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

    public KafkaBridgeHttpConfig getHttp() {
        return this.http;
    }

    /**
     * Transforms properties to log4j2 properties file format and adds property for reloading the config
     * @param properties map with properties
     * @return modified string with monitorInterval
     */
    @Override
    public String createLog4jProperties(OrderedProperties properties) {
        if (!properties.asMap().keySet().contains("monitorInterval")) {
            properties.addPair("monitorInterval", "30");
        }
        return super.createLog4jProperties(properties);
    }
}
