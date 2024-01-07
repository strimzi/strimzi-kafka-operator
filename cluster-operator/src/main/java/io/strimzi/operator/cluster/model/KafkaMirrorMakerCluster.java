/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.ProbeBuilder;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.tracing.JaegerTracing;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerSpec;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerTemplate;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.common.template.DeploymentStrategy.ROLLING_UPDATE;

/**
 * Kafka Mirror Maker 1 model
 */
@SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
public class KafkaMirrorMakerCluster extends AbstractModel implements SupportsMetrics, SupportsLogging {
    protected static final String COMPONENT_TYPE = "kafka-mirror-maker";

    protected static final String TLS_CERTS_VOLUME_MOUNT_CONSUMER = "/opt/kafka/consumer-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT_CONSUMER = "/opt/kafka/consumer-password/";
    protected static final String TLS_CERTS_VOLUME_MOUNT_PRODUCER = "/opt/kafka/producer-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT_PRODUCER = "/opt/kafka/producer-password/";
    protected static final String OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER = "/opt/kafka/consumer-oauth-certs/";
    protected static final String OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER = "/opt/kafka/producer-oauth-certs/";
    private static final String LOG_AND_METRICS_CONFIG_VOLUME_NAME = "kafka-metrics-and-logging";
    private static final String LOG_AND_METRICS_CONFIG_VOLUME_MOUNT = "/opt/kafka/custom-config/";

    // Configuration defaults
    private static final int DEFAULT_HEALTHCHECK_PERIOD = 10; // This is used inside the Mirror Maker agent
    private static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder().withTimeoutSeconds(5).withInitialDelaySeconds(60).build();

    // Kafka Mirror Maker configuration keys (EnvVariables)
    protected static final String ENV_VAR_PREFIX = "KAFKA_MIRRORMAKER_";

    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_METRICS_ENABLED = "KAFKA_MIRRORMAKER_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER = "KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_CONSUMER = "KAFKA_MIRRORMAKER_TLS_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER = "KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER = "KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER = "KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER = "KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER = "KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER = "KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_GROUPID_CONSUMER = "KAFKA_MIRRORMAKER_GROUPID_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER = "KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER = "KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER = "KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_CONSUMER = "KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_CONSUMER = "KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_CONSUMER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_PASSWORD_GRANT_PASSWORD_CONSUMER = "KAFKA_MIRRORMAKER_OAUTH_PASSWORD_GRANT_PASSWORD_CONSUMER";

    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER = "KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_PRODUCER = "KAFKA_MIRRORMAKER_TLS_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER = "KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER = "KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER = "KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER = "KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER = "KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER = "KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER = "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER = "KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER = "KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_PRODUCER = "KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_PRODUCER = "KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_PRODUCER";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_PASSWORD_GRANT_PASSWORD_PRODUCER = "KAFKA_MIRRORMAKER_OAUTH_PASSWORD_GRANT_PASSWORD_PRODUCER";

    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_INCLUDE = "KAFKA_MIRRORMAKER_INCLUDE";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_NUMSTREAMS = "KAFKA_MIRRORMAKER_NUMSTREAMS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_OFFSET_COMMIT_INTERVAL = "KAFKA_MIRRORMAKER_OFFSET_COMMIT_INTERVAL";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_ABORT_ON_SEND_FAILURE = "KAFKA_MIRRORMAKER_ABORT_ON_SEND_FAILURE";

    protected static final String ENV_VAR_STRIMZI_READINESS_PERIOD = "STRIMZI_READINESS_PERIOD";
    protected static final String ENV_VAR_STRIMZI_LIVENESS_PERIOD = "STRIMZI_LIVENESS_PERIOD";
    protected static final String ENV_VAR_STRIMZI_TRACING = "STRIMZI_TRACING";

    protected static final String CO_ENV_VAR_CUSTOM_MIRROR_MAKER_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_MIRROR_MAKER_LABELS";

    private int replicas;
    protected String include;
    protected Tracing tracing;
    private MetricsModel metrics;
    private LoggingModel logging;

    protected KafkaMirrorMakerProducerSpec producer;
    protected KafkaMirrorMakerConsumerSpec consumer;

    // Templates
    private PodDisruptionBudgetTemplate templatePodDisruptionBudget;
    private DeploymentTemplate templateDeployment;
    private PodTemplate templatePod;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_MIRROR_MAKER_POD_LABELS);
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
    protected KafkaMirrorMakerCluster(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, KafkaMirrorMakerResources.componentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);
    }

    /**
     * Creates the Kafka Mirror Maker 1 model from the KafkaMirrorMaker CR
     *
     * @param reconciliation        Reconciliation marker
     * @param kafkaMirrorMaker      KafkaMirrorMaker custom resource
     * @param versions              Supported Kafka versions
     * @param sharedEnvironmentProvider Shared environment provider.
     *
     * @return  Kafka Mirror Maker model instance
     */
    @SuppressWarnings("deprecation")
    public static KafkaMirrorMakerCluster fromCrd(Reconciliation reconciliation,
                                                  KafkaMirrorMaker kafkaMirrorMaker,
                                                  KafkaVersion.Lookup versions,
                                                  SharedEnvironmentProvider sharedEnvironmentProvider) {
        KafkaMirrorMakerCluster result = new KafkaMirrorMakerCluster(reconciliation, kafkaMirrorMaker, sharedEnvironmentProvider);

        KafkaMirrorMakerSpec spec = kafkaMirrorMaker.getSpec();
        if (spec != null) {
            result.replicas = spec.getReplicas();
            result.resources = spec.getResources();
            result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(spec, DEFAULT_HEALTHCHECK_OPTIONS);
            result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(spec, DEFAULT_HEALTHCHECK_OPTIONS);

            String whitelist = spec.getWhitelist();
            String include = spec.getInclude();

            if (include == null && whitelist == null)   {
                throw new InvalidResourceException("One of the fields include or whitelist needs to be specified.");
            } else if (whitelist != null && include != null) {
                LOGGER.warnCr(reconciliation, "Both include and whitelist fields are present. Whitelist is deprecated and will be ignored.");
            }

            result.include = include != null ? include : whitelist;

            String warnMsg = AuthenticationUtils.validateClientAuthentication(spec.getProducer().getAuthentication(), spec.getProducer().getTls() != null);
            if (!warnMsg.isEmpty()) {
                LOGGER.warnCr(reconciliation, warnMsg);
            }

            result.producer = spec.getProducer();

            warnMsg = AuthenticationUtils.validateClientAuthentication(spec.getConsumer().getAuthentication(), spec.getConsumer().getTls() != null);
            if (!warnMsg.isEmpty()) {
                LOGGER.warnCr(reconciliation, warnMsg);
            }

            result.consumer = spec.getConsumer();

            result.image = versions.kafkaMirrorMakerImage(spec.getImage(), spec.getVersion());

            result.gcLoggingEnabled = spec.getJvmOptions() == null ? JvmOptions.DEFAULT_GC_LOGGING_ENABLED : spec.getJvmOptions().isGcLoggingEnabled();
            result.jvmOptions = spec.getJvmOptions();
            result.metrics = new MetricsModel(spec);
            result.logging = new LoggingModel(spec, result.getClass().getSimpleName(), false, true);

            if (spec.getTemplate() != null) {
                KafkaMirrorMakerTemplate template = spec.getTemplate();

                result.templatePodDisruptionBudget = template.getPodDisruptionBudget();
                result.templateDeployment = template.getDeployment();
                result.templatePod = template.getPod();
                result.templateServiceAccount = template.getServiceAccount();
                result.templateContainer = template.getMirrorMakerContainer();
            }

            result.tracing = spec.getTracing();
        }

        return result;
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);
        if (metrics.isEnabled()) {
            portList.add(ContainerUtils.createContainerPort(MetricsModel.METRICS_PORT_NAME, MetricsModel.METRICS_PORT));
        }

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(2);

        volumeList.add(VolumeUtils.createTempDirVolume(templatePod));
        volumeList.add(VolumeUtils.createConfigMapVolume(LOG_AND_METRICS_CONFIG_VOLUME_NAME, KafkaMirrorMakerResources.metricsAndLogConfigMapName(cluster)));

        if (producer.getTls() != null) {
            VolumeUtils.createSecretVolume(volumeList, producer.getTls().getTrustedCertificates(), isOpenShift);
        }
        AuthenticationUtils.configureClientAuthenticationVolumes(producer.getAuthentication(), volumeList, "producer-oauth-certs", isOpenShift);

        if (consumer.getTls() != null) {
            VolumeUtils.createSecretVolume(volumeList, consumer.getTls().getTrustedCertificates(), isOpenShift);
        }
        AuthenticationUtils.configureClientAuthenticationVolumes(consumer.getAuthentication(), volumeList, "consumer-oauth-certs", isOpenShift);

        return volumeList;
    }

    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(2);

        volumeMountList.add(VolumeUtils.createTempDirVolumeMount());
        volumeMountList.add(VolumeUtils.createVolumeMount(LOG_AND_METRICS_CONFIG_VOLUME_NAME, LOG_AND_METRICS_CONFIG_VOLUME_MOUNT));
        /* producer auth*/
        if (producer.getTls() != null) {
            VolumeUtils.createSecretVolumeMount(volumeMountList, producer.getTls().getTrustedCertificates(), TLS_CERTS_VOLUME_MOUNT_PRODUCER);
        }
        AuthenticationUtils.configureClientAuthenticationVolumeMounts(producer.getAuthentication(), volumeMountList, TLS_CERTS_VOLUME_MOUNT_PRODUCER, PASSWORD_VOLUME_MOUNT_PRODUCER, OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER, "producer-oauth-certs");

        /* consumer auth*/
        if (consumer.getTls() != null) {
            VolumeUtils.createSecretVolumeMount(volumeMountList, consumer.getTls().getTrustedCertificates(), TLS_CERTS_VOLUME_MOUNT_CONSUMER);
        }
        AuthenticationUtils.configureClientAuthenticationVolumeMounts(consumer.getAuthentication(), volumeMountList, TLS_CERTS_VOLUME_MOUNT_CONSUMER, PASSWORD_VOLUME_MOUNT_CONSUMER, OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER, "consumer-oauth-certs");

        return volumeMountList;
    }

    /**
     * Generates Kafka Mirror Maker Deployment
     *
     * @param annotations       Map with annotations
     * @param isOpenShift       Flag indicating if we are on OpenShift
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List with image pull secrets
     *
     * @return  Generated Deployment
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
                WorkloadUtils.deploymentStrategy(TemplateUtils.deploymentStrategy(templateDeployment, ROLLING_UPDATE)),
                WorkloadUtils.createPodTemplateSpec(
                        componentName,
                        labels,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        annotations,
                        templatePod != null ? templatePod.getAffinity() : null,
                        null,
                        List.of(createContainer(imagePullPolicy)),
                        getVolumes(isOpenShift),
                        imagePullSecrets,
                        securityProvider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(templatePod))
                )
        );
    }

    /* test */ Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                componentName,
                image,
                List.of("/opt/kafka/kafka_mirror_maker_run.sh"),
                securityProvider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                getContainerPortList(),
                getVolumeMounts(),
                ProbeUtils.defaultBuilder(livenessProbeOptions).withNewExec().withCommand("/opt/kafka/kafka_mirror_maker_liveness.sh").endExec().build(),
                // The mirror-maker-agent will create /tmp/mirror-maker-ready in the container
                ProbeUtils.defaultBuilder(readinessProbeOptions).withNewExec().withCommand("test", "-f", "/tmp/mirror-maker-ready").endExec().build(),
                imagePullPolicy
        );
    }

    @SuppressWarnings("deprecation")
    private KafkaMirrorMakerConsumerConfiguration getConsumerConfiguration() {
        KafkaMirrorMakerConsumerConfiguration config = new KafkaMirrorMakerConsumerConfiguration(reconciliation, consumer.getConfig().entrySet());

        if (tracing != null) {
            if (JaegerTracing.TYPE_JAEGER.equals(tracing.getType())) {
                LOGGER.warnCr(reconciliation, "Tracing type \"{}\" is not supported anymore and will be ignored", JaegerTracing.TYPE_JAEGER);
            } else if (OpenTelemetryTracing.TYPE_OPENTELEMETRY.equals(tracing.getType())) {
                config.setConfigOption("interceptor.classes", OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME);
            }
        }

        return config;
    }

    @SuppressWarnings("deprecation")
    private KafkaMirrorMakerProducerConfiguration getProducerConfiguration()    {
        KafkaMirrorMakerProducerConfiguration config = new KafkaMirrorMakerProducerConfiguration(reconciliation, producer.getConfig().entrySet());

        if (tracing != null) {
            if (JaegerTracing.TYPE_JAEGER.equals(tracing.getType())) {
                LOGGER.warnCr(reconciliation, "Tracing type \"{}\" is not supported anymore and will be ignored", JaegerTracing.TYPE_JAEGER);
            } else if (OpenTelemetryTracing.TYPE_OPENTELEMETRY.equals(tracing.getType())) {
                config.setConfigOption("interceptor.classes", OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME);
            }
        }

        return config;
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER,
                getConsumerConfiguration().getConfiguration()));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER,
                getProducerConfiguration().getConfiguration()));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_METRICS_ENABLED, String.valueOf(metrics.isEnabled())));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER, consumer.getBootstrapServers()));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER, producer.getBootstrapServers()));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_INCLUDE, include));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_GROUPID_CONSUMER, consumer.getGroupId()));
        if (consumer.getNumStreams() != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_NUMSTREAMS, Integer.toString(consumer.getNumStreams())));
        }
        if (consumer.getOffsetCommitInterval() != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_OFFSET_COMMIT_INTERVAL, Integer.toString(consumer.getOffsetCommitInterval())));
        }
        if (producer.getAbortOnSendFailure() != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_ABORT_ON_SEND_FAILURE, Boolean.toString(producer.getAbortOnSendFailure())));
        }
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));

        if (tracing != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_TRACING, tracing.getType()));
        }

        JvmOptionUtils.heapOptions(varList, 75, 0L, jvmOptions, resources);
        JvmOptionUtils.jvmPerformanceOptions(varList, jvmOptions);
        JvmOptionUtils.jvmSystemProperties(varList, jvmOptions);

        /* consumer */
        addConsumerEnvVars(varList);

        /* producer */
        addProducerEnvVars(varList);

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_LIVENESS_PERIOD,
                String.valueOf(livenessProbeOptions.getPeriodSeconds() != null ? livenessProbeOptions.getPeriodSeconds() : DEFAULT_HEALTHCHECK_PERIOD)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_READINESS_PERIOD,
                String.valueOf(readinessProbeOptions.getPeriodSeconds() != null ? readinessProbeOptions.getPeriodSeconds() : DEFAULT_HEALTHCHECK_PERIOD)));

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    /**
     * Sets the consumer related environment variables in the provided List.
     *
     * @param varList   List with environment variables
     */
    private void addConsumerEnvVars(List<EnvVar> varList) {
        if (consumer.getTls() != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TLS_CONSUMER, "true"));

            if (consumer.getTls().getTrustedCertificates() != null && consumer.getTls().getTrustedCertificates().size() > 0) {
                StringBuilder sb = new StringBuilder();
                boolean separator = false;
                for (CertSecretSource certSecretSource : consumer.getTls().getTrustedCertificates()) {
                    if (separator) {
                        sb.append(";");
                    }
                    sb.append(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
                    separator = true;
                }
                varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER, sb.toString()));
            }
        }

        AuthenticationUtils.configureClientAuthenticationEnvVars(consumer.getAuthentication(), varList, name -> ENV_VAR_PREFIX + name + "_CONSUMER");
    }

    /**
     * Sets the producer related environment variables in the provided List.
     *
     * @param varList   List with environment variables
     */
    private void addProducerEnvVars(List<EnvVar> varList) {
        if (producer.getTls() != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TLS_PRODUCER, "true"));

            if (producer.getTls().getTrustedCertificates() != null && producer.getTls().getTrustedCertificates().size() > 0) {
                StringBuilder sb = new StringBuilder();
                boolean separator = false;
                for (CertSecretSource certSecretSource : producer.getTls().getTrustedCertificates()) {
                    if (separator) {
                        sb.append(";");
                    }
                    sb.append(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
                    separator = true;
                }
                varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER, sb.toString()));
            }
        }

        AuthenticationUtils.configureClientAuthenticationEnvVars(producer.getAuthentication(), varList, name -> ENV_VAR_PREFIX + name + "_PRODUCER");
    }

    /**
     * Generates the PodDisruptionBudget
     *
     * @return The pod disruption budget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return PodDisruptionBudgetUtils.createPodDisruptionBudget(componentName, namespace, labels, ownerReference, templatePodDisruptionBudget);
    }

    protected String getInclude() {
        return include;
    }

    /**
     * Generates a metrics and logging ConfigMap according to the configuration. If this operand doesn't support logging
     * or metrics, they will nto be set.
     *
     * @param metricsAndLogging     The external CMs with logging and metrics configuration
     *
     * @return The generated ConfigMap
     */
    public ConfigMap generateMetricsAndLogConfigMap(MetricsAndLogging metricsAndLogging) {
        return ConfigMapUtils
                .createConfigMap(
                        KafkaMirrorMakerResources.metricsAndLogConfigMapName(cluster),
                        namespace,
                        labels,
                        ownerReference,
                        ConfigMapUtils.generateMetricsAndLogConfigMapData(reconciliation, this, metricsAndLogging)
                );
    }

    /**
     * @return The number of replicas
     */
    public int getReplicas() {
        return replicas;
    }

    /**
     * @return  Metrics Model instance for configuring Prometheus metrics
     */
    public MetricsModel metrics()   {
        return metrics;
    }

    /**
     * @return  Logging Model instance for configuring logging
     */
    public LoggingModel logging()   {
        return logging;
    }
}
