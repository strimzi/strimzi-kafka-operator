/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.JmxTransResources;
import io.strimzi.api.kafka.model.JmxTransSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplate;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplate;
import io.strimzi.api.kafka.model.template.JmxTransTemplate;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.operator.cluster.model.components.JmxTransOutputWriter;
import io.strimzi.operator.cluster.model.components.JmxTransQueries;
import io.strimzi.operator.cluster.model.components.JmxTransServer;
import io.strimzi.operator.cluster.model.components.JmxTransServers;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.template.DeploymentStrategy.ROLLING_UPDATE;

/**
 * Class for handling JmxTrans configuration passed by the user. Used to get the resources needed to create the
 * JmxTrans deployment including: config map, deployment, and service accounts.
 */
@SuppressWarnings("deprecation") // JMX Trans is deprecated
@SuppressFBWarnings({"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"}) // Spotbugs is complaining about outputDefinitions and kafkaQueries which are required in the CRD about not being initialized
public class JmxTrans extends AbstractModel {
    private static final String COMPONENT_TYPE = "jmx-trans";

    // Configuration defaults
    private static final String STRIMZI_DEFAULT_JMXTRANS_IMAGE = "STRIMZI_DEFAULT_JMXTRANS_IMAGE";
    private static final Probe READINESS_PROBE_OPTIONS = new ProbeBuilder().withTimeoutSeconds(5).withInitialDelaySeconds(15).build();
    private static final io.strimzi.api.kafka.model.Probe DEFAULT_JMX_TRANS_PROBE = new io.strimzi.api.kafka.model.ProbeBuilder()
            .withInitialDelaySeconds(JmxTransSpec.DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(JmxTransSpec.DEFAULT_HEALTHCHECK_TIMEOUT)
            .build();

    // Configuration for mounting `config.json` to be used as Config during run time of the JmxTrans
    /**
     * Key under which the JMX Trans configuration is stored in the Config Map
     */
    public static final String JMXTRANS_CONFIGMAP_KEY = "config.json";
    /* test */ static final String JMXTRANS_VOLUME_NAME = "jmx-config";

    /**
     * JMX Trans Config Map revision used to trigger rolling update of JMX Trans when the configuration changes
     */
    public static final String ANNO_JMXTRANS_CONFIG_MAP_HASH = Annotations.STRIMZI_DOMAIN + "config-map-revision";
    /* test */ static final String JMX_FILE_PATH = "/var/lib/jmxtrans";

    protected static final String ENV_VAR_JMXTRANS_LOGGING_LEVEL = "JMXTRANS_LOGGING_LEVEL";

    protected static final String CO_ENV_VAR_CUSTOM_JMX_TRANS_POD_LABELS = "STRIMZI_CUSTOM_JMX_TRANS_LABELS";

    private boolean isJmxAuthenticated;
    private String loggingLevel;
    private int numberOfBrokers;
    private List<JmxTransOutputDefinitionTemplate> outputDefinitions;
    private List<JmxTransQueryTemplate> kafkaQueries;

    private DeploymentTemplate templateDeployment;
    private PodTemplate templatePod;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_JMX_TRANS_POD_LABELS);
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
    protected JmxTrans(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, JmxTransResources.deploymentName(resource.getMetadata().getName()), COMPONENT_TYPE);

        this.replicas = 1;
        this.readinessProbeOptions = READINESS_PROBE_OPTIONS;
    }

    /**
     * Builds the JMX Trans model from the Kafka custom resource. If JMX Trans is not enabled, it will return null.
     *
     * @param reconciliation    Reconciliation marker for logging
     * @param kafkaAssembly     The Kafka CR
     *
     * @return                  JMX Trans model object when JMX Trans is enabled or null if it is disabled.
     */
    public static JmxTrans fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly) {
        JmxTransSpec jmxTransSpec = kafkaAssembly.getSpec().getJmxTrans();

        if (jmxTransSpec != null) {
            // JMX Trans requires JMX to be enabled in Kafka brokers. If it is not enabled, we throw InvalidResourceException
            if (kafkaAssembly.getSpec().getKafka().getJmxOptions() == null) {
                String error = String.format("Can't start up JmxTrans '%s' in '%s' as Kafka spec.kafka.jmxOptions is not specified",
                        JmxTransResources.deploymentName(kafkaAssembly.getMetadata().getName()),
                        kafkaAssembly.getMetadata().getNamespace());
                LOGGER.warnCr(reconciliation, error);
                throw new InvalidResourceException(error);
            }

            JmxTrans result = new JmxTrans(reconciliation, kafkaAssembly);

            result.isJmxAuthenticated = kafkaAssembly.getSpec().getKafka().getJmxOptions().getAuthentication() instanceof KafkaJmxAuthenticationPassword;
            result.numberOfBrokers = kafkaAssembly.getSpec().getKafka().getReplicas();
            result.kafkaQueries = jmxTransSpec.getKafkaQueries();
            result.outputDefinitions = jmxTransSpec.getOutputDefinitions();
            result.loggingLevel = jmxTransSpec.getLogLevel() == null ? "INFO" : jmxTransSpec.getLogLevel();
            result.resources = jmxTransSpec.getResources();

            String image = jmxTransSpec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(STRIMZI_DEFAULT_JMXTRANS_IMAGE, "quay.io/strimzi/jmxtrans:latest");
            }
            result.image = image;

            if (jmxTransSpec.getTemplate() != null) {
                JmxTransTemplate template = jmxTransSpec.getTemplate();

                result.templateDeployment = template.getDeployment();
                result.templatePod = template.getPod();
                result.templateServiceAccount = template.getServiceAccount();
                result.templateContainer = template.getContainer();
            }

            return result;
        } else {
            return null;
        }
    }

    /**
     * Generates the JMX Trans deployment
     *
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  Image pull secrets
     *
     * @return  Kubernetes Deployment with the JMX Trans
     */
    public Deployment generateDeployment(ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        return WorkloadUtils.createDeployment(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateDeployment,
                replicas,
                WorkloadUtils.deploymentStrategy(TemplateUtils.deploymentStrategy(templateDeployment, ROLLING_UPDATE)),
                WorkloadUtils.createPodTemplateSpec(
                        componentName,
                        labels,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        Map.of(),
                        templatePod != null ? templatePod.getAffinity() : null,
                        null,
                        List.of(createContainer(imagePullPolicy)),
                        getVolumes(),
                        imagePullSecrets,
                        securityProvider.jmxTransPodSecurityContext(new PodSecurityProviderContextImpl(templatePod))
                )
        );
    }

    /**
     * Generates the string'd config that the JmxTrans deployment needs to run. It is configured by the user in the yaml
     * and this method will convert that into the config the JmxTrans understands.
     *
     * @return the jmx trans config file that targets each broker
     */
    private String generateJMXConfig() {
        List<JmxTransQueries> queries = jmxTransQueries();
        List<JmxTransServer> servers = new ArrayList<>(numberOfBrokers);

        String headlessService = KafkaResources.brokersServiceName(cluster);

        for (int brokerNumber = 0; brokerNumber < numberOfBrokers; brokerNumber++) {
            String brokerServiceName = KafkaResources.kafkaStatefulSetName(cluster) + "-" + brokerNumber + "." + headlessService;
            servers.add(jmxTransServer(queries, brokerServiceName));
        }

        JmxTransServers jmxTransConfiguration = new JmxTransServers();
        jmxTransConfiguration.setServers(servers);

        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(jmxTransConfiguration);
        } catch (JsonProcessingException e) {
            LOGGER.errorCr(reconciliation, "Failed to convert JMX Trans configuration to JSON", e);
            throw new RuntimeException("Failed to convert JMX Trans configuration to JSON", e);
        }
    }

    /**
     * Generates the queries for the JMX Trans configuration
     *
     * @return  List with JMX Trans queries
     */
    private List<JmxTransQueries> jmxTransQueries() {
        List<JmxTransQueries> queries = new ArrayList<>(kafkaQueries.size());

        for (JmxTransQueryTemplate queryTemplate : kafkaQueries) {
            JmxTransQueries query = new JmxTransQueries();
            query.setObj(queryTemplate.getTargetMBean());
            query.setAttr(queryTemplate.getAttributes());
            query.setOutputWriters(new ArrayList<>());

            for (JmxTransOutputDefinitionTemplate outputDefinitionTemplate : outputDefinitions) {
                if (queryTemplate.getOutputs().contains(outputDefinitionTemplate.getName())) {
                    JmxTransOutputWriter outputWriter = new JmxTransOutputWriter();
                    outputWriter.setAtClass(outputDefinitionTemplate.getOutputType());

                    if (outputDefinitionTemplate.getHost() != null) {
                        outputWriter.setHost(outputDefinitionTemplate.getHost());
                    }

                    if (outputDefinitionTemplate.getPort() != null) {
                        outputWriter.setPort(outputDefinitionTemplate.getPort());
                    }

                    if (outputDefinitionTemplate.getFlushDelayInSeconds()  != null) {
                        outputWriter.setFlushDelayInSeconds(outputDefinitionTemplate.getFlushDelayInSeconds());
                    }

                    outputWriter.setTypeNames(outputDefinitionTemplate.getTypeNames());
                    query.getOutputWriters().add(outputWriter);
                }
            }

            queries.add(query);
        }

        return queries;
    }

    /**
     * Generates the server configuration for given Kafka broker
     *
     * @param queries           List of Queries which should be used for this broker
     * @param brokerServiceName Address of the service where to connect to the broker
     *
     * @return  JMX Trans server instance for given Kafka broker
     */
    private JmxTransServer jmxTransServer(List<JmxTransQueries> queries, String brokerServiceName) {
        JmxTransServer server = new JmxTransServer();

        server.setHost(brokerServiceName);
        server.setPort(AbstractModel.JMX_PORT);
        server.setQueries(queries);

        if (isJmxAuthenticated) {
            server.setUsername("${kafka.username}");
            server.setPassword("${kafka.password}");
        }

        return server;
    }

    /**
     * Generates the JmxTrans config map
     *
     * @return the config map that mounts the JmxTrans config
     */
    public ConfigMap generateConfigMap() {
        Map<String, String> data = new HashMap<>(1);
        data.put(JMXTRANS_CONFIGMAP_KEY, generateJMXConfig());

        return ConfigMapUtils.createConfigMap(JmxTransResources.configMapName(cluster), namespace, labels, ownerReference, data);
    }

    private List<Volume> getVolumes() {
        List<Volume> volumes = new ArrayList<>(2);

        volumes.add(VolumeUtils.createTempDirVolume(templatePod));
        volumes.add(VolumeUtils.createConfigMapVolume(JMXTRANS_VOLUME_NAME, JmxTransResources.configMapName(cluster)));

        return volumes;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(2);

        volumeMountList.add(VolumeUtils.createTempDirVolumeMount());
        volumeMountList.add(VolumeUtils.createVolumeMount(JMXTRANS_VOLUME_NAME, JMX_FILE_PATH));
        return volumeMountList;
    }

    /* test */ Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                componentName,
                image,
                null,
                securityProvider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                null,
                getVolumeMounts(),
                null,
                jmxTransReadinessProbe(readinessProbeOptions, cluster),
                imagePullPolicy
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        if (isJmxAuthenticated) {
            varList.add(ContainerUtils.createEnvVarFromSecret(KafkaCluster.ENV_VAR_KAFKA_JMX_USERNAME, KafkaResources.kafkaJmxSecretName(cluster), KafkaCluster.SECRET_JMX_USERNAME_KEY));
            varList.add(ContainerUtils.createEnvVarFromSecret(KafkaCluster.ENV_VAR_KAFKA_JMX_PASSWORD, KafkaResources.kafkaJmxSecretName(cluster), KafkaCluster.SECRET_JMX_PASSWORD_KEY));
        }

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_JMXTRANS_LOGGING_LEVEL, loggingLevel));

        // Add shared environment variables used for all containers
        varList.addAll(ContainerUtils.requiredEnvVars());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    protected static io.fabric8.kubernetes.api.model.Probe jmxTransReadinessProbe(io.strimzi.api.kafka.model.Probe  kafkaJmxMetricsReadinessProbe, String clusterName) {
        String internalBootstrapServiceName = KafkaResources.brokersServiceName(clusterName);
        String metricsPortValue = String.valueOf(KafkaCluster.JMX_PORT);
        kafkaJmxMetricsReadinessProbe = kafkaJmxMetricsReadinessProbe == null ? DEFAULT_JMX_TRANS_PROBE : kafkaJmxMetricsReadinessProbe;
        return ProbeGenerator.execProbe(kafkaJmxMetricsReadinessProbe, Arrays.asList("/opt/jmx/jmxtrans_readiness_check.sh", internalBootstrapServiceName, metricsPortValue));
    }
}