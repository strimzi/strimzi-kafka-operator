/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.JmxTransResources;
import io.strimzi.api.kafka.model.JmxTransSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplate;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplate;
import io.strimzi.api.kafka.model.template.JmxTransTemplate;
import io.strimzi.operator.cluster.model.components.JmxTransOutputWriter;
import io.strimzi.operator.cluster.model.components.JmxTransQueries;
import io.strimzi.operator.cluster.model.components.JmxTransServer;
import io.strimzi.operator.cluster.model.components.JmxTransServers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Class for handling JmxTrans configuration passed by the user. Used to get the resources needed to create the
 * JmxTrans deployment including: config map, deployment, and service accounts.
 */
public class JmxTrans extends AbstractModel {
    private static final String APPLICATION_NAME = "jmx-trans";

    // Configuration defaults
    private static final String STRIMZI_DEFAULT_JMXTRANS_IMAGE = "STRIMZI_DEFAULT_JMXTRANS_IMAGE";
    public static final Probe READINESS_PROBE_OPTIONS = new ProbeBuilder().withTimeoutSeconds(5).withInitialDelaySeconds(15).build();
    private static final io.strimzi.api.kafka.model.Probe DEFAULT_JMX_TRANS_PROBE = new io.strimzi.api.kafka.model.ProbeBuilder()
            .withInitialDelaySeconds(JmxTransSpec.DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(JmxTransSpec.DEFAULT_HEALTHCHECK_TIMEOUT)
            .build();

    // Configuration for mounting `config.json` to be used as Config during run time of the JmxTrans
    public static final String JMXTRANS_CONFIGMAP_KEY = "config.json";
    public static final String JMXTRANS_VOLUME_NAME = "jmx-config";
    public static final String CONFIG_MAP_ANNOTATION_KEY = "config-map-revision";
    protected static final String JMX_METRICS_CONFIG_SUFFIX = "-jmxtrans-config";
    public static final String JMX_FILE_PATH = "/var/lib/jmxtrans";

    protected static final String ENV_VAR_JMXTRANS_LOGGING_LEVEL = "JMXTRANS_LOGGING_LEVEL";


    private boolean isDeployed;
    private boolean isJmxAuthenticated;
    private String configMapName;
    private String clusterName;
    private String loggingLevel;

    protected List<ContainerEnvVar> templateContainerEnvVars;
    protected SecurityContext templateContainerSecurityContext;

    /**
     * Constructor
     *
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected JmxTrans(HasMetadata resource) {
        super(resource, APPLICATION_NAME);
        this.name = JmxTransResources.deploymentName(cluster);
        this.clusterName = cluster;
        this.replicas = 1;
        this.readinessPath = "/metrics";
        this.livenessPath = "/metrics";
        this.readinessProbeOptions = READINESS_PROBE_OPTIONS;

        this.mountPath = "/var/lib/kafka";

        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/usr/share/jmxtrans/conf/";

        // Metrics must be enabled as JmxTrans is all about gathering JMX metrics from the Kafka brokers and pushing it to remote sources.
        this.isMetricsEnabled = true;
    }

    public static JmxTrans fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        JmxTrans result = null;
        JmxTransSpec spec = kafkaAssembly.getSpec().getJmxTrans();
        if (spec != null) {
            if (kafkaAssembly.getSpec().getKafka().getJmxOptions() == null) {
                String error = String.format("Can't start up JmxTrans '%s' in '%s' as Kafka spec.kafka.jmxOptions is not specified",
                        JmxTransResources.deploymentName(kafkaAssembly.getMetadata().getName()),
                        kafkaAssembly.getMetadata().getNamespace());
                log.warn(error);
                throw new InvalidResourceException(error);
            }
            result = new JmxTrans(kafkaAssembly);
            result.isDeployed = true;

            if (kafkaAssembly.getSpec().getKafka().getJmxOptions().getAuthentication() instanceof KafkaJmxAuthenticationPassword) {
                result.isJmxAuthenticated = true;
            }

            result.loggingLevel = spec.getLogLevel() == null ? "" : spec.getLogLevel();

            result.setResources(spec.getResources());

            String image = spec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(STRIMZI_DEFAULT_JMXTRANS_IMAGE, "strimzi/jmxtrans:latest");
            }
            result.setImage(image);

            result.setOwnerReference(kafkaAssembly);

            if (spec.getTemplate() != null) {
                JmxTransTemplate template = spec.getTemplate();

                if (template.getDeployment() != null && template.getDeployment().getMetadata() != null)  {
                    result.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                    result.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
                }

                ModelUtils.parsePodTemplate(result, template.getPod());

                if (template.getContainer() != null && template.getContainer().getEnv() != null) {
                    result.templateContainerEnvVars = template.getContainer().getEnv();
                }

                if (template.getContainer() != null && template.getContainer().getSecurityContext() != null) {
                    result.templateContainerSecurityContext = template.getContainer().getSecurityContext();
                }
            }

        }

        return result;
    }

    public Deployment generateDeployment(ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
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
                getVolumes(),
                imagePullSecrets
        );
    }

    /**
     * Generates the string'd config that the JmxTrans deployment needs to run. It is configured by the user in the yaml
     * and this method will convert that into the config the JmxTrans understands.
     * @param spec The JmxTrans that was defined by the user
     * @param numOfBrokers number of kafka brokers
     * @return the jmx trans config file that targets each broker
     */
    private String generateJMXConfig(JmxTransSpec spec, int numOfBrokers) throws JsonProcessingException {
        JmxTransServers servers = new JmxTransServers();
        servers.setServers(new ArrayList<>());
        ObjectMapper mapper = new ObjectMapper();
        String headlessService = KafkaCluster.headlessServiceName(cluster);
        for (int brokerNumber = 0; brokerNumber < numOfBrokers; brokerNumber++) {
            String brokerServiceName = KafkaCluster.externalServiceName(clusterName, brokerNumber) + "." + headlessService;
            servers.getServers().add(convertSpecToServers(spec, brokerServiceName));
        }
        try {
            return mapper.writeValueAsString(servers);
        } catch (JsonProcessingException e) {
            log.error("Could not create JmxTrans config json because: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Generates the JmxTrans config map
     *
     * @param spec The JmxTransSpec that was defined by the user
     * @param numOfBrokers number of kafka brokers
     * @return the config map that mounts the JmxTrans config
     * @throws JsonProcessingException when JmxTrans config can't be created properly
     */
    public ConfigMap generateJmxTransConfigMap(JmxTransSpec spec, int numOfBrokers) throws JsonProcessingException {
        Map<String, String> data = new HashMap<>(1);
        String jmxConfig = generateJMXConfig(spec, numOfBrokers);
        data.put(JMXTRANS_CONFIGMAP_KEY, jmxConfig);
        configMapName = jmxTransConfigName(clusterName);
        return createConfigMap(jmxTransConfigName(clusterName), data);
    }

    public List<Volume> getVolumes() {
        List<Volume> volumes = new ArrayList<>(2);
        volumes.add(VolumeUtils.createConfigMapVolume(JMXTRANS_VOLUME_NAME, configMapName));
        volumes.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, KafkaCluster.metricAndLogConfigsName(clusterName)));
        return volumes;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(2);

        volumeMountList.add(VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
        volumeMountList.add(VolumeUtils.createVolumeMount(JMXTRANS_VOLUME_NAME, JMX_FILE_PATH));
        return volumeMountList;
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>(1);
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withReadinessProbe(jmxTransReadinessProbe(readinessProbeOptions, clusterName))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateContainerSecurityContext)
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        if (isJmxAuthenticated()) {
            varList.add(buildEnvVarFromSecret(KafkaCluster.ENV_VAR_KAFKA_JMX_USERNAME, KafkaCluster.jmxSecretName(cluster), KafkaCluster.SECRET_JMX_USERNAME_KEY));
            varList.add(buildEnvVarFromSecret(KafkaCluster.ENV_VAR_KAFKA_JMX_PASSWORD, KafkaCluster.jmxSecretName(cluster), KafkaCluster.SECRET_JMX_PASSWORD_KEY));
        }
        varList.add(buildEnvVar(ENV_VAR_JMXTRANS_LOGGING_LEVEL, loggingLevel));

        // Add shared environment variables used for all containers
        varList.addAll(getSharedEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateContainerEnvVars);

        return varList;
    }

    /**
     * Generates the name of the JmxTrans deployment
     *
     * @param kafkaCluster  Name of the Kafka Custom Resource
     * @return  Name of the JmxTrans deployment
     */
    public static String jmxTransName(String kafkaCluster) {
        return JmxTransResources.deploymentName(kafkaCluster);
    }

    /**
     * Get the name of the JmxTrans service account given the name of the {@code kafkaCluster}.
     * @param kafkaCluster The cluster name
     * @return The name of the JmxTrans service account.
     */
    public static String containerServiceAccountName(String kafkaCluster) {
        return JmxTransResources.serviceAccountName(kafkaCluster);
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return null;
    }

    @Override
    protected String getServiceAccountName() {
        return JmxTransResources.serviceAccountName(cluster);
    }

    public static String jmxTransConfigName(String cluster) {
        return cluster + JMX_METRICS_CONFIG_SUFFIX;
    }

    public boolean isDeployed() {
        return isDeployed;
    }

    public boolean isJmxAuthenticated() {
        return isJmxAuthenticated;
    }

    protected static io.fabric8.kubernetes.api.model.Probe jmxTransReadinessProbe(io.strimzi.api.kafka.model.Probe  kafkaJmxMetricsReadinessProbe, String clusterName) {
        String internalBootstrapServiceName = KafkaCluster.headlessServiceName(clusterName);
        String metricsPortValue = String.valueOf(KafkaCluster.JMX_PORT);
        kafkaJmxMetricsReadinessProbe = kafkaJmxMetricsReadinessProbe == null ? DEFAULT_JMX_TRANS_PROBE : kafkaJmxMetricsReadinessProbe;
        return ModelUtils.createExecProbe(Arrays.asList("/opt/jmx/jmxtrans_readiness_check.sh", internalBootstrapServiceName, metricsPortValue), kafkaJmxMetricsReadinessProbe);
    }

    private JmxTransServer convertSpecToServers(JmxTransSpec spec, String brokerServiceName) {
        JmxTransServer server = new JmxTransServer();
        server.setHost(brokerServiceName);
        server.setPort(AbstractModel.JMX_PORT);
        if (isJmxAuthenticated()) {
            server.setUsername("${kafka.username}");
            server.setPassword("${kafka.password}");
        }
        List<JmxTransQueries> queries = new ArrayList<>();
        for (JmxTransQueryTemplate queryTemplate : spec.getKafkaQueries()) {
            JmxTransQueries query = new JmxTransQueries();
            query.setObj(queryTemplate.getTargetMBean());
            query.setAttr(queryTemplate.getAttributes());
            query.setOutputWriters(new ArrayList<>());

            for (JmxTransOutputDefinitionTemplate outputDefinitionTemplate : spec.getOutputDefinitions()) {
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
        server.setQueries(queries);
        return server;
    }

}