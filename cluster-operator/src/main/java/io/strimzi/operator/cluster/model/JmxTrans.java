/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.strimzi.api.kafka.model.JmxTransResources;
import io.strimzi.api.kafka.model.JmxTransSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.KafkaJmxOptions;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplate;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplate;
import io.strimzi.operator.common.model.Labels;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

/*
 * Class for handling JmxTrans configuration passed by the user. Used to get the resources needed to create the
 * JmxTrans deployment including: config map, deployment, and service accounts.
 */
public class JmxTrans extends AbstractModel {

    // Configuration defaults
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final String DEFAULT_JMXTRANS_IMAGE = "strimzi/jmxtrans:latest";
    public static final Probe LIVENESS_PROBE_OPTIONS = new ProbeBuilder().withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT).withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).build();

    // Configuration for mounting `config.json` to be used as Config during run time of the JmxTrans
    public static final String JMXTRANS_CONFIGMAP_KEY = "config.json";
    public static final String JMXTRANS_VOLUME_NAME = "jmx-config";
    public static final String CONFIG_MAP_ANNOTATION_KEY = "config-map-revision";
    protected static final String JMX_METRICS_CONFIG_SUFFIX = "-jmxtrans-config";
    public static final String JMX_FILE_PATH = "/var/lib/jmxtrans";

    private boolean isDeployed;
    private boolean isJmxAuthenticated;
    private String configMapName;
    private String clusterName;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where JmxTrans resources are going to be created
     * @param kafkaCluster kafkaCluster name
     * @param labels    labels to add to the kafkaCluster
     */
    protected JmxTrans(String namespace, String kafkaCluster, Labels labels) {
        super(namespace, kafkaCluster, labels);
        this.name = JmxTransResources.deploymentName(kafkaCluster);
        this.clusterName = kafkaCluster;
        this.replicas = 1;
        this.readinessPath = "/metrics";
        this.livenessPath = "/metrics";
        this.livenessProbeOptions = LIVENESS_PROBE_OPTIONS;

        this.mountPath = "/var/lib/kafka";

        // Metrics must be enabled as JmxTrans is all about gathering JMX metrics from the Kafka brokers and pushing it to remote sources.
        this.isMetricsEnabled = true;
    }

    public static JmxTrans fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        JmxTrans result = null;

        KafkaJmxOptions spec = kafkaAssembly.getSpec().getKafka().getJmxOptions();
        if (spec != null && spec.getJmxTransSpec() != null) {
            result = new JmxTrans(kafkaAssembly.getMetadata().getNamespace(),
                    kafkaAssembly.getMetadata().getName(),
                    Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));
            result.isDeployed = true;

            if (spec.getAuthentication() instanceof KafkaJmxAuthenticationPassword) {
                result.isJmxAuthenticated = true;
            }

            result.setResources(spec.getJmxTransSpec().getResources());

            result.setImage(DEFAULT_JMXTRANS_IMAGE);

            result.setOwnerReference(kafkaAssembly);
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
    private String generateJMXConfig(JmxTransSpec spec, int numOfBrokers) {
        Servers servers = new Servers();
        servers.servers = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        String headlessService = KafkaCluster.headlessServiceName(cluster) + "." + namespace + ".svc";
        for (int brokerNumber = 0; brokerNumber < numOfBrokers; brokerNumber++) {
            String brokerServiceName = KafkaCluster.externalServiceName(clusterName, brokerNumber) + "." + headlessService;
            servers.servers.add(convertSpecToServers(spec, brokerServiceName, JMX_PORT));
        }
        try {
            return mapper.writeValueAsString(servers);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Generates the JmxTrans config map
     *
     * @param spec The JmxTransSpec that was defined by the user
     * @param numOfBrokers number of kafka brokers
     * @return the config map that mounts the jmx config
     */
    public ConfigMap generateJmxTransConfigMap(JmxTransSpec spec, int numOfBrokers) {
        Map<String, String> data = new HashMap<>();
        String jmxConfig = generateJMXConfig(spec, numOfBrokers);
        data.put(JMXTRANS_CONFIGMAP_KEY, jmxConfig);
        configMapName = jmxTransConfigName(clusterName);
        return createConfigMap(jmxTransConfigName(clusterName), data);
    }

    public List<Volume> getVolumes() {
        return singletonList(createConfigMapVolume(JMXTRANS_VOLUME_NAME, configMapName));
    }

    private List<VolumeMount> getVolumeMounts() {
        return singletonList(createVolumeMount(JMXTRANS_VOLUME_NAME, JMX_FILE_PATH));
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>();
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withReadinessProbe(ModelUtils.kafkaJmxMetricsBrokerProbe(readinessProbeOptions, clusterName))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
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

        addContainerEnvsToExistingEnvs(varList, Collections.emptyList());

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

    /**
     * Wrapper class used to create the overall config file
     * Servers: A list of servers and what they will query and output
     */
    @SuppressWarnings("SE_BAD_FIELD_INNER_CLASS")
    static class Servers implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<Server> servers;
    }

    /**
     * Wrapper class used to create the per broker JmxTrans queries and which remote hosts it will output to
     * host: references which broker the JmxTrans will read from
     * port: port of the host
     * queries: what queries are passed in
     */
    @SuppressWarnings("SE_BAD_FIELD_INNER_CLASS")
    static class Server implements Serializable {
        private static final long serialVersionUID = 1L;
        public String host;
        public int port;
        @JsonProperty("queries")
        public List<Queries> queriesTemplate;
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public String username;
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public String password;

    }

    /**
     * Wrapper class used to create the JmxTrans queries and which remote hosts to output the results to
     * obj: references what Kafka MBean to reference
     * attr: an array of what MBean properties to target
     * outputDefinitionTemplates: Which hosts and how to output to the hosts
     */
    @SuppressWarnings("SE_BAD_FIELD_INNER_CLASS")
    static class Queries implements Serializable {
        private static final long serialVersionUID = 1L;
        public String obj;
        public List<String> attr;
        @JsonProperty("outputWriters")
        public List<OutputWriter> outputDefinitionTemplates = null;
    }

    /**
     * Wrapper class used specify which remote host to output to and in what format to push it in
     * atClasses: the format of the data to push to remote host
     * host: The host of the remote host to push to
     * port: The port of the remote host to push to
     * flushDelayInSeconds: how often to push the data in seconds
     */
    @SuppressWarnings("SE_BAD_FIELD_INNER_CLASS")
    static class OutputWriter implements Serializable {
        private static final long serialVersionUID = 1L;
        @JsonProperty("@class")
        public String atClasses;
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public String host;
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public int port;
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public int flushDelayInSeconds;
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public List<String> typeNames;
    }

    private Server convertSpecToServers(JmxTransSpec spec, String brokerServiceName, int metricsPort) {
        Server server = new Server();
        server.host = brokerServiceName;
        server.port = metricsPort;
        if (isJmxAuthenticated()) {
            server.username = "${kafka.username}";
            server.password = "${kafka.password}";
        }
        List<Queries> queriesTemplates = new ArrayList<>();
        for (JmxTransQueryTemplate queryTemplate : spec.getQueries()) {
            Queries query = new Queries();
            query.obj = queryTemplate.getTargetMBean();
            query.attr = queryTemplate.getAttributes();
            query.outputDefinitionTemplates = new ArrayList<>();

            for (JmxTransOutputDefinitionTemplate outputDefinitionTemplate : spec.getOutputDefinitionTemplates()) {
                if (queryTemplate.getOutputs().contains(outputDefinitionTemplate.getName())) {
                    OutputWriter outputWriter = new OutputWriter();
                    outputWriter.atClasses = outputDefinitionTemplate.getOutputType();
                    if (outputDefinitionTemplate.getHost() != null) {
                        outputWriter.host = outputDefinitionTemplate.getHost();
                    }
                    if (outputDefinitionTemplate.getPort() != null) {
                        outputWriter.port = outputDefinitionTemplate.getPort();
                    }
                    if (outputDefinitionTemplate.getFlushDelay()  != null) {
                        outputWriter.flushDelayInSeconds = outputDefinitionTemplate.getFlushDelay();
                    }
                    outputWriter.typeNames = outputDefinitionTemplate.getTypeNames();
                    query.outputDefinitionTemplates.add(outputWriter);
                }
            }

            queriesTemplates.add(query);
        }
        server.queriesTemplate = queriesTemplates;
        return server;
    }

}