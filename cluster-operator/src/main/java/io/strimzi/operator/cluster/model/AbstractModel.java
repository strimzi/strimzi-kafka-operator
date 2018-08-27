/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSource;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetUpdateStrategyBuilder;
import io.strimzi.api.kafka.model.CpuMemory;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.common.model.Labels;

import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.Quantities.normalizeCpu;
import static io.strimzi.api.kafka.model.Quantities.normalizeMemory;
import static java.util.Arrays.asList;

public abstract class AbstractModel {

    protected static final Logger log = LogManager.getLogger(AbstractModel.class.getName());

    // the Kubernetes service DNS domain is customizable on cluster creation but it's "cluster.local" by default
    // there is no clean way to get it from a running application so we are passing it through an env var
    public static final String KUBERNETES_SERVICE_DNS_DOMAIN =
            System.getenv().getOrDefault("KUBERNETES_SERVICE_DNS_DOMAIN", "cluster.local");

    protected static final int CERTS_EXPIRATION_DAYS = 365;

    private static final String VOLUME_MOUNT_HACK_IMAGE = "busybox";
    protected static final String VOLUME_MOUNT_HACK_NAME = "volume-mount-hack";
    private static final Long VOLUME_MOUNT_HACK_GROUPID = 1001L;

    public static final String ANCILLARY_CM_KEY_METRICS = "metrics-config.yml";
    public static final String ANCILLARY_CM_KEY_LOG_CONFIG = "log4j.properties";
    public static final String ENV_VAR_DYNAMIC_HEAP_FRACTION = "DYNAMIC_HEAP_FRACTION";
    public static final String ENV_VAR_KAFKA_HEAP_OPTS = "KAFKA_HEAP_OPTS";
    public static final String ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS = "KAFKA_JVM_PERFORMANCE_OPTS";
    public static final String ENV_VAR_DYNAMIC_HEAP_MAX = "DYNAMIC_HEAP_MAX";
    public static final String NETWORK_POLICY_KEY_SUFFIX = "-network-policy";

    private static final String DELETE_CLAIM_ANNOTATION =
            ClusterOperator.STRIMZI_CLUSTER_OPERATOR_DOMAIN + "/delete-claim";

    protected final String cluster;
    protected final String namespace;
    protected final Labels labels;

    // Docker image configuration
    protected String image;
    // Number of replicas
    protected int replicas;

    protected String readinessPath;
    protected int readinessTimeout;
    protected int readinessInitialDelay;
    protected String livenessPath;
    protected int livenessTimeout;
    protected int livenessInitialDelay;

    protected String serviceName;
    protected String headlessServiceName;
    protected String name;

    protected static final int METRICS_PORT = 9404;
    protected static final String METRICS_PORT_NAME = "metrics";
    protected boolean isMetricsEnabled;

    protected Iterable<Map.Entry<String, Object>> metricsConfig;
    protected String ancillaryConfigName;
    protected String logConfigName;

    protected Storage storage;

    protected AbstractConfiguration configuration;

    protected String mountPath;
    public static final String VOLUME_NAME = "data";
    protected String logAndMetricsConfigMountPath;

    protected String logAndMetricsConfigVolumeName;

    private JvmOptions jvmOptions;
    private Resources resources;
    private Affinity userAffinity;
    private List<Toleration> tolerations;

    protected Map validLoggerFields;
    private String[] validLoggerValues = new String[]{"INFO", "ERROR", "WARN", "TRACE", "DEBUG", "FATAL", "OFF" };
    private Logging logging;

    protected CertAndKey clusterCA;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected AbstractModel(String namespace, String cluster, Labels labels) {
        this.cluster = cluster;
        this.namespace = namespace;
        this.labels = labels.withCluster(cluster);
    }

    public Labels getLabels() {
        return labels;
    }

    public int getReplicas() {
        return replicas;
    }

    protected void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    protected void setImage(String image) {
        this.image = image;
    }

    protected void setReadinessTimeout(int readinessTimeout) {
        this.readinessTimeout = readinessTimeout;
    }

    protected void setReadinessInitialDelay(int readinessInitialDelay) {
        this.readinessInitialDelay = readinessInitialDelay;
    }

    protected void setLivenessTimeout(int livenessTimeout) {
        this.livenessTimeout = livenessTimeout;
    }

    protected void setLivenessInitialDelay(int livenessInitialDelay) {
        this.livenessInitialDelay = livenessInitialDelay;
    }

    /**
     * Returns the Docker image which should be used by this cluster
     *
     * @return
     */
    public String getName() {
        return name;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getHeadlessServiceName() {
        return headlessServiceName;
    }


    protected Map<String, String> getSelectorLabels() {
        return labels.withName(name).strimziLabels().toMap();
    }

    protected Map<String, String> getLabelsWithName() {
        return getLabelsWithName(name);
    }

    protected Map<String, String> getLabelsWithName(String name) {
        return labels.withName(name).toMap();
    }

    public boolean isMetricsEnabled() {
        return isMetricsEnabled;
    }

    protected void setMetricsEnabled(boolean isMetricsEnabled) {
        this.isMetricsEnabled = isMetricsEnabled;
    }


    protected abstract String getDefaultLogConfigFileName();

    /**
     * Returns map with all available loggers for current pod and default values.
     * @return
     */
    protected Properties getDefaultLogConfig() {
        Properties properties = new Properties();
        String defaultLogConfigFileName = getDefaultLogConfigFileName();
        try {
            properties = getDefaultLoggingProperties(defaultLogConfigFileName);
        } catch (IOException e) {
            log.warn("Unable to read default log config from '{}'", defaultLogConfigFileName);
        }
        return properties;
    }

    /**
     * Takes resource file containing default log4j properties and returns it as a Properties.
     * @param defaultConfigResourceFileName name of file, where default log4j properties are stored
     * @return
     */
    protected Properties getDefaultLoggingProperties(String defaultConfigResourceFileName) throws IOException {
        Properties defaultSettings = new Properties();
        InputStream is = null;
        try {
            is = AbstractModel.class.getResourceAsStream("/" + defaultConfigResourceFileName);
            defaultSettings.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return defaultSettings;
    }

    /**
     * Transforms map to log4j properties file format
     * @param newSettings map with properties
     * @return
     */
    protected static String createPropertiesString(Properties newSettings) {
        StringWriter sw = new StringWriter();
        try {
            newSettings.store(sw, "Do not change this generated file. Logging can be configured in the corresponding kubernetes/openshift resource.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // remove date comment, because it is updated with each reconciliation which leads to restarting pods
        return sw.toString().replaceAll("#[A-Za-z]+ [A-Za-z]+ [0-9]+ [0-9]+:[0-9]+:[0-9]+ [A-Z]+ [0-9]+", "");
    }

    public Logging getLogging() {
        return logging;
    }

    protected void setLogging(Logging logging) {
        this.logging = logging;
    }

    public String parseLogging(Logging logging, ConfigMap externalCm) {
        if (logging instanceof InlineLogging) {
            // validate all entries
            ((InlineLogging) logging).getLoggers().forEach((key, tmpEntry) -> {
                if (validLoggerFields.containsKey(key)) {
                    // correct logger, test appender appearance for log4j.rootLogger
                    String appender = tmpEntry.replaceAll(" ", "");
                    if (key.equals("log4j.rootLogger") && !appender.contains(",CONSOLE")) {
                        ((InlineLogging) logging).getLoggers().replace(key, tmpEntry + ", CONSOLE");
                        log.warn("Appender for {} was not set. Using \"{}: {}, CONSOLE\"", key, key, tmpEntry);
                    }
                } else {
                    // incorrect logger
                    log.warn(key + " is not a valid logger");
                    return;
                }
                if (key.toString().contains("log4j.appender.CONSOLE")) {
                    log.warn("You cannot set appender");
                    return;
                }
                if ((asList(validLoggerValues).contains(tmpEntry.toString().replaceAll(",[ ]+CONSOLE", ""))) || (asList(validLoggerValues).contains(tmpEntry))) {
                    // correct value
                } else {
                    Pattern p = Pattern.compile("\\$\\{(.*)\\}, ([A-Z]+)");
                    Matcher m = p.matcher(tmpEntry.toString());

                    String logger = "";
                    String value = "";
                    boolean regexMatch = false;
                    while (m.find()) {
                        logger = m.group(1);
                        value = m.group(2);
                        regexMatch = true;
                    }
                    if (regexMatch) {
                        if (!validLoggerFields.containsKey(logger)) {
                            log.warn(logger + " is not a valid logger");
                            return;
                        }
                        if (!value.equals("CONSOLE")) {
                            log.warn(value + " is not a valid value.");
                            return;
                        }
                    } else {
                        log.warn(tmpEntry + " is not a valid value. Use one of " + Arrays.toString(validLoggerValues));
                        return;
                    }
                }
            });
            // update fields otherwise use default values
            Properties newSettings = getDefaultLogConfig();
            newSettings.putAll(((InlineLogging) logging).getLoggers());
            return createPropertiesString(newSettings);
        } else if (logging instanceof ExternalLogging) {
            if (externalCm != null) {
                return externalCm.getData().get(getAncillaryConfigMapKeyLogConfig());
            } else {
                log.warn("Configmap " + ((ExternalLogging) getLogging()).getName() + " does not exist. Default settings are used");
                return createPropertiesString(getDefaultLogConfig());
            }

        } else {
            // field is not in the cluster CM
            return createPropertiesString(getDefaultLogConfig());

        }
    }

    /**
     * Generates a metrics and logging ConfigMap according to configured defaults
     * @return The generated ConfigMap
     */
    public ConfigMap generateMetricsAndLogConfigMap(ConfigMap cm) {
        Map<String, String> data = new HashMap<>();
        data.put(getAncillaryConfigMapKeyLogConfig(), parseLogging(getLogging(), cm));
        if (isMetricsEnabled()) {
            HashMap m = new HashMap();
            for (Map.Entry<String, Object> entry : getMetricsConfig()) {
                m.put(entry.getKey(), entry.getValue());
            }
            data.put(ANCILLARY_CM_KEY_METRICS, new JsonObject(m).toString());
        }

        return createConfigMap(getAncillaryConfigName(), data);
    }

    public String getLogConfigName() {
        return logConfigName;
    }

    /**
     * Sets name of field in cluster config map, where logging configuration is stored
     * @param logConfigName
     */
    protected void setLogConfigName(String logConfigName) {
        this.logConfigName = logConfigName;
    }

    protected Iterable<Map.Entry<String, Object>>  getMetricsConfig() {
        return metricsConfig;
    }

    protected void setMetricsConfig(Iterable<Map.Entry<String, Object>> metricsConfig) {
        this.metricsConfig = metricsConfig;
    }

    /**
     * Returns name of config map used for storing metrics and logging configuration
     * @return
     */
    public String getAncillaryConfigName() {
        return ancillaryConfigName;
    }

    protected void setMetricsConfigName(String metricsAndLogsConfigName) {
        this.ancillaryConfigName = metricsAndLogsConfigName;
    }

    protected List<EnvVar> getEnvVars() {
        return null;
    }

    public Storage getStorage() {
        return storage;
    }

    protected void setStorage(Storage storage) {
        this.storage = storage;
    }

    /**
     * Returns the Configuration object which is passed to the cluster as EnvVar
     *
     * @return  Configuration object with cluster configuration
     */
    public AbstractConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Set the configuration object which might be passed to the cluster as EnvVar
     *
     * @param configuration Configuration object with cluster configuration
     */
    protected void setConfiguration(AbstractConfiguration configuration) {
        this.configuration = configuration;
    }

    public String getVolumeName() {
        return this.VOLUME_NAME;
    }

    public String getImage() {
        return this.image;
    }

    /**
     * @return the service account used by the deployed cluster for Kubernetes/OpenShift API operations
     */
    protected String getServiceAccountName() {
        return null;
    }

    /**
     * @return the cluster name
     */
    public String getCluster() {
        return cluster;
    }

    public String getPersistentVolumeClaimName(int podId) {
        return getPersistentVolumeClaimName(name,  podId);
    }

    public static String getPersistentVolumeClaimName(String kafkaClusterName, int podId) {
        return VOLUME_NAME + "-" + kafkaClusterName + "-" + podId;
    }

    public String getPodName(int podId) {
        return name + "-" + podId;
    }

    /**
     * Sets the affinity as configured by the user in the cluster CR
     * @param affinity
     */
    protected void setUserAffinity(Affinity affinity) {
        this.userAffinity = affinity;
    }

    /**
     * Gets the affinity as configured by the user in the cluster CR
     */
    protected Affinity getUserAffinity() {
        return this.userAffinity;
    }

    /**
     * Gets the tolerations as configured by the user in the cluster CR
     */
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    /**
     * Sets the tolerations as configured by the user in the cluster CR
     *
     * @param tolerations
     */
    public void setTolerations(List<Toleration> tolerations) {
        this.tolerations = tolerations;
    }

    /**
     * Gets the affinity to use in a template Pod (in a StatefulSet, or Deployment).
     * In general this may include extra rules than just the {@link #userAffinity}.
     * By default it is just the {@link #userAffinity}.
     */
    protected Affinity getMergedAffinity() {
        return getUserAffinity();
    }

    /**
     * @return a list of init containers to add to the StatefulSet/Deployment
     */
    protected List<Container> getInitContainers() {
        return null;
    }

    /**
     * @return a list of containers to add to the StatefulSet/Deployment
     */
    protected abstract List<Container> getContainers();

    protected VolumeMount createVolumeMount(String name, String path) {
        VolumeMount volumeMount = new VolumeMountBuilder()
                .withName(name)
                .withMountPath(path)
                .build();
        log.trace("Created volume mount {}", volumeMount);
        return volumeMount;
    }

    protected ContainerPort createContainerPort(String name, int port, String protocol) {
        ContainerPort containerPort = new ContainerPortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withContainerPort(port)
                .build();
        log.trace("Created container port {}", containerPort);
        return containerPort;
    }

    protected ServicePort createServicePort(String name, int port, int targetPort, String protocol) {
        ServicePort servicePort = new ServicePortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withPort(port)
                .withNewTargetPort(targetPort)
                .build();
        log.trace("Created service port {}", servicePort);
        return servicePort;
    }

    protected PersistentVolumeClaim createPersistentVolumeClaim(String name) {

        PersistentClaimStorage storage = (PersistentClaimStorage) this.storage;
        Map<String, Quantity> requests = new HashMap<>();
        requests.put("storage", new Quantity(storage.getSize(), null));
        LabelSelector selector = null;
        if (storage.getSelector() != null && !storage.getSelector().isEmpty()) {
            selector = new LabelSelector(null, storage.getSelector());
        }

        PersistentVolumeClaimBuilder pvcb = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withAccessModes("ReadWriteOnce")
                .withNewResources()
                .withRequests(requests)
                .endResources()
                .withStorageClassName(storage.getStorageClass())
                .withSelector(selector)
                .endSpec();

        return pvcb.build();
    }

    protected Volume createEmptyDirVolume(String name) {
        Volume volume = new VolumeBuilder()
                .withName(name)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
        log.trace("Created emptyDir Volume named '{}'", name);
        return volume;
    }

    protected Volume createConfigMapVolume(String name, String configMapName) {

        ConfigMapVolumeSource configMapVolumeSource = new ConfigMapVolumeSourceBuilder()
                .withName(configMapName)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(name)
                .withConfigMap(configMapVolumeSource)
                .build();
        log.trace("Created configMap Volume named '{}' with source configMap '{}'", name, configMapName);
        return volume;
    }

    protected ConfigMap createConfigMap(String name, Map<String, String> data) {

        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                .endMetadata()
                .withData(data)
                .build();
    }

    protected Volume createSecretVolume(String name, String secretName) {

        SecretVolumeSource secretVolumeSource = new SecretVolumeSourceBuilder()
                .withSecretName(secretName)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(name)
                .withSecret(secretVolumeSource)
                .build();
        log.trace("Created secret Volume named '{}' with source secret '{}'", name, secretName);
        return volume;
    }

    protected Secret createSecret(String name, Map<String, String> data) {

        Secret s = new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                .endMetadata()
                .withData(data)
                .build();

        return s;
    }

    protected Probe createExecProbe(String command, int initialDelay, int timeout) {
        Probe probe = new ProbeBuilder().withNewExec()
                .withCommand(command)
                .endExec()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
        log.trace("Created exec probe {}", probe);
        return probe;
    }

    protected Probe createTcpSocketProbe(int port, int initialDelay, int timeout) {
        Probe probe = new ProbeBuilder()
                .withNewTcpSocket()
                    .withNewPort()
                        .withIntVal(port)
                    .endPort()
                .endTcpSocket()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
        log.trace("Created TCP socket probe {}", probe);
        return probe;
    }

    protected Probe createHttpProbe(String path, String port, int initialDelay, int timeout) {
        Probe probe = new ProbeBuilder().withNewHttpGet()
                .withPath(path)
                .withNewPort(port)
                .endHttpGet()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
        log.trace("Created http probe {}", probe);
        return probe;
    }

    protected Service createService(String name, List<ServicePort> ports) {
        return createService(name, ports, Collections.emptyMap());
    }

    protected Service createService(String type, List<ServicePort> ports,  Map<String, String> annotations) {
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(serviceName)
                    .withLabels(getLabelsWithName(serviceName))
                    .withNamespace(namespace)
                    .withAnnotations(annotations)
                .endMetadata()
                .withNewSpec()
                    .withType(type)
                    .withSelector(getSelectorLabels())
                    .withPorts(ports)
                .endSpec()
                .build();
        log.trace("Created service {}", service);
        return service;
    }

    protected Service createHeadlessService(List<ServicePort> ports) {
        return createHeadlessService(ports, Collections.emptyMap());
    }

    protected Service createHeadlessService(List<ServicePort> ports, Map<String, String> annotations) {
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(headlessServiceName)
                    .withLabels(getLabelsWithName(headlessServiceName))
                    .withNamespace(namespace)
                    .withAnnotations(annotations)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withClusterIP("None")
                    .withSelector(getSelectorLabels())
                    .withPorts(ports)
                .endSpec()
                .build();
        log.trace("Created headless service {}", service);
        return service;
    }

    protected StatefulSet createStatefulSet(
            List<Volume> volumes,
            List<PersistentVolumeClaim> volumeClaims,
            List<VolumeMount> volumeMounts,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            boolean isOpenShift) {

        Map<String, String> annotations = new HashMap<>();

        annotations.put(DELETE_CLAIM_ANNOTATION,
                String.valueOf(storage instanceof PersistentClaimStorage
                        && ((PersistentClaimStorage) storage).isDeleteClaim()));


        List<Container> initContainersInternal = new ArrayList<>();
        PodSecurityContext securityContext = null;
        // if a persistent volume claim is requested and the running cluster is a Kubernetes one
        // there is an hack on volume mounting which needs an "init-container"
        if (this.storage instanceof PersistentClaimStorage && !isOpenShift) {

            String chown = String.format("chown -R %d:%d %s",
                    AbstractModel.VOLUME_MOUNT_HACK_GROUPID,
                    AbstractModel.VOLUME_MOUNT_HACK_GROUPID,
                    volumeMounts.get(0).getMountPath());

            Container initContainer = new ContainerBuilder()
                    .withName(AbstractModel.VOLUME_MOUNT_HACK_NAME)
                    .withImage(AbstractModel.VOLUME_MOUNT_HACK_IMAGE)
                    .withVolumeMounts(volumeMounts.get(0))
                    .withCommand("sh", "-c", chown)
                    .build();

            initContainersInternal.add(initContainer);

            securityContext = new PodSecurityContextBuilder()
                    .withFsGroup(AbstractModel.VOLUME_MOUNT_HACK_GROUPID)
                    .build();
        }
        // add all the other init containers provided by the specific model implementation
        if (initContainers != null) {
            initContainersInternal.addAll(initContainers);
        }

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithName())
                    .withNamespace(namespace)
                    .withAnnotations(annotations)
                .endMetadata()
                .withNewSpec()
                    .withPodManagementPolicy("Parallel")
                    .withUpdateStrategy(new StatefulSetUpdateStrategyBuilder().withType("OnDelete").build())
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels()).build())
                    .withServiceName(headlessServiceName)
                    .withReplicas(replicas)
                    .withNewTemplate()
                        .withNewMetadata()
                            .withName(name)
                            .withLabels(getLabelsWithName())
                        .endMetadata()
                        .withNewSpec()
                            .withServiceAccountName(getServiceAccountName())
                            .withAffinity(affinity)
                            .withSecurityContext(securityContext)
                            .withInitContainers(initContainersInternal)
                            .withContainers(containers)
                            .withVolumes(volumes)
                            .withTolerations(getTolerations())
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(volumeClaims)
                .endSpec()
                .build();

        return statefulSet;
    }

    protected Deployment createDeployment(
            DeploymentStrategy updateStrategy,
            Map<String, String> deploymentAnnotations,
            Map<String, String> podAnnotations,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<Volume> volumes) {

        Deployment dep = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithName())
                    .withNamespace(namespace)
                    .withAnnotations(deploymentAnnotations)
                .endMetadata()
                .withNewSpec()
                    .withStrategy(updateStrategy)
                    .withReplicas(replicas)
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels()).build())
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(getLabelsWithName())
                            .withAnnotations(podAnnotations)
                        .endMetadata()
                        .withNewSpec()
                            .withAffinity(affinity)
                            .withServiceAccountName(getServiceAccountName())
                            .withInitContainers(initContainers)
                            .withContainers(containers)
                            .withVolumes(volumes)
                            .withTolerations(getTolerations())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        return dep;
    }

    /**
     * Build an environment variable instance with the provided name and value
     *
     * @param name The name of the environment variable
     * @param value The value of the environment variable
     * @return The environment variable instance
     */
    protected static EnvVar buildEnvVar(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    /**
     * Build an environment variable instance with the provided name from a field reference
     * using Downward API
     *
     * @param name The name of the environment variable
     * @param field The field path from which getting the value
     * @return The environment variable instance
     */
    protected static EnvVar buildEnvVarFromFieldRef(String name, String field) {

        EnvVarSource envVarSource = new EnvVarSourceBuilder()
                .withNewFieldRef()
                    .withFieldPath(field)
                .endFieldRef()
                .build();

        return new EnvVarBuilder().withName(name).withValueFrom(envVarSource).build();
    }

    /**
     * Gets the given container's environment.
     */
    public static Map<String, String> containerEnvVars(Container container) {
        return container.getEnv().stream().collect(
            Collectors.toMap(EnvVar::getName, EnvVar::getValue,
                // On duplicates, last in wins
                (u, v) -> v));
    }

    public static ResourceRequirements resources(Resources resources) {
        if (resources != null) {
            ResourceRequirementsBuilder builder = new ResourceRequirementsBuilder();
            CpuMemory limits = resources.getLimits();
            if (limits != null
                    && limits.milliCpuAsInt() > 0) {
                builder.addToLimits("cpu", new Quantity(normalizeCpu(limits.getMilliCpu())));
            }
            if (limits != null
                    && limits.memoryAsLong() > 0) {
                builder.addToLimits("memory", new Quantity(normalizeMemory(limits.getMemory())));
            }
            CpuMemory requests = resources.getRequests();
            if (requests != null
                    && requests.milliCpuAsInt() > 0) {
                builder.addToRequests("cpu", new Quantity(normalizeCpu(requests.getMilliCpu())));
            }
            if (requests != null
                    && requests.memoryAsLong() > 0) {
                builder.addToRequests("memory", new Quantity(normalizeMemory(requests.getMemory())));
            }
            return builder.build();
        }
        return null;
    }

    public void setResources(Resources resources) {
        this.resources = resources;
    }

    public Resources getResources() {
        return resources;
    }

    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }

    /**
     * Adds KAFKA_HEAP_OPTS variable to the EnvVar list if any heap related options were specified.
     *
     * @param envVars List of Environment Variables
     */
    protected void heapOptions(List<EnvVar> envVars, double dynamicHeapFraction, long dynamicHeapMaxBytes) {
        StringBuilder kafkaHeapOpts = new StringBuilder();
        String xms = jvmOptions != null ? jvmOptions.getXms() : null;

        if (xms != null) {
            kafkaHeapOpts.append("-Xms").append(xms);
        }

        String xmx = jvmOptions != null ? jvmOptions.getXmx() : null;
        if (xmx != null) {
            // Honour explicit max heap
            kafkaHeapOpts.append(' ').append("-Xmx").append(xmx);
        } else {
            // Otherwise delegate to the container to figure out
            // Using whatever cgroup memory limit has been set by the k8s infra
            envVars.add(buildEnvVar(ENV_VAR_DYNAMIC_HEAP_FRACTION, Double.toString(dynamicHeapFraction)));
            if (dynamicHeapMaxBytes > 0) {
                envVars.add(buildEnvVar(ENV_VAR_DYNAMIC_HEAP_MAX, Long.toString(dynamicHeapMaxBytes)));
            }
        }
        String trim = kafkaHeapOpts.toString().trim();
        if (!trim.isEmpty()) {
            envVars.add(buildEnvVar(ENV_VAR_KAFKA_HEAP_OPTS, trim));
        }
    }

    /**
     * Adds KAFKA_JVM_PERFORMANCE_OPTS variable to the EnvVar list if any performance related options were specified.
     *
     * @param envVars List of Environment Variables
     */
    protected void jvmPerformanceOptions(List<EnvVar> envVars) {
        StringBuilder jvmPerformanceOpts = new StringBuilder();
        Boolean server = jvmOptions != null ? jvmOptions.isServer() : null;

        if (server != null && server) {
            jvmPerformanceOpts.append("-server");
        }

        Map<String, String> xx = jvmOptions != null ? jvmOptions.getXx() : null;
        if (xx != null) {
            xx.forEach((k, v) -> {
                jvmPerformanceOpts.append(' ').append("-XX:");

                if ("true".equalsIgnoreCase(v))   {
                    jvmPerformanceOpts.append("+").append(k);
                } else if ("false".equalsIgnoreCase(v)) {
                    jvmPerformanceOpts.append("-").append(k);
                } else  {
                    jvmPerformanceOpts.append(k).append("=").append(v);
                }
            });
        }

        String trim = jvmPerformanceOpts.toString().trim();
        if (!trim.isEmpty()) {
            envVars.add(buildEnvVar(ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS, trim));
        }
    }

    /**
     * Decode from Base64 a keyed value from a Secret
     *
     * @param secret Secret from which decoding the value
     * @param key Key of the value to decode
     * @return decoded value
     */
    protected byte[] decodeFromSecret(Secret secret, String key) {
        return Base64.getDecoder().decode(secret.getData().get(key));
    }

    /**
     * Copy already existing certificates from provided Secret based on number of effective replicas
     * and maybe generate new ones for new replicas (i.e. scale-up)
     *
     * @param certManager CertManager instance for handling certificates creation
     * @param secret The Secret from which getting already existing certificates
     * @param replicasInSecret How many certificates are in the Secret
     * @param caCert CA certificate to use for signing new certificates
     * @param podName A function for resolving the Pod name
     * @return Collection with certificates
     * @throws IOException
     */
    protected Map<String, CertAndKey> maybeCopyOrGenerateCerts(CertManager certManager, Optional<Secret> secret, int replicasInSecret, CertAndKey caCert, BiFunction<String, Integer, String> podName) throws IOException {

        Map<String, CertAndKey> certs = new HashMap<>();

        // copying the minimum number of certificates already existing in the secret
        // scale up -> it will copy all certificates
        // scale down -> it will copy just the requested number of replicas
        for (int i = 0; i < Math.min(replicasInSecret, replicas); i++) {
            log.debug("{} already exists", podName.apply(cluster, i));
            certs.put(
                    podName.apply(cluster, i),
                    new CertAndKey(
                            decodeFromSecret(secret.get(), podName.apply(cluster, i) + ".key"),
                            decodeFromSecret(secret.get(), podName.apply(cluster, i) + ".crt")));
        }

        File brokerCsrFile = File.createTempFile("tls", "broker-csr");
        File brokerKeyFile = File.createTempFile("tls", "broker-key");
        File brokerCertFile = File.createTempFile("tls", "broker-cert");

        // generate the missing number of certificates
        // scale up -> generate new certificates for added replicas
        // scale down -> does nothing
        for (int i = replicasInSecret; i < replicas; i++) {
            log.debug("{} to generate", podName.apply(cluster, i));

            Subject sbj = new Subject();
            sbj.setOrganizationName("io.strimzi");
            sbj.setCommonName(getName());

            Map<String, String> sbjAltNames = new HashMap<>();
            sbjAltNames.put("DNS.1", getServiceName());
            sbjAltNames.put("DNS.2", String.format("%s.%s.svc.%s", getServiceName(), namespace, KUBERNETES_SERVICE_DNS_DOMAIN));
            sbjAltNames.put("DNS.3", String.format("%s.%s.%s.svc.%s", podName.apply(cluster, i), getHeadlessServiceName(), namespace, KUBERNETES_SERVICE_DNS_DOMAIN));
            sbj.setSubjectAltNames(sbjAltNames);

            certManager.generateCsr(brokerKeyFile, brokerCsrFile, sbj);
            certManager.generateCert(brokerCsrFile, caCert.key(), caCert.cert(), brokerCertFile, sbj, CERTS_EXPIRATION_DAYS);

            certs.put(podName.apply(cluster, i),
                    new CertAndKey(Files.readAllBytes(brokerKeyFile.toPath()), Files.readAllBytes(brokerCertFile.toPath())));
        }

        if (!brokerCsrFile.delete()) {
            log.warn("{} cannot be deleted", brokerCsrFile.getName());
        }
        if (!brokerKeyFile.delete()) {
            log.warn("{} cannot be deleted", brokerKeyFile.getName());
        }
        if (!brokerCertFile.delete()) {
            log.warn("{} cannot be deleted", brokerCertFile.getName());
        }

        return certs;
    }

    public static boolean deleteClaim(StatefulSet ss) {
        if (!ss.getSpec().getVolumeClaimTemplates().isEmpty()
                && ss.getMetadata().getAnnotations() != null) {
            return Boolean.valueOf(ss.getMetadata().getAnnotations().computeIfAbsent(DELETE_CLAIM_ANNOTATION, s -> "false"));
        } else {
            return false;
        }
    }

    /**
     * Generated a Map with Prometheus annotations
     *
     * @return Map with Prometheus annotations using the default port (9404) and path (/metrics)
     */
    protected Map<String, String> getPrometheusAnnotations()    {
        Map<String, String> annotations = new HashMap<String, String>(3);
        annotations.put("prometheus.io/port", String.valueOf(METRICS_PORT));
        annotations.put("prometheus.io/scrape", "true");
        annotations.put("prometheus.io/path", "/metrics");

        return annotations;
    }

    String getAncillaryConfigMapKeyLogConfig() {
        return ANCILLARY_CM_KEY_LOG_CONFIG;
    }

    public static String getClusterCaName(String cluster)  {
        return cluster + "-cluster-ca";
    }
}
