package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.*;
import io.strimzi.controller.cluster.ClusterController;
import io.strimzi.controller.cluster.K8SUtils;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaConnectCluster extends AbstractCluster {

    public static final String TYPE = "kafka-connect";

    // Port configuration
    private final int restApiPort = 8083;
    private final String restApiPortName = "rest-api";

    private static String NAME_SUFFIX = "-connect";

    // Kafka Connect configuration
    private String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private String groupId = DEFAULT_GROUP_ID;
    private String keyConverter = DEFAULT_KEY_CONVERTER;
    private Boolean keyConverterSchemasEnable = DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE;
    private String valueConverter = DEFAULT_VALUE_CONVERTER;
    private Boolean valueConverterSchemasEnable = DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE;
    private int configStorageReplicationFactor = DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR;
    private int offsetStorageReplicationFactor = DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR;
    private int statusStorageReplicationFactor = DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR;

    // S2I
    private Source2Image s2i = null;

    // Configuration defaults
    private static String DEFAULT_IMAGE = "strimzi/kafka-connect:latest";
    private static int DEFAULT_REPLICAS = 3;
    private static int DEFAULT_HEALTHCHECK_DELAY = 60;
    private static int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    // Kafka Connect configuration defaults
    private static String DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092";
    private static String DEFAULT_GROUP_ID = "connect-cluster";
    private static String DEFAULT_KEY_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static Boolean DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE = true;
    private static String DEFAULT_VALUE_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static Boolean DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE = true;
    private static int DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR = 3;
    private static int DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR = 3;
    private static int DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR = 3;

    // Configuration keys
    private static String KEY_IMAGE = "image";
    private static String KEY_REPLICAS = "nodes";
    private static String KEY_HEALTHCHECK_DELAY = "healthcheck-delay";
    private static String KEY_HEALTHCHECK_TIMEOUT = "healthcheck-timeout";
    private static String KEY_S2I = "s2i";

    // Kafka Connect configuration keys
    private static String KEY_BOOTSTRAP_SERVERS = "KAFKA_CONNECT_BOOTSTRAP_SERVERS";
    private static String KEY_GROUP_ID = "KAFKA_CONNECT_GROUP_ID";
    private static String KEY_KEY_CONVERTER = "KAFKA_CONNECT_KEY_CONVERTER";
    private static String KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE = "KAFKA_CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE";
    private static String KEY_VALUE_CONVERTER = "KAFKA_CONNECT_VALUE_CONVERTER";
    private static String KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE = "KAFKA_CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE";
    private static String KEY_CONFIG_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR";
    private static String KEY_OFFSET_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR";
    private static String KEY_STATUS_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_STATUS_STORAGE_REPLICATION_FACTOR";

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    private KafkaConnectCluster(String namespace, String cluster) {

        super(namespace, cluster);
        this.name = cluster + KafkaConnectCluster.NAME_SUFFIX;
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
    }

    /**
     * Create a Kafka Connect cluster from the related ConfigMap resource
     *
     * @param cm    ConfigMap with cluster configuration
     * @param k8s   K8SUtils instance, which is needed to check whether we are on OpenShift due to S2I support
     * @return  Kafka Connect cluster instance
     */
    public static KafkaConnectCluster fromConfigMap(ConfigMap cm, K8SUtils k8s) {
        KafkaConnectCluster kafkaConnect = new KafkaConnectCluster(cm.getMetadata().getNamespace(), cm.getMetadata().getName());

        kafkaConnect.setLabels(cm.getMetadata().getLabels());

        kafkaConnect.setReplicas(Integer.parseInt(cm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafkaConnect.setImage(cm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafkaConnect.setHealthCheckInitialDelay(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafkaConnect.setHealthCheckTimeout(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafkaConnect.setBootstrapServers(cm.getData().getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
        kafkaConnect.setGroupId(cm.getData().getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID));
        kafkaConnect.setKeyConverter(cm.getData().getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER));
        kafkaConnect.setKeyConverterSchemasEnable(Boolean.parseBoolean(cm.getData().getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setValueConverter(cm.getData().getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER));
        kafkaConnect.setValueConverterSchemasEnable(Boolean.parseBoolean(cm.getData().getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setConfigStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setOffsetStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setStatusStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR))));

        if (cm.getData().containsKey(KEY_S2I)) {
            if (k8s.isOpenShift()) {
                JsonObject config = new JsonObject(cm.getData().get(KEY_S2I));
                if (config.getBoolean(Source2Image.KEY_ENABLED, false)) {
                    kafkaConnect.setS2I(Source2Image.fromJson(cm.getMetadata().getNamespace(), kafkaConnect.getName(), kafkaConnect.getLabelsWithName(), config));
                }
            }
            else {
                log.error("S2I is supported only on OpenShift. S2I will be ignored in Kafka Connect cluster {} in namespace {}", cm.getMetadata().getName(), cm.getMetadata().getNamespace());
            }
        }

        return kafkaConnect;
    }

    /**
     * Create a Kafka Connect cluster from the deployed Deployment resource
     *
     * @param k8s   K8SUtils client instance for accessing Kubernetes/OpenShift cluster
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @return  Kafka Connect cluster instance
     */
    public static KafkaConnectCluster fromDeployment(K8SUtils k8s, String namespace, String cluster) {

        Deployment dep = k8s.getDeployment(namespace, cluster + KafkaConnectCluster.NAME_SUFFIX);
log.info("Namespace: {}, Cluster: {}", namespace, cluster);
        KafkaConnectCluster kafkaConnect =  new KafkaConnectCluster(namespace, cluster);

        kafkaConnect.setLabels(dep.getMetadata().getLabels());
        kafkaConnect.setReplicas(dep.getSpec().getReplicas());
        kafkaConnect.setImage(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        kafkaConnect.setHealthCheckInitialDelay(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        kafkaConnect.setHealthCheckInitialDelay(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        kafkaConnect.setBootstrapServers(vars.getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
        kafkaConnect.setGroupId(vars.getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID));
        kafkaConnect.setKeyConverter(vars.getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER));
        kafkaConnect.setKeyConverterSchemasEnable(Boolean.parseBoolean(vars.getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setValueConverter(vars.getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER));
        kafkaConnect.setValueConverterSchemasEnable(Boolean.parseBoolean(vars.getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setConfigStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setOffsetStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setStatusStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR))));

        String s2iAnnotation = String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Source2Image.ANNOTATION_S2I);
        if (dep.getMetadata().getAnnotations().containsKey(s2iAnnotation) && Boolean.parseBoolean(dep.getMetadata().getAnnotations().getOrDefault(s2iAnnotation, "false"))) {
            if (k8s.isOpenShift()) {
                kafkaConnect.setS2I(Source2Image.fromOpenShift(namespace, kafkaConnect.getName(), k8s.getOpenShiftUtils()));
            }
            else {
                log.error("S2I is supported only on OpenShift. S2I will be ignored in Kafka Connect cluster {} in namespace {}", cluster, namespace);
            }
        }

        return kafkaConnect;
    }

    /**
     * Return the differences between the current Kafka Connect cluster and the deployed one
     *
     * @param k8s   K8SUtils client instance for accessing Kubernetes/OpenShift cluster
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @return  ClusterDiffResult instance with differences
     */
    public ClusterDiffResult diff(K8SUtils k8s, String namespace) {

        Deployment dep = k8s.getDeployment(namespace, getName());

        ClusterDiffResult diff = new ClusterDiffResult();

        if (replicas > dep.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, dep.getSpec().getReplicas());
            diff.setScaleUp(true);
        }
        else if (replicas < dep.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, dep.getSpec().getReplicas());
            diff.setScaleDown(true);
        }

        if (!getLabelsWithName().equals(dep.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), dep.getMetadata().getLabels());
            diff.setDifferent(true);
        }

        if (!getImage().equals(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage())) {
            log.info("Diff: Expected image {}, actual image {}", getImage(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        Map<String, String> vars = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        if (!bootstrapServers.equals(vars.getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS))
                || !groupId.equals(vars.getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID))
                || !keyConverter.equals(vars.getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER))
                || keyConverterSchemasEnable != Boolean.parseBoolean(vars.getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE)))
                || !valueConverter.equals(vars.getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER))
                || keyConverterSchemasEnable != Boolean.parseBoolean(vars.getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE)))
                || configStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR)))
                || offsetStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR)))
                || statusStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR)))
                ) {
            log.info("Diff: Kafka Connect options changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (healthCheckInitialDelay != dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Kafka Connect healthcheck timing changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (k8s.isOpenShift()) {
            Source2Image realS2I = null;
            String s2iAnnotation = String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Source2Image.ANNOTATION_S2I);
            if (Boolean.parseBoolean(dep.getMetadata().getAnnotations().getOrDefault(s2iAnnotation, "false"))) {
                realS2I = Source2Image.fromOpenShift(namespace, getName(), k8s.getOpenShiftUtils());
            }

            if (s2i == null && realS2I != null) {
                log.info("Diff: Kafka Connect S2I should be removed");
                diff.setDifferent(true);
                diff.setS2i(Source2Image.Source2ImageDiff.DELETE);
            } else if (s2i != null && realS2I == null) {
                log.info("Diff: Kafka Connect S2I should be added");
                diff.setDifferent(true);
                diff.setS2i(Source2Image.Source2ImageDiff.CREATE);
            } else if (s2i != null && realS2I != null) {
                if (s2i.diff(k8s.getOpenShiftUtils()).getDifferent()) {
                    log.info("Diff: Kafka Connect S2I should be updated");
                    diff.setS2i(Source2Image.Source2ImageDiff.UPDATE);
                    diff.setDifferent(true);
                }
            }
        }

        return diff;
    }

    public Service generateService() {

        return createService("ClusterIP",
                Collections.singletonList(createServicePort(restApiPortName, restApiPort, restApiPort, "TCP")));
    }

    public Deployment generateDeployment() {

        return createDeployment(
                Collections.singletonList(createContainerPort(restApiPortName, restApiPort, "TCP")),
                createHttpProbe(healthCheckPath, restApiPortName, healthCheckInitialDelay, healthCheckTimeout),
                createHttpProbe(healthCheckPath, restApiPortName, healthCheckInitialDelay, healthCheckTimeout),
                getDeploymentAnnotations(),
                getPodAnnotations()
                );
    }

    public Deployment patchDeployment(Deployment dep) {
        return patchDeployment(dep,
                createHttpProbe(healthCheckPath, restApiPortName, healthCheckInitialDelay, healthCheckTimeout),
                createHttpProbe(healthCheckPath, restApiPortName, healthCheckInitialDelay, healthCheckTimeout),
                getDeploymentAnnotations(),
                getPodAnnotations()
                );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(new EnvVarBuilder().withName(KEY_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        varList.add(new EnvVarBuilder().withName(KEY_GROUP_ID).withValue(groupId).build());
        varList.add(new EnvVarBuilder().withName(KEY_KEY_CONVERTER).withValue(keyConverter).build());
        varList.add(new EnvVarBuilder().withName(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE).withValue(String.valueOf(keyConverterSchemasEnable)).build());
        varList.add(new EnvVarBuilder().withName(KEY_VALUE_CONVERTER).withValue(valueConverter).build());
        varList.add(new EnvVarBuilder().withName(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE).withValue(String.valueOf(valueConverterSchemasEnable)).build());
        varList.add(new EnvVarBuilder().withName(KEY_CONFIG_STORAGE_REPLICATION_FACTOR).withValue(String.valueOf(configStorageReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_OFFSET_STORAGE_REPLICATION_FACTOR).withValue(String.valueOf(offsetStorageReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_STATUS_STORAGE_REPLICATION_FACTOR).withValue(String.valueOf(statusStorageReplicationFactor)).build());

        return varList;
    }

    /**
     * Returns map with annotations which should be set at Deployment level
     *
     * @return
     */
    protected Map<String, String> getDeploymentAnnotations() {
        Map<String, String> annotations = new HashMap<>();

        if (s2i != null)    {
            annotations.put(Source2Image.ANNOTATION_RESOLVE_NAMES, "*");
            annotations.put(String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Source2Image.ANNOTATION_S2I), "true");
        }

        return annotations;
    }

    /**
     * Returns map with annotations which should be set at Template / Pod level
     *
     * @return
     */
    protected Map<String, String> getPodAnnotations() {
        Map<String, String> annotations = new HashMap<>();

        if (s2i != null)    {
            annotations.put(Source2Image.ANNOTATION_RESOLVE_NAMES, "*");
        }

        return annotations;
    }

    protected void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    protected void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    protected void setKeyConverter(String keyConverter) {
        this.keyConverter = keyConverter;
    }

    protected void setKeyConverterSchemasEnable(Boolean keyConverterSchemasEnable) {
        this.keyConverterSchemasEnable = keyConverterSchemasEnable;
    }

    protected void setValueConverter(String valueConverter) {
        this.valueConverter = valueConverter;
    }

    protected void setValueConverterSchemasEnable(Boolean valueConverterSchemasEnable) {
        this.valueConverterSchemasEnable = valueConverterSchemasEnable;
    }

    protected void setConfigStorageReplicationFactor(int configStorageReplicationFactor) {
        this.configStorageReplicationFactor = configStorageReplicationFactor;
    }

    protected void setOffsetStorageReplicationFactor(int offsetStorageReplicationFactor) {
        this.offsetStorageReplicationFactor = offsetStorageReplicationFactor;
    }

    protected void setStatusStorageReplicationFactor(int statusStorageReplicationFactor) {
        this.statusStorageReplicationFactor = statusStorageReplicationFactor;
    }

    /**
     * Get Source2Image resource belonging to this cluster
     *
     * @return
     */
    public Source2Image getS2I() {
        return s2i;
    }

    /**
     * Set the Source2Image resource related to this cluster
     *
     * @param s2i
     */
    public void setS2I(Source2Image s2i) {
        this.s2i = s2i;
    }


    /**
     * Return the Docker image which should be used. The image differs for Source2Image deployments and regular deployments.
     *
     * @return
     */
    public String getImage()    {
        if (s2i != null) {
            return s2i.getTargetImage();
        } else {
            return super.getImage();
        }
    }
}
