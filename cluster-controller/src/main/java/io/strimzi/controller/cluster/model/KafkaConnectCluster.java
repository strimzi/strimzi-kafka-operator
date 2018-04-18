/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.extensions.RollingUpdateDeploymentBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaConnectCluster extends AbstractModel {

    // Port configuration
    protected static final int REST_API_PORT = 8083;
    protected static final String REST_API_PORT_NAME = "rest-api";

    private static final String NAME_SUFFIX = "-connect";

    // Kafka Connect configuration
    protected String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    protected String groupId = DEFAULT_GROUP_ID;
    protected String keyConverter = DEFAULT_KEY_CONVERTER;
    protected Boolean keyConverterSchemasEnable = DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE;
    protected String valueConverter = DEFAULT_VALUE_CONVERTER;
    protected Boolean valueConverterSchemasEnable = DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE;
    protected int configStorageReplicationFactor = DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR;
    protected int offsetStorageReplicationFactor = DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR;
    protected int statusStorageReplicationFactor = DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR;

    // Configuration defaults
    protected static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_CONNECT_IMAGE", "strimzi/kafka-connect:latest");
    protected static final int DEFAULT_REPLICAS = 3;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 60;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    // Kafka Connect configuration defaults
    protected static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092";
    protected static final String DEFAULT_GROUP_ID = "connect-cluster";
    protected static final String DEFAULT_KEY_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    protected static final Boolean DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE = true;
    protected static final String DEFAULT_VALUE_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    protected static final Boolean DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE = true;
    protected static final int DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR = 3;
    protected static final int DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR = 3;
    protected static final int DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR = 3;

    // Configuration keys
    public static final String KEY_IMAGE = "image";
    public static final String KEY_REPLICAS = "nodes";
    public static final String KEY_HEALTHCHECK_DELAY = "healthcheck-delay";
    public static final String KEY_HEALTHCHECK_TIMEOUT = "healthcheck-timeout";
    public static final String KEY_JVM_OPTIONS = "jvmOptions";
    public static final String KEY_RESOURCES = "resources";

    // Kafka Connect configuration keys
    public static final String KEY_BOOTSTRAP_SERVERS = "KAFKA_CONNECT_BOOTSTRAP_SERVERS";
    public static final String KEY_GROUP_ID = "KAFKA_CONNECT_GROUP_ID";
    public static final String KEY_KEY_CONVERTER = "KAFKA_CONNECT_KEY_CONVERTER";
    public static final String KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE = "KAFKA_CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE";
    public static final String KEY_VALUE_CONVERTER = "KAFKA_CONNECT_VALUE_CONVERTER";
    public static final String KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE = "KAFKA_CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE";
    public static final String KEY_CONFIG_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR";
    public static final String KEY_OFFSET_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR";
    public static final String KEY_STATUS_STORAGE_REPLICATION_FACTOR = "KAFKA_CONNECT_STATUS_STORAGE_REPLICATION_FACTOR";
    public static final String KEY_KAFKA_HEAP_OPTS = "KAFKA_HEAP_OPTS";

    public static String kafkaConnectClusterName(String cluster) {
        return cluster + KafkaConnectCluster.NAME_SUFFIX;
    }

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected KafkaConnectCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = kafkaConnectClusterName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
    }

    /**
     * Create a Kafka Connect cluster from the related ConfigMap resource
     *
     * @param cm ConfigMap with cluster configuration
     * @return Kafka Connect cluster instance
     */
    public static KafkaConnectCluster fromConfigMap(ConfigMap cm) {
        KafkaConnectCluster kafkaConnect = new KafkaConnectCluster(cm.getMetadata().getNamespace(),
                cm.getMetadata().getName(),
                Labels.fromResource(cm));

        Map<String, String> data = cm.getData();
        kafkaConnect.setReplicas(Integer.parseInt(data.getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafkaConnect.setImage(data.getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafkaConnect.setResources(Resources.fromJson(data.get(KEY_RESOURCES)));
        kafkaConnect.setJvmOptions(JvmOptions.fromJson(data.get(KEY_JVM_OPTIONS)));
        kafkaConnect.setHealthCheckInitialDelay(Integer.parseInt(data.getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafkaConnect.setHealthCheckTimeout(Integer.parseInt(data.getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafkaConnect.setBootstrapServers(data.getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
        kafkaConnect.setGroupId(data.getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID));
        kafkaConnect.setKeyConverter(data.getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER));
        kafkaConnect.setKeyConverterSchemasEnable(Boolean.parseBoolean(data.getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setValueConverter(data.getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER));
        kafkaConnect.setValueConverterSchemasEnable(Boolean.parseBoolean(data.getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setConfigStorageReplicationFactor(Integer.parseInt(data.getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setOffsetStorageReplicationFactor(Integer.parseInt(data.getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setStatusStorageReplicationFactor(Integer.parseInt(data.getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR))));

        return kafkaConnect;
    }

    /**
     * Create a Kafka Connect cluster from the deployed Deployment resource
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @param dep The deployment from which to recover the cluster state
     * @return  Kafka Connect cluster instance
     */
    public static KafkaConnectCluster fromAssembly(
            String namespace, String cluster,
            Deployment dep) {
        if (dep == null) {
            return null;
        }

        KafkaConnectCluster kafkaConnect =  new KafkaConnectCluster(namespace, cluster, Labels.fromResource(dep));

        kafkaConnect.setReplicas(dep.getSpec().getReplicas());
        Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        kafkaConnect.setImage(container.getImage());
        kafkaConnect.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
        kafkaConnect.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = containerEnvVars(container);

        kafkaConnect.setBootstrapServers(vars.getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
        kafkaConnect.setGroupId(vars.getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID));
        kafkaConnect.setKeyConverter(vars.getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER));
        kafkaConnect.setKeyConverterSchemasEnable(Boolean.parseBoolean(vars.getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setValueConverter(vars.getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER));
        kafkaConnect.setValueConverterSchemasEnable(Boolean.parseBoolean(vars.getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setConfigStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setOffsetStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setStatusStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR))));

        return kafkaConnect;
    }

    public Service generateService() {

        return createService("ClusterIP",
                Collections.singletonList(createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP")));
    }

    public Deployment generateDeployment() {
        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("RollingUpdate")
                .withRollingUpdate(new RollingUpdateDeploymentBuilder()
                        .withMaxSurge(new IntOrString(1))
                        .withMaxUnavailable(new IntOrString(0))
                        .build())
                .build();

        return createDeployment(
                Collections.singletonList(createContainerPort(REST_API_PORT_NAME, REST_API_PORT, "TCP")),
                createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout),
                createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout),
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                resources()
                );
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(KEY_BOOTSTRAP_SERVERS, bootstrapServers));
        varList.add(buildEnvVar(KEY_GROUP_ID, groupId));
        varList.add(buildEnvVar(KEY_KEY_CONVERTER, keyConverter));
        varList.add(buildEnvVar(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(keyConverterSchemasEnable)));
        varList.add(buildEnvVar(KEY_VALUE_CONVERTER, valueConverter));
        varList.add(buildEnvVar(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(valueConverterSchemasEnable)));
        varList.add(buildEnvVar(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(configStorageReplicationFactor)));
        varList.add(buildEnvVar(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(offsetStorageReplicationFactor)));
        varList.add(buildEnvVar(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(statusStorageReplicationFactor)));
        varList.add(buildEnvVar(KEY_KAFKA_HEAP_OPTS, javaHeapOptions(0, 1.0)));
        return varList;
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
}
