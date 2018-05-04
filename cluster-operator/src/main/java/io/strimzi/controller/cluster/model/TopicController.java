/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.strimzi.controller.cluster.ClusterController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents the topic controller deployment
 */
public class TopicController extends AbstractModel {

    /**
     * The default kind of CMs that the Topic Controller will be configured to watch for
     */
    public static final String TOPIC_CM_KIND = "topic";

    private static final String NAME_SUFFIX = "-topic-controller";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // Configuration defaults
    protected static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_TOPIC_CONTROLLER_IMAGE", "strimzi/topic-controller:latest");
    protected static final int DEFAULT_REPLICAS = 1;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 10;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    protected static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9092;
    protected static final String DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = "900000";
    protected static final String DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS = "20000";
    protected static final int DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS = 6;

    // Configuration keys
    public static final String KEY_CONFIG = "topic-controller-config";

    // Topic Controller configuration keys
    public static final String KEY_CONFIGMAP_LABELS = "STRIMZI_CONFIGMAP_LABELS";
    public static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String KEY_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String KEY_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String KEY_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String KEY_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String KEY_TOPIC_METADATA_MAX_ATTEMPTS = "STRIMZI_TOPIC_METADATA_MAX_ATTEMPTS";

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    private String kafkaBootstrapServers;
    private String zookeeperConnect;

    private String watchedNamespace;
    private String reconciliationIntervalMs;
    private String zookeeperSessionTimeoutMs;
    private String topicConfigMapLabels;
    private int topicMetadataMaxAttempts;

    /**
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected TopicController(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels.withType(AssemblyType.KAFKA));
        this.name = topicControllerName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;

        // create a default configuration
        this.kafkaBootstrapServers = defaultBootstrapServers(cluster);
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.watchedNamespace = namespace;
        this.reconciliationIntervalMs = DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
        this.zookeeperSessionTimeoutMs = DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS;
        this.topicConfigMapLabels = defaultTopicConfigMapLabels(cluster);
        this.topicMetadataMaxAttempts = DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setTopicConfigMapLabels(String topicConfigMapLabels) {
        this.topicConfigMapLabels = topicConfigMapLabels;
    }

    public String getTopicConfigMapLabels() {
        return topicConfigMapLabels;
    }

    public void setReconciliationIntervalMs(String reconciliationIntervalMs) {
        this.reconciliationIntervalMs = reconciliationIntervalMs;
    }

    public String getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    public void setZookeeperSessionTimeoutMs(String zookeeperSessionTimeoutMs) {
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
    }

    public String getZookeeperSessionTimeoutMs() {
        return zookeeperSessionTimeoutMs;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public void setTopicMetadataMaxAttempts(int topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
    }

    public int getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
    }

    public static String topicControllerName(String cluster) {
        return cluster + TopicController.NAME_SUFFIX;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return ZookeeperCluster.zookeeperClusterName(cluster) + ":" + DEFAULT_ZOOKEEPER_PORT;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.kafkaClusterName(cluster) + ":" + DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    protected static String defaultTopicConfigMapLabels(String cluster) {
        return String.format("%s=%s,%s=%s",
                Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_KIND_LABEL, TopicController.TOPIC_CM_KIND);
    }

    /**
     * Create a Topic Controller from the related ConfigMap resource
     *
     * @param kafkaClusterCm ConfigMap with cluster configuration containing the topic controller one
     * @return Topic Controller instance, null if not configured in the ConfigMap
     */
    public static TopicController fromConfigMap(ConfigMap kafkaClusterCm) {
        TopicController topicController = null;

        String config = kafkaClusterCm.getData().get(KEY_CONFIG);
        if (config != null) {
            String namespace = kafkaClusterCm.getMetadata().getNamespace();
            topicController = new TopicController(namespace,
                    kafkaClusterCm.getMetadata().getName(),
                    Labels.fromResource(kafkaClusterCm));

            TopicControllerConfig tcConfig = TopicControllerConfig.fromJson(config);

            topicController.setImage(tcConfig.getImage());
            topicController.setWatchedNamespace(tcConfig.getWatchedNamespace() != null ? tcConfig.getWatchedNamespace() : namespace);
            topicController.setReconciliationIntervalMs(tcConfig.getReconciliationInterval());
            topicController.setZookeeperSessionTimeoutMs(tcConfig.getZookeeperSessionTimeout());
            topicController.setTopicMetadataMaxAttempts(tcConfig.getTopicMetadataMaxAttempts());
            topicController.setResources(tcConfig.getResources());
        }

        return topicController;
    }

    /**
     * Create a Topic Controller from the deployed Deployment resource
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster overall cluster name
     * @param dep the deployment from which to recover the topic controller state
     * @return Topic Controller instance, null if the corresponding Deployment doesn't exist
     */
    public static TopicController fromAssembly(String namespace, String cluster, Deployment dep) {

        TopicController topicController = null;

        if (dep != null) {

            topicController = new TopicController(namespace, cluster,
                    Labels.fromResource(dep));
            topicController.setReplicas(dep.getSpec().getReplicas());
            Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
            topicController.setImage(container.getImage());
            topicController.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
            topicController.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

            Map<String, String> vars = containerEnvVars(container);

            topicController.setKafkaBootstrapServers(vars.getOrDefault(KEY_KAFKA_BOOTSTRAP_SERVERS, defaultBootstrapServers(cluster)));
            topicController.setZookeeperConnect(vars.getOrDefault(KEY_ZOOKEEPER_CONNECT, defaultZookeeperConnect(cluster)));
            topicController.setWatchedNamespace(vars.getOrDefault(KEY_WATCHED_NAMESPACE, namespace));
            topicController.setReconciliationIntervalMs(vars.get(KEY_FULL_RECONCILIATION_INTERVAL_MS));
            topicController.setZookeeperSessionTimeoutMs(vars.get(KEY_ZOOKEEPER_SESSION_TIMEOUT_MS));
            topicController.setTopicConfigMapLabels(vars.getOrDefault(KEY_CONFIGMAP_LABELS, defaultTopicConfigMapLabels(cluster)));
            topicController.setTopicMetadataMaxAttempts(Integer.parseInt(vars.getOrDefault(KEY_TOPIC_METADATA_MAX_ATTEMPTS, String.valueOf(DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS))));
        }

        return topicController;
    }

    public Deployment generateDeployment() {
        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("Recreate")
                .build();

        return createDeployment(
                Collections.singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")),
                createHttpProbe(healthCheckPath + "healthy", HEALTHCHECK_PORT_NAME, DEFAULT_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_TIMEOUT),
                createHttpProbe(healthCheckPath + "ready", HEALTHCHECK_PORT_NAME, DEFAULT_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_TIMEOUT),
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                resources());
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(KEY_CONFIGMAP_LABELS, topicConfigMapLabels));
        varList.add(buildEnvVar(KEY_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(buildEnvVar(KEY_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(KEY_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(buildEnvVar(KEY_FULL_RECONCILIATION_INTERVAL_MS, reconciliationIntervalMs));
        varList.add(buildEnvVar(KEY_ZOOKEEPER_SESSION_TIMEOUT_MS, zookeeperSessionTimeoutMs));
        varList.add(buildEnvVar(KEY_TOPIC_METADATA_MAX_ATTEMPTS, String.valueOf(topicMetadataMaxAttempts)));

        return varList;
    }

    @Override
    protected String getServiceAccountName() {
        return ClusterController.STRIMZI_CLUSTER_CONTROLLER_SERVICE_ACCOUNT;
    }
}
