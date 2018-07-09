/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.operator.cluster.ClusterOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Represents the topic operator deployment
 */
public class TopicOperator extends AbstractModel {

    /**
     * The default kind of CMs that the Topic Operator will be configured to watch for
     */
    public static final String TOPIC_CM_KIND = "topic";

    private static final String NAME_SUFFIX = "-topic-operator";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // Configuration defaults


    // Configuration keys
    public static final String KEY_CONFIG = "topic-operator-config";

    // Topic Operator configuration keys
    public static final String KEY_CONFIGMAP_LABELS = "STRIMZI_CONFIGMAP_LABELS";
    public static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String KEY_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String KEY_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String KEY_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String KEY_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String KEY_TOPIC_METADATA_MAX_ATTEMPTS = "STRIMZI_TOPIC_METADATA_MAX_ATTEMPTS";
    public static final String KEY_TOPIC_LOG_CONFIG = "topic-logging";

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
    protected TopicOperator(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels);
        this.name = topicOperatorName(cluster);
        this.image = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_IMAGE;
        this.replicas = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_REPLICAS;
        this.readinessPath = "/";
        this.readinessTimeout = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT;
        this.readinessInitialDelay = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_DELAY;
        this.livenessPath = "/";
        this.livenessTimeout = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_TIMEOUT;
        this.livenessInitialDelay = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_HEALTHCHECK_DELAY;

        // create a default configuration
        this.kafkaBootstrapServers = defaultBootstrapServers(cluster);
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.watchedNamespace = namespace;
        this.reconciliationIntervalMs = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
        this.zookeeperSessionTimeoutMs = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS;
        this.topicConfigMapLabels = defaultTopicConfigMapLabels(cluster);
        this.topicMetadataMaxAttempts = io.strimzi.api.kafka.model.TopicOperator.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;
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

    public static String topicOperatorName(String cluster) {
        return cluster + io.strimzi.operator.cluster.model.TopicOperator.NAME_SUFFIX;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return ZookeeperCluster.zookeeperClusterName(cluster) + ":" + io.strimzi.api.kafka.model.TopicOperator.DEFAULT_ZOOKEEPER_PORT;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.kafkaClusterName(cluster) + ":" + io.strimzi.api.kafka.model.TopicOperator.DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    protected static String defaultTopicConfigMapLabels(String cluster) {
        return String.format("%s=%s,%s=%s",
                Labels.STRIMZI_CLUSTER_LABEL, cluster,
                Labels.STRIMZI_KIND_LABEL, io.strimzi.operator.cluster.model.TopicOperator.TOPIC_CM_KIND);
    }

    /**
     * Create a Topic Operator from given desired resource
     *
     * @param resource desired resource with cluster configuration containing the topic operator one
     * @return Topic Operator instance, null if not configured in the ConfigMap
     */
    public static TopicOperator fromCrd(KafkaAssembly resource) {
        TopicOperator result;
        if (resource.getSpec().getTopicOperator() != null) {
            String namespace = resource.getMetadata().getNamespace();
            result = new TopicOperator(
                    namespace,
                    resource.getMetadata().getName(),
                    Labels.fromResource(resource));
            io.strimzi.api.kafka.model.TopicOperator tcConfig = resource.getSpec().getTopicOperator();
            result.setImage(tcConfig.getImage());
            result.setWatchedNamespace(tcConfig.getWatchedNamespace() != null ? tcConfig.getWatchedNamespace() : namespace);
            result.setReconciliationIntervalMs(tcConfig.getReconciliationIntervalSeconds());
            result.setZookeeperSessionTimeoutMs(tcConfig.getZookeeperSessionTimeoutSeconds());
            result.setTopicMetadataMaxAttempts(tcConfig.getTopicMetadataMaxAttempts());
            result.setLogging(tcConfig.getLogging());
            result.setResources(tcConfig.getResources());
            result.setUserAffinity(tcConfig.getAffinity());
        } else {
            result = null;
        }
        return result;
    }

    public Deployment generateDeployment() {
        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("Recreate")
                .build();

        return createDeployment(
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                getMergedAffinity(),
                getInitContainers(),
                getContainers(),
                null
        );
    }

    @Override
    protected List<Container> getContainers() {
        List<Container> containers = new ArrayList<>();
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withPorts(Collections.singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")))
                .withLivenessProbe(createHttpProbe(livenessPath + "healthy", HEALTHCHECK_PORT_NAME, livenessInitialDelay, livenessTimeout))
                .withReadinessProbe(createHttpProbe(readinessPath + "ready", HEALTHCHECK_PORT_NAME, readinessInitialDelay, readinessTimeout))
                .withResources(resources())
                .build();

        containers.add(container);

        return containers;
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
        return ClusterOperator.STRIMZI_CLUSTER_OPERATOR_SERVICE_ACCOUNT;
    }

    @Override
    protected Properties getDefaultLogConfig() {
        Properties defaultSettings = new Properties();

        defaultSettings.put("name", "TOConfig");
        defaultSettings.put("appender.console.type", "Console");
        defaultSettings.put("appender.console.name", "STDOUT");
        defaultSettings.put("appender.console.layout.type", "PatternLayout");
        defaultSettings.put("appender.console.layout.pattern", "[%d] %-5p <%-12.12c{1}:%L> [%-12.12t] %m%n");

        defaultSettings.put("rootLogger.level", "${env:STRIMZI_LOG_LEVEL:-INFO}");
        defaultSettings.put("rootLogger.appenderRefs", "stdout");
        defaultSettings.put("rootLogger.appenderRef.console.ref", "STDOUT");
        defaultSettings.put("rootLogger.additivity", "false");

        defaultSettings.put("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender");
        defaultSettings.put("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout");
        defaultSettings.put("log4j.appender.CONSOLE.layout.ConversionPattern", "%d{ISO8601} %p %m (%c) [%t]%n");
        return  defaultSettings;
    }
}
