/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.controller.cluster.ClusterController;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the topic controller deployment
 */
public class TopicController extends AbstractCluster {

    public static final String KIND = "topic";

    private static final String NAME_SUFFIX = "-topic-controller";

    // Topic Controller configuration
    protected TopicControllerConfig config;

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "hcheck-port";

    // Configuration defaults
    protected static final String DEFAULT_IMAGE = "strimzi/topic-controller:latest";
    protected static final int DEFAULT_REPLICAS = 1;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 10;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    protected static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9092;
    protected static final String DEFAULT_FULL_RECONCILIATION_INTERVAL = "15 minutes";
    protected static final String DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = "20 seconds";

    // Configuration keys
    public static final String KEY_CONFIG = "topic-controller-config";

    // Topic Controller configuration keys
    public static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String KEY_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String KEY_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String KEY_FULL_RECONCILIATION_INTERVAL = "STRIMZI_FULL_RECONCILIATION_INTERVAL";
    public static final String KEY_ZOOKEEPER_SESSION_TIMEOUT = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT";

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected TopicController(String namespace, String cluster) {

        super(namespace, cluster);
        this.name = topicControllerName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;

        // create a default configuration
        this.config = new TopicControllerConfig();
        this.config
                .withImage(DEFAULT_IMAGE)
                .withKafkaBootstrapServers(defaultBootstrapServers(cluster))
                .withZookeeperConnect(defaultZookeeperConnect(cluster))
                .withNamespace(namespace)
                .withReconciliationInterval(DEFAULT_FULL_RECONCILIATION_INTERVAL)
                .withZookeeperSessionTimeout(DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
    }

    public static String topicControllerName(String cluster) {
        return cluster + TopicController.NAME_SUFFIX;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return cluster + ZookeeperCluster.NAME_SUFFIX + ":" + DEFAULT_ZOOKEEPER_PORT;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return cluster + KafkaCluster.NAME_SUFFIX + ":" + DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    public void setConfig(TopicControllerConfig config) {
        this.config = config;
    }

    public TopicControllerConfig getConfig() {
        return this.config;
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
            topicController = new TopicController(kafkaClusterCm.getMetadata().getNamespace(), kafkaClusterCm.getMetadata().getName());
            topicController.setLabels(kafkaClusterCm.getMetadata().getLabels());

            TopicControllerConfig.fromJson(topicController.getConfig(), new JsonObject(config));
            topicController.setImage(topicController.getConfig().image());
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
    public static TopicController fromDeployment(String namespace, String cluster, Deployment dep) {

        TopicController topicController = null;

        if (dep != null) {

            topicController = new TopicController(namespace, cluster);

            topicController.setLabels(dep.getMetadata().getLabels());
            topicController.setReplicas(dep.getSpec().getReplicas());
            topicController.setImage(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
            topicController.setHealthCheckInitialDelay(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
            topicController.setHealthCheckTimeout(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

            Map<String, String> vars = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                    Collectors.toMap(EnvVar::getName, EnvVar::getValue));

            TopicControllerConfig config = new TopicControllerConfig();

            config.withImage(topicController.getImage())
                    .withKafkaBootstrapServers(vars.getOrDefault(KEY_KAFKA_BOOTSTRAP_SERVERS, defaultBootstrapServers(cluster)))
                    .withZookeeperConnect(vars.getOrDefault(KEY_ZOOKEEPER_CONNECT, defaultZookeeperConnect(cluster)))
                    .withNamespace(vars.getOrDefault(KEY_NAMESPACE, namespace))
                    .withReconciliationInterval(vars.getOrDefault(KEY_FULL_RECONCILIATION_INTERVAL, DEFAULT_FULL_RECONCILIATION_INTERVAL))
                    .withZookeeperSessionTimeout(vars.getOrDefault(KEY_ZOOKEEPER_SESSION_TIMEOUT, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT));

            topicController.setConfig(config);
        }

        return topicController;
    }

    /**
     * Return the differences between the current Topic Controller and the deployed one
     *
     * @param dep Deployment which should be diffed
     * @return  ClusterDiffResult instance with differences
     */
    public ClusterDiffResult diff(Deployment dep) {

        if (dep != null) {
            String image = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();

            Map<String, String> vars = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                    Collectors.toMap(EnvVar::getName, EnvVar::getValue));

            // get the current (deployed) topic controller configuration
            TopicControllerConfig depConfig = new TopicControllerConfig();

            depConfig.withKafkaBootstrapServers(vars.getOrDefault(KEY_KAFKA_BOOTSTRAP_SERVERS, defaultBootstrapServers(cluster)))
                    .withZookeeperConnect(vars.getOrDefault(KEY_ZOOKEEPER_CONNECT, defaultZookeeperConnect(cluster)))
                    .withNamespace(vars.getOrDefault(KEY_NAMESPACE, namespace))
                    .withReconciliationInterval(vars.getOrDefault(KEY_FULL_RECONCILIATION_INTERVAL, DEFAULT_FULL_RECONCILIATION_INTERVAL))
                    .withZookeeperSessionTimeout(vars.getOrDefault(KEY_ZOOKEEPER_SESSION_TIMEOUT, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT))
                    .withImage(image);

            // compute the differences with the requested configuration (from the updated ConfigMap)
            TopicControllerConfig.TopicControllerConfigResult configDiffResult = config.diff(depConfig);

            boolean isDifferent = configDiffResult.isImage() || configDiffResult.isNamespace() ||
                    configDiffResult.isReconciliationInterval() || configDiffResult.isZookeeperSessionTimeout();

            return new ClusterDiffResult(isDifferent);
        } else {
            return null;
        }
    }

    public Deployment generateDeployment() {

        return createDeployment(
                Collections.singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")),
                createHttpProbe(healthCheckPath + "healthy", HEALTHCHECK_PORT_NAME, DEFAULT_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_TIMEOUT),
                createHttpProbe(healthCheckPath + "ready", HEALTHCHECK_PORT_NAME, DEFAULT_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_TIMEOUT),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public Deployment patchDeployment(Deployment dep) {
        return patchDeployment(dep,
                createHttpProbe(healthCheckPath + "healthy", HEALTHCHECK_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout),
                createHttpProbe(healthCheckPath + "ready", HEALTHCHECK_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout),
                Collections.emptyMap(),
                Collections.emptyMap()
        );
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_BOOTSTRAP_SERVERS).withValue(this.config.kafkaBootstrapServers()).build());
        varList.add(new EnvVarBuilder().withName(KEY_ZOOKEEPER_CONNECT).withValue(this.config.zookeeperConnect()).build());
        varList.add(new EnvVarBuilder().withName(KEY_NAMESPACE).withValue(this.config.namespace()).build());
        varList.add(new EnvVarBuilder().withName(KEY_FULL_RECONCILIATION_INTERVAL).withValue(this.config.reconciliationInterval()).build());
        varList.add(new EnvVarBuilder().withName(KEY_ZOOKEEPER_SESSION_TIMEOUT).withValue(this.config.zookeeperSessionTimeout()).build());

        return varList;
    }

    @Override
    protected String getServiceAccountName() {
        return ClusterController.STRMIZI_CONTROLLER_SERVICE_ACCOUNT;
    }

    @Override
    protected void setLabels(Map<String, String> labels) {
        Map<String, String> newLabels = new HashMap<>(labels);
        newLabels.put(ClusterController.STRIMZI_KIND_LABEL, TopicController.KIND);
        newLabels.remove(ClusterController.STRIMZI_TYPE_LABEL);
        super.setLabels(newLabels);
    }
}
