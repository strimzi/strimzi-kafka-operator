/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.strimzi.controller.cluster.ClusterController;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the topic controller deployment
 */
public class TopicController extends AbstractCluster {

    public static final String KIND = "topic";

    private static final String NAME_SUFFIX = "-topic-controller";

    private static final String NAMESPACE_FIELD = "namespace";
    private static final String IMAGE_FIELD = "image";
    private static final String RECONCILIATION_INTERVAL_FIELD_MS = "reconciliationIntervalMs";
    private static final String ZOOKEEPER_SESSION_TIMEOUT_FIELD_MS = "zookeeperSessionTimeoutMs";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // Configuration defaults
    protected static final String DEFAULT_IMAGE = "strimzi/topic-controller:latest";
    protected static final int DEFAULT_REPLICAS = 1;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 10;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    protected static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9092;
    protected static final String DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = "900000";
    protected static final String DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS = "20000";

    // Configuration keys
    public static final String KEY_CONFIG = "topic-controller-config";

    // Topic Controller configuration keys
    public static final String KEY_CONFIGMAP_LABELS = "STRIMZI_CONFIGMAP_LABELS";
    public static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String KEY_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String KEY_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String KEY_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String KEY_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    private String kafkaBootstrapServers;
    private String zookeeperConnect;

    private String topicNamespace;
    private String reconciliationIntervalMs;
    private String zookeeperSessionTimeoutMs;
    private String topicConfigMapLabels;

    /**
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected TopicController(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels.withKind(TopicController.KIND).withoutType());
        this.name = topicControllerName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;

        // create a default configuration
        this.kafkaBootstrapServers = defaultBootstrapServers(cluster);
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.topicNamespace = namespace;
        this.reconciliationIntervalMs = DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
        this.zookeeperSessionTimeoutMs = DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS;
        this.topicConfigMapLabels = defaultTopicConfigMapLabels(cluster);
    }

    public void setTopicNamespace(String topicNamespace) {
        this.topicNamespace = topicNamespace;
    }

    public String getTopicNamespace() {
        return topicNamespace;
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
                Labels.STRIMZI_KIND_LABEL, TopicController.KIND);
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
            topicController = new TopicController(kafkaClusterCm.getMetadata().getNamespace(),
                    kafkaClusterCm.getMetadata().getName(),
                    Labels.fromResource(kafkaClusterCm));

            JsonObject json = new JsonObject(config);

            String image = json.getString(TopicController.IMAGE_FIELD);
            if (image != null) {
                topicController.setImage(image);
            }

            String topicNamespace = json.getString(TopicController.NAMESPACE_FIELD);
            if (topicNamespace != null) {
                topicController.setTopicNamespace(topicNamespace);
            }

            String reconciliationIntervalMs = json.getString(TopicController.RECONCILIATION_INTERVAL_FIELD_MS);
            if (reconciliationIntervalMs != null) {
                // TODO : add parsing and validation
                topicController.setReconciliationIntervalMs(reconciliationIntervalMs);
            }

            String zookeeperSessionTimeoutMs = json.getString(TopicController.ZOOKEEPER_SESSION_TIMEOUT_FIELD_MS);
            if (zookeeperSessionTimeoutMs != null) {
                // TODO : add parsing and validation
                topicController.setZookeeperSessionTimeoutMs(zookeeperSessionTimeoutMs);
            }
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

            topicController = new TopicController(namespace, cluster,
                    Labels.fromResource(dep));
            topicController.setReplicas(dep.getSpec().getReplicas());
            Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
            topicController.setImage(container.getImage());
            topicController.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
            topicController.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

            Map<String, String> vars = container.getEnv().stream().collect(
                    Collectors.toMap(EnvVar::getName, EnvVar::getValue));

            topicController.setKafkaBootstrapServers(vars.getOrDefault(KEY_KAFKA_BOOTSTRAP_SERVERS, defaultBootstrapServers(cluster)));
            topicController.setZookeeperConnect(vars.getOrDefault(KEY_ZOOKEEPER_CONNECT, defaultZookeeperConnect(cluster)));
            topicController.setTopicNamespace(vars.getOrDefault(KEY_NAMESPACE, namespace));
            topicController.setReconciliationIntervalMs(vars.getOrDefault(KEY_FULL_RECONCILIATION_INTERVAL_MS, DEFAULT_FULL_RECONCILIATION_INTERVAL_MS));
            topicController.setZookeeperSessionTimeoutMs(vars.getOrDefault(KEY_ZOOKEEPER_SESSION_TIMEOUT_MS, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS));
            topicController.setTopicConfigMapLabels(vars.getOrDefault(KEY_CONFIGMAP_LABELS, defaultTopicConfigMapLabels(cluster)));
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

            boolean isDifferent = false;

            Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
            if (!image.equals(container.getImage())) {
                log.info("Diff: Expected image {}, actual image {}", image, container.getImage());
                isDifferent = true;
            }

            Map<String, String> vars = container.getEnv().stream().collect(
                    Collectors.toMap(EnvVar::getName, EnvVar::getValue));

            if (!kafkaBootstrapServers.equals(vars.getOrDefault(KEY_KAFKA_BOOTSTRAP_SERVERS, defaultBootstrapServers(cluster)))) {
                log.info("Diff: Kafka bootstrap servers changed");
                isDifferent = true;
            }

            if (!zookeeperConnect.equals(vars.getOrDefault(KEY_ZOOKEEPER_CONNECT, defaultZookeeperConnect(cluster)))) {
                log.info("Diff: Zookeeper connect changed");
                isDifferent = true;
            }

            if (!topicNamespace.equals(vars.getOrDefault(KEY_NAMESPACE, namespace))) {
                log.info("Diff: Namespace in which watching for topics changed");
                isDifferent = true;
            }

            if (!reconciliationIntervalMs.equals(vars.getOrDefault(KEY_FULL_RECONCILIATION_INTERVAL_MS, DEFAULT_FULL_RECONCILIATION_INTERVAL_MS))) {
                log.info("Diff: Reconciliation interval changed");
                isDifferent = true;
            }

            if (!zookeeperSessionTimeoutMs.equals(vars.getOrDefault(KEY_ZOOKEEPER_SESSION_TIMEOUT_MS, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS))) {
                log.info("Diff: Zookeeper session timeout changed");
                isDifferent = true;
            }

            return new ClusterDiffResult(isDifferent);
        } else {
            return null;
        }
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
        varList.add(new EnvVarBuilder().withName(KEY_CONFIGMAP_LABELS).withValue(topicConfigMapLabels).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_BOOTSTRAP_SERVERS).withValue(kafkaBootstrapServers).build());
        varList.add(new EnvVarBuilder().withName(KEY_ZOOKEEPER_CONNECT).withValue(zookeeperConnect).build());
        varList.add(new EnvVarBuilder().withName(KEY_NAMESPACE).withValue(topicNamespace).build());
        varList.add(new EnvVarBuilder().withName(KEY_FULL_RECONCILIATION_INTERVAL_MS).withValue(reconciliationIntervalMs).build());
        varList.add(new EnvVarBuilder().withName(KEY_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(zookeeperSessionTimeoutMs).build());

        return varList;
    }

    @Override
    protected String getServiceAccountName() {
        return ClusterController.STRIMZI_CLUSTER_CONTROLLER_SERVICE_ACCOUNT;
    }
}
