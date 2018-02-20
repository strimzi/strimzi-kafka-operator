/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the topic controller deployment
 */
public class TopicController extends AbstractCluster {

    private static final String NAME_SUFFIX = "-topic-controller";

    // Kafka Connect configuration
    protected String bootstrapServers;
    protected String zookeeperConnect;
    protected String topicNamespace;

    // Port configuration
    private static final int HEALTHCHECK_PORT = 8080;
    private static final String HEALTHCHECK_PORT_NAME = "hcheck-port";

    // Configuration defaults
    private static final String DEFAULT_IMAGE = "strimzi/topic-controller:latest";
    private static final int DEFAULT_REPLICAS = 1;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 10;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    // TODO : we need the period for topic controller healthcheck or we are going to use a default one ?
    private static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    private static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9092;

    // Configuration keys
    public static final String KEY_CONFIG = "topic-controller-config";

    // Topic Controller configuration keys
    public static final String KEY_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String KEY_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String KEY_TOPIC_NAMESPACE = "STRIMZI_NAMESPACE";

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
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.bootstrapServers = defaultBootstrapServers(cluster);
        this.topicNamespace = namespace;
    }

    public static String topicControllerName(String cluster) {
        return cluster + TopicController.NAME_SUFFIX;
    }

    private static String defaultZookeeperConnect(String cluster) {
        // TODO : exposing the ZookeeperCluster.NAME_SUFFIX as public
        return cluster + "-zookeeper" + ":" + DEFAULT_ZOOKEEPER_PORT;
    }

    private static String defaultBootstrapServers(String cluster) {
        // TODO : exposing the KafkaCluster.NAME_SUFFIX as public
        return cluster + "-kafka" + ":" + DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    /**
     * Create a Topic Controller from the related ConfigMap resource
     *
     * @param cm ConfigMap with topic controller configuration
     * @return Topic Controller instance
     */
    public static TopicController fromConfigMap(ConfigMap cm) {

        TopicController topicController = null;

        String config = cm.getData().get(KEY_CONFIG);
        if (config != null) {

            topicController = new TopicController(cm.getMetadata().getNamespace(), cm.getMetadata().getName());
            // TODO : building the topic controller instance with the provided configuration
        }

        return topicController;
    }

    public Deployment generateDeployment() {

        return createDeployment(
                Collections.singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")),
                createHttpProbe(healthCheckPath + "healthy", HEALTHCHECK_PORT_NAME, DEFAULT_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_TIMEOUT),
                createHttpProbe(healthCheckPath + "ready", HEALTHCHECK_PORT_NAME, DEFAULT_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_TIMEOUT),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(new EnvVarBuilder().withName(KEY_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        varList.add(new EnvVarBuilder().withName(KEY_ZOOKEEPER_CONNECT).withValue(zookeeperConnect).build());
        varList.add(new EnvVarBuilder().withName(KEY_TOPIC_NAMESPACE).withValue(topicNamespace).build());

        return varList;
    }

    @Override
    protected String getServiceAccountName() {
        return "strimzi-cluster-controller";
    }
}
