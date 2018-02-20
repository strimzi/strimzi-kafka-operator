/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.vertx.core.json.JsonObject;

/**
 * Represents the configuration for the Topic Controller to deploy
 */
public class TopicControllerConfig {

    public static final String BOOTSTRAP_SERVERS_FIELD = "bootstrapServers";
    public static final String ZOOKEEPER_CONNECT_FIELD = "zookeeperConnect";
    public static final String TOPIC_NAMESPACE_FIELD = "topicNamespace";
    public static final String IMAGE_FIELD = "image";

    private String bootstrapServers;
    private String zookeeperConnect;
    private String topicNamespace;
    private String image;

    /**
     * Specify the bootstrap servers
     *
     * @param bootstrapServers the bootstrap servers
     * @return current TopicControllerConfig instance
     */
    public TopicControllerConfig withBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /**
     * Specify the zookeeper connect
     *
     * @param zookeeperConnect the zookeeper connect
     * @return current TopicControllerConfig instance
     */
    public TopicControllerConfig withZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
        return this;
    }

    /**
     * Specify the namespace in which watching for topics ConfigMap
     *
     * @param topicNamespace the namespace in which watching for topics ConfigMap
     * @return current TopicControllerConfig instance
     */
    public TopicControllerConfig withTopicNamespace(String topicNamespace) {
        this.topicNamespace = topicNamespace;
        return this;
    }

    /**
     * Specify the Docker image to use for the Topic Controller
     *
     * @param image the Docker image to use for the Topic Controller
     * @return current TopicControllerConfig instance
     */
    public TopicControllerConfig withImage(String image) {
        this.image = image;
        return  this;
    }

    /**
     * Returns a TopicControllerConfig instance from a corresponding JSON representation
     *
     * @param json  topic controller configuration JSON representation
     * @return  TopicControllerConfig instance
     */
    public static TopicControllerConfig fromJson(JsonObject json) {

        TopicControllerConfig config = new TopicControllerConfig();

        // TODO : filling configuration from JSON

        return config;
    }
}
