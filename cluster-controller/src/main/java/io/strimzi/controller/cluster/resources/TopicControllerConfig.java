/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.vertx.core.json.JsonObject;

import java.util.Objects;

/**
 * Represents the configuration for the Topic Controller to deploy
 */
public class TopicControllerConfig {

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String IMAGE_FIELD = "image";
    public static final String RECONCILIATION_INTERVAL_FIELD = "reconciliationInterval";
    public static final String ZOOKEEPER_SESSION_TIMEOUT_FIELD = "zookeeperSessionTimeout";

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    private String kafkaBootstrapServers;
    private String zookeeperConnect;

    private String namespace;
    private String image;
    private String reconciliationInterval;
    private String zookeeperSessionTimeout;

    /**
     * Specify the Kafka bootstrap servers
     *
     * @param kafkaBootstrapServers the bootstrap servers
     * @return current TopicControllerConfig instance
     */
    public TopicControllerConfig withKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        return this;
    }

    /**
     * Specify the Zookeeper connect
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
     * @param namespace the namespace in which watching for topics ConfigMap
     * @return current TopicControllerConfig instance
     */
    public TopicControllerConfig withNamespace(String namespace) {
        this.namespace = namespace;
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
        return this;
    }

    /**
     * Specify the interval between periodic reconciliations
     *
     * @param reconciliationInterval the interval between periodic reconciliations
     * @return current TopicControllerConfig instance
     */
    public TopicControllerConfig withReconciliationInterval(String reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
        return this;
    }

    /**
     * Specify the Zookeeper session timeout
     *
     * @param zookeeperSessionTimeout the Zookeeper session timeout
     * @return current TopicControllerConfig instance
     */
    public TopicControllerConfig withZookeeperSessionTimeout(String zookeeperSessionTimeout) {
        this.zookeeperSessionTimeout = zookeeperSessionTimeout;
        return this;
    }

    /**
     * Returns a TopicControllerConfig instance from a corresponding JSON representation
     *
     * @param json  topic controller configuration JSON representation
     * @return  TopicControllerConfig instance
     */
    public static TopicControllerConfig fromJson(TopicControllerConfig config, JsonObject json) {

        String image = json.getString(TopicControllerConfig.IMAGE_FIELD);
        if (image != null) {
            config.withImage(image);
        }

        String namespace = json.getString(TopicControllerConfig.NAMESPACE_FIELD);
        if (namespace != null) {
            config.withNamespace(namespace);
        }

        String reconciliationInterval = json.getString(TopicControllerConfig.RECONCILIATION_INTERVAL_FIELD);
        if (reconciliationInterval != null) {
            // TODO : add parsing and validation
            config.withReconciliationInterval(reconciliationInterval);
        }

        String zookeeperSessionTimeout = json.getString(TopicControllerConfig.ZOOKEEPER_SESSION_TIMEOUT_FIELD);
        if (zookeeperSessionTimeout != null) {
            // TODO : add parsing and validation
            config.withZookeeperSessionTimeout(zookeeperSessionTimeout);
        }

        return config;
    }

    /**
     * Compute the difference between two TopicControllerConfig instances
     *
     * @param other the other instance to compare with
     * @return the result with all differences
     */
    public TopicControllerConfigResult diff(TopicControllerConfig other) {

        TopicControllerConfigResult diffResult = new TopicControllerConfigResult();

        diffResult
                .withIsNamespace(!Objects.equals(this.namespace, other.namespace()))
                .withIsImage(!Objects.equals(this.image, other.image()))
                .withIsReconciliationInterval(!Objects.equals(this.reconciliationInterval, other.reconciliationInterval()))
                .withIsZookeeperSessionTimeout(!Objects.equals(this.zookeeperSessionTimeout, other.zookeeperSessionTimeout()));

        return diffResult;
    }

    /**
     * Result after comparing two TopicControllerConfig instances
     */
    public static class TopicControllerConfigResult {

        private boolean isNamespace;
        private boolean isImage;
        private boolean isReconciliationInterval;
        private boolean isZookeeperSessionTimeout;

        /**
         * @return if the namespace in which watching for topics ConfigMap is different
         */
        public boolean isNamespace() {
            return this.isNamespace;
        }

        /**
         * @return if the Docker image to use for the Topic Controller is different
         */
        public boolean isImage() {
            return this.isImage;
        }

        /**
         * @return if the interval between periodic reconciliations is different
         */
        public boolean isReconciliationInterval() {
            return this.isReconciliationInterval;
        }

        /**
         * @return if the Zookeeper session timeout is different
         */
        public boolean isZookeeperSessionTimeout() {
            return this.isZookeeperSessionTimeout;
        }

        /**
         * Set if the namespace in which watching for topics ConfigMap is different
         *
         * @param isNamespace if the namespace in which watching for topics ConfigMap is different
         * @return current TopicControllerConfigResult instance
         */
        public TopicControllerConfigResult withIsNamespace(boolean isNamespace) {
            this.isNamespace = isNamespace;
            return this;
        }

        /**
         * Set if the Docker image to use for the Topic Controller is different
         *
         * @param isImage if the Docker image to use for the Topic Controller is different
         * @return current TopicControllerConfigResult instance
         */
        public TopicControllerConfigResult withIsImage(boolean isImage) {
            this.isImage = isImage;
            return this;
        }

        /**
         * Set if the interval between periodic reconciliations is different
         *
         * @param isReconciliationInterval if the interval between periodic reconciliations is different
         * @return current TopicControllerConfigResult instance
         */
        public TopicControllerConfigResult withIsReconciliationInterval(boolean isReconciliationInterval) {
            this.isReconciliationInterval = isReconciliationInterval;
            return this;
        }

        /**
         * Set if the Zookeeper session timeout is different
         *
         * @param isZookeeperSessionTimeout if the Zookeeper session timeout is different
         * @return current TopicControllerConfigResult instance
         */
        public TopicControllerConfigResult withIsZookeeperSessionTimeout(boolean isZookeeperSessionTimeout) {
            this.isZookeeperSessionTimeout = isZookeeperSessionTimeout;
            return this;
        }
    }

    /**
     * @return the bootstrap servers
     */
    public String kafkaBootstrapServers() {
        return this.kafkaBootstrapServers;
    }

    /**
     * @return the zookeeper connect
     */
    public String zookeeperConnect() {
        return this.zookeeperConnect;
    }

    /**
     * @return the namespace in which watching for topics ConfigMap
     */
    public String namespace() {
        return this.namespace;
    }

    /**
     * @return the Docker image to use for the Topic Controller
     */
    public String image() {
        return this.image;
    }

    /**
     * @return the interval between periodic reconciliations
     */
    public String reconciliationInterval() {
        return this.reconciliationInterval;
    }

    /**
     * @return the Zookeeper session timeout
     */
    public String zookeeperSessionTimeout() {
        return this.zookeeperSessionTimeout;
    }
}
