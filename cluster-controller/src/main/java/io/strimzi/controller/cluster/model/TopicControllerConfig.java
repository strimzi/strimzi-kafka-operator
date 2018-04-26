/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicControllerConfig {
    private String watchedNamespace;
    private String image = TopicController.DEFAULT_IMAGE;
    private String reconciliationInterval = TopicController.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
    private String zookeeperSessionTimeout = TopicController.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS;
    private int topicMetadataMaxAttempts = TopicController.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;
    private Resources resources;

    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getReconciliationInterval() {
        return reconciliationInterval;
    }

    public void setReconciliationInterval(String reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
    }

    public String getZookeeperSessionTimeout() {
        return zookeeperSessionTimeout;
    }

    public void setZookeeperSessionTimeout(String zookeeperSessionTimeout) {
        this.zookeeperSessionTimeout = zookeeperSessionTimeout;
    }

    public int getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
    }

    public void setTopicMetadataMaxAttempts(int topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
    }

    public Resources getResources() {
        return resources;
    }

    public void setResources(Resources resources) {
        this.resources = resources;
    }

    public static TopicControllerConfig fromJson(String json) {
        return JsonUtils.fromJson(json, TopicControllerConfig.class);
    }
}
