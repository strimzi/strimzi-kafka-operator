/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.kubernetes.api.model.Affinity;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicOperatorConfig {
    private String watchedNamespace;
    private String image = TopicOperator.DEFAULT_IMAGE;
    private String reconciliationInterval = TopicOperator.DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
    private String zookeeperSessionTimeout = TopicOperator.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS;
    private int topicMetadataMaxAttempts = TopicOperator.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;
    private Resources resources;
    private Affinity affinity;

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

    public Affinity getAffinity() {
        return affinity;
    }

    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    public static TopicOperatorConfig fromJson(String json) {
        return JsonUtils.fromJson(json, TopicOperatorConfig.class);
    }
}
