/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Affinity;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a Strimzi-managed topic operator deployment..
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"watchedNamespace", "image",
        "reconciliationIntervalSeconds", "zookeeperSessionTimeoutSeconds",
        "affinity", "resources", "topicMetadataMaxAttempts"})
public class TopicOperator {

    public static final String DEFAULT_IMAGE = System.getenv().getOrDefault(
            "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE",
            "strimzi/topic-operator:latest");
    public static final int DEFAULT_REPLICAS = 1;
    public static final int DEFAULT_HEALTHCHECK_DELAY = 10;
    public static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    public static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9092;
    public static final String DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = "900000";
    public static final String DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS = "20000";
    public static final int DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS = 6;

    private String watchedNamespace;
    private String image = DEFAULT_IMAGE;
    private String reconciliationIntervalSeconds = DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
    private String zookeeperSessionTimeoutSeconds = DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS;
    private int topicMetadataMaxAttempts = DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;
    private Resources resources;
    private Affinity affinity;
    private Logging logging;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The namespace the Topic Operator should watch.")
    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    @Description("The image to use for the topic operator")
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Interval between periodic reconciliations.")
    public String getReconciliationIntervalSeconds() {
        return reconciliationIntervalSeconds;
    }

    public void setReconciliationIntervalSeconds(String reconciliationIntervalSeconds) {
        this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
    }

    @Description("Timeout for the Zookeeper session")
    public String getZookeeperSessionTimeoutSeconds() {
        return zookeeperSessionTimeoutSeconds;
    }

    public void setZookeeperSessionTimeoutSeconds(String zookeeperSessionTimeoutSeconds) {
        this.zookeeperSessionTimeoutSeconds = zookeeperSessionTimeoutSeconds;
    }

    @Description("The number of attempts at getting topic metadata")
    public int getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
    }

    public void setTopicMetadataMaxAttempts(int topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
    }

    public Resources getResources() {
        return resources;
    }

    @Description("Resource constraints (limits and requests).")
    public void setResources(Resources resources) {
        this.resources = resources;
    }

    @Description("Pod affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    public Affinity getAffinity() {
        return affinity;
    }

    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @Description("Logging configuration")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Logging getLogging() {
        return logging;
    }

    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
