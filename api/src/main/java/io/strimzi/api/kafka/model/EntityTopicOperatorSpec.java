/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a Strimzi-managed Topic Operator deployment.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"watchedNamespace", "image",
        "reconciliationIntervalSeconds", "zookeeperSessionTimeoutSeconds",
        "resources", "topicMetadataMaxAttempts", "logging", "jvmOptions"})
@EqualsAndHashCode
public class EntityTopicOperatorSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_REPLICAS = 1;
    public static final int DEFAULT_HEALTHCHECK_DELAY = 10;
    public static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    public static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9091;
    public static final int DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS = 90;
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS = 20;
    public static final int DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS = 6;

    protected String watchedNamespace;
    protected String image;
    protected int reconciliationIntervalSeconds = DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS;
    protected int zookeeperSessionTimeoutSeconds = DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS;
    protected int topicMetadataMaxAttempts = DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;
    protected ResourceRequirements resources;
    protected Logging logging;
    private EntityOperatorJvmOptions jvmOptions;
    protected Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The namespace the Topic Operator should watch.")
    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    @Description("The image to use for the Topic Operator")
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Interval between periodic reconciliations.")
    @Minimum(0)
    public int getReconciliationIntervalSeconds() {
        return reconciliationIntervalSeconds;
    }

    public void setReconciliationIntervalSeconds(int reconciliationIntervalSeconds) {
        this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
    }

    @Description("Timeout for the Zookeeper session")
    @Minimum(0)
    public int getZookeeperSessionTimeoutSeconds() {
        return zookeeperSessionTimeoutSeconds;
    }

    public void setZookeeperSessionTimeoutSeconds(int zookeeperSessionTimeoutSeconds) {
        this.zookeeperSessionTimeoutSeconds = zookeeperSessionTimeoutSeconds;
    }

    @Description("The number of attempts at getting topic metadata")
    @Minimum(0)
    public int getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
    }

    public void setTopicMetadataMaxAttempts(int topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
    }

    @Description("Resource constraints (limits and requests).")
    public ResourceRequirements getResources() {
        return resources;
    }

    @Description("Resource constraints (limits and requests).")
    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @Description("Logging configuration")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Logging getLogging() {
        return logging;
    }

    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("JVM Options for pods")
    public EntityOperatorJvmOptions getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(EntityOperatorJvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }
}
