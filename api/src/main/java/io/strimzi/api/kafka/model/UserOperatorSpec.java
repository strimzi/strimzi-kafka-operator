/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the User Operator.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserOperatorSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_USER_OPERATOR_IMAGE", "strimzi/user-operator:latest");
    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = 120_000;
    public static final long DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS = 6_000;

    private String watchedNamespace;
    private String image = DEFAULT_IMAGE;
    private long reconciliationIntervalMilliseconds = DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
    private long zookeeperSessionTimeoutMilliseconds = DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS;
    private Resources resources;
    private Logging logging;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The namespace the User Operator should watch.")
    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    @Description("The image to use for the User Operator")
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Interval between periodic reconciliations.")
    @Minimum(0)
    public long getReconciliationIntervalMilliseconds() {
        return reconciliationIntervalMilliseconds;
    }

    public void setReconciliationIntervalMilliseconds(long reconciliationIntervalMilliseconds) {
        this.reconciliationIntervalMilliseconds = reconciliationIntervalMilliseconds;
    }

    @Description("Timeout for the Zookeeper session")
    @Minimum(0)
    public long getZookeeperSessionTimeoutMilliseconds() {
        return zookeeperSessionTimeoutMilliseconds;
    }

    public void setZookeeperSessionTimeoutMilliseconds(long zookeeperSessionTimeoutMilliseconds) {
        this.zookeeperSessionTimeoutMilliseconds = zookeeperSessionTimeoutMilliseconds;
    }

    @Description("Resource constraints (limits and requests).")
    public Resources getResources() {
        return resources;
    }

    @Description("Resource constraints (limits and requests).")
    public void setResources(Resources resources) {
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

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
