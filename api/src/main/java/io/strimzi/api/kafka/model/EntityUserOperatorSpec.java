/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the User Operator.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"watchedNamespace", "image",
        "reconciliationIntervalSeconds", "zookeeperSessionTimeoutSeconds",
        "livenessProbe", "readinessProbe",
        "resources", "logging", "jvmOptions"})
@EqualsAndHashCode
public class EntityUserOperatorSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_HEALTHCHECK_DELAY = 10;
    public static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    public static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9091;
    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS = 120;
    public static final long DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS = 6;

    private String watchedNamespace;
    private String image;
    private long reconciliationIntervalSeconds = DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS;
    private long zookeeperSessionTimeoutSeconds = DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private ResourceRequirements resources;
    private Logging logging;
    private JvmOptions jvmOptions;
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
    public long getReconciliationIntervalSeconds() {
        return reconciliationIntervalSeconds;
    }

    public void setReconciliationIntervalSeconds(long reconciliationIntervalSeconds) {
        this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
    }

    @Description("Timeout for the ZooKeeper session")
    @Minimum(0)
    public long getZookeeperSessionTimeoutSeconds() {
        return zookeeperSessionTimeoutSeconds;
    }

    public void setZookeeperSessionTimeoutSeconds(long zookeeperSessionTimeoutSeconds) {
        this.zookeeperSessionTimeoutSeconds = zookeeperSessionTimeoutSeconds;
    }

    @Description("CPU and memory resources to reserve.")
    @KubeLink(group = "core", version = "v1", kind = "resourcerequirements")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod liveness checking.")
    public Probe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(Probe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod readiness checking.")
    public Probe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(Probe readinessProbe) {
        this.readinessProbe = readinessProbe;
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
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }
}
