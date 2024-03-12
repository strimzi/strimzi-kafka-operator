/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.entityoperator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.HasConfigurableLogging;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Logging;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the User Operator.
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"watchedNamespace", "image",
    "reconciliationIntervalSeconds", "zookeeperSessionTimeoutSeconds",
    "secretPrefix", "livenessProbe", "readinessProbe",
    "resources", "logging", "jvmOptions"})
@EqualsAndHashCode
@ToString
public class EntityUserOperatorSpec implements HasConfigurableLogging, HasLivenessProbe, HasReadinessProbe, UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9091;
    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS = 120;
    public static final String DEFAULT_SECRET_PREFIX = "";

    private String watchedNamespace;
    private String image;
    private String secretPrefix;
    private long reconciliationIntervalSeconds = DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS;
    private Long zookeeperSessionTimeoutSeconds;
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
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long getReconciliationIntervalSeconds() {
        return reconciliationIntervalSeconds;
    }

    public void setReconciliationIntervalSeconds(long reconciliationIntervalSeconds) {
        this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
    }

    @Description("Timeout for the ZooKeeper session")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DeprecatedProperty(description = "This property has been deprecated because ZooKeeper is not used anymore by the User Operator.")
    @PresentInVersions("v1alpha1-v1beta2")
    @Deprecated
    public Long getZookeeperSessionTimeoutSeconds() {
        return zookeeperSessionTimeoutSeconds;
    }

    public void setZookeeperSessionTimeoutSeconds(Long zookeeperSessionTimeoutSeconds) {
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
    @Override
    public Logging getLogging() {
        return logging;
    }

    @Override
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

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("The prefix that will be added to the KafkaUser name to be used as the Secret name.")
    public String getSecretPrefix() {
        return secretPrefix;
    }

    public void setSecretPrefix(String secretPrefix) {
        this.secretPrefix = secretPrefix;
    }
}
