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
import io.strimzi.api.kafka.model.common.HasStartupProbe;
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
 * Representation of a Strimzi-managed Topic Operator deployment.
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"watchedNamespace", "image",
    "reconciliationIntervalSeconds", "zookeeperSessionTimeoutSeconds",
    "startupProbe", "livenessProbe", "readinessProbe",
    "resources", "topicMetadataMaxAttempts", "logging", "jvmOptions"})
@EqualsAndHashCode
@ToString
public class EntityTopicOperatorSpec implements HasConfigurableLogging, HasLivenessProbe, HasReadinessProbe, HasStartupProbe, UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_REPLICAS = 1;
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    public static final int DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS = 120;
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS = 18;
    public static final int DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS = 6;
    public static final String DEFAULT_SECURITY_PROTOCOL = "SSL";

    protected String watchedNamespace;
    protected String image;
    protected int reconciliationIntervalSeconds = DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS;
    protected Integer zookeeperSessionTimeoutSeconds;
    protected Integer topicMetadataMaxAttempts;
    private Probe startupProbe;
    private Probe livenessProbe;
    private Probe readinessProbe;
    protected ResourceRequirements resources;
    protected Logging logging;
    private JvmOptions jvmOptions;
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
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getReconciliationIntervalSeconds() {
        return reconciliationIntervalSeconds;
    }

    public void setReconciliationIntervalSeconds(int reconciliationIntervalSeconds) {
        this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
    }
    
    @Description("Timeout for the ZooKeeper session")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DeprecatedProperty(description = "This property is not used anymore in Strimzi 0.41.0 and it is ignored.")
    @PresentInVersions("v1alpha1-v1beta2")
    @Deprecated
    public Integer getZookeeperSessionTimeoutSeconds() {
        return zookeeperSessionTimeoutSeconds;
    }

    public void setZookeeperSessionTimeoutSeconds(Integer zookeeperSessionTimeoutSeconds) {
        this.zookeeperSessionTimeoutSeconds = zookeeperSessionTimeoutSeconds;
    }
    
    @Description("The number of attempts at getting topic metadata")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DeprecatedProperty(description = "This property is not used anymore in Strimzi 0.41.0 and it is ignored.")
    @PresentInVersions("v1alpha1-v1beta2")
    @Deprecated
    public Integer getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
    }

    public void setTopicMetadataMaxAttempts(Integer topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
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
    @Description("Pod startup checking.")
    public Probe getStartupProbe() {
        return startupProbe;
    }

    public void setStartupProbe(Probe startupProbe) {
        this.startupProbe = startupProbe;
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
}
