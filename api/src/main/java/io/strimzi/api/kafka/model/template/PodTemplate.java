/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a pod template for Strimzi resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"metadata", "imagePullSecrets", "securityContext", "terminationGracePeriodSeconds", "affinity",
    "tolerations", "topologySpreadConstraint", "priorityClassName", "schedulerName", "hostAliases", "tmpDirSizeLimit"})
@EqualsAndHashCode
@DescriptionFile
public class PodTemplate implements HasMetadataTemplate, Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private MetadataTemplate metadata;
    private List<LocalObjectReference> imagePullSecrets;
    private PodSecurityContext securityContext;
    private int terminationGracePeriodSeconds = 30;
    private Affinity affinity;
    private List<Toleration> tolerations;
    private List<TopologySpreadConstraint> topologySpreadConstraints;
    private String priorityClassName;
    private String schedulerName;
    private List<HostAlias> hostAliases;
    private Boolean enableServiceLinks;
    private String tmpDirSizeLimit;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Metadata applied to the resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public MetadataTemplate getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataTemplate metadata) {
        this.metadata = metadata;
    }

    @Description("Configures pod-level security attributes and common container settings.")
    @KubeLink(group = "core", version = "v1", kind = "podsecuritycontext")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodSecurityContext getSecurityContext() {
        return securityContext;
    }

    public void setSecurityContext(PodSecurityContext securityContext) {
        this.securityContext = securityContext;
    }

    @Description("List of references to secrets in the same namespace to use for pulling any of the images used by this Pod. " +
            "When the `STRIMZI_IMAGE_PULL_SECRETS` environment variable in Cluster Operator and the `imagePullSecrets` option are specified, only the `imagePullSecrets` variable is used and the `STRIMZI_IMAGE_PULL_SECRETS` variable is ignored.")
    @KubeLink(group = "core", version = "v1", kind = "localobjectreference")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<LocalObjectReference> getImagePullSecrets() {
        return imagePullSecrets;
    }

    public void setImagePullSecrets(List<LocalObjectReference> imagePullSecrets) {
        this.imagePullSecrets = imagePullSecrets;
    }

    @Description("The grace period is the duration in seconds after the processes running in the pod are sent a termination signal, and the time when the processes are forcibly halted with a kill signal. " +
            "Set this value to longer than the expected cleanup time for your process. " +
            "Value must be a non-negative integer. " +
            "A zero value indicates delete immediately. " +
            "You might need to increase the grace period for very large Kafka clusters, so that the Kafka brokers have enough time to transfer their work to another broker before they are terminated. " +
            "Defaults to 30 seconds.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @JsonProperty(defaultValue = "30")
    @Minimum(0)
    public int getTerminationGracePeriodSeconds() {
        return terminationGracePeriodSeconds;
    }

    public void setTerminationGracePeriodSeconds(int terminationGracePeriodSeconds) {
        this.terminationGracePeriodSeconds = terminationGracePeriodSeconds;
    }

    @Description("The pod's affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Affinity getAffinity() {
        return affinity;
    }

    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @Description("The pod's tolerations.")
    @KubeLink(group = "core", version = "v1", kind = "toleration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    public void setTolerations(List<Toleration> tolerations) {
        this.tolerations = tolerations;
    }

    @Description("The pod's topology spread constraints.")
    @KubeLink(group = "core", version = "v1", kind = "topologyspreadconstraint")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<TopologySpreadConstraint> getTopologySpreadConstraints() {
        return topologySpreadConstraints;
    }

    public void setTopologySpreadConstraints(List<TopologySpreadConstraint> topologySpreadConstraints) {
        this.topologySpreadConstraints = topologySpreadConstraints;
    }

    @Description("The name of the priority class used to assign priority to the pods. " +
            "For more information about priority classes, see {K8sPriorityClass}.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getPriorityClassName() {
        return priorityClassName;
    }

    public void setPriorityClassName(String priorityClassName) {
        this.priorityClassName = priorityClassName;
    }

    @Description("The name of the scheduler used to dispatch this `Pod`. " +
            "If not specified, the default scheduler will be used.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getSchedulerName() {
        return schedulerName;
    }

    public void setSchedulerName(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    @Description("The pod's HostAliases. " +
            "HostAliases is an optional list of hosts and IPs that will be injected into the Pod's hosts file if specified.")
    @KubeLink(group = "core", version = "v1", kind = "hostalias")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<HostAlias> getHostAliases() {
        return hostAliases;
    }

    public void setHostAliases(List<HostAlias> hostAliases) {
        this.hostAliases = hostAliases;
    }

    @Description("Indicates whether information about services should be injected into Pod's environment variables.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public Boolean getEnableServiceLinks() {
        return enableServiceLinks;
    }

    public void setEnableServiceLinks(Boolean enableServiceLinks) {
        this.enableServiceLinks = enableServiceLinks;
    }

    @Pattern(Constants.MEMORY_REGEX)
    @JsonProperty(defaultValue = "5Mi")
    @Description("Defines the total amount (for example `1Gi`) of local storage required for temporary EmptyDir volume (`/tmp`). " +
            "Default value is `5Mi`.")
    public String getTmpDirSizeLimit() {
        return tmpDirSizeLimit;
    }

    public void setTmpDirSizeLimit(String tmpDirSizeLimit) {
        this.tmpDirSizeLimit = tmpDirSizeLimit;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
