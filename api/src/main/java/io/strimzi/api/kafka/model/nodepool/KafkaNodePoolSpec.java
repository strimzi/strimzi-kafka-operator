/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.nodepool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Represents the .spec section of a KafkaNodePool resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"replicas", "storage", "roles", "resources", "jvmOptions", "template"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaNodePoolSpec extends Spec {
    private static final long serialVersionUID = 1L;

    private int replicas;
    private Storage storage;
    private List<ProcessRoles> roles;
    private ResourceRequirements resources;
    private JvmOptions jvmOptions;
    private KafkaNodePoolTemplate template;

    @Description("The number of pods in the pool.")
    @Minimum(0)
    @JsonProperty(required = true)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("Storage configuration (disk). Cannot be updated.")
    @JsonProperty(required = true)
    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    @Description("The roles that the nodes in this pool will have when KRaft mode is enabled. " +
            "Supported values are 'broker' and 'controller'. " +
            "This field is required. " +
            "When KRaft mode is disabled, the only allowed value if `broker`.")
    @JsonProperty(required = true)
    public List<ProcessRoles> getRoles() {
        return roles;
    }

    public void setRoles(List<ProcessRoles> roles) {
        this.roles = roles;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @KubeLink(group = "core", version = "v1", kind = "resourcerequirements")
    @Description("CPU and memory resources to reserve.")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("JVM Options for pods")
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }

    @Description("Template for pool resources. " +
            "The template allows users to specify how the resources belonging to this pool are generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaNodePoolTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaNodePoolTemplate template) {
        this.template = template;
    }
}
