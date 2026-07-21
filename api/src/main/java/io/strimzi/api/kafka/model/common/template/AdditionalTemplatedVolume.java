/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.OneOf;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of additional templated volumes for Strimzi resources.
 */
@Buildable(editableEnabled = false, builderPackage = Constants.FABRIC8_KUBERNETES_API)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "secret", "configMap", "persistentVolumeClaim" })
@OneOf({
    @OneOf.Alternative({
        @OneOf.Alternative.Property(value = "secret", required = false),
        @OneOf.Alternative.Property(value = "configMap", required = false),
        @OneOf.Alternative.Property(value = "persistentVolumeClaim", required = false),
        })
})
@EqualsAndHashCode
@ToString
public class AdditionalTemplatedVolume implements UnknownPropertyPreserving {
    private String name;
    private SecretVolumeSource secret;
    private ConfigMapVolumeSource configMap;
    private PersistentVolumeClaimVolumeSource persistentVolumeClaim;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Name to use for the volume. Required.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Description("`Secret` to use to populate the volume. " +
            "The name of the Secret and the items key and path fields can use placeholders that would be replaced for every individual node. " +
            "Valid placeholders that you can use in the template are `{nodeId}` and `{nodePodName}`")
    @KubeLink(group = "core", version = "v1", kind = "secretvolumesource")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public SecretVolumeSource getSecret() {
        return secret;
    }

    public void setSecret(SecretVolumeSource secret) {
        this.secret = secret;
    }

    @Description("`ConfigMap` to use to populate the volume. " +
            "The name of the ConfigMap and the items key and path fields can use placeholders that would be replaced for every individual node. " +
            "Valid placeholders that you can use in the template are `{nodeId}` and `{nodePodName}`")
    @KubeLink(group = "core", version = "v1", kind = "configmapvolumesource")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ConfigMapVolumeSource getConfigMap() {
        return configMap;
    }

    public void setConfigMap(ConfigMapVolumeSource configMap) {
        this.configMap = configMap;
    }

    @Description("`PersistentVolumeClaim` object to use to populate the volume. " +
            "The name of the Persistent Volume Claim can use placeholders that would be replaced for every individual node. " +
            "Valid placeholders that you can use in the template are `{nodeId}` and `{nodePodName}`")
    @KubeLink(group = "core", version = "v1", kind = "persistentvolumeclaimvolumesource")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PersistentVolumeClaimVolumeSource getPersistentVolumeClaim() {
        return persistentVolumeClaim;
    }

    public void setPersistentVolumeClaim(PersistentVolumeClaimVolumeSource persistentVolumeClaim) {
        this.persistentVolumeClaim = persistentVolumeClaim;
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