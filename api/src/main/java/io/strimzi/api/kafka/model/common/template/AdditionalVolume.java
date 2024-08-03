/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
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
 * Representation of additional volumes for Strimzi resources.
 */
@Buildable(editableEnabled = false, builderPackage = Constants.FABRIC8_KUBERNETES_API)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "path", "subPath", "secret", "configMap", "emptyDir", "persistentVolumeClaim" })
@OneOf({
    @OneOf.Alternative({
        @OneOf.Alternative.Property(value = "secret", required = false),
        @OneOf.Alternative.Property(value = "configMap", required = false),
        @OneOf.Alternative.Property(value = "emptyDir", required = false),
        @OneOf.Alternative.Property(value = "persistentVolumeClaim", required = false)
        })
})
@EqualsAndHashCode
@ToString
public class AdditionalVolume implements UnknownPropertyPreserving {
    private String name;
    private SecretVolumeSource secret;
    private ConfigMapVolumeSource configMap;
    private EmptyDirVolumeSource emptyDir;
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

    @Description("Secret to use populate the volume.")
    @KubeLink(group = "core", version = "v1", kind = "secretvolumesource")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public SecretVolumeSource getSecret() {
        return secret;
    }

    public void setSecret(SecretVolumeSource secret) {
        this.secret = secret;
    }

    @Description("ConfigMap to use to populate the volume.")
    @KubeLink(group = "core", version = "v1", kind = "configmapvolumesource")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ConfigMapVolumeSource getConfigMap() {
        return configMap;
    }

    public void setConfigMap(ConfigMapVolumeSource configMap) {
        this.configMap = configMap;
    }

    @Description("EmptyDir to use to populate the volume.")
    @KubeLink(group = "core", version = "v1", kind = "emptydirvolumesource")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public EmptyDirVolumeSource getEmptyDir() {
        return emptyDir;
    }

    public void setEmptyDir(EmptyDirVolumeSource emptyDir) {
        this.emptyDir = emptyDir;
    }

    @Description("PersistentVolumeClaim object to use to populate the volume.")
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