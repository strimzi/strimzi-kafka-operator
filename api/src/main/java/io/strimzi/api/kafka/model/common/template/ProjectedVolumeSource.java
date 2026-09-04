/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ServiceAccountTokenProjection;
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
 * Representation for the individual projected volume sources in additional volumes. We use our own class instead of
 * Fabric8's class to control which projected volumes are supported. However, the ServiceAccountTokenProjection class
 * is already the end of the API path and can be reused from Fabric8.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"serviceAccountToken"})
@OneOf({
    @OneOf.Alternative({
        @OneOf.Alternative.Property(value = "serviceAccountToken", required = false)
    })
})
@EqualsAndHashCode
@ToString
public class ProjectedVolumeSource implements UnknownPropertyPreserving {
    private ServiceAccountTokenProjection serviceAccountToken;
    private Map<String, Object> additionalProperties;

    @Description("Information about the serviceAccountToken data to project.")
    @KubeLink(group = "core", version = "v1", kind = "serviceaccounttokenprojection")
    public ServiceAccountTokenProjection getServiceAccountToken() {
        return serviceAccountToken;
    }

    public void setServiceAccountToken(ServiceAccountTokenProjection serviceAccountToken) {
        this.serviceAccountToken = serviceAccountToken;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
