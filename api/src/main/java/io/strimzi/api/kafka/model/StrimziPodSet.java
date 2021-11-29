/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.status.StrimziPodSetStatus;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

@JsonDeserialize
@Crd(
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = StrimziPodSet.RESOURCE_KIND,
                        plural = StrimziPodSet.RESOURCE_PLURAL,
                        shortNames = {StrimziPodSet.SHORT_NAME},
                        categories = {Constants.STRIMZI_CATEGORY}
                ),
                group = StrimziPodSet.RESOURCE_GROUP,
                scope = StrimziPodSet.SCOPE,
                versions = {
                    @Crd.Spec.Version(name = StrimziPodSet.V1BETA2, served = true, storage = true)
                },
                subresources = @Crd.Spec.Subresources(
                    status = @Crd.Spec.Subresources.Status()
                ),
                additionalPrinterColumns = {
                    @Crd.Spec.AdditionalPrinterColumn(
                            name = "Pods",
                            description = "Number of pods managed by the StrimziPodSet",
                            jsonPath = ".status.pods",
                            type = "integer"
                        ),
                    @Crd.Spec.AdditionalPrinterColumn(
                            name = "Ready Pods",
                            description = "Number of ready pods managed by the StrimziPodSet",
                            jsonPath = ".status.readyPods",
                            type = "integer"
                        ),
                    @Crd.Spec.AdditionalPrinterColumn(
                            name = "Current Pods",
                            description = "Number of up-to-date pods managed by the StrimziPodSet",
                            jsonPath = ".status.currentPods",
                            type = "integer"
                        ),
                    @Crd.Spec.AdditionalPrinterColumn(
                            name = "Age",
                            description = "Age of the StrimziPodSet",
                            jsonPath = ".metadata.creationTimestamp",
                            type = "date"
                        )
                }
        )
)
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API,
        refs = {@BuildableReference(ObjectMeta.class)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
@EqualsAndHashCode(callSuper = true)
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_CORE_GROUP_NAME)
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
public class StrimziPodSet extends CustomResource<StrimziPodSetSpec, StrimziPodSetStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2);
    public static final String RESOURCE_KIND = "StrimziPodSet";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_CORE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "strimzipodsets";
    public static final String RESOURCE_SINGULAR = "strimzipodset";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "sps";

    private String apiVersion;
    private final String kind = RESOURCE_KIND;
    private ObjectMeta metadata;
    private StrimziPodSetSpec spec;
    private StrimziPodSetStatus status;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    @Override
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    @Override
    public ObjectMeta getMetadata() {
        return metadata;
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        this.metadata = metadata;
    }

    @Override
    @Description("The specification of the StrimziPodSet.")
    public StrimziPodSetSpec getSpec() {
        return spec;
    }

    @Override
    public void setSpec(StrimziPodSetSpec spec) {
        this.spec = spec;
    }

    @Override
    @Description("The status of the StrimziPodSet.")
    public StrimziPodSetStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(StrimziPodSetStatus status) {
        this.status = status;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        YAMLMapper mapper = new YAMLMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}