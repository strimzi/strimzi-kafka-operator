/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.CelValidation;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.MinimumItems;
import io.strimzi.crdgenerator.annotations.OneOf;

import java.util.List;

/**
 * Synthetic CRD used solely by {@link CrdGeneratorIT} to verify, against a real Kubernetes API server, that the
 * schema produced by {@link CrdGenerator} is actually enforced by the API server. Covers the generic schema
 * behaviours that production CRDs rely on: required fields (top-level and nested), enum values, polymorphic
 * discriminators, {@code @Minimum}, {@code @MinimumItems}, {@code @OneOf}, {@code @CelValidation}, and the
 * {@code status} / {@code scale} subresources.
 *
 * Intentionally kept separate from {@link ExampleCrd}, which serves the YAML golden-file tests in
 * {@link CrdGeneratorTest} and the reflection tests in {@link PropertyTest}.
 */
@Crd(
    spec = @Crd.Spec(
        group = "it.crdgenerator.strimzi.io",
        names = @Crd.Spec.Names(
            kind = "ExampleIntegration",
            plural = "exampleintegrations",
            categories = {"strimzi"}),
        scope = "Namespaced",
        versions = {
            @Crd.Spec.Version(name = "v1", served = true, storage = true)
        },
        subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status(),
            scale = @Crd.Spec.Subresources.Scale(
                specReplicasPath = ".spec.replicas",
                statusReplicasPath = ".status.replicas"
            )
        )
    )
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
public class ExampleIntegrationCrd extends CustomResource<ExampleIntegrationCrd.Spec, ExampleIntegrationCrd.Status> implements Namespaced {
    @Override
    public Spec getSpec() {
        return super.getSpec();
    }

    @Override
    public Status getStatus() {
        return super.getStatus();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"requiredString", "enumProperty", "numericProperty", "listProperty",
        "polymorphic", "nested", "cel", "replicas", "either", "or"})
    @OneOf({
        @OneOf.Alternative(@OneOf.Alternative.Property("either")),
        @OneOf.Alternative(@OneOf.Alternative.Property("or"))
    })
    public static class Spec {
        private String requiredString;
        private NormalEnum enumProperty;
        private int numericProperty;
        private List<String> listProperty;
        private PolymorphicTop polymorphic;
        private Nested nested;
        private Cel cel;
        private int replicas;
        private String either;
        private String or;

        @JsonProperty(required = true)
        public String getRequiredString() {
            return requiredString;
        }

        public void setRequiredString(String requiredString) {
            this.requiredString = requiredString;
        }

        public NormalEnum getEnumProperty() {
            return enumProperty;
        }

        public void setEnumProperty(NormalEnum enumProperty) {
            this.enumProperty = enumProperty;
        }

        @Minimum(5)
        public int getNumericProperty() {
            return numericProperty;
        }

        public void setNumericProperty(int numericProperty) {
            this.numericProperty = numericProperty;
        }

        @MinimumItems(1)
        public List<String> getListProperty() {
            return listProperty;
        }

        public void setListProperty(List<String> listProperty) {
            this.listProperty = listProperty;
        }

        public PolymorphicTop getPolymorphic() {
            return polymorphic;
        }

        public void setPolymorphic(PolymorphicTop polymorphic) {
            this.polymorphic = polymorphic;
        }

        public Nested getNested() {
            return nested;
        }

        public void setNested(Nested nested) {
            this.nested = nested;
        }

        public Cel getCel() {
            return cel;
        }

        public void setCel(Cel cel) {
            this.cel = cel;
        }

        public int getReplicas() {
            return replicas;
        }

        public void setReplicas(int replicas) {
            this.replicas = replicas;
        }

        public String getEither() {
            return either;
        }

        public void setEither(String either) {
            this.either = either;
        }

        public String getOr() {
            return or;
        }

        public void setOr(String or) {
            this.or = or;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"replicas"})
    public static class Status {
        private int replicas;

        public int getReplicas() {
            return replicas;
        }

        public void setReplicas(int replicas) {
            this.replicas = replicas;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"requiredField"})
    public static class Nested {
        private String requiredField;

        @JsonProperty(required = true)
        public String getRequiredField() {
            return requiredField;
        }

        public void setRequiredField(String requiredField) {
            this.requiredField = requiredField;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"value"})
    @CelValidation(rules = {
        @CelValidation.CelValidationRule(rule = "size(self.value) >= 5", message = "value needs to be at least 5 characters long")
    })
    public static class Cel {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Left.class, name = "left"),
        @JsonSubTypes.Type(value = Right.class, name = "right")
    })
    public abstract static class PolymorphicTop {
        private String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "leftValue"})
    public static class Left extends PolymorphicTop {
        private String leftValue;

        public String getLeftValue() {
            return leftValue;
        }

        public void setLeftValue(String leftValue) {
            this.leftValue = leftValue;
        }

        @Override
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String getType() {
            return "left";
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "rightValue"})
    public static class Right extends PolymorphicTop {
        private String rightValue;

        public String getRightValue() {
            return rightValue;
        }

        public void setRightValue(String rightValue) {
            this.rightValue = rightValue;
        }

        @Override
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String getType() {
            return "right";
        }
    }
}
