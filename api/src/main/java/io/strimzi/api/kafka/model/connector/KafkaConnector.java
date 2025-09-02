/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connector;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.CustomResourceConditions;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@Crd(
    spec = @Crd.Spec(
        names = @Crd.Spec.Names(
            kind = KafkaConnector.RESOURCE_KIND,
            plural = KafkaConnector.RESOURCE_PLURAL,
            shortNames = {"kctr"},
            categories = {Constants.STRIMZI_CATEGORY}
        ),
        group = KafkaConnector.RESOURCE_GROUP,
        scope = KafkaConnector.SCOPE,
        versions = {
            @Crd.Spec.Version(name = Constants.V1BETA2, served = true, storage = true)
        },
        subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status(),
            scale = @Crd.Spec.Subresources.Scale(
                specReplicasPath = ".spec.tasksMax",
                statusReplicasPath = ".status.tasksMax"
            )
        ),
        additionalPrinterColumns = {
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Cluster",
                description = "The name of the Kafka Connect cluster this connector belongs to",
                jsonPath = ".metadata.labels.strimzi\\.io/cluster",
                type = "string"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Connector class",
                description = "The class used by this connector",
                jsonPath = ".spec.class",
                type = "string"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Max Tasks",
                description = "Maximum number of tasks",
                jsonPath = ".spec.tasksMax",
                type = "integer"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Ready",
                description = "The state of the custom resource",
                jsonPath = ".status.conditions[?(@.type==\"Ready\")].status",
                type = "string")
        }
    )
)
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API,
        refs = {@BuildableReference(CustomResource.class)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_GROUP_NAME)
public class KafkaConnector extends CustomResource<KafkaConnectorSpec, KafkaConnectorStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    public static final List<String> VERSIONS = List.of(Constants.V1BETA2);
    public static final String SCOPE = Constants.SCOPE_NAMESPACED;
    public static final String RESOURCE_PLURAL = "kafkaconnectors";
    public static final String RESOURCE_SINGULAR = "kafkaconnector";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_KIND = "KafkaConnector";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";

    private Map<String, Object> additionalProperties;

    // Added to avoid duplication during JSON serialization
    @SuppressWarnings({"UnusedDeclaration"})
    private String apiVersion;
    @SuppressWarnings({"UnusedDeclaration"})
    private String kind;

    public KafkaConnector() {
        super();
    }
    
    public KafkaConnector(KafkaConnectorSpec spec, KafkaConnectorStatus status) {
        super();
        this.spec = spec;
        this.status = status;
    }

    @Override
    @Description("The specification of the Kafka Connector.")
    public KafkaConnectorSpec getSpec() {
        return super.getSpec();
    }

    @Override
    @Description("The status of the Kafka Connector.")
    public KafkaConnectorStatus getStatus() {
        return super.getStatus();
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

    /**
     * Returns a predicate that determines if KafkaConnector is ready. A KafkaConnector CRD is
     * ready if the observedGeneration of its status is equal to the generation of its metadata
     * and any of the conditions of its status has type:"Ready" and status:"True"
     * <p>
     * See {@link CustomResourceConditions CustomResourceConditions} for explanation/examples
     *
     * @return a predicate that checks if a KafkaConnector is ready
     */
    public static Predicate<KafkaConnector> isReady() {
        return CustomResourceConditions.isReady();
    }
}
