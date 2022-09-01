/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
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

import static java.util.Collections.emptyMap;

@Crd(
    spec = @Crd.Spec(
        names = @Crd.Spec.Names(
            kind = KafkaConnector.RESOURCE_KIND,
            plural = KafkaConnector.RESOURCE_PLURAL,
            shortNames = {KafkaConnector.SHORT_NAME},
            categories = {Constants.STRIMZI_CATEGORY}
        ),
        group = KafkaConnector.RESOURCE_GROUP,
        scope = KafkaConnector.SCOPE,
        versions = {
            @Crd.Spec.Version(name = KafkaConnector.V1BETA2, served = true, storage = false),
            @Crd.Spec.Version(name = KafkaConnector.V1ALPHA1, served = true, storage = true)
        },
        subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status(),
            scale = @Crd.Spec.Subresources.Scale(
                specReplicasPath = KafkaConnector.SPEC_REPLICAS_PATH,
                statusReplicasPath = KafkaConnector.STATUS_REPLICAS_PATH
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
@EqualsAndHashCode
@ToString
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_GROUP_NAME)
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
public class KafkaConnector extends CustomResource<KafkaConnectorSpec, KafkaConnectorStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2, V1ALPHA1);
    public static final String SCOPE = "Namespaced";
    public static final String RESOURCE_PLURAL = "kafkaconnectors";
    public static final String RESOURCE_SINGULAR = "kafkaconnector";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_KIND = "KafkaConnector";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "kctr";
    public static final String SPEC_REPLICAS_PATH = ".spec.tasksMax";
    public static final String STATUS_REPLICAS_PATH = ".status.tasksMax";

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    // Added to avoid duplication during Json serialization
    private String apiVersion;
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
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
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

