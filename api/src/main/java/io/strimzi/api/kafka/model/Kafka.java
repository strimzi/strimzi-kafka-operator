/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@JsonDeserialize
@Crd(
    spec = @Crd.Spec(
        names = @Crd.Spec.Names(
            kind = Kafka.RESOURCE_KIND,
            plural = Kafka.RESOURCE_PLURAL,
            shortNames = {Kafka.SHORT_NAME},
            categories = {Constants.STRIMZI_CATEGORY}
        ),
        group = Kafka.RESOURCE_GROUP,
        scope = Kafka.SCOPE,
        versions = {
            @Crd.Spec.Version(name = Kafka.V1BETA2, served = true, storage = false),
            @Crd.Spec.Version(name = Kafka.V1BETA1, served = true, storage = true),
            @Crd.Spec.Version(name = Kafka.V1ALPHA1, served = true, storage = false)
        },
        subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status()
        ),
        additionalPrinterColumns = {
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Desired Kafka replicas",
                description = "The desired number of Kafka replicas in the cluster",
                jsonPath = ".spec.kafka.replicas",
                type = "integer"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Desired ZK replicas",
                description = "The desired number of ZooKeeper replicas in the cluster",
                jsonPath = ".spec.zookeeper.replicas",
                type = "integer"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Ready",
                description = "The state of the custom resource",
                jsonPath = ".status.conditions[?(@.type==\"Ready\")].status",
                type = "string"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Warnings",
                description = "Warnings related to the custom resource",
                jsonPath = ".status.conditions[?(@.type==\"Warning\")].status",
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
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_GROUP_NAME)
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
public class Kafka extends CustomResource<KafkaSpec, KafkaStatus> implements Namespaced, UnknownPropertyPreserving {

    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String V1BETA1 = Constants.V1BETA1;
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2, V1BETA1, V1ALPHA1);
    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String RESOURCE_KIND = "Kafka";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "kafkas";
    public static final String RESOURCE_SINGULAR = "kafka";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "k";
    public static final List<String> RESOURCE_SHORTNAMES = List.of(SHORT_NAME);

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    // Added to avoid duplication during Json serialization
    private String apiVersion;
    private String kind;

    public Kafka() {
        super();
    }

    public Kafka(KafkaSpec spec, KafkaStatus status) {
        super();
        this.spec = spec;
        this.status = status;
    }

    @Override
    @Description("The specification of the Kafka and ZooKeeper clusters, and Topic Operator.")
    public KafkaSpec getSpec() {
        return super.getSpec();
    }

    @Override
    @Description("The status of the Kafka and ZooKeeper clusters, and Topic Operator.")
    public KafkaStatus getStatus() {
        return super.getStatus();
    }

    @Override
    public String toString() {
        YAMLMapper mapper = new YAMLMapper().disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID);
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    /**
     * Returns a predicate that determines if Kafka is ready. A Kafka CRD is
     * ready if the observedGeneration of its status is equal to the generation of its metadata
     * and any of the conditions of its status has type:"Ready" and status:"True"
     * <p>
     * See {@link CustomResourceConditions CustomResourceConditions} for explanation/examples
     *
     * @return a predicate that checks if a Kafka is ready
     */
    public static Predicate<Kafka> isReady() {
        return CustomResourceConditions.isReady();
    }

}
