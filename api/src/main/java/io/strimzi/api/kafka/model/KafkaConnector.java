/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.ObjectMeta;
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;

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
                                type = "string"
                        ),
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Connector class",
                                description = "The class used by this connector",
                                jsonPath = ".spec.class",
                                type = "string"
                        ),
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Max Tasks",
                                description = "Maximum number of tasks",
                                jsonPath = ".spec.tasksMax",
                                type = "integer"
                        ),
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Ready",
                                description = "The state of the custom resource",
                                jsonPath = ".status.conditions[?(@.type==\"Ready\")].status",
                                type = "string"
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
@EqualsAndHashCode
@ToString
@Version(Constants.V1BETA2)
@Group(Constants.STRIMZI_GROUP)
public class KafkaConnector extends CustomResource<KafkaConnectorSpec, KafkaConnectorStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = unmodifiableList(asList(V1BETA2, V1ALPHA1));
    public static final String SCOPE = "Namespaced";
    public static final String CRD_API_VERSION = Constants.V1BETA1_API_VERSION;
    public static final String RESOURCE_PLURAL = "kafkaconnectors";
    public static final String RESOURCE_SINGULAR = "kafkaconnector";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_KIND = "KafkaConnector";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String SHORT_NAME = "kctr";
    public static final String SPEC_REPLICAS_PATH = ".spec.tasksMax";
    public static final String STATUS_REPLICAS_PATH = ".status.tasksMax";

    private KafkaConnectorSpec spec;
    private KafkaConnectorStatus status;
    private Map<String, Object> additionalProperties;
    private ObjectMeta metadata;
    private String apiVersion;

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    @Override
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    @JsonProperty("kind")
    @Override
    public String getKind() {
        return RESOURCE_KIND;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public ObjectMeta getMetadata() {
        return metadata;
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        this.metadata = metadata;
    }

    @Override
    @Description("The specification of the Kafka Connector.")
    public KafkaConnectorSpec getSpec() {
        return spec;
    }

    @Override
    public void setSpec(KafkaConnectorSpec spec) {
        this.spec = spec;
    }

    @Override
    @Description("The status of the Kafka Connector.")
    public KafkaConnectorStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(KafkaConnectorStatus status) {
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
}

