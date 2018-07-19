/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
@Crd(
        apiVersion = KafkaTopic.CRD_API_VERSION,
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = KafkaTopic.RESOURCE_KIND,
                        plural = KafkaTopic.RESOURCE_PLURAL
                ),
                group = KafkaTopic.RESOURCE_GROUP,
                scope = "Namespaced",
                version = KafkaTopic.VERSION
        )
)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "partitions", "replicas", "config"})
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
public class KafkaTopic extends CustomResource {

    private static final long serialVersionUID = 1L;

    public static final String VERSION = "v1alpha1";
    public static final String RESOURCE_KIND = "KafkaTopic";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = "kafka.strimzi.io";
    public static final String RESOURCE_PLURAL = "kafkatopics";
    public static final String RESOURCE_SINGULAR = "kafkatopic";
    public static final String CRD_API_VERSION = "apiextensions.k8s.io/v1beta1";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final List<String> RESOURCE_SHORTNAMES = Collections.unmodifiableList(asList("kt"));

    private String apiVersion;
    private ObjectMeta metadata;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    private String topicName;

    private int partitions;

    private int replicas;

    private Map<String, Object> config;

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
        return super.getMetadata();
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        super.setMetadata(metadata);
    }

    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, Object> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    @Description("The name of the topic. " +
            "It is recommended to not set this, and for kafka topic names to be compatible Kubernetes resource names." +
            "When absent this will default to the metadata.name of the topic. ")
    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitions() {
        return partitions;
    }

    @Description("The number of partitions the topic should have. " +
            "This cannot be decreased after topic creation. " +
            "It can be increased after topic creation, " +
            "but it is important to understand the consequences that has for topics with semantic partitioning.")
    @Minimum(1)
    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    @Description("The number of replicas the topic should have.")
    @Minimum(1)
    @Maximum(Short.MAX_VALUE)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("The topic configuration.")
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}
