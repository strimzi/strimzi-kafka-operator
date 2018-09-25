/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;

/**
 * Representation of a template for Kafka cluster resources.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "statefulset", "pod", "bootstrapService", "brokersService"})
public class KafkaClusterTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    private StatefulSetTemplate statefulset;
    private PodTemplate pod;
    private ServiceTemplate bootstrapService;
    private ServiceTemplate brokersService;

    @Description("Template for Kafka `StatefulSet`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public StatefulSetTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(StatefulSetTemplate statefulset) {
        this.statefulset = statefulset;
    }

    @Description("Template for Kafka `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for Kafka bootstrap `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ServiceTemplate getBootstrapService() {
        return bootstrapService;
    }

    public void setBootstrapService(ServiceTemplate bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

    @Description("Template for Kafka broker `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ServiceTemplate getBrokersService() {
        return brokersService;
    }

    public void setBrokersService(ServiceTemplate brokersService) {
        this.brokersService = brokersService;
    }
}
