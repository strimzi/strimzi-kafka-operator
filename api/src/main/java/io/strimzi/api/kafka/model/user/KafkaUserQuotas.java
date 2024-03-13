/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.api.kafka.model.user;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Represent the Quotas configuration for Kafka User. Kafka documentation for Kafka User quotas can be found at http://kafka.apache.org/documentation/#design_quotas
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"producerByteRate", "consumerByteRate", "requestPercentage", "controllerMutationRate"})
@EqualsAndHashCode
@ToString
public class KafkaUserQuotas implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Integer producerByteRate;
    private Integer consumerByteRate;
    private Integer requestPercentage;
    private Double controllerMutationRate;

    @Description("A quota on the rate at which mutations are accepted for the create topics request, the create partitions request and the delete topics request. The rate is accumulated by the number of partitions created or deleted.")
    @Minimum(0)
    public Double getControllerMutationRate() {
        return controllerMutationRate;
    }

    public void setControllerMutationRate(Double controllerMutationRate) {
        this.controllerMutationRate = controllerMutationRate;
    }

    private Map<String, Object> additionalProperties;

    @Description("A quota on the maximum bytes per-second that each client group can publish to a broker before the clients in the group are throttled. Defined on a per-broker basis.")
    @Minimum(0)
    public Integer getProducerByteRate() {
        return producerByteRate;
    }

    public void setProducerByteRate(Integer producerByteRate) {
        this.producerByteRate = producerByteRate;
    }

    @Description("A quota on the maximum bytes per-second that each client group can fetch from a broker before the clients in the group are throttled. Defined on a per-broker basis.")
    @Minimum(0)
    public Integer getConsumerByteRate() {
        return consumerByteRate;
    }

    public void setConsumerByteRate(Integer consumerByteRate) {
        this.consumerByteRate = consumerByteRate;
    }

    @Description("A quota on the maximum CPU utilization of each client group as a percentage of network and I/O threads.")
    @Minimum(0)
    public Integer getRequestPercentage() {
        return requestPercentage;
    }

    public void setRequestPercentage(Integer requestPercentage) {
        this.requestPercentage = requestPercentage;
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
