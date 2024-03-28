/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.quotas;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"type", "producerByteRate", "consumerByteRate", "requestPercentage", "controllerMutationRate"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class QuotasPluginKafka extends QuotasPlugin {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_KAFKA = "kafka";

    private Long producerByteRate;
    private Long consumerByteRate;
    private Integer requestPercentage;
    private Double controllerMutationRate;

    @Description("Must be `" + TYPE_KAFKA + "`")
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getType() {
        return TYPE_KAFKA;
    }

    @Description("The default client quota on the maximum bytes per-second that each client can publish to each broker before " +
        "it is throttled. Applied on a per-broker basis.")
    @Minimum(0)
    public Long getProducerByteRate() {
        return producerByteRate;
    }

    public void setProducerByteRate(Long producerByteRate) {
        this.producerByteRate = producerByteRate;
    }

    @Description("The default client quota on the maximum bytes per-second that each client can fetch from each broker before " +
        "it is throttled. Applied on a per-broker basis.")
    @Minimum(0)
    public Long getConsumerByteRate() {
        return consumerByteRate;
    }

    public void setConsumerByteRate(Long consumerByteRate) {
        this.consumerByteRate = consumerByteRate;
    }

    @Description("The default client quota limits the maximum CPU utilization of each client as a percentage of the network and I/O threads of each broker. " +
        "Applied on a per-broker basis.")
    @Minimum(0)
    public Integer getRequestPercentage() {
        return requestPercentage;
    }

    public void setRequestPercentage(Integer requestPercentage) {
        this.requestPercentage = requestPercentage;
    }

    @Description("The default client quota on the rate at which mutations are accepted per second for create topic requests, create partition requests, and delete topic requests, defined for each broker. " +
        "The mutations rate is measured by the number of partitions created or deleted. " +
        "Applied on a per-broker basis.")
    @Minimum(0)
    public Double getControllerMutationRate() {
        return controllerMutationRate;
    }

    public void setControllerMutationRate(Double controllerMutationRate) {
        this.controllerMutationRate = controllerMutationRate;
    }
}
