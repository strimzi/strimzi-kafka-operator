/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.quotas;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "producerByteRate", "consumerByteRate", "minAvailableBytesPerVolume", "minAvailableRatioPerVolume", "excludedPrincipals"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class QuotasPluginStrimzi extends QuotasPlugin {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_STRIMZI = "strimzi";

    private Long producerByteRate;
    private Long consumerByteRate;
    private Long minAvailableBytesPerVolume;
    private Double minAvailableRatioPerVolume;
    private List<String> excludedPrincipals;

    @Description("Must be `" + TYPE_STRIMZI + "`")
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getType() {
        return TYPE_STRIMZI;
    }

    @Description("A per-broker byte-rate quota for clients producing to a broker, independent of their number. " +
        "If clients produce at maximum speed, the quota is shared equally between all non-excluded producers. " +
        "Otherwise, the quota is divided based on each client's production rate.")
    @Minimum(0)
    public Long getProducerByteRate() {
        return producerByteRate;
    }

    public void setProducerByteRate(Long producerByteRate) {
        this.producerByteRate = producerByteRate;
    }

    @Description("A per-broker byte-rate quota for clients consuming from a broker, independent of their number. " +
        "If clients consume at maximum speed, the quota is shared equally between all non-excluded consumers. " +
        "Otherwise, the quota is divided based on each client's consumption rate.")
    @Minimum(0)
    public Long getConsumerByteRate() {
        return consumerByteRate;
    }

    public void setConsumerByteRate(Long consumerByteRate) {
        this.consumerByteRate = consumerByteRate;
    }

    @Description("Stop message production if the available size (in bytes) of the storage is lower than or equal " +
        "to this specified value. This condition is mutually exclusive with `minAvailableRatioPerVolume`.")
    @Minimum(0)
    public Long getMinAvailableBytesPerVolume() {
        return minAvailableBytesPerVolume;
    }

    public void setMinAvailableBytesPerVolume(Long minAvailableBytesPerVolume) {
        this.minAvailableBytesPerVolume = minAvailableBytesPerVolume;
    }

    @Description("Stop message production if the percentage of available storage space falls below or equals the specified ratio (set as a decimal representing a percentage). " +
        "This condition is mutually exclusive with `minAvailableBytesPerVolume`.")
    @Minimum(0)
    @Maximum(1)
    public Double getMinAvailableRatioPerVolume() {
        return minAvailableRatioPerVolume;
    }

    public void setMinAvailableRatioPerVolume(Double minAvailableRatioPerVolume) {
        this.minAvailableRatioPerVolume = minAvailableRatioPerVolume;
    }

    @Description("List of principals that are excluded from the quota. " +
        "The principals have to be prefixed with `User:`, for example `User:my-user;User:CN=my-other-user`.")
    public List<String> getExcludedPrincipals() {
        return excludedPrincipals;
    }

    public void setExcludedPrincipals(List<String> excludedPrincipals) {
        this.excludedPrincipals = excludedPrincipals;
    }
}
