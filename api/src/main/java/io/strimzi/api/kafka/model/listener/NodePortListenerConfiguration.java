/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures External node port listeners
 */

@DescriptionFile
@JsonPropertyOrder({"brokerCertChainAndKey", "preferredAddressType"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode(callSuper = true)
public class NodePortListenerConfiguration extends KafkaListenerExternalConfiguration {
    private static final long serialVersionUID = 1L;

    private NodeAddressType preferredAddressType;

    @Description("Defines which address type should be used as the node address. " +
            "Available types are: `ExternalDNS`, `ExternalIP`, `InternalDNS`, `InternalIP` and `Hostname`. " +
            "By default, the addresses will be used in the following order (the first one found will be used):\n" +
            "* `ExternalDNS`\n" +
            "* `ExternalIP`\n" +
            "* `InternalDNS`\n" +
            "* `InternalIP`\n" +
            "* `Hostname`\n" +
            "\n" +
            "This field can be used to select the address type which will be used as the preferred type and checked first. " +
            "In case no address will be found for this address type, the other types will be used in the default order.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public NodeAddressType getPreferredAddressType() {
        return preferredAddressType;
    }

    public void setPreferredAddressType(NodeAddressType preferredAddressType) {
        this.preferredAddressType = preferredAddressType;
    }
}
