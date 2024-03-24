/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"numStreams", "offsetCommitInterval", "bootstrapServers", "groupId", "logging",
    "authentication", "tls", "config", "livenessProbe", "readinessProbe"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaMirrorMakerConsumerSpec extends KafkaMirrorMakerClientSpec {
    private static final long serialVersionUID = 1L;

    public static final String FORBIDDEN_PREFIXES = "ssl., bootstrap.servers, group.id, sasl., security., interceptor.classes";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "ssl.endpoint.identification.algorithm, ssl.cipher.suites, ssl.protocol, ssl.enabled.protocols";

    private Integer numStreams;

    private String groupId;

    private Integer offsetCommitInterval;

    @Override
    @Description("The MirrorMaker consumer config. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES + " (with the exception of: " + FORBIDDEN_PREFIX_EXCEPTIONS + ").")
    public Map<String, Object> getConfig() {
        return config;
    }

    @Description("Specifies the number of consumer stream threads to create.")
    @Minimum(1)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNumStreams() {
        return numStreams;
    }

    public void setNumStreams(Integer numStreams) {
        this.numStreams = numStreams;
    }

    @Description("A unique string that identifies the consumer group this consumer belongs to.")
    @JsonProperty(required = true)
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Description("Specifies the offset auto-commit interval in ms. Default value is 60000.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public Integer getOffsetCommitInterval() {
        return offsetCommitInterval;
    }

    public void setOffsetCommitInterval(Integer offsetCommitInterval) {
        this.offsetCommitInterval = offsetCommitInterval;
    }
}
