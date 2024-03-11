/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

/**
 * Logging config is given inline with the resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"type", "loggers"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class InlineLogging extends Logging {

    private static final long serialVersionUID = 1L;

    public static final String TYPE_INLINE = "inline";

    private Map<String, String> loggers = null;

    @Description("Must be `" + TYPE_INLINE + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_INLINE;
    }

    @Description("A Map from logger name to logger level.")
    public Map<String, String> getLoggers() {
        return loggers;
    }

    public void setLoggers(Map<String, String> loggers) {
        this.loggers = loggers;
    }
}
