/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Class representing the values section in the YAML.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"allowList"})
public class StrimziMetricsReporterValues {
    private List<String> allowList;
    public static final String ALLOWLIST_CONFIG = "prometheus.metrics.reporter.allowlist";

    /**
     * @return The allowList of metrics to be collected
     */
    @Description("A list of allowed metrics for the Strimzi Metrics Reporter.")
    public Optional<String> getAllowlist() throws Exception {
        if (allowList == null || allowList.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(allowList.stream().map(Pattern::compile).map(Pattern::pattern).collect(Collectors.joining("|")));
        } catch (PatternSyntaxException e) {
            throw new Exception("Invalid regular expression in " + ALLOWLIST_CONFIG + ": " + e.getMessage());
        }
    }

    // for tests
    public void setAllowList(List<String> allowList) {
        this.allowList = allowList;
    }
}
