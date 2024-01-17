/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.operator.common.ReconciliationLogger;

import java.util.regex.Pattern;

/**
 * Diffs status section of a custom resource
 */
public class StatusDiff extends AbstractJsonDiff {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(StatusDiff.class.getName());
    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
            "^(/conditions/[0-9]+/lastTransitionTime)$");

    private final boolean isEmpty;

    /**
     * Constructs the status diff
     *
     * @param current   Current status
     * @param desired   Desired status
     */
    public StatusDiff(Status current, Status desired) {
        JsonNode source = PATCH_MAPPER.valueToTree(current == null ? "{}" : current);
        JsonNode target = PATCH_MAPPER.valueToTree(desired == null ? "{}" : desired);
        JsonNode diff = JsonDiff.asJson(source, target);

        int num = 0;

        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();

            if (IGNORABLE_PATHS.matcher(pathValue).matches()) {
                LOGGER.debugOp("Ignoring Status diff {}", d);
                continue;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debugOp("Status differs: {}", d);
                LOGGER.debugOp("Current Status path {} has value {}", pathValue, lookupPath(source, pathValue));
                LOGGER.debugOp("Desired Status path {} has value {}", pathValue, lookupPath(target, pathValue));
            }

            num++;
        }

        this.isEmpty = num == 0;
    }

    /**
     * Returns whether the Diff is empty or not
     *
     * @return true when the diffed statuses match
     */
    @Override
    public boolean isEmpty() {
        return isEmpty;
    }
}
