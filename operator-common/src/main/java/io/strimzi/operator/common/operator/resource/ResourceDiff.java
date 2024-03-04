/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.AbstractJsonDiff;

import java.util.regex.Pattern;

/**
 * Diffs two Kubernetes resources of the same type to see if the changed
 *
 * @param <T>   Type of the resource which is being diffed
 */
public class ResourceDiff<T extends HasMetadata> extends AbstractJsonDiff {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ResourceDiff.class.getName());

    /**
     * Pattern with JSON paths which should be ignored if they differ
     */
    public static final Pattern DEFAULT_IGNORABLE_PATHS = Pattern.compile(
            "^(/metadata/managedFields" +
                    "|/metadata/creationTimestamp" +
                    "|/metadata/deletionTimestamp" +
                    "|/metadata/deletionGracePeriodSeconds" +
                    "|/metadata/resourceVersion" +
                    "|/metadata/generation" +
                    "|/metadata/uid" +
                    "|/status)$");

    private final boolean isEmpty;

    /**
     * Constructs the diff
     *
     * @param reconciliation    Reconciliation marker
     * @param resourceKind      Kind of the resource
     * @param resourceName      Name of the resource
     * @param current           Current resource
     * @param desired           Desired resource
     * @param ignorableFields   Pattern with fields which should be ignored
     */
    public ResourceDiff(Reconciliation reconciliation, String resourceKind, String resourceName, T current, T desired, Pattern ignorableFields) {
        JsonNode source = PATCH_MAPPER.valueToTree(current == null ? "{}" : current);
        JsonNode target = PATCH_MAPPER.valueToTree(desired == null ? "{}" : desired);
        JsonNode diff = JsonDiff.asJson(source, target);

        int num = 0;

        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();

            if (ignorableFields.matcher(pathValue).matches()) {
                LOGGER.debugCr(reconciliation, "Ignoring {} {} diff {}", resourceKind, resourceName, d);
                continue;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debugCr(reconciliation, "{} {} differs: {}", resourceKind, resourceName, d);
                LOGGER.debugCr(reconciliation, "Current {} {} path {} has value {}", resourceKind, resourceName, pathValue, lookupPath(source, pathValue));
                LOGGER.debugCr(reconciliation, "Desired {} {} path {} has value {}", resourceKind, resourceName, pathValue, lookupPath(target, pathValue));
            }

            num++;
            break;
        }

        this.isEmpty = num == 0;
    }

    @Override
    public boolean isEmpty() {
        return isEmpty;
    }
}
