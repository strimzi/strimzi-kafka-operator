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

import java.util.regex.Pattern;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

class ResourceDiff<T extends HasMetadata> extends AbstractJsonDiff {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ResourceDiff.class.getName());

    private final boolean isEmpty;

    public ResourceDiff(Reconciliation reconciliation, String resourceKind, String resourceName, T current, T desired, Pattern ignorableFields) {
        JsonNode source = patchMapper().valueToTree(current == null ? "{}" : current);
        JsonNode target = patchMapper().valueToTree(desired == null ? "{}" : desired);
        JsonNode diff = JsonDiff.asJson(source, target);

        int num = 0;

        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();

            if (ignorableFields.matcher(pathValue).matches()) {
                LOGGER.debugCr(reconciliation, "Ignoring {} {} diff {}", resourceKind, resourceName, d);
                continue;
            }

            LOGGER.debugCr(reconciliation, "{} {} differs: {}", resourceKind, resourceName, d);
            LOGGER.debugCr(reconciliation, "Current {} {} path {} has value {}", resourceKind, resourceName, pathValue, lookupPath(source, pathValue));
            LOGGER.debugCr(reconciliation, "Desired {} {} path {} has value {}", resourceKind, resourceName, pathValue, lookupPath(target, pathValue));

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
