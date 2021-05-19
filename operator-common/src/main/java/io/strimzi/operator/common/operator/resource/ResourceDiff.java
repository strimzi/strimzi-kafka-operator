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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

class ResourceDiff<T extends HasMetadata> extends AbstractJsonDiff {
    private static final Logger LOGGER = LogManager.getLogger(ResourceDiff.class.getName());
    private static final ReconciliationLogger RECONCILIATION_LOGGER = ReconciliationLogger.create(LOGGER);

    private final boolean isEmpty;

    public ResourceDiff(Reconciliation reconciliation, String resourceKind, String resourceName, T current, T desired, Pattern ignorableFields) {
        JsonNode source = patchMapper().valueToTree(current == null ? "{}" : current);
        JsonNode target = patchMapper().valueToTree(desired == null ? "{}" : desired);
        JsonNode diff = JsonDiff.asJson(source, target);

        int num = 0;

        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();

            if (ignorableFields.matcher(pathValue).matches()) {
                LOGGER.debug("Ignoring {} {} diff {}", resourceKind, resourceName, d);
                continue;
            }

            if (LOGGER.isDebugEnabled()) {
                RECONCILIATION_LOGGER.debug(reconciliation, "{} {} differs: {}", resourceKind, resourceName, d);
                RECONCILIATION_LOGGER.debug(reconciliation, "Current {} {} path {} has value {}", resourceKind, resourceName, pathValue, lookupPath(source, pathValue));
                RECONCILIATION_LOGGER.debug(reconciliation, "Desired {} {} path {} has value {}", resourceKind, resourceName, pathValue, lookupPath(target, pathValue));
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
