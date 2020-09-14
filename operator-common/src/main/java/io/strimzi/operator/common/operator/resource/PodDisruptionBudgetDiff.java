/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.zjsonpatch.JsonDiff;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

class PodDisruptionBudgetDiff extends AbstractResourceDiff  {
    private static final Logger log = LogManager.getLogger(PodDisruptionBudgetDiff.class.getName());

    private final boolean isEmpty;

    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
            "^(/metadata/managedFields" +
                    "|/status)$");

    public PodDisruptionBudgetDiff(PodDisruptionBudget current, PodDisruptionBudget desired) {
        JsonNode source = patchMapper().valueToTree(current == null ? "{}" : current);
        JsonNode target = patchMapper().valueToTree(desired == null ? "{}" : desired);
        JsonNode diff = JsonDiff.asJson(source, target);

        int num = 0;

        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();

            if (IGNORABLE_PATHS.matcher(pathValue).matches()) {
                log.debug("Ignoring PodDisruptionBudget diff {}", d);
                continue;
            }

            if (log.isDebugEnabled()) {
                log.debug("PodDisruptionBudget differs: {}", d);
                log.debug("Current PodDisruptionBudget path {} has value {}", pathValue, lookupPath(source, pathValue));
                log.debug("Desired PodDisruptionBudget path {} has value {}", pathValue, lookupPath(target, pathValue));
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
