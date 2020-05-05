/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.common.operator.resource.AbstractResourceDiff;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

public class StatusDiff extends AbstractResourceDiff {
    private static final Logger log = LogManager.getLogger(StatusDiff.class.getName());

    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
            "^(/conditions/[0-9]+/lastTransitionTime)$");

    private final boolean isEmpty;

    public StatusDiff(Status current, Status desired) {
        // use SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS just for better human readability in the logs
        JsonNode source = patchMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).valueToTree(current == null ? "{}" : current);
        JsonNode target = patchMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).valueToTree(desired == null ? "{}" : desired);
        JsonNode diff = JsonDiff.asJson(source, target);

        int num = 0;

        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();

            if (IGNORABLE_PATHS.matcher(pathValue).matches()) {
                log.debug("Ignoring Status diff {}", d);
                continue;
            }

            if (log.isDebugEnabled()) {
                log.debug("Status differs: {}", d);
                log.debug("Current Status path {} has value {}", pathValue, lookupPath(source, pathValue));
                log.debug("Desired Status path {} has value {}", pathValue, lookupPath(target, pathValue));
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
