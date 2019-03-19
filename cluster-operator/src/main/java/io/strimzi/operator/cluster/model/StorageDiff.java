/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.api.kafka.model.Storage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;
import static java.lang.Integer.parseInt;

public class StorageDiff {

    private static final Logger log = LogManager.getLogger(StorageDiff.class.getName());

    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
        "^(/deleteClaim"
        + "|/volumes/[0-9]+/deleteClaim)$");

    private final boolean isEmpty;
    private final boolean changesType;
    private final boolean changesSize;

    public StorageDiff(Storage current, Storage desired) {
        JsonNode diff = JsonDiff.asJson(patchMapper().valueToTree(current), patchMapper().valueToTree(desired));
        int num = 0;
        boolean changesType = false;
        boolean changesSize = false;

        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();
            if (IGNORABLE_PATHS.matcher(pathValue).matches()) {
                log.debug("Ignoring Storage diff {}", d);
                continue;
            }
            if (log.isDebugEnabled()) {
                log.debug("Storage differs: {}", d);
                log.debug("Current Storage path {} has value {}", pathValue, getFromPath(current, pathValue));
                log.debug("Desired Storage path {} has value {}", pathValue, getFromPath(desired, pathValue));
            }

            num++;
            changesType |= pathValue.endsWith("/type");
            changesSize |= pathValue.endsWith("/size");
        }

        this.isEmpty = num == 0;
        this.changesType = changesType;
        this.changesSize = changesSize;
    }

    private JsonNode getFromPath(Storage current, String pathValue) {
        JsonNode node1 = patchMapper().valueToTree(current);
        for (String field : pathValue.replaceFirst("^/", "").split("/")) {
            JsonNode node2 = node1.get(field);
            if (node2 == null) {
                try {
                    int index = parseInt(field);
                    node2 = node1.get(index);
                } catch (NumberFormatException e) {
                }
            }
            if (node2 == null) {
                node1 = null;
                break;
            } else {
                node1 = node2;
            }
        }
        return node1;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    /** Returns true if there's a difference in {@code /type} */
    public boolean changesType() {
        return changesType;
    }

    /** Returns true if there's a difference in {@code /size} */
    public boolean changesSize() {
        return changesSize;
    }
}
