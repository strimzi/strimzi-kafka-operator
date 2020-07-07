/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;

public abstract class AbstractResourceDiff {
    protected static JsonNode lookupPath(JsonNode source, String path) {
        JsonNode s = source;
        for (String component : path.substring(1).split("/")) {
            if (s.isArray()) {
                try {
                    s = s.path(Integer.parseInt(component));
                } catch (NumberFormatException e) {
                    return MissingNode.getInstance();
                }
            } else {
                s = s.path(component);
            }
        }
        return s;
    }

    /**
     * Returns whether the Diff is empty or not.
     *
     * @return whether the Diff is empty or not.
     */
    public abstract boolean isEmpty();
}
