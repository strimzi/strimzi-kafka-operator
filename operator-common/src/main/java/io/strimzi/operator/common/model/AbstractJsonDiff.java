/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.client.utils.Serialization;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.node.MissingNode;

/**
 * Abstract class for diffing Json and YAML resources
 */
public abstract class AbstractJsonDiff {
    // use SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS just for better human readability in the logs
    @SuppressWarnings("deprecation") // Suppress deprecated warning of SerializationFeature.WRITE_EMPTY_JSON_ARRAYS which currently does not have proper alternative
    protected static final ObjectMapper PATCH_MAPPER = Serialization.jsonMapper().rebuild()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .disable(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS)
            .build();

    /**
     * Constructor
     */
    public AbstractJsonDiff() { }

    protected static JsonNode lookupPath(JsonNode source, String path) {
        if (path.isEmpty() || "/".equals(path)) {
            return source;
        } else {
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
    }

    /**
     * Returns whether the Diff is empty or not.
     *
     * @return whether the Diff is empty or not.
     */
    public abstract boolean isEmpty();
}
