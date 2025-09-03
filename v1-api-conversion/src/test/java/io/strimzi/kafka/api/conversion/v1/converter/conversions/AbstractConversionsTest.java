/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;

abstract class AbstractConversionsTest {
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    protected <T> T jsonNodeToTyped(JsonNode json, Class<T> type)    {
        return JSON_MAPPER.convertValue(json, type);
    }

    protected JsonNode typedToJsonNode(Object resource)    {
        return JSON_MAPPER.valueToTree(resource);
    }
}