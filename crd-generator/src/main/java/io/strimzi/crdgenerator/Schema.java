/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class Schema {
    private Schema() { }

    static boolean isBoxedPrimitive(Class<?> cls) {
        boolean intLike = Short.class.equals(cls)
                || Integer.class.equals(cls)
                || Long.class.equals(cls);
        return Boolean.class.equals(cls)
                || intLike
                || Float.class.equals(cls)
                || Double.class.equals(cls)
                || Character.class.equals(cls);
    }

    static boolean isJsonScalarType(Class<?> cls) {
        return cls.isPrimitive()
                || isBoxedPrimitive(cls)
                || cls.equals(String.class)
                || cls.isEnum();
    }

    static List<JsonNode> enumCases(Enum<?>[] values) {
        try {
            List<JsonNode> result = new ArrayList<>();
            ObjectMapper objectMapper = new ObjectMapper();
            for (Object o : values) {
                result.add(objectMapper.readTree(objectMapper.writeValueAsString(o)));
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
