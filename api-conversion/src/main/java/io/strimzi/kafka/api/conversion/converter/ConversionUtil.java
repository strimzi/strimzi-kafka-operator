/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConversionUtil {

    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    static List<String> pathTokens(String xpath) {
        return Arrays.stream(xpath.split("/")).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    static ObjectNode toObjectNode(JsonNode node, int index) {
        if (!(node instanceof ObjectNode)) {
            throw new IllegalStateException("Node at " + index + " is not ObjectNode: " + node);
        }
        return (ObjectNode) node;
    }

    static JsonNode get(JsonNode root, String xpath) {
        List<String> tokens = pathTokens(xpath);
        for (String token : tokens) {
            root = root.get(token);
            if (root == null) {
                break;
            }
        }
        return root;
    }

    static void set(JsonNode root, String xpath, JsonNode value) {
        List<String> tokens = pathTokens(xpath);
        int i = 0;
        for (; i < tokens.size() - 1; i++) {
            String field = tokens.get(i);
            JsonNode node = root.get(field);
            if (node == null) {
                if (value == null) {
                    break; // no point in setting new ObjectNode
                }
                ObjectNode on = toObjectNode(root, i);
                node = JSON_MAPPER.getNodeFactory().objectNode();
                on.set(field, node);
            }
            root = node;
        }
        // did we get to the end
        if (i == tokens.size() - 1) {
            ObjectNode on = toObjectNode(root, i);
            String lastToken = tokens.get(i);
            if (value != null) {
                on.set(lastToken, value);
            } else {
                on.remove(lastToken);
            }
        }
    }

    static <T, R> JsonNode replace(JsonNode original, Class<T> clazz, Function<T, R> fn) {
        try {
            T t = JSON_MAPPER.readerFor(clazz).readValue(original);
            R r = fn.apply(t);
            byte[] bytes = JSON_MAPPER.writeValueAsBytes(r);
            return JSON_MAPPER.readTree(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void replace(JsonNode node, String xpath, BiFunction<JsonNode, JsonNodeCreator, JsonNode> fn) {
        JsonNode leaf = get(node, xpath);
        JsonNode newValue = fn.apply(leaf, JSON_MAPPER.getNodeFactory());
        set(node, xpath, newValue);
    }

    static void move(JsonNode node, String fromPath, String toPath) {
        JsonNode value = get(node, fromPath);
        if (value != null) {
            JsonNode target = get(node, toPath);
            if (target != null) {
                throw new RuntimeException("Cannot move " + fromPath + " to " + toPath + ". The target path already exists. Please resolve the issue manually and run the API conversion tool again.");
            }

            set(node, fromPath, null);
            set(node, toPath, value);
        }
    }
}
