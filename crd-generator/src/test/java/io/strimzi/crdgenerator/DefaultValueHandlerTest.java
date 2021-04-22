/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.api.annotations.ApiVersion;
import io.vertx.core.cli.annotations.DefaultValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultValueHandlerTest {

    private static final Consumer<String> NOOP_LOGGER = s -> { };
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private DefaultValueHandler v1DefaultHandler;
    private ObjectNode blankNode;

    @BeforeEach
    void setup() {
        v1DefaultHandler = new DefaultValueHandler(ApiVersion.V1, NOOP_LOGGER);
        blankNode = MAPPER.createObjectNode();
    }

    @Test
    void testSchemaNotModifiedForOlderVersions() throws NoSuchMethodException {
        Map<ApiVersion, DefaultValueHandler> handlers = List.of(
                ApiVersion.V1ALPHA1,
                ApiVersion.V1BETA1,
                ApiVersion.V1BETA2,
                ApiVersion.V1)
            .stream()
            .collect(Collectors.toMap(version -> version, version -> new DefaultValueHandler(version, NOOP_LOGGER)));

        Property property = new Property(DefaultsGalore.class.getDeclaredMethod("getString"));

        ObjectNode withDefault = MAPPER.createObjectNode();
        withDefault.put("default", "ABCD");

        for (ApiVersion version : handlers.keySet()) {
            DefaultValueHandler handler = handlers.get(version);
            ObjectNode result = handler.addDefaultValueIfApplicable(blankNode.deepCopy(), property);

            if (version == ApiVersion.V1) {
                assertEquals(withDefault, result);
            } else {
                assertEquals(blankNode, result, "API version " + version + " has default when it shouldn't");
            }
        }
    }
    
    @Test
    void testStringDefault() throws NoSuchMethodException {
        Property property = new Property(DefaultsGalore.class.getDeclaredMethod("getString"));
        ObjectNode result = v1DefaultHandler.addDefaultValueIfApplicable(blankNode, property);
        assertEquals("ABCD", result.get("default").asText());
    }

    @Test
    void testShortDefaults() throws NoSuchMethodException {
        Property primitiveProperty = new Property(DefaultsGalore.class.getDeclaredMethod("getShort"));
        Property boxedProperty = new Property(DefaultsGalore.class.getDeclaredMethod("getBoxedShort"));
        
        ObjectNode primitiveResult = v1DefaultHandler.addDefaultValueIfApplicable(blankNode.deepCopy(), primitiveProperty);
        ObjectNode boxedResult = v1DefaultHandler.addDefaultValueIfApplicable(blankNode.deepCopy(), boxedProperty);
        
        assertEquals(1, primitiveResult.get("default").asInt());
        assertEquals(2, boxedResult.get("default").asInt());
    }

    @Test
    void testIntDefaults() throws NoSuchMethodException {
        Property primitiveProperty = new Property(DefaultsGalore.class.getDeclaredMethod("getInt"));
        Property boxedProperty = new Property(DefaultsGalore.class.getDeclaredMethod("getBoxedInt"));

        ObjectNode primitiveResult = v1DefaultHandler.addDefaultValueIfApplicable(blankNode.deepCopy(), primitiveProperty);
        ObjectNode boxedResult = v1DefaultHandler.addDefaultValueIfApplicable(blankNode.deepCopy(), boxedProperty);

        assertEquals(3, primitiveResult.get("default").asInt());
        assertEquals(4, boxedResult.get("default").asInt());
    }

    @Test
    void testLongDefaults() throws NoSuchMethodException {
        Property primitiveProperty = new Property(DefaultsGalore.class.getDeclaredMethod("getLong"));
        Property boxedProperty = new Property(DefaultsGalore.class.getDeclaredMethod("getBoxedLong"));

        ObjectNode primitiveResult = v1DefaultHandler.addDefaultValueIfApplicable(blankNode.deepCopy(), primitiveProperty);
        ObjectNode boxedResult = v1DefaultHandler.addDefaultValueIfApplicable(blankNode.deepCopy(), boxedProperty);

        assertEquals(5, primitiveResult.get("default").asInt());
        assertEquals(6, boxedResult.get("default").asInt());
    }

    @Test
    void testNormalEnumDefaults() throws NoSuchMethodException {
        Property property = new Property(DefaultsGalore.class.getDeclaredMethod("getEnum"));
        ObjectNode result = v1DefaultHandler.addDefaultValueIfApplicable(blankNode, property);
        assertEquals("BAR", result.get("default").asText());
    }

    @Test
    void testCustomisedEnumDefaults() throws NoSuchMethodException {
        Property property = new Property(DefaultsGalore.class.getDeclaredMethod("getCustomisedEnum"));
        ObjectNode result = v1DefaultHandler.addDefaultValueIfApplicable(blankNode, property);
        assertEquals(CustomisedEnum.TWO.toValue(), result.get("default").asText());
    }

    @Test
    void testErrorLoggedWhenDefaultNotParsableWholeNumber() throws NoSuchMethodException {

        AtomicReference<String> errorLogger = new AtomicReference<>();
        DefaultValueHandler handler = new DefaultValueHandler(ApiVersion.V1, errorLogger::set);

        Property property = new Property(DefaultsGalore.class.getDeclaredMethod("getUnparseableDefaultNumber"));

        ObjectNode result = handler.addDefaultValueIfApplicable(blankNode.deepCopy(), property);

        assertEquals(blankNode, result);
        assertEquals("Default value Not_A_Number cannot be converted to an OpenAPI integer for " +
                "property unparseableDefaultNumber with return type of long", errorLogger.get());
    }

    @Test
    void testErrorLoggedWhenDefaultNotFoundInEnum() throws NoSuchMethodException {

        AtomicReference<String> errorLogger = new AtomicReference<>();
        DefaultValueHandler handler = new DefaultValueHandler(ApiVersion.V1, errorLogger::set);

        Property property1 = new Property(DefaultsGalore.class.getDeclaredMethod("getWrongCasedNormalEnum"));
        Property property2 = new Property(DefaultsGalore.class.getDeclaredMethod("getMissingEnumMember"));

        ObjectNode result = handler.addDefaultValueIfApplicable(blankNode.deepCopy(), property1);

        assertEquals(blankNode, result);
        assertEquals("Default value foo was not a member of the enum of type class " +
                "io.strimzi.crdgenerator.NormalEnum returned by property wrongCasedNormalEnum", errorLogger.get());

        result = handler.addDefaultValueIfApplicable(blankNode.deepCopy(), property2);

        assertEquals(blankNode, result);
        assertEquals("Default value onnne was not a member of the enum of type class " +
                "io.strimzi.crdgenerator.CustomisedEnum returned by property missingEnumMember", errorLogger.get());
    }

    static class DefaultsGalore {
        
        @DefaultValue("ABCD")
        String getString() {
            return null;
        }

        @DefaultValue("1")
        short getShort() {
            return -1;
        }

        @DefaultValue("2")
        Short getBoxedShort() {
            return -1;
        }

        @DefaultValue("3")
        int getInt() {
            return -1;
        }

        @DefaultValue("4")
        Integer getBoxedInt() {
            return -1;
        }

        @DefaultValue("5")
        long getLong() {
            return -1;
        }

        @DefaultValue("6")
        Long getBoxedLong() {
            return -1L;
        }

        @DefaultValue("BAR")
        NormalEnum getEnum() {
            return null;
        }

        @DefaultValue("two")
        CustomisedEnum getCustomisedEnum() {
            return null;
        }

        @DefaultValue("Not_A_Number")
        long getUnparseableDefaultNumber() {
            return 1L;
        }

        @DefaultValue("foo")
        NormalEnum getWrongCasedNormalEnum() {
            return null;
        }
        
        @DefaultValue("onnne")
        CustomisedEnum getMissingEnumMember() {
            return null;
        }
    }

}