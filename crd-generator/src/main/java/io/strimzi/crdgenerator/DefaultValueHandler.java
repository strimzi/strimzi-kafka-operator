/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.api.annotations.ApiVersion;
import io.vertx.core.cli.annotations.DefaultValue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;

import static io.strimzi.api.annotations.ApiVersion.V1;

/**
 * Processes {@link io.vertx.core.cli.annotations.DefaultValue} annotations on API model properties to populate
 * the CRD schema with the default value. It seeks to ensure that annotated default values are valid for the type of the
 * given property.
 */
class DefaultValueHandler {

    private final ApiVersion crdApiVersion;
    private final Consumer<String> err;

    DefaultValueHandler(ApiVersion crdApiVersion, Consumer<String> err) {
        this.crdApiVersion = crdApiVersion;
        this.err = err;
    }

    @SuppressWarnings("ConstantConditions")
    ObjectNode addDefaultValueIfApplicable(ObjectNode schemaNode, Property property) {

        //Default values only supported in v1 up
        if (crdApiVersion.compareTo(V1) < 0 || !property.isAnnotationPresent(DefaultValue.class)) {
            return schemaNode;
        }

        String defaultValue = property.getAnnotation(DefaultValue.class).value();
        Class<?> returnType = property.getType().getType();

        if (String.class.equals(returnType)) {
            schemaNode.put("default", defaultValue);
        } else if (Schema.isIntegerType(returnType)) {
            try {
                // Parse as long as ints and shorts will fit happily into it
                schemaNode.put("default", Long.parseLong(defaultValue));
            } catch (NumberFormatException e) {
                err.accept("Default value " + defaultValue + " cannot be converted to an OpenAPI integer for property "
                             + property.getName() + " with return type of " + returnType);
            }
        } else if (property.getType().isEnum()) {
            schemaNode = enumDefault(schemaNode, property, defaultValue, returnType);
        }

        return schemaNode;
    }

    private ObjectNode enumDefault(ObjectNode schemaNode, Property property, String defaultValue, Class<?> returnType) {
        boolean defaultExists = false;

        Optional<Method> jsonCreatorMethod = Arrays.stream(returnType.getDeclaredMethods())
                .filter(m -> m.getAnnotation(JsonCreator.class) != null)
                .findFirst();

        // If it's of the enums with a custom serialization factory method, use it instead of straight enum names.
        // As some enum values have specific serialisation representations
        if (jsonCreatorMethod.isPresent()) {
            try {
                Object enumValue = jsonCreatorMethod.get().invoke(null, defaultValue);
                if (enumValue != null) {
                    defaultExists = true;
                }
            } catch (IllegalAccessException | InvocationTargetException ignored) {
                //noinspection ConstantConditions - I'm being explicit, Intellij
                defaultExists = false;
            }
        } else {
            Enum<?>[] elements = property.getType().getEnumElements();
            for (Enum<?> e : elements) {
                if (e.name().equals(defaultValue)) {
                    defaultExists = true;
                    break;
                }
            }
        }

        if (defaultExists) {
            schemaNode.put("default", defaultValue);
        } else {
            err.accept("Default value " + defaultValue + " was not a member of the enum of type " +
                          returnType + " returned by property " + property.getName());
        }

        return schemaNode;
    }
}
