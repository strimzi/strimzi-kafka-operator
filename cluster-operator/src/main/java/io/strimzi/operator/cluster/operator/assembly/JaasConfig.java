/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * JaasConfig is a Java class that provides a utility method for generating Java Authentication and
 * Authorization Service (JAAS) configuration strings.
 */
public class JaasConfig {
    /**
     * Generates a JAAS configuration string based on the provided module name and options.
     *
     * @param moduleName The name of the JAAS module to be configured.
     * @param options A Map containing the options to be set for the JAAS module.
     *               The options are represented as key-value pairs, where both the key and
     *               the value must be non-null String objects.
     * @return A String representing the JAAS configuration.
     * @throws IllegalArgumentException If the moduleName is empty, or it contains '=' or ';',
     *                                  or if any key or value in the options map is empty,
     *                                  or they contain '=' or ';'.
     */
    public static String config(String moduleName, Map<String, String> options) {
        StringJoiner joiner = new StringJoiner(" ");
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);
            if (key.contains("=") || key.contains(";") || value.contains("=") || value.contains(";")){
                throw new IllegalArgumentException("Keys and values must not contain '=' or ';'");
            }if(moduleName.contains("=") || moduleName.contains(";") || moduleName.isEmpty()) {
                throw new IllegalArgumentException("module name must be not empty and must not contain '=' or ';'");
            } else{
                String s = key + "=\"" + value + "\"";
                joiner.add(s);
            }
        }
        var stringStream = joiner.toString();

        return moduleName + " required " + stringStream + ";";
    }
}