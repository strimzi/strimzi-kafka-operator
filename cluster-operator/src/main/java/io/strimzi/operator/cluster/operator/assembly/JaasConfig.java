/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class JaasConfig {

    public static String config(String moduleName, Map<String, String> options) {
        StringJoiner joiner = new StringJoiner(" ");
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);
            // Check if the key or value contains invalid characters and if moduleName is empty
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
//verify key doesn't have = or value  ", module no spaces
//