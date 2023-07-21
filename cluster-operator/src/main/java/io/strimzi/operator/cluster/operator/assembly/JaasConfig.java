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
            Objects.requireNonNull(entry.getKey());
            Objects.requireNonNull(entry.getValue());
            String s = entry.getKey() + "=\"" + entry.getValue() + "\"";
            joiner.add(s);
        }
        var stringStream = joiner.toString();

        return moduleName + " required " + stringStream + ";";
    }
}
//verify key doesn't have = or value  ", module no spaces
//