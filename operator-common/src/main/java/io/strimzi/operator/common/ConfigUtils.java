/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for Configration parsing and handling
 */
public class ConfigUtils {
    /**
     * Parse Labels from String into Map. The expected format of the String with labels is `key1=value1,key2=value2`
     *
     * @param stringLabels  String with labels
     * @return  Map with parsed labels
     * @throws InvalidConfigurationException
     */
    public static Map<String, String> parseLabels(String stringLabels) throws InvalidConfigurationException {
        Map<String, String> labels = new HashMap<>();

        try {
            if (stringLabels != null && !stringLabels.isEmpty()) {
                String[] labelsArray = stringLabels.split(",");
                for (String label : labelsArray) {
                    String[] fields = label.split("=");
                    labels.put(fields[0].trim(), fields[1].trim());
                }
            }
        } catch (ArrayIndexOutOfBoundsException e)  {
            throw new InvalidConfigurationException("Failed to parse labels", e);
        }

        return labels;
    }
}
