/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Quantity;

/**
 * Shared methods for working with Storage - for example comparing volume sizes
 */
public class StorageUtils {
    /**
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi},
     * into the equivalent number of bytes represented as a long.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of bytes.
     */
    public static long parseMemory(Quantity size) {
        String amount = size.getAmount();
        String format = size.getFormat();

        if (format != null) {
            return parseMemory(amount + format);
        } else  {
            return parseMemory(amount);
        }
    }

    /**
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi},
     * into the equivalent number of bytes represented as a long.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of bytes.
     */
    public static long parseMemory(String size) {
        boolean seenE = false;
        long factor = 1L;
        int end = size.length();
        for (int i = 0; i < size.length(); i++) {
            char ch = size.charAt(i);
            if (ch == 'e') {
                seenE = true;
            } else if (ch < '0' || '9' < ch) {
                end = i;
                factor = memoryFactor(size.substring(i));
                break;
            }
        }
        long result;
        if (seenE) {
            result = (long) Double.parseDouble(size);
        } else {
            result = Long.parseLong(size.substring(0, end)) * factor;
        }
        return result;
    }

    /**
     * Returns the factor which can be used to convert different units to bytes.
     *
     * @param suffix    The Unit which should be converted
     * @return          The factor corresponding to the suffix unit
     */
    private static long memoryFactor(String suffix) {
        long factor;
        switch (suffix) {
            case "E":
                factor = 1_000L * 1_000L * 1_000L * 1_000 * 1_000L * 1_000L;
                break;
            case "P":
                factor = 1_000L * 1_000L * 1_000L * 1_000 * 1_000L;
                break;
            case "T":
                factor = 1_000L * 1_000L * 1_000L * 1_000;
                break;
            case "G":
                factor = 1_000L * 1_000L * 1_000L;
                break;
            case "M":
                factor = 1_000L * 1_000L;
                break;
            case "K":
                factor = 1_000L;
                break;
            case "Ei":
                factor = 1_024L * 1_024L * 1_024L * 1_024L * 1_024L * 1_024L;
                break;
            case "Pi":
                factor = 1_024L * 1_024L * 1_024L * 1_024L * 1_024L;
                break;
            case "Ti":
                factor = 1_024L * 1_024L * 1_024L * 1_024L;
                break;
            case "Gi":
                factor = 1_024L * 1_024L * 1_024L;
                break;
            case "Mi":
                factor = 1_024L * 1_024L;
                break;
            case "Ki":
                factor = 1_024L;
                break;
            default:
                throw new IllegalArgumentException("Invalid memory suffix: " + suffix);
        }
        return factor;
    }
}
