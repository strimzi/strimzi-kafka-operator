/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.Storage;

/**
 * Shared methods for working with Storage - for example comparing volume sizes
 */
public class StorageUtils {
    /**
     * Parse a K8S-style representation of a quantity of memory, such as {@code 512Mi},
     * into the equivalent number of bytes in the specified units.
     * For example, a memory value of "100Gb" and a unit value of "Mb" will return 100000,
     *
     * @param memory The String representation of the quantity of memory.
     * @param units The units which the bytes should be returned in. The format of units
     *              follows the K8S-style representation of memory units.
     *
     * @return The equivalent number of bytes in the specified units.
     */
    public static double parseMemory(String memory, String units) {
        return parseMemory(memory) / (double) memoryFactor(units);
    }

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
            } else if ((ch < '0' || '9' < ch) && ch != '.') {
                end = i;
                factor = memoryFactor(size.substring(i));
                break;
            }
        }
        long result;
        if (seenE) {
            result = (long) Double.parseDouble(size);
        } else {
            result = (long) (Double.parseDouble(size.substring(0, end)) * factor);
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


    private static boolean isEphemeral(Storage storage) {
        return Storage.TYPE_EPHEMERAL.equals(storage.getType());
    }

    /**
     * Helper method to check if the provided storage uses ephemeral storage.
     * (Either if it is an ephemeral storage volume, or a jbod including at
     * least one ephemeral volume).
     *
     * @param storage volume to check
     * @return true if it uses ephemeral
     */
    public static boolean usesEphemeral(Storage storage) {
        if (storage != null) {
            if (isEphemeral(storage)) {
                return true;
            }
            if (Storage.TYPE_JBOD.equals(storage.getType()) && storage instanceof JbodStorage) {
                JbodStorage jbodStorage = (JbodStorage) storage;
                return jbodStorage.getVolumes().stream().anyMatch(StorageUtils::isEphemeral);
            }
        }
        return false;
    }
}
