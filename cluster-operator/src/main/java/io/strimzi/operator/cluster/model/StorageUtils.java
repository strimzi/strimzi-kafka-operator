/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.common.model.InvalidResourceException;

/**
 * Shared methods for working with Storage - for example comparing volume sizes
 */
public class StorageUtils {
    /**
     * Parse a K8S-style representation of a quantity of memory, such as {@code 512Mi}, into the equivalent number in
     * the specified units. For example, a memory value of "100Gb" and a unit value of "Mb" will return 100000. This
     * handles values up to around 9Pi, then the long overflows. This should not cause issues in reality since it is
     * unlikely we will have broker with disks over petabyte of size.
     *
     * @param memory The String representation of the quantity of memory.
     * @param units The units which the bytes should be returned in. The format of units
     *              follows the K8S-style representation of memory units.
     *
     * @return The equivalent number in the specified units.
     */
    public static double convertTo(String memory, String units) {
        return convertToMillibytes(memory) / (double) memoryFactor(units);
    }

    /**
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi}, into the equivalent number of millibytes
     * represented as a long. This handles values up to around 9Pi, then the long overflows. This should not cause
     * issues in reality since it is unlikely we will have broker with disks over petabyte of size.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of millibytes.
     */
    public static long convertToMillibytes(Quantity size) {
        String amount = size.getAmount();
        String format = size.getFormat();

        if (format != null) {
            return convertToMillibytes(amount + format);
        } else  {
            return convertToMillibytes(amount);
        }
    }

    /**
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi}, into the equivalent number of millibytes
     * represented as a long. This handles values up to around 9Pi, then the long overflows. This should not cause
     * issues in reality since it is unlikely we will have broker with disks over petabyte of size.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of millibytes.
     */
    public static long convertToMillibytes(String size) {
        boolean seenE = false;
        long factor = 1_000L; // Default unit is bytes -> so we need to set the default factor to 1000 to convert it to millibytes
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
            result = (long) Double.parseDouble(size) * factor;
        } else {
            result = (long) (Double.parseDouble(size.substring(0, end)) * factor);
        }
        return result;
    }

    /**
     * Returns the factor which can be used to convert different units to millibytes.
     *
     * @param suffix    The Unit which should be converted
     * @return          The factor corresponding to the suffix unit
     */
    private static long memoryFactor(String suffix) {
        long factor;
        switch (suffix) {
            case "E":
                factor = 1_000L * 1_000L * 1_000L * 1_000 * 1_000L * 1_000L * 1_000L;
                break;
            case "P":
                factor = 1_000L * 1_000L * 1_000L * 1_000 * 1_000L * 1_000L;
                break;
            case "T":
                factor = 1_000L * 1_000L * 1_000L * 1_000 * 1_000L;
                break;
            case "G":
                factor = 1_000L * 1_000L * 1_000L * 1_000L;
                break;
            case "M":
                factor = 1_000L * 1_000L * 1_000L;
                break;
            case "K":
                factor = 1_000L * 1_000L;
                break;
            case "Ei":
                factor = 1_024L * 1_024L * 1_024L * 1_024L * 1_024L * 1_024L * 1_000L;
                break;
            case "Pi":
                factor = 1_024L * 1_024L * 1_024L * 1_024L * 1_024L * 1_000L;
                break;
            case "Ti":
                factor = 1_024L * 1_024L * 1_024L * 1_024L * 1_000L;
                break;
            case "Gi":
                factor = 1_024L * 1_024L * 1_024L * 1_000L;
                break;
            case "Mi":
                factor = 1_024L * 1_024L * 1_000L;
                break;
            case "Ki":
                factor = 1_024L * 1_000L;
                break;
            case "m":
                factor = 1L;
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
            if (Storage.TYPE_JBOD.equals(storage.getType()) && storage instanceof JbodStorage jbodStorage) {
                return jbodStorage.getVolumes().stream().anyMatch(StorageUtils::isEphemeral);
            }
        }
        return false;
    }

    /**
     * Validates persistent storage
     * - If storage is of a persistent type, validations are made
     * - If storage is not of a persistent type, validation passes
     *
     * @param storage   Persistent Storage configuration
     * @param path      Path in the custom resource where the problem occurs
     *
     * @throws InvalidResourceException if validations fails for any reason
     */
    public static void validatePersistentStorage(Storage storage, String path)   {
        if (storage instanceof PersistentClaimStorage persistentClaimStorage) {
            checkPersistentStorageSizeIsValid(persistentClaimStorage, path);

        } else if (storage instanceof JbodStorage jbodStorage)  {

            if (jbodStorage.getVolumes() == null || jbodStorage.getVolumes().size() == 0)   {
                throw new InvalidResourceException("JbodStorage needs to contain at least one volume (" + path + ")");
            }

            for (Storage jbodVolume : jbodStorage.getVolumes()) {
                if (jbodVolume instanceof PersistentClaimStorage persistentClaimStorage) {
                    checkPersistentStorageSizeIsValid(persistentClaimStorage, path);
                }
            }
        }
    }

    /**
     * Checks if the supplied PersistentClaimStorage has a valid size
     *
     * @param storage   PersistentClaimStorage configuration
     * @param path      Path in the custom resource where the problem occurs
     *
     * @throws InvalidResourceException if the persistent storage size is not valid
     */
    private static void checkPersistentStorageSizeIsValid(PersistentClaimStorage storage, String path)   {
        if (storage.getSize() == null || storage.getSize().isEmpty()) {
            throw new InvalidResourceException("The size is mandatory for a persistent-claim storage (" + path + ")");
        }
    }
}
