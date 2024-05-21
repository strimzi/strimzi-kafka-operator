/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.utils;

import java.util.List;

public class PerformanceUtils {

    // ensuring that object can not be created outside of class
    private PerformanceUtils() {}

    /**
     * Converts a List of Long objects to a primitive long array.
     * @param longList The List<Long> to be converted.
     * @return A primitive long array containing the elements of the given List.
     */
    public static long[] convertListToPrimitiveArray(List<Long> longList) {
        return longList.stream()
            .mapToLong(Long::longValue)
            .toArray();
    }
}
