/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Pod;

import java.util.Map;

/**
 * Shared methods for working with StrimziPodSet resources
 */
public class PodSetUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> POD_TYPE = new TypeReference<>() { };

    /**
     * Converts Pod to Map for storing it in StrimziPodSets
     *
     * @param pod   Pod which should be converted
     *
     * @return      Map representing the pod
     */
    public static Map<String, Object> podToMap(Pod pod) {
        return MAPPER.convertValue(pod, POD_TYPE);
    }

    /**
     * Converts Map to Pod for decoding of StrimziPodSets
     *
     * @param map   Pod represented as Map which should be decoded
     *
     * @return      Pod object decoded from the map
     */
    public static Pod mapToPod(Map<String, Object> map) {
        return MAPPER.convertValue(map, Pod.class);
    }
}
