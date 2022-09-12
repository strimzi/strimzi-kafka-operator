/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Pod;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
     * Converts Pod to String
     *
     * @param pod   Pod which should be converted
     *
     * @throws JsonProcessingException  Throws JsonProcessingException when the conversion to String fails
     *
     * @return      String with the Pod definition
     */
    public static String podToString(Pod pod) throws JsonProcessingException {
        return MAPPER.writeValueAsString(pod);
    }

    /**
     * Converts List of Pods to List of Maps which can be used in StrimziPodSets
     *
     * @param pods  List of Pods which should be converted
     *
     * @return      List of Maps with the Pod structures
     */
    public static List<Map<String, Object>> podsToMaps(List<Pod> pods)  {
        return pods.stream().map(p -> podToMap(p)).collect(Collectors.toList());
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

    /**
     * Converts List of Maps to List of Pods which can be used in StrimziPodSets
     *
     * @param maps  List of Maps which should be converted
     *
     * @return      List of Maps with the Pod structures
     */
    public static List<Pod> mapsToPods(List<Map<String, Object>> maps)  {
        return maps.stream().map(m -> mapToPod(m)).collect(Collectors.toList());
    }

    /**
     * Check whether the Pod reached one of its terminal phases: Succeeded or Failed. This is checked based on
     * the .status.phase field.
     *
     * @param pod   The Pod object
     *
     * @return  True if the Pod is in terminal phase. False otherwise.
     */
    public static boolean isInTerminalState(Pod pod)   {
        return pod.getStatus() != null
                && ("Failed".equals(pod.getStatus().getPhase()) || "Succeeded".equals(pod.getStatus().getPhase()));
    }
}
