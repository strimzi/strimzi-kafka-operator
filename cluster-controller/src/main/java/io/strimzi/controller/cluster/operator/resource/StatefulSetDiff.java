/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.zjsonpatch.JsonDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

public class StatefulSetDiff {

    private static final Logger log = LoggerFactory.getLogger(StatefulSetDiff.class.getName());

    private static boolean containsPathOrChild(Iterable<String> paths, String path) {
        for (String pathValue : paths) {
            if (pathValue.equals(path)
                    || pathValue.startsWith(path + "/")) {
                return true;
            }
        }
        return false;
    }

    private final boolean changesVolumeClaimTemplate;
    private final boolean isEmpty;
    private final boolean changesSpecTemplateSpec;
    private final boolean changesLabels;
    private final boolean changesSpecReplicas;

    public StatefulSetDiff(StatefulSet current, StatefulSet updated) {
        JsonNode diff = JsonDiff.asJson(patchMapper().valueToTree(current), patchMapper().valueToTree(updated));
        Set<String> paths = new HashSet<>();
        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();
            log.debug("StatefulSet {}/{} differs at path {}", current.getMetadata().getNamespace(), current.getMetadata().getName(), pathValue);
            paths.add(pathValue);
        }
        isEmpty = paths.isEmpty();
        changesVolumeClaimTemplate = containsPathOrChild(paths, "/spec/volumeClaimTemplates");
        // Change changes to /spec/template/spec, except to imagePullPolicy, which gets changed
        // by k8s
        changesSpecTemplateSpec = containsPathOrChild(paths.stream().filter(path ->
                        !path.matches("/spec/template/spec/containers/[0-9]+/imagePullPolicy"))
                        .collect(Collectors.toSet()),
                "/spec/template/spec");
        changesLabels = containsPathOrChild(paths, "/metadata/labels");
        changesSpecReplicas = containsPathOrChild(paths, "/spec/replicas");
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public boolean changesVolumeClaimTemplates() {
        return changesVolumeClaimTemplate;
    }

    public boolean changesSpecTemplateSpec() {
        return changesSpecTemplateSpec;
    }

    public boolean changesLabels() {
        return changesLabels;
    }

    public boolean changesSpecReplicas() {
        return changesSpecReplicas;
    }
}
