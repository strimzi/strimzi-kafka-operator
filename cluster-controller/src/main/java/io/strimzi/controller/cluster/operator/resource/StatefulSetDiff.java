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

    private final Set<String> paths = new HashSet<>();

    public StatefulSetDiff(StatefulSet current, StatefulSet updated) {
        JsonNode diff = JsonDiff.asJson(patchMapper().valueToTree(current), patchMapper().valueToTree(updated));
        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();
            paths.add(pathValue);
        }
    }

    public boolean isEmpty() {
        return paths.isEmpty();
    }

    private boolean containsPathOrChild(Set<String> paths, String path) {
        for (String pathValue : paths) {
            if (pathValue.equals(path)
                    || pathValue.startsWith(path + "/")) {
                return true;
            }
        }
        return false;
    }

    public boolean changesVolumeClaimTemplates() {
        return containsPathOrChild(paths, "/spec/volumeClaimTemplates");
    }

    public boolean changesSpecTemplateSpec() {
        // Change changes to /spec/template/spec, except to imagePullPolicy, which gets changed
        // the k8s
        return containsPathOrChild(paths.stream().filter(path ->
            !path.matches("/spec/template/spec/containers/[0-9]+/imagePullPolicy"))
                .collect(Collectors.toSet()),
            "/spec/template/spec");
    }

    public boolean changesLabels() {
        return containsPathOrChild(paths, "/metadata/labels");
    }

    public boolean changesSpecReplicas() {
        return containsPathOrChild(paths, "/spec/replicas");
    }
}
