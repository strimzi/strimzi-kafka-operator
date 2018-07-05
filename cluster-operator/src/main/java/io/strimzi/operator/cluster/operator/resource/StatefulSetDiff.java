/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.zjsonpatch.JsonDiff;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;
import static java.util.Arrays.asList;

public class StatefulSetDiff {

    private static final Logger log = LogManager.getLogger(StatefulSetDiff.class.getName());

    private static final List<Pattern> IGNORABLE_PATHS;
    static {
        IGNORABLE_PATHS = asList(
            "/spec/revisionHistoryLimit",
            "/spec/template/spec/initContainers/[0-9]+/imagePullPolicy",
            "/spec/template/spec/initContainers/[0-9]+/resources",
            "/spec/template/spec/initContainers/[0-9]+/terminationMessagePath",
            "/spec/template/spec/initContainers/[0-9]+/terminationMessagePolicy",
            "/spec/template/spec/initContainers/[0-9]+/env/[0-9]+/valueFrom/fieldRef/apiVersion",
            "/spec/template/spec/initContainers/[0-9]+/env/[0-9]+/value",
            "/spec/template/spec/containers/[0-9]+/imagePullPolicy",
            "/spec/template/spec/containers/[0-9]+/livenessProbe/failureThreshold",
            "/spec/template/spec/containers/[0-9]+/livenessProbe/periodSeconds",
            "/spec/template/spec/containers/[0-9]+/livenessProbe/successThreshold",
            "/spec/template/spec/containers/[0-9]+/readinessProbe/failureThreshold",
            "/spec/template/spec/containers/[0-9]+/readinessProbe/periodSeconds",
            "/spec/template/spec/containers/[0-9]+/readinessProbe/successThreshold",
            "/spec/template/spec/containers/[0-9]+/resources",
            "/spec/template/spec/containers/[0-9]+/terminationMessagePath",
            "/spec/template/spec/containers/[0-9]+/terminationMessagePolicy",
            "/spec/template/spec/dnsPolicy",
            "/spec/template/spec/restartPolicy",
            "/spec/template/spec/schedulerName",
            "/spec/template/spec/securityContext",
            "/spec/template/spec/terminationGracePeriodSeconds",
            "/spec/template/spec/volumes/[0-9]+/configMap/defaultMode",
            "/spec/template/spec/volumes/[0-9]+/secret/defaultMode",
            "/spec/volumeClaimTemplates/[0-9]+/status",
            "/spec/template/spec/serviceAccount",
            "/status").stream().map(Pattern::compile).collect(Collectors.toList());
    }

    private static boolean containsPathOrChild(Iterable<String> paths, String path) {
        for (String pathValue : paths) {
            if (equalsOrPrefix(path, pathValue)) {
                return true;
            }
        }
        return false;
    }

    private static boolean equalsOrPrefix(String path, String pathValue) {
        return pathValue.equals(path)
                || pathValue.startsWith(path + "/");
    }

    private final boolean changesVolumeClaimTemplate;
    private final boolean isEmpty;
    private final boolean changesSpecTemplateSpec;
    private final boolean changesLabels;
    private final boolean changesSpecReplicas;

    public StatefulSetDiff(StatefulSet current, StatefulSet updated) {
        JsonNode diff = JsonDiff.asJson(patchMapper().valueToTree(current), patchMapper().valueToTree(updated));
        Set<String> paths = new HashSet<>();
        outer: for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();
            for (Pattern pattern : IGNORABLE_PATHS) {
                if (pattern.matcher(pathValue).matches()) {
                    log.debug("StatefulSet {}/{} ignoring diff {}", current.getMetadata().getNamespace(), current.getMetadata().getName(), d);
                    continue outer;
                }
            }
            log.debug("StatefulSet {}/{} differs at path {}", current.getMetadata().getNamespace(), current.getMetadata().getName(), pathValue);
            paths.add(pathValue);
        }
        isEmpty = paths.isEmpty();
        changesVolumeClaimTemplate = containsPathOrChild(paths, "/spec/volumeClaimTemplates");
        // Change changes to /spec/template/spec, except to imagePullPolicy, which gets changed
        // by k8s
        changesSpecTemplateSpec = containsPathOrChild(paths, "/spec/template/spec");
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
