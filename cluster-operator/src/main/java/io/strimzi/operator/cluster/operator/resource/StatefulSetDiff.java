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

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;

public class StatefulSetDiff {

    private static final Logger log = LogManager.getLogger(StatefulSetDiff.class.getName());

    private static final List<Pattern> IGNORABLE_PATHS;
    static {
        IGNORABLE_PATHS = asList(
            "/spec/revisionHistoryLimit",
            "/spec/template/metadata/annotations", // Actually it's only the statefulset-generation annotation we care about
            "/spec/template/spec/initContainers/[0-9]+/imagePullPolicy",
            "/spec/template/spec/initContainers/[0-9]+/resources",
            "/spec/template/spec/initContainers/[0-9]+/terminationMessagePath",
            "/spec/template/spec/initContainers/[0-9]+/terminationMessagePolicy",
            "/spec/template/spec/initContainers/[0-9]+/env/[0-9]+/valueFrom/fieldRef/apiVersion",
            "/spec/template/spec/initContainers/[0-9]+/env/[0-9]+/value",
            "/spec/template/spec/containers/0/imagePullPolicy",
            "/spec/template/spec/containers/0/livenessProbe/failureThreshold",
            "/spec/template/spec/containers/0/livenessProbe/periodSeconds",
            "/spec/template/spec/containers/0/livenessProbe/successThreshold",
            "/spec/template/spec/containers/0/readinessProbe/failureThreshold",
            "/spec/template/spec/containers/0/readinessProbe/periodSeconds",
            "/spec/template/spec/containers/0/readinessProbe/successThreshold",
            "/spec/template/spec/containers/0/resources",
            "/spec/template/spec/containers/0/terminationMessagePath",
            "/spec/template/spec/containers/0/terminationMessagePolicy",
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

    private static boolean equalsOrPrefix(String path, String pathValue) {
        return pathValue.equals(path)
                || pathValue.startsWith(path + "/");
    }

    private final boolean changesVolumeClaimTemplate;
    private final boolean isEmpty;
    private final boolean changesSpecTemplateSpec;
    private final boolean changesLabels;
    private final boolean changesSpecReplicas;

    public StatefulSetDiff(StatefulSet current, StatefulSet desired) {
        JsonNode diff = JsonDiff.asJson(patchMapper().valueToTree(current), patchMapper().valueToTree(desired));
        int num = 0;
        boolean changesVolumeClaimTemplate = false;
        boolean changesSpecTemplateSpec = false;
        boolean changesLabels = false;
        boolean changesSpecReplicas = false;
        outer: for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();
            for (Pattern pattern : IGNORABLE_PATHS) {
                if (pattern.matcher(pathValue).matches()) {
                    log.debug("StatefulSet {}/{} ignoring diff {}", current.getMetadata().getNamespace(), current.getMetadata().getName(), d);
                    continue outer;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("StatefulSet {}/{} differs: {}", current.getMetadata().getNamespace(), current.getMetadata().getName(), d);
                log.debug("Current StatefulSet path {} has value {}", pathValue, getFromPath(current, pathValue));
                log.debug("Desired StatefulSet path {} has value {}", pathValue, getFromPath(desired, pathValue));
            }

            num++;
            changesVolumeClaimTemplate |= equalsOrPrefix("/spec/volumeClaimTemplates", pathValue);
            // Change changes to /spec/template/spec, except to imagePullPolicy, which gets changed
            // by k8s
            changesSpecTemplateSpec |= equalsOrPrefix("/spec/template/spec", pathValue);
            changesLabels |= equalsOrPrefix("/metadata/labels", pathValue);
            changesSpecReplicas |= equalsOrPrefix("/spec/replicas", pathValue);
        }
        this.isEmpty = num == 0;
        this.changesLabels = changesLabels;
        this.changesSpecReplicas = changesSpecReplicas;
        this.changesSpecTemplateSpec = changesSpecTemplateSpec;
        this.changesVolumeClaimTemplate = changesVolumeClaimTemplate;
    }

    private JsonNode getFromPath(StatefulSet current, String pathValue) {
        JsonNode node1 = patchMapper().valueToTree(current);
        for (String field : pathValue.replaceFirst("^/", "").split("/")) {
            JsonNode node2 = node1.get(field);
            if (node2 == null) {
                try {
                    int index = parseInt(field);
                    node2 = node1.get(index);
                } catch (NumberFormatException e) {
                }
            }
            if (node2 == null) {
                node1 = null;
                break;
            } else {
                node1 = node2;
            }
        }
        return node1;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    /** Returns true if there's a difference in {@code /spec/volumeClaimTemplates} */
    public boolean changesVolumeClaimTemplates() {
        return changesVolumeClaimTemplate;
    }

    /** Returns true if there's a difference in {@code /spec/template/spec} */
    public boolean changesSpecTemplateSpec() {
        return changesSpecTemplateSpec;
    }

    /** Returns true if there's a difference in {@code /metadata/labels} */
    public boolean changesLabels() {
        return changesLabels;
    }

    /** Returns true if there's a difference in {@code /spec/replicas} */
    public boolean changesSpecReplicas() {
        return changesSpecReplicas;
    }
}
