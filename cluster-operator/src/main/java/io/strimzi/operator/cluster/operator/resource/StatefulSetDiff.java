/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.zjsonpatch.JsonDiff;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;
import static java.lang.Integer.parseInt;

public class StatefulSetDiff {

    private static final Logger log = LogManager.getLogger(StatefulSetDiff.class.getName());

    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
        "^(/spec/revisionHistoryLimit"
        + "|/spec/template/metadata/annotations/strimzi.io~1generation"
        + "|/spec/template/spec/initContainers/[0-9]+/terminationMessagePath"
        + "|/spec/template/spec/initContainers/[0-9]+/terminationMessagePolicy"
        + "|/spec/template/spec/initContainers/[0-9]+/env/[0-9]+/valueFrom/fieldRef/apiVersion"
        + "|/spec/template/spec/containers/[0-9]+/env/[0-9]+/valueFrom/fieldRef/apiVersion"
        + "|/spec/template/spec/containers/[0-9]+/livenessProbe/failureThreshold"
        + "|/spec/template/spec/containers/[0-9]+/livenessProbe/periodSeconds"
        + "|/spec/template/spec/containers/[0-9]+/livenessProbe/successThreshold"
        + "|/spec/template/spec/containers/[0-9]+/readinessProbe/failureThreshold"
        + "|/spec/template/spec/containers/[0-9]+/readinessProbe/periodSeconds"
        + "|/spec/template/spec/containers/[0-9]+/readinessProbe/successThreshold"
        + "|/spec/template/spec/containers/[0-9]+/terminationMessagePath"
        + "|/spec/template/spec/containers/[0-9]+/terminationMessagePolicy"
        + "|/spec/template/spec/dnsPolicy"
        + "|/spec/template/spec/restartPolicy"
        + "|/spec/template/spec/schedulerName"
        + "|/spec/template/spec/securityContext"
        + "|/spec/template/spec/terminationGracePeriodSeconds"
        + "|/spec/template/spec/volumes/[0-9]+/configMap/defaultMode"
        + "|/spec/template/spec/volumes/[0-9]+/secret/defaultMode"
        + "|/spec/volumeClaimTemplates/[0-9]+/status"
        + "|/spec/volumeClaimTemplates/[0-9]+/spec/volumeMode"
        + "|/spec/volumeClaimTemplates/[0-9]+/spec/dataSource"
        + "|/spec/template/spec/serviceAccount"
        + "|/status)$");

    private static final Pattern RESOURCE_PATH = Pattern.compile("^/spec/template/spec/(?:initContainers|containers)/[0-9]+/resources/(?:limits|requests)/(memory|cpu)$");

    private static boolean equalsOrPrefix(String path, String pathValue) {
        return pathValue.equals(path)
                || pathValue.startsWith(path + "/");
    }

    private final boolean changesVolumeClaimTemplate;
    private final boolean isEmpty;
    private final boolean changesSpecTemplate;
    private final boolean changesLabels;
    private final boolean changesSpecReplicas;

    public StatefulSetDiff(StatefulSet current, StatefulSet desired) {
        JsonNode source = patchMapper().valueToTree(current);
        JsonNode target = patchMapper().valueToTree(desired);
        JsonNode diff = JsonDiff.asJson(source, target);
        int num = 0;
        boolean changesVolumeClaimTemplate = false;
        boolean changesSpecTemplate = false;
        boolean changesLabels = false;
        boolean changesSpecReplicas = false;
        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();
            if (IGNORABLE_PATHS.matcher(pathValue).matches()) {
                ObjectMeta md = current.getMetadata();
                log.debug("StatefulSet {}/{} ignoring diff {}", md.getNamespace(), md.getName(), d);
                continue;
            }
            Matcher resourceMatchers = RESOURCE_PATH.matcher(pathValue);
            if (resourceMatchers.matches()) {
                if ("replace".equals(d.path("op").asText())) {
                    boolean same = compareResources(source, target, pathValue, resourceMatchers);
                    if (same) {
                        continue;
                    }
                }
            }
            if (log.isDebugEnabled()) {
                ObjectMeta md = current.getMetadata();
                log.debug("StatefulSet {}/{} differs: {}", md.getNamespace(), md.getName(), d);
                log.debug("Current StatefulSet path {} has value {}", pathValue, getFromPath(current, pathValue));
                log.debug("Desired StatefulSet path {} has value {}", pathValue, getFromPath(desired, pathValue));
            }

            num++;
            changesVolumeClaimTemplate |= equalsOrPrefix("/spec/volumeClaimTemplates", pathValue);
            // Change changes to /spec/template/spec, except to imagePullPolicy, which gets changed
            // by k8s
            changesSpecTemplate |= equalsOrPrefix("/spec/template", pathValue);
            changesLabels |= equalsOrPrefix("/metadata/labels", pathValue);
            changesSpecReplicas |= equalsOrPrefix("/spec/replicas", pathValue);
        }
        this.isEmpty = num == 0;
        this.changesLabels = changesLabels;
        this.changesSpecReplicas = changesSpecReplicas;
        this.changesSpecTemplate = changesSpecTemplate;
        this.changesVolumeClaimTemplate = changesVolumeClaimTemplate;
    }

    boolean compareResources(JsonNode source, JsonNode target, String pathValue, Matcher resourceMatchers) {
        JsonNode s = source;
        JsonNode t = target;
        for (String component : pathValue.substring(1).split("/")) {
            if (s.isArray()) {
                s = s.path(Integer.parseInt(component));
            } else {
                s = s.path(component);
            }
            if (t.isArray()) {
                t = t.path(Integer.parseInt(component));
            } else {
                t = t.path(component);
            }
        }
        String group = resourceMatchers.group(1);
        if (!s.isMissingNode()
            && !t.isMissingNode()) {
            if ("cpu".equals(group)) {
                // Ignore single millicpu differences as they could be due to rounding error
                if (Math.abs(Quantities.parseCpuAsMilliCpus(s.asText()) - Quantities.parseCpuAsMilliCpus(t.asText())) < 1) {
                    return true;
                }
            } else {
                // Ignore single byte differences as they could be due to rounding error
                if (Math.abs(Quantities.parseMemory(s.asText()) - Quantities.parseMemory(t.asText())) < 1) {
                    return true;
                }
            }
        }
        return false;
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
    public boolean changesSpecTemplate() {
        return changesSpecTemplate;
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
