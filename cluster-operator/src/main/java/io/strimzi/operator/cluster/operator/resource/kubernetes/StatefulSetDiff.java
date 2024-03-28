/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.operator.cluster.model.Quantities;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.AbstractJsonDiff;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class for diffing StatefulSets
 */
public class StatefulSetDiff extends AbstractJsonDiff {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(StatefulSetDiff.class.getName());

    private static final String SHORTENED_STRIMZI_DOMAIN = Annotations.STRIMZI_DOMAIN.substring(0, Annotations.STRIMZI_DOMAIN.length() - 1);

    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
        "^(/metadata/managedFields"
        + "|/metadata/creationTimestamp"
        + "|/metadata/resourceVersion"
        + "|/metadata/generation"
        + "|/metadata/uid"
        + "|/spec/revisionHistoryLimit"
        + "|/spec/template/metadata/annotations/" + SHORTENED_STRIMZI_DOMAIN + "~1generation"
        + "|/spec/template/spec/initContainers/[0-9]+/resources"
        + "|/spec/template/spec/initContainers/[0-9]+/terminationMessagePath"
        + "|/spec/template/spec/initContainers/[0-9]+/terminationMessagePolicy"
        + "|/spec/template/spec/initContainers/[0-9]+/env/[0-9]+/valueFrom/fieldRef/apiVersion"
        + "|/spec/template/spec/containers/[0-9]+/resources"
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
        + "|/spec/template/spec/securityContext"
        + "|/spec/template/spec/volumes/[0-9]+/configMap/defaultMode"
        + "|/spec/template/spec/volumes/[0-9]+/secret/defaultMode"
        + "|/spec/volumeClaimTemplates/[0-9]+/status"
        + "|/spec/volumeClaimTemplates/[0-9]+/spec/volumeMode"
        + "|/spec/volumeClaimTemplates/[0-9]+/spec/dataSource"
        + "|/spec/template/spec/serviceAccount"
        + "|/status)$");

    private static final Pattern RESOURCE_PATH = Pattern.compile("^/spec/template/spec/(?:initContainers|containers)/[0-9]+/resources/(?:limits|requests)/(memory|cpu)$");
    private static final Pattern VOLUME_SIZE = Pattern.compile("^/spec/volumeClaimTemplates/[0-9]+/spec/resources/.*$");

    private static boolean equalsOrPrefix(String path, String pathValue) {
        return pathValue.equals(path)
                || pathValue.startsWith(path + "/");
    }

    private final boolean changesVolumeClaimTemplate;
    private final boolean changesVolumeSize;
    private final boolean isEmpty;
    private final boolean changesSpecTemplate;
    private final boolean changesLabels;
    private final boolean changesSpecReplicas;

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param current           Current StatefulSet
     * @param desired           Desired StatefulSet
     */
    public StatefulSetDiff(Reconciliation reconciliation, StatefulSet current, StatefulSet desired) {
        JsonNode source = PATCH_MAPPER.valueToTree(current);
        JsonNode target = PATCH_MAPPER.valueToTree(desired);
        JsonNode diff = JsonDiff.asJson(source, target);
        int num = 0;
        boolean changesVolumeClaimTemplate = false;
        boolean changesVolumeSize = false;
        boolean changesSpecTemplate = false;
        boolean changesLabels = false;
        boolean changesSpecReplicas = false;
        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();
            if (IGNORABLE_PATHS.matcher(pathValue).matches()) {
                ObjectMeta md = current.getMetadata();
                LOGGER.debugCr(reconciliation, "StatefulSet {}/{} ignoring diff {}", md.getNamespace(), md.getName(), d);
                continue;
            }
            Matcher resourceMatchers = RESOURCE_PATH.matcher(pathValue);
            if (resourceMatchers.matches()) {
                if ("replace".equals(d.path("op").asText())) {
                    boolean same = compareMemoryAndCpuResources(source, target, pathValue, resourceMatchers);
                    if (same) {
                        ObjectMeta md = current.getMetadata();
                        LOGGER.debugCr(reconciliation, "StatefulSet {}/{} ignoring diff {}", md.getNamespace(), md.getName(), d);
                        continue;
                    }
                }
            }

            if (LOGGER.isDebugEnabled()) {
                ObjectMeta md = current.getMetadata();
                LOGGER.debugCr(reconciliation, "StatefulSet {}/{} differs: {}", md.getNamespace(), md.getName(), d);
                LOGGER.debugCr(reconciliation, "Current StatefulSet path {} has value {}", pathValue, lookupPath(source, pathValue));
                LOGGER.debugCr(reconciliation, "Desired StatefulSet path {} has value {}", pathValue, lookupPath(target, pathValue));
            }

            num++;
            // Any volume claim template changes apart from size change should trigger rolling update
            // Size changes should not trigger rolling update. Therefore we need to separate these two in the diff.
            changesVolumeClaimTemplate |= equalsOrPrefix("/spec/volumeClaimTemplates", pathValue) && !VOLUME_SIZE.matcher(pathValue).matches();
            changesVolumeSize |= isVolumeSizeChanged(pathValue, source, target);
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
        this.changesVolumeSize = changesVolumeSize;
    }

    private boolean isVolumeSizeChanged(String pathValue, JsonNode source, JsonNode target) {
        if (VOLUME_SIZE.matcher(pathValue).matches()) {
            JsonNode current = lookupPath(source, pathValue);
            JsonNode desired = lookupPath(target, pathValue);
            return StorageUtils.convertToMillibytes(current.asText()) != StorageUtils.convertToMillibytes(desired.asText());
        }
        return false;
    }

    boolean compareMemoryAndCpuResources(JsonNode source, JsonNode target, String pathValue, Matcher resourceMatchers) {
        JsonNode s = lookupPath(source, pathValue);
        JsonNode t = lookupPath(target, pathValue);
        String group = resourceMatchers.group(1);
        if (!s.isMissingNode()
            && !t.isMissingNode()) {
            if ("cpu".equals(group)) {
                // Ignore single millicpu differences as they could be due to rounding error
                return Math.abs(Quantities.parseCpuAsMilliCpus(s.asText()) - Quantities.parseCpuAsMilliCpus(t.asText())) < 1;
            } else {
                // Ignore single byte differences as they could be due to rounding error
                return Math.abs(Quantities.parseMemory(s.asText()) - Quantities.parseMemory(t.asText())) < 1;
            }
        }
        return false;
    }

    /**
     * Returns whether the Diff is empty or not
     *
     * @return true when the StatefulSets are identical
     */
    @Override
    public boolean isEmpty() {
        return isEmpty;
    }

    /** @return True if there's a difference in {@code /spec/volumeClaimTemplates} but not to {@code /spec/volumeClaimTemplates/[0-9]+/spec/resources} */
    public boolean changesVolumeClaimTemplates() {
        return changesVolumeClaimTemplate;
    }

    /** @return True if there's a difference in {@code /spec/volumeClaimTemplates/[0-9]+/spec/resources} */
    public boolean changesVolumeSize() {
        return changesVolumeSize;
    }

    /** @return True if there's a difference in {@code /spec/template/spec} */
    public boolean changesSpecTemplate() {
        return changesSpecTemplate;
    }

    /** @return True if there's a difference in {@code /metadata/labels} */
    public boolean changesLabels() {
        return changesLabels;
    }

    /** @return True if there's a difference in {@code /spec/replicas} */
    public boolean changesSpecReplicas() {
        return changesSpecReplicas;
    }
}
