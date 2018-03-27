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

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

public class StatefulSetDiff {

    private static final Logger log = LoggerFactory.getLogger(StatefulSetDiff.class.getName());

    private final JsonNode diff;

    public StatefulSetDiff(StatefulSet current, StatefulSet updated) {
        this.diff = JsonDiff.asJson(patchMapper().valueToTree(current), patchMapper().valueToTree(updated));
    }

    public boolean isEmpty() {
        return !diff.iterator().hasNext();
    }

    private boolean containsPathOrChild(String path) {
        for (JsonNode d : diff) {
            String pathValue = d.get("path").asText();
            if (pathValue.equals(path)
                    || pathValue.startsWith(path + "/")) {
                log.info("{} differs: {}", path, d);
                return true;
            }
        }
        return false;
    }

    public boolean changesVolumeClaimTemplates() {
        return containsPathOrChild("/spec/volumeClaimTemplates");
    }

    public boolean changesSpecTemplateSpec() {
        return containsPathOrChild("/spec/template/spec");
    }

    public boolean changesLabels() {
        return containsPathOrChild("/metadata/labels");
    }

    public boolean changesSpecReplicas() {
        return containsPathOrChild("/spec/replicas");
    }
}
