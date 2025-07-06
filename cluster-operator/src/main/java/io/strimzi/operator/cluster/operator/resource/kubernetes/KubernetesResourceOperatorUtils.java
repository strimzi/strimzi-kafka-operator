/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.operator.common.Util;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Static utility methods for working with Kubernetes resources
 */
class KubernetesResourceOperatorUtils {
    /**
     * Finds annotations managed by some other entities (for example by Rancher cattle agents), and merge them with
     * the annotations from the Strimzi desired resource.
     * <p>
     * This makes sure there is no infinite loop where other tools try to add annotations, while Strimzi keeps
     * removing them during reconciliation.
     *
     * @param current    Current resource
     * @param desired    Desired resource
     * @param ignoreList List with predicates to detect annotations that should be preserved
     */
    static void patchAnnotations(HasMetadata current, HasMetadata desired, List<Predicate<String>> ignoreList) {
        Map<String, String> currentAnnotations = current.getMetadata().getAnnotations();
        if (currentAnnotations != null) {
            Map<String, String> matchedAnnotations = currentAnnotations.keySet().stream()
                    .filter(annotation -> ignoreList.stream().anyMatch(ignoreAnnotation -> ignoreAnnotation.test(annotation)))
                    .collect(Collectors.toMap(Function.identity(), currentAnnotations::get));

            if (!matchedAnnotations.isEmpty()) {
                desired.getMetadata().setAnnotations(Util.mergeLabelsOrAnnotations(
                        desired.getMetadata().getAnnotations(),
                        matchedAnnotations
                ));
            }
        }
    }
}
