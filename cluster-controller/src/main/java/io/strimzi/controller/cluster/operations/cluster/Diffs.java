/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import static io.strimzi.controller.cluster.resources.AbstractCluster.containerEnvVars;

class Diffs {
    static boolean differingScale(StatefulSet a, StatefulSet b) {
        return (a == null) != (b == null)
                || !a.getSpec().getReplicas().equals(b.getSpec().getReplicas());
    }

    static <T extends HasMetadata> boolean differingLabels(T a, T b) {
        return (a == null) != (b == null)
                || !a.getMetadata().getLabels().equals(b.getMetadata().getLabels());
    }

    private static <T> boolean differingLists(List<T> a, List<T> b, BiPredicate<T, T> elementsDiffer) {
        if ((a == null) != (b == null)) {
            return true;
        }
        if (a == null) { // => b is null too
            return false;
        }
        if (a.size() != b.size()) {
            return true;
        }
        Iterator<T> ai = a.iterator();
        Iterator<T> bi = b.iterator();
        while (ai.hasNext()) {
            if (elementsDiffer.test(ai.next(), bi.next())) {
                return true;
            }
        }
        return false;
    }

    static boolean differingContainers(StatefulSet a, StatefulSet b) {
        return (a == null) != (b == null)
                || differingLists(a.getSpec().getTemplate().getSpec().getContainers(),
                    b.getSpec().getTemplate().getSpec().getContainers(),
                    Diffs::differingContainers);
    }

    static boolean differingContainers(Container a, Container b) {
        return (a == null) != (b == null)
                || !Objects.equals(a.getImage(), b.getImage())
                || (a.getReadinessProbe() == null) != (b.getReadinessProbe() == null)
                || !Objects.equals(a.getReadinessProbe().getInitialDelaySeconds(), b.getReadinessProbe().getInitialDelaySeconds())
                || !Objects.equals(a.getReadinessProbe().getTimeoutSeconds(), b.getReadinessProbe().getTimeoutSeconds());
    }

    static boolean differingEnvironments(Container a, Container b, Set<String> keys) {
        Map<String, String> aVars = containerEnvVars(a);
        Map<String, String> bVars = containerEnvVars(b);
        if (keys != null) {
            aVars.keySet().retainAll(keys);
            bVars.keySet().retainAll(keys);
        }
        return !aVars.equals(bVars);
    }
}
