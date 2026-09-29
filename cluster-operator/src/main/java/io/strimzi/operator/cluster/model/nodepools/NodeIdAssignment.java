/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import java.util.Set;

/**
 * Record for holding the assignment of node IDs for a single node pool
 *
 * @param current           Current node IDs
 * @param desired           Desired node IDs
 * @param toBeRemoved       Node IDs which should be removed
 * @param toBeAdded         Node IDs which should be added
 * @param usedToBeBroker    Node IDs that used to have the broker role but should not have it anymore
 */
public record NodeIdAssignment(Set<Integer> current, Set<Integer> desired, Set<Integer> toBeRemoved, Set<Integer> toBeAdded, Set<Integer> usedToBeBroker) {
}
