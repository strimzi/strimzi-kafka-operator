/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Collects reasons to restart a pod, along with addiiontal logging messages for each kind of restart.
 */
public class RestartReasons {

    EnumMap<RestartReason, List<String>> reasons = new EnumMap<>(RestartReason.class);


    public RestartReasons add(RestartReason reason, String message) {
        reasons.computeIfAbsent(reason, unused -> new ArrayList<>()).add(message);
        return this;
    }

    public Set<RestartReason> getReasons() {
        return reasons.keySet();
    }

    public List<String> getReasonMessages() {
        return reasons.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public boolean isEmpty() {
        return reasons.isEmpty();
    }

    public boolean contains(RestartReason reason) {
        return reasons.containsKey(reason);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestartReasons reasons1 = (RestartReasons) o;
        return reasons.equals(reasons1.reasons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reasons);
    }
}
