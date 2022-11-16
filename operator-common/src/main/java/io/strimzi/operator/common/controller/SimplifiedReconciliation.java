/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.controller;

import io.strimzi.operator.common.Reconciliation;

/**
 * This simplified class is used initially instead of the regular Reconciliation class. It also has a custom equals
 * implementation to detect if the same resource is already enqueued or not. It doesn't yet request the
 * reconciliation ID. Not issuing the reconciliation ID right away makes the IDs more linear and means they are not
 * requested unless a reconciliation really starts.
 */
public class SimplifiedReconciliation {
    final String kind;
    final String namespace;
    final String name;
    final String trigger;

    /**
     * SimplifiedReconciliation constructor with default (watch) trigger
     *
     * @param kind      Kind of the resource
     * @param namespace Namespace of the resource
     * @param name      Name of the resource
     */
    public SimplifiedReconciliation(String kind, String namespace, String name) {
        this(kind, namespace, name, "watch");
    }

    /**
     * SimplifiedReconciliation constructor with custom trigger
     *
     * @param kind      Kind of the resource
     * @param namespace Namespace of the resource
     * @param name      Name of the resource
     * @param trigger   Type of the trigger
     */
    public SimplifiedReconciliation(String kind, String namespace, String name, String trigger) {
        this.kind = kind;
        this.namespace = namespace;
        this.name = name;
        this.trigger = trigger;
    }

    /**
     * Converts the simplified reconciliation to a proper reconciliation
     *
     * @return Reconciliation object
     */
    public Reconciliation toReconciliation() {
        return new Reconciliation(trigger, kind, namespace, name);
    }

    /**
     * Generates a lock name for this reconciliation and its resource. The lock name consists of the kind, name and
     * namespace.
     *
     * @return Name of the lock which should be used for this resource
     */
    public String lockName() {
        return kind + "::" + namespace + "::" + name;
    }

    /**
     * Compares two SimplifiedReconciliation objects. This is used to avoid having the same resource queued multiple
     * times.
     *
     * @param o SimplifiedReconciliation to be compared
     * @return True if the objects equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        } else {
            SimplifiedReconciliation reconciliation = (SimplifiedReconciliation) o;

            return this.kind.equals(reconciliation.kind)
                    && this.name.equals(reconciliation.name)
                    && this.namespace.equals(reconciliation.namespace);
        }
    }

    /**
     * Generates the hashcode based on the kind, name and namespace hash codes.
     *
     * @return The hashcode of this object
     */
    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + (kind != null ? kind.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
        return result;
    }
}
