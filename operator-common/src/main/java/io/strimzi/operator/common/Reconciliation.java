/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Represents an attempt synchronize the state of some K8S resources (an "assembly") in a single namespace with a
 * desired state, expressed as a ConfigMap.</p>
 *
 * <p>Each instance has a unique id and a trigger (description of the event which initiated the reconciliation),
 * which are used to provide consistent context for logging.</p>
 */
public class Reconciliation {
    private static final AtomicInteger IDS = new AtomicInteger();

    /**
     * Dummy reconciliation marker used in tests
     */
    public static final Reconciliation DUMMY_RECONCILIATION = new Reconciliation("test", "kind", "namespace", "name");

    private final String trigger;
    private final String kind;
    private final String namespace;
    private final String name;
    private final int id;
    private final Marker marker;

    /**
     * Constructs the reconciliation marker
     *
     * @param trigger       Trigger of the reconciliation
     * @param kind          Kind of the resource
     * @param namespace     Namespace of the resource
     * @param name  Name of the resource
     */
    public Reconciliation(String trigger, String kind, String namespace, String name) {
        this.trigger = trigger;
        this.kind = kind;
        this.namespace = namespace;
        this.name = name;
        this.id = IDS.getAndIncrement();
        this.marker = MarkerManager.getMarker(this.kind + "(" + this.namespace + "/" + this.name + ")");
    }

    /**
     * @return  Kind of the reconciled resource
     */
    public String kind() {
        return kind;
    }

    /**
     * @return  Namespace of the reconciled resource
     */
    public String namespace() {
        return namespace;
    }

    /**
     * @return  Name of the reconciled resource
     */
    public String name() {
        return name;
    }

    /**
     * @return  The logging marker
     */
    public Marker getMarker() {
        return marker;
    }

    @Override
    public String toString() {
        return "Reconciliation #" + id + "(" + trigger + ") " + kind() + "(" + namespace() + "/" + name() + ")";
    }
}
