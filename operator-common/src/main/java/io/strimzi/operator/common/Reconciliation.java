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

    public static final Reconciliation DUMMY_RECONCILIATION = new Reconciliation("test", "kind", "namespace", "name");

    private final String trigger;
    private final String kind;
    private final String namespace;
    private final String name;
    private final int id;
    private final Marker marker;

    public Reconciliation(String trigger, String kind, String namespace, String assemblyName) {
        this.trigger = trigger;
        this.kind = kind;
        this.namespace = namespace;
        this.name = assemblyName;
        this.id = IDS.getAndIncrement();
        this.marker = MarkerManager.getMarker(this.kind + "(" + this.namespace + "/" + this.name + ")");
    }

    public String kind() {
        return kind;
    }

    public String namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    public Marker getMarker() {
        return marker;
    }

    public String toString() {
        return "Reconciliation #" + id + "(" + trigger + ") " + kind() + "(" + namespace() + "/" + name() + ")";
    }
}
