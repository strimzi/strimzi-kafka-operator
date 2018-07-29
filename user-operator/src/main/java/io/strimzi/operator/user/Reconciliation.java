/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Represents an attempt synchronize the state of some K8S resources in a single namespace with a
 * desired state, expressed as a Custom Resource.</p>
 *
 * <p>Each instance has a unique id and a trigger (description of the event which initiated the reconciliation),
 * which are used to provide consistent context for logging.</p>
 */
public class Reconciliation {
    private static final AtomicInteger IDS = new AtomicInteger();

    private final String trigger;
    private final String type;
    private final String namespace;
    private final String name;
    private final int id;

    public Reconciliation(String trigger, String type, String namespace, String assemblyName) {
        this.trigger = trigger;
        this.type = type;
        this.namespace = namespace;
        this.name = assemblyName;
        this.id = IDS.getAndIncrement();
    }

    public String type() {
        return type;
    }

    public String namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    public String toString() {
        return "Reconciliation #" + id + "(" + trigger + ") " + type() + "(" + namespace() + "/" + name() + ")";
    }
}
