/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.model.ResourceType;

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

    private final String trigger;
    private final ResourceType type;
    private final String namespace;
    private final String name;
    private final int id;

    public Reconciliation(String trigger, ResourceType type, String namespace, String assemblyName) {
        this.trigger = trigger;
        this.type = type;
        this.namespace = namespace;
        this.name = assemblyName;
        this.id = IDS.getAndIncrement();
    }

    public ResourceType type() {
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
