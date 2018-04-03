/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import java.util.concurrent.atomic.AtomicInteger;

public class Reconciliation {

    private static final AtomicInteger IDS = new AtomicInteger();

    private final String trigger;
    private final String type;
    private final String namespace;
    private final String assemblyName;
    private final int id;

    public Reconciliation(String trigger, String type, String namespace, String assemblyName) {
        this.trigger = trigger;
        this.type = type;
        this.namespace = namespace;
        this.assemblyName = assemblyName;
        this.id = IDS.getAndIncrement();
    }

    public String type() {
        return type;
    }

    public String namespace() {
        return namespace;
    }

    public String assemblyName() {
        return assemblyName;
    }

    public String toString() {
        return "Reconciliation #" + id + "(" + trigger + ") " + type() + "(" + namespace() + "/" + assemblyName() + ")";
    }
}
