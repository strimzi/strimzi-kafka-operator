/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    /* test */ public static final Reconciliation DUMMY_RECONCILIATION = new Reconciliation("test", "kind", "namespace", "name");

    private final String trigger;
    private final String kind;
    private final String namespace;
    private final String name;
    private final int id;
    private final Marker marker;
    private ExecutorService executorService;

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

    public <T> Future<T> run(Vertx vertx, Callable<T> callable) {
        synchronized (this) {
            if (executorService == null) {
                executorService = Executors.newSingleThreadScheduledExecutor();
            }
        }
        Promise<T> p = Promise.promise();
        executorService.submit(() -> {
            try {
                T result = callable.call();
                vertx.runOnContext(i -> p.tryComplete(result));
            } catch (Exception e) {
                vertx.runOnContext(i -> p.tryFail(e));
            }
        });
        return p.future();
    }
}
