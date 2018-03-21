/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

public class ReconcileResult<D> {
    private static final ReconcileResult CREATED = new ReconcileResult() {
        public String toString() {
            return "CREATED";
        }
    };
    private static final ReconcileResult DELETED = new ReconcileResult() {
        public String toString() {
            return "DELETED";
        }
    };
    private static final ReconcileResult NOOP = new ReconcileResult() {
        public String toString() {
            return "NOOP";
        }
    };

    public static class Patched<D> extends ReconcileResult<D> {
        private final D differences;

        private Patched(D differences) {
            this.differences = differences;
        }

        public D differences() {
            return differences;
        }

        public String toString() {
            return "PATCH";
        }
    }

    /** The resource was patched. */
    public static final <D> Patched<D> patched(D differences) {
        return new Patched(differences);
    }
    /** The resource was created. */
    public static final <P> ReconcileResult<P> created() {
        return CREATED;
    }
    /** The resource was deleted. */
    public static final <P> ReconcileResult<P> deleted() {
        return DELETED;
    }
    /** No action was performed. */
    public static final <P> ReconcileResult<P> noop() {
        return NOOP;
    }
    private ReconcileResult() {}
}
