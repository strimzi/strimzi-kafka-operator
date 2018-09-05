/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

public abstract class ReconcileResult<R> {

    private static final ReconcileResult DELETED = new ReconcileResult(null) {
        public String toString() {
            return "DELETED";
        }
    };

    public static class Noop<R> extends ReconcileResult<R> {
        private Noop() {
            super(null);
        }

        public String toString() {
            return "NOOP";
        }
    }

    private static final ReconcileResult NOOP = new Noop();

    public static class Created<R> extends ReconcileResult<R> {
        private Created(R resource) {
            super(resource);
        }

        public String toString() {
            return "CREATED";
        }
    }

    public static class Patched<R> extends ReconcileResult<R> {

        private Patched(R resource) {
            super(resource);
        }

        public String toString() {
            return "PATCH";
        }
    }

    /** The resource was patched. */
    public static final <D> Patched<D> patched(D resource) {
        return new Patched(resource);
    }

    /** The resource was created. */
    public static final <D> ReconcileResult<D> created(D resource) {
        return new Created<>(resource);
    }

    /** The resource was deleted. */
    public static final <P> ReconcileResult<P> deleted() {
        return DELETED;
    }

    /** No action was performed. */
    public static final <P> ReconcileResult<P> noop() {
        return NOOP;
    }

    private final R resource;

    private ReconcileResult(R resource) {
        this.resource = resource;
    }

    public R resource() {
        return this.resource;
    }
}
