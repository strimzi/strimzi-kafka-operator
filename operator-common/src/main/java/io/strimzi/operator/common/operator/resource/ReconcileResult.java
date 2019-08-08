/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

public abstract class ReconcileResult<R> {

    @SuppressWarnings("unchecked")
    private static final ReconcileResult DELETED = new ReconcileResult(null) {
        @Override
        public String toString() {
            return "DELETED";
        }
    };

    public static class Noop<R> extends ReconcileResult<R> {
        private Noop(R resource) {
            super(resource);
        }

        @Override
        public String toString() {
            return "NOOP";
        }
    }


    public static class Created<R> extends ReconcileResult<R> {
        private Created(R resource) {
            super(resource);
        }

        @Override
        public String toString() {
            return "CREATED";
        }
    }

    public static class Patched<R> extends ReconcileResult<R> {

        private Patched(R resource) {
            super(resource);
        }

        @Override
        public String toString() {
            return "PATCH";
        }
    }

    /**
     * Return a reconciliation result that indicates the resource was patched.
     * @return a reconciliation result that indicates the resource was patched.
     * @param resource The patched resource.
     * @param <D> The type of resource.
     */
    public static final <D> Patched<D> patched(D resource) {
        return new Patched<>(resource);
    }

    /**
     * Return a reconciliation result that indicates the resource was created.
     * @return a reconciliation result that indicates the resource was created.
     * @param resource The created resource.
     * @param <D> The type of resource.
     */
    public static final <D> ReconcileResult<D> created(D resource) {
        return new Created<>(resource);
    }

    /**
     * Return a reconciliation result that indicates the resource was deleted.
     * @return a reconciliation result that indicates the resource was deleted.
     * @param <P> The type of resource.
     */
    @SuppressWarnings("unchecked")
    public static final <P> ReconcileResult<P> deleted() {
        return DELETED;
    }

    /**
     * Return a reconciliation result that indicates the resource was not modified.
     * @return a reconciliation result that indicates the resource was not modified.
     * @param resource The unmodified resource.
     * @param <P> The type of resource.
     */
    public static final <P> ReconcileResult<P> noop(P resource) {
        return new Noop<>(resource);
    }

    private final R resource;

    private ReconcileResult(R resource) {
        this.resource = resource;
    }

    public R resource() {
        return this.resource;
    }
}
