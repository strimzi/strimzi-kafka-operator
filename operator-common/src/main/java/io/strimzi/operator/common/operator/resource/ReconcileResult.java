/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import java.util.Optional;

/**
 * Indicates reconciliation result
 *
 * @param <R>   Resource type for which the result is being indicated
 */
public abstract class ReconcileResult<R> {
    @SuppressWarnings("unchecked")
    private static final ReconcileResult DELETED = new ReconcileResult(Optional.empty()) {
        @Override
        public String toString() {
            return "DELETED";
        }
    };

    /**
     * Nothing was changed during the reocnciliation
     *
     * @param <R>   Resource type for which the result is being indicated
     */
    public static class Noop<R> extends ReconcileResult<R> {
        private Noop(R resource) {
            super(Optional.ofNullable(resource));
        }

        @Override
        public String toString() {
            return "NOOP";
        }
    }

    /**
     * The resource was created during the reconciliation
     *
     * @param <R>   Resource type for which the result is being indicated
     */
    public static class Created<R> extends ReconcileResult<R> {
        private Created(R resource) {
            super(Optional.of(resource));
        }

        @Override
        public String toString() {
            return "CREATED";
        }
    }

    /**
     * the resource was modified during the reconciliation
     *
     * @param <R>   Resource type for which the result is being indicated
     */
    public static class Patched<R> extends ReconcileResult<R> {
        private Patched(R resource) {
            super(Optional.of(resource));
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

    private final Optional<R> resource;

    private ReconcileResult(Optional<R> resource) {
        this.resource = resource;
    }

    /**
     * @return  The resource which was reconciled as an Optional instance
     */
    public Optional<R> resourceOpt() {
        return this.resource;
    }

    /**
     * @return  The resource which was reconciled
     */
    public R resource() {
        return resourceOpt().orElseThrow(() -> new RuntimeException("Resource was concurrently deleted"));
    }
}
