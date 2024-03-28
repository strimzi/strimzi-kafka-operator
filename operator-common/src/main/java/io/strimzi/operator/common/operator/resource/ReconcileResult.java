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
    /**
     * Type identifier for the ReconcileResult
     */
    public enum Type {
        /**
         * The resource was not modified
         */
        NOOP,

        /**
         * The resource was created
         */
        CREATED,

        /**
         * The resource was patched
         */
        PATCHED,

        /**
         * The resource was deleted
         */
        DELETED
    }

    /**
     * The resource was deleted between the reconciliations
     *
     * @param <R>   Resource type for which the result is being indicated
     */
    public static class Deleted<R> extends ReconcileResult<R> {
        private Deleted() {
            super(Optional.empty());
        }

        @Override
        public Type getType() {
            return Type.DELETED;
        }
    }

    /**
     * Nothing was changed during the reconciliation
     *
     * @param <R>   Resource type for which the result is being indicated
     */
    public static class Noop<R> extends ReconcileResult<R> {
        private Noop(R resource) {
            super(Optional.ofNullable(resource));
        }

        @Override
        public Type getType() {
            return Type.NOOP;
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
        public Type getType() {
            return Type.CREATED;
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
        public Type getType() {
            return Type.PATCHED;
        }
    }

    /**
     * Return a reconciliation result that indicates the resource was patched.
     * @return a reconciliation result that indicates the resource was patched.
     * @param resource The patched resource.
     * @param <D> The type of resource.
     */
    public static <D> Patched<D> patched(D resource) {
        return new Patched<>(resource);
    }

    /**
     * Return a reconciliation result that indicates the resource was created.
     * @return a reconciliation result that indicates the resource was created.
     * @param resource The created resource.
     * @param <D> The type of resource.
     */
    public static <D> ReconcileResult<D> created(D resource) {
        return new Created<>(resource);
    }

    /**
     * Return a reconciliation result that indicates the resource was deleted.
     * @return a reconciliation result that indicates the resource was deleted.
     * @param <P> The type of resource.
     */
    public static <P> ReconcileResult<P> deleted() {
        return new Deleted<>();
    }

    /**
     * Return a reconciliation result that indicates the resource was not modified.
     * @return a reconciliation result that indicates the resource was not modified.
     * @param resource The unmodified resource.
     * @param <P> The type of resource.
     */
    public static <P> ReconcileResult<P> noop(P resource) {
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

    /**
     * @return Type of the ReconsileResult
     */
    public abstract Type getType();

    @Override
    public String toString() {
        return getType().name();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ReconcileResult<?> other) {
            return getType() == other.getType() && resourceOpt().equals(other.resourceOpt());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getType().hashCode() + resourceOpt().hashCode();
    }
}
