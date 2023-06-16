/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import java.util.Objects;
import java.util.function.Function;

/**
 * A functional-style immutable holder for one of two possible <em>cases</em>, known as "left" and "right".
 * @param <L> The type of the left hand case, which often represents the error case.
 * @param <R> The type of the right hand case, which often represents the success case.
 */
class Either<L, R> {
    private final boolean left;
    private final Object value;

    private Either(boolean left, Object value) {
        this.left = left;
        this.value = value;
    }

    static <X, Y> Either<X, Y> ofLeft(X x) {
        return new Either<X, Y>(true, x);
    }

    static <X, Y> Either<X, Y> ofRight(Y y) {
        return new Either<X, Y>(false, y);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <Z> Either<Z, R> mapLeft(Function<L, Z> fn) {
        if (left) {
            return ofLeft(fn.apply((L) value));
        } else {
            return (Either) this;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <Z> Either<Z, R> flatMapLeft(Function<L, Either<Z, R>> fn) {
        if (left) {
            return fn.apply((L) value);
        } else {
            return (Either) this;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Either<?, ?> either = (Either<?, ?>) o;
        return left == either.left && Objects.equals(value, either.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, value);
    }

    @Override
    public String toString() {
        if (left) {
            return "Left(" +
                    value +
                    ')';
        } else {
            return "Right(" +
                    value +
                    ')';
        }
    }

    public boolean isLeft() {
        return this.left;
    }

    public boolean isLeftEqual(L b) {
        return this.left && Objects.equals(this.value, b);
    }

    @SuppressWarnings("unchecked")
    public R right() {
        if (left) {
            throw new IllegalStateException();
        }
        return (R) this.value;
    }

    @SuppressWarnings("unchecked")
    public L left() {
        if (left) {
            return (L) this.value;
        }
        throw new IllegalStateException();
    }
}
