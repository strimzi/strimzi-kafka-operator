/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.util.Objects;
import java.util.function.Function;

/**
 * A functional-style immutable holder for one of two possible <em>cases</em>, known as "left" and "right".
 * @param <L> The type of the left hand case, which often represents the error case.
 * @param <R> The type of the right hand case, which often represents the success case.
 */
class Either<L, R> {
    private final boolean right;
    private final Object value;

    private Either(boolean right, Object value) {
        this.right = right;
        this.value = value;
    }

    static <L, R> Either<L, R> ofRight(R right) {
        return new Either<L, R>(true, right);
    }

    static <L, R> Either<L, R> ofLeft(L left) {
        return new Either<L, R>(false, left);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <R2> Either<L, R2> mapRight(Function<R, R2> fn) {
        if (right) {
            return ofRight(fn.apply((R) value));
        } else {
            return (Either) this;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <R2> Either<L, R2> flatMapRight(Function<R, Either<L, R2>> fn) {
        if (right) {
            return fn.apply((R) value);
        } else {
            return (Either) this;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Either<?, ?> either = (Either<?, ?>) o;
        return right == either.right && Objects.equals(value, either.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(right, value);
    }

    @Override
    public String toString() {
        if (right) {
            return "Right(" +
                    value +
                    ')';
        } else {
            return "Left(" +
                    value +
                    ')';
        }
    }

    public boolean isRight() {
        return this.right;
    }

    public boolean isRightEqual(R b) {
        return this.right && Objects.equals(this.value, b);
    }

    @SuppressWarnings("unchecked")
    public L left() {
        if (right) {
            throw new IllegalStateException();
        }
        return (L) this.value;
    }

    @SuppressWarnings("unchecked")
    public R right() {
        if (right) {
            return (R) this.value;
        }
        throw new IllegalStateException();
    }
}
