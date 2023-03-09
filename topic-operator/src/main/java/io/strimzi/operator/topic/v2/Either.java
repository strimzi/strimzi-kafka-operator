/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import java.util.Objects;
import java.util.function.Function;

class Either<X, Y> {
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
    public <Z> Either<Z, Y> mapLeft(Function<X, Z> fn) {
        if (left) {
            return ofLeft(fn.apply((X) value));
        } else {
            return (Either) this;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <Z> Either<Z, Y> flatMapLeft(Function<X, Either<Z, Y>> fn) {
        if (left) {
            return fn.apply((X) value);
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

    public boolean isLeftEqual(X b) {
        return this.left && Objects.equals(this.value, b);
    }

    @SuppressWarnings("unchecked")
    public Y right() {
        if (left) {
            throw new IllegalStateException();
        }
        return (Y) this.value;
    }

    @SuppressWarnings("unchecked")
    public X left() {
        if (left) {
            return (X) this.value;
        }
        throw new IllegalStateException();
    }
}
