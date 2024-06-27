/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.model;

import java.util.Objects;
import java.util.function.Function;

/**
 * A functional-style wrapper that can contain either a value of type L or type R.
 * <br/><br/>
 * It is used to represents the two possible outcomes from a computation.
 * The right outcome represents the successful computation of the function, 
 * and the left outcome represents the unsuccessful computation of the function.
 * 
 * @param <L> Left value type.
 * @param <R> Right value type.
 */
public class Either<L, R> {
    private final boolean right;
    private final Object value;

    private Either(boolean right, Object value) {
        this.right = right;
        this.value = value;
    }

    /**
     * Create either with right value (success holder).
     * 
     * @param right Result value.
     * @return Either instance.
     * @param <L> Left value type.
     * @param <R> Right value type.
     */
    public static <L, R> Either<L, R> ofRight(R right) {
        return new Either<>(true, right);
    }

    /**
     * Create either with left value (error holder).
     *      
     * @param left Error value.
     * @return Either instance.
     * @param <L> Left value type.
     * @param <R> Right value type.
     */
    public static <L, R> Either<L, R> ofLeft(L left) {
        return new Either<>(false, left);
    }

    /**
     * @param fn Map function to apply.
     * @return Either with updated right value.
     * @param <R2> New right value type.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <R2> Either<L, R2> mapRight(Function<R, R2> fn) {
        if (right) {
            return ofRight(fn.apply((R) value));
        } else {
            return (Either) this;
        }
    }

    /**
     * @param fn Flat map function to apply.
     * @return Either with updated right value.
     * @param <R2> New right value type.
     */
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
            return "Right(" + value + ')';
        } else {
            return "Left(" + value + ')';
        }
    }

    /**
     * @return Whether is right.
     */
    public boolean isRight() {
        return this.right;
    }

    /**
     * @param b Some value.
     * @return Whether is right and equal.
     */
    public boolean isRightEqual(R b) {
        return this.right && Objects.equals(this.value, b);
    }

    /**
     * @return Left value.
     */
    @SuppressWarnings("unchecked")
    public L left() {
        if (right) {
            throw new IllegalStateException();
        }
        return (L) this.value;
    }

    /**
     * @return Right value.
     */
    @SuppressWarnings("unchecked")
    public R right() {
        if (right) {
            return (R) this.value;
        }
        throw new IllegalStateException();
    }
}
